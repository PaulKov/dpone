"""Real pinned decoder/lifecycle privacy without shared logger mutation."""

import ast
import inspect
import io
import logging
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest


@pytest.mark.parametrize(
    ("peer", "version", "name", "connected", "expected"),
    [
        (("127.0.0.1", 9000), (24, 8, 14), "ClickHouse", True, True),
        (("127.0.0.2", 9000), (24, 8, 14), "ClickHouse", True, False),
        (("127.0.0.1", 9001), (24, 8, 14), "ClickHouse", True, False),
        (("127.0.0.1", 9000), (25, 8, 14), "ClickHouse", True, False),
        (("127.0.0.1", 9000), (24, 9, 14), "ClickHouse", True, False),
        (("127.0.0.1", 9000), (24, 8, 15), "ClickHouse", True, False),
        (("127.0.0.1", 9000), (24, 8, 14), "Other", True, False),
        (("127.0.0.1", 9000), (24, 8, 14), "ClickHouse", False, False),
    ],
)
def test_native_peer_profile_without_connecting_or_sending(peer, version, name, connected, expected):
    from dpone.adapters import clickhouse_native_driver as boundary

    # No connect/send/retry methods: the predicate may only inspect captured facts
    # and the already-owned socket. Each transport retains lifecycle ownership.
    connection = SimpleNamespace(socket=SimpleNamespace(getpeername=lambda: peer), connected=connected)
    endpoint = SimpleNamespace(host="127.0.0.1", port=9000)
    info = SimpleNamespace(name=name)
    assert boundary.matches_native_peer(connection, endpoint, version, info) is expected


def test_native_peer_lookup_error_propagates_to_transport_boundary():
    from dpone.adapters import clickhouse_native_driver as boundary

    failure = OSError("private-socket-marker")

    def unavailable():
        raise failure

    connection = SimpleNamespace(socket=SimpleNamespace(getpeername=unavailable))
    endpoint = SimpleNamespace(host="127.0.0.1", port=9000)
    with pytest.raises(OSError) as caught:
        boundary.matches_native_peer(connection, endpoint, (24, 8, 14), SimpleNamespace(name="ClickHouse"))
    assert caught.value is failure


def test_native_peer_mismatch_short_circuits_later_checks():
    from dpone.adapters import clickhouse_native_driver as boundary

    connection = SimpleNamespace(socket=SimpleNamespace(getpeername=lambda: ("127.0.0.2", 9000)))
    endpoint = SimpleNamespace(host="127.0.0.1", port=9000)
    assert boundary.matches_native_peer(connection, endpoint, (24, 8, 14), object()) is False


def test_transport_suppresses_real_driver_query_and_log_payload(monkeypatch, caplog):
    from clickhouse_driver.connection import Connection, ServerInfo
    from clickhouse_driver.streams.native import BlockOutputStream

    from dpone.adapters.clickhouse_native_publication import DirectNativePublicationTransport
    from tests.test_clickhouse_native_publication import endpoint, request

    caplog.set_level(logging.DEBUG)
    original = request()
    fixture = log_connection("private-log-marker")

    class PacketReader:
        def __init__(self):
            self.types = iter((10, 5))

        def read_one(self):
            return next(self.types)

    class Socket:
        def getpeername(self):
            return "127.0.0.1", 9000

        def shutdown(self, how):
            raise OSError("private-shutdown-marker")

        def close(self):
            pass

    def initialize(connection, host, port):
        connection.connected = True
        connection.server_info = ServerInfo("ClickHouse", 24, 8, 14, 54470, "UTC", "fixture", 54401)
        connection.context.server_info = connection.server_info
        connection.socket, connection.fin, connection.fout = Socket(), PacketReader(), io.BytesIO()
        connection.block_out = BlockOutputStream(connection.fout, connection.context)
        connection.receive_data = fixture.receive_data

    monkeypatch.setattr(Connection, "_init_connection", initialize)
    completion = DirectNativePublicationTransport(endpoint()).execute(original)
    assert completion.query_id == original.query_id
    assert "private-log-marker" not in caplog.text
    assert "private-shutdown-marker" not in caplog.text
    assert original.statement not in caplog.text


def protect(connection):
    from dpone.adapters.clickhouse_native_driver import isolate_native_logging

    isolate_native_logging(connection)
    return connection


def log_connection(text):
    from clickhouse_driver.block import RowOrientedBlock
    from clickhouse_driver.connection import Connection

    class PacketTypeReader:
        def read_one(self):
            return 10  # Real native LOG dispatch, with a fixture decoded block.

    columns = [(name, "String") for name in ("host_name", "thread_id", "query_id", "priority", "source", "text")]
    block = RowOrientedBlock(columns, [("node", 1, "query", 4, "Fixture", text)])
    connection = Connection("127.0.0.1")
    connection.fin = PacketTypeReader()
    connection.receive_data = lambda **kwargs: block
    return connection


def test_real_log_decoder_is_private_and_ordinary_connection_still_logs(caplog):
    from clickhouse_driver.connection import Connection

    original = Connection.receive_packet
    caplog.set_level(logging.INFO, logger="clickhouse_driver.log")
    protected = protect(log_connection("private-log-marker"))
    ordinary = log_connection("ordinary-log-marker")
    with ThreadPoolExecutor(2) as executor:
        futures = [executor.submit(connection.receive_packet) for connection in (protected, ordinary)]
        assert [future.result(timeout=5).type for future in futures] == [10, 10]
    assert "private-log-marker" not in caplog.text
    assert "ordinary-log-marker" in caplog.text
    assert Connection.receive_packet is original
    assert protected.receive_packet.__func__.__code__ is original.__code__
    assert protected.receive_packet.__func__.__defaults__ == original.__defaults__


def test_real_connect_error_is_private_and_other_connection_keeps_warning(caplog):
    from clickhouse_driver.connection import Connection

    caplog.set_level(logging.DEBUG, logger="clickhouse_driver.connection")

    def fail_private(*args):
        raise OSError("private-socket-marker")

    def fail_ordinary(*args):
        raise OSError("ordinary-socket-marker")

    protected, ordinary = protect(Connection("127.0.0.1")), Connection("127.0.0.1")
    protected._init_connection, ordinary._init_connection = fail_private, fail_ordinary
    for connection in (protected, ordinary):
        with pytest.raises(Exception):
            connection.connect()
    assert "private-socket-marker" not in caplog.text
    assert "ordinary-socket-marker" in caplog.text


def test_logging_methods_match_pinned_driver_audit():
    from clickhouse_driver.connection import Connection

    from dpone.adapters.clickhouse_native_driver import LOGGING_METHODS

    tree = ast.parse(inspect.getsource(Connection))
    actual = {
        node.name
        for node in tree.body[0].body
        if isinstance(node, ast.FunctionDef)
        and any(isinstance(child, ast.Name) and child.id in {"logger", "log_block"} for child in ast.walk(node))
    }
    assert actual == set(LOGGING_METHODS)


def test_unexpected_driver_method_fails_before_any_rebinding():
    from clickhouse_driver.connection import Connection

    connection = Connection("127.0.0.1")
    connection.send_query = lambda *args, **kwargs: None
    with pytest.raises(RuntimeError):
        protect(connection)
    assert "connect" not in connection.__dict__
