"""DDL profile tests: fixed requests, one send and explicit successful EOS."""

import hashlib
import importlib
import io
import json
import subprocess
import sys
from dataclasses import replace
from types import SimpleNamespace

import pytest

from dpone.contracts.clickhouse_authority import OperationBinding
from dpone.contracts.clickhouse_publication import choose_publication
from tests.test_clickhouse_authority_sqlite import subject
from tests.test_clickhouse_publication_codec import example_record


def _api():
    return importlib.import_module("dpone.adapters.clickhouse_native_publication")


def original_publication(method="replace_partition", partition="all"):
    before = example_record().intent.before
    if method == "rename":
        before = replace(before, target=None)
    elif method == "exchange":
        before = replace(before, candidate=replace(before.candidate, rows=0, partitions=()))
    elif method == "noop":
        before = replace(before, candidate=replace(before.target, uuid="new"))
    else:
        before = replace(
            before,
            target=replace(before.target, partitions=(partition,)),
            candidate=replace(before.candidate, partitions=(partition,)),
        )
    return (
        OperationBinding("deployment:one", subject(), "candidate", 1),
        choose_publication("deployment:one", before),
    )


def request(method="replace_partition", partition="all"):
    contract = importlib.import_module("dpone.ports.clickhouse_publication_transport")
    binding, intent = original_publication(method, partition)
    return contract.NativePublicationRequest(binding, intent.method, intent.query_id, intent.partition_id)


def endpoint(**changes):
    values = dict(
        server_id="server",
        host="127.0.0.1",
        port=9000,
        user="publisher",
        password="synthetic-secret",
        secure=False,
        connect_timeout=2,
        send_receive_timeout=2,
    )
    return _api().NativePublicationEndpoint(**(values | changes))


def packet(kind, rows=0, columns=0):
    return SimpleNamespace(type=kind, block=SimpleNamespace(num_rows=rows, num_columns=columns))


class DriverProbe:
    """Replace external I/O only; capture contract arguments and call ordering."""

    def __init__(self, packets):
        self.packets = iter(packets)
        self.events = []
        self.connected = False
        self.socket = SimpleNamespace(getpeername=lambda: ("127.0.0.1", 9000))
        self.server_info = SimpleNamespace(
            name="ClickHouse", version_major=24, version_minor=8, version_patch=14, revision=54470, used_revision=54468
        )
        self.fail = None

    def connect(self):
        self.events.append("connect")
        self.connected = True

    def send_query(self, query, **kwargs):
        assert self.connected
        self.events.append(("query", query, kwargs))
        if self.fail:
            raise self.fail

    def send_external_tables(self, tables):
        assert tables is None
        self.events.append("end-data")

    def receive_packet(self):
        result = next(self.packets, EOFError("synthetic-secret"))
        if isinstance(result, BaseException):
            raise result
        self.events.append(result.type)
        return result

    def disconnect(self):
        self.events.append("disconnect")


def install_probe(monkeypatch, packets):
    import clickhouse_driver

    probe = DriverProbe(packets)
    created = []

    def client(**kwargs):
        created.append(kwargs)
        return SimpleNamespace(connection=probe, disconnect=probe.disconnect)

    monkeypatch.setattr(clickhouse_driver, "Client", client)
    return probe, created


@pytest.mark.parametrize(
    "method,expected",
    [
        ("replace_partition", "ALTER TABLE `db`.`target` REPLACE PARTITION ID 'all' FROM `db`.`candidate`"),
        ("exchange", "EXCHANGE TABLES `db`.`target` AND `db`.`candidate`"),
        ("rename", "RENAME TABLE `db`.`candidate` TO `db`.`target`"),
        ("noop", None),
    ],
)
def test_fixed_statements_and_no_arbitrary_sql(method, expected):
    assert request(method).statement == expected
    with pytest.raises(TypeError):
        replace(request(method), sql="DROP TABLE target")


@pytest.mark.parametrize("partition", ["x' FROM db.other;--", "tuple()", "a\\b", "a b", "雪"])
def test_partition_id_outside_profile_rejected_before_transport(partition):
    with pytest.raises(ValueError):
        request(partition=partition)


def test_wire_request_does_not_carry_original_catalog_or_authority_grant():
    from dataclasses import asdict

    assert set(asdict(request())) == {"binding", "method", "query_id", "partition_id"}


@pytest.mark.parametrize(
    "changes",
    [
        {"binding": object()},
        {"method": "DROP TABLE target"},
        {"query_id": ""},
        {"query_id": "foreign"},
        {"method": "exchange", "partition_id": "all"},
    ],
)
def test_invalid_wire_request_rejected(changes):
    with pytest.raises(ValueError):
        replace(request(), **changes)


def test_native_requires_explicit_successful_eos(monkeypatch):
    probe, created = install_probe(monkeypatch, [packet(i) for i in (3, 6, 10, 14, 17, 1, 5)])
    original = request()
    completion = _api().DirectNativePublicationTransport(endpoint()).execute(original)
    assert probe.events == [
        "connect",
        ("query", original.statement, {"query_id": original.query_id, "params": None}),
        "end-data",
        3,
        6,
        10,
        14,
        17,
        1,
        5,
        "disconnect",
    ]
    assert len(created) == 1
    assert created[0]["disable_reconnect"] is True
    assert created[0]["compression"] is False
    assert not created[0].get("alt_hosts") and not created[0].get("round_robin")
    assert created[0]["database"] == "db"
    assert completion.operation_id == "deployment:one"
    assert completion.query_id == "dpone-publication-" + hashlib.sha256(b"deployment:one").hexdigest()
    assert completion.server_version == (24, 8, 14)
    assert completion.driver_version == "0.2.10"
    assert completion.statement_digest == hashlib.sha256(original.statement.encode()).hexdigest()
    payload = dict(
        profile="dpone.clickhouse.native-completion.v1",
        operation_id=completion.operation_id,
        query_id=completion.query_id,
        server_id="server",
        statement_digest=completion.statement_digest,
        server_version=[24, 8, 14],
        server_revision=54470,
        driver_version="0.2.10",
    )
    assert (
        completion.digest
        == hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    )


@pytest.mark.parametrize(
    "response",
    [
        packet(2),
        packet(4),
        packet(7),
        packet(8),
        packet(11),
        packet(12),
        packet(13),
        packet(1, rows=1),
        packet(1, columns=1),
        EOFError("synthetic-secret"),
        TimeoutError("synthetic-secret"),
    ],
)
def test_nonterminal_or_failed_response_cannot_close(monkeypatch, response):
    probe, created = install_probe(monkeypatch, [packet(3), response])
    with pytest.raises(_api().NativePublicationError) as caught:
        _api().DirectNativePublicationTransport(endpoint()).execute(request())
    assert caught.value.safe_to_retry is False
    assert "synthetic-secret" not in str(caught.value)
    assert caught.value.__suppress_context__
    assert len(created) == 1 and probe.events.count("connect") == 1
    assert sum(isinstance(event, tuple) for event in probe.events) == 1
    assert probe.events[-1] == "disconnect"


def test_partial_send_has_no_retry_or_fallback(monkeypatch):
    probe, created = install_probe(monkeypatch, [packet(5)])
    probe.fail = ConnectionError("synthetic-secret")
    with pytest.raises(_api().NativePublicationError):
        _api().DirectNativePublicationTransport(endpoint()).execute(request())
    assert len(created) == 1
    assert probe.events.count("connect") == 1
    assert "end-data" not in probe.events
    assert probe.events[-1] == "disconnect"


@pytest.mark.parametrize(
    "bad",
    [
        dict(host="localhost"),
        dict(host="::1"),
        dict(port=True),
        dict(port=0),
        dict(connect_timeout=float("inf")),
        dict(send_receive_timeout=0),
        dict(secure="false"),
    ],
)
def test_invalid_endpoint_rejected_before_socket(bad):
    with pytest.raises(ValueError):
        endpoint(**bad)


@pytest.mark.parametrize("bad", ["peer", "version", "driver", "subject"])
def test_one_connection_one_query_no_reconnect(monkeypatch, bad):
    import clickhouse_driver

    probe, created = install_probe(monkeypatch, [packet(5)])
    config = endpoint()
    if bad == "peer":
        probe.socket.getpeername = lambda: ("127.0.0.2", 9000)
    elif bad == "version":
        probe.server_info.version_patch = 15
    elif bad == "driver":
        monkeypatch.setattr(clickhouse_driver, "__version__", "0.2.11")
    else:
        config = endpoint(server_id="foreign")
    with pytest.raises(_api().NativePublicationError):
        _api().DirectNativePublicationTransport(config).execute(request())
    assert len(created) <= 1
    assert not any(isinstance(event, tuple) for event in probe.events)


def test_noop_opens_no_connection(monkeypatch):
    _, created = install_probe(monkeypatch, [])
    with pytest.raises(_api().NativePublicationError):
        _api().DirectNativePublicationTransport(endpoint()).execute(request("noop"))
    assert not created


def test_optional_driver_not_imported_on_base_path():
    program = """
import sys
sys.modules['clickhouse_driver'] = None
import dpone
from dpone.adapters.clickhouse_native_publication import DirectNativePublicationTransport
assert sys.modules['clickhouse_driver'] is None
"""
    result = subprocess.run([sys.executable, "-c", program], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr


def test_real_driver_initialization_and_query_serialization(monkeypatch):
    from clickhouse_driver.connection import Connection, ServerInfo
    from clickhouse_driver.streams.native import BlockOutputStream

    wire = io.BytesIO()
    calls = []

    def connect(connection):
        calls.append("connect")
        connection.connected = True
        connection.server_info = ServerInfo("ClickHouse", 24, 8, 14, 54470, "UTC", "fixture", 54401)
        connection.context.server_info = connection.server_info
        connection.socket = SimpleNamespace(getpeername=lambda: ("127.0.0.1", 9000))
        connection.fout = wire
        connection.block_out = BlockOutputStream(wire, connection.context)

    monkeypatch.setattr(Connection, "connect", connect)
    monkeypatch.setattr(Connection, "receive_packet", lambda self: packet(5))
    monkeypatch.setattr(Connection, "disconnect", lambda self: calls.append("disconnect"))
    original = request()
    _api().DirectNativePublicationTransport(endpoint()).execute(original)
    assert calls == ["connect", "disconnect"]
    assert wire.getvalue()[0] == 1  # Actual native QUERY, not high-level execute.
    assert wire.getvalue().count(original.statement.encode()) == 1
    assert original.query_id.encode() in wire.getvalue()
    assert "synthetic-secret" not in repr(endpoint())
