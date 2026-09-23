from hashlib import sha256
from time import monotonic
from types import SimpleNamespace

import pytest

from dpone.app import mssql_sqlclient_restricted_writer_verify_bootstrap as bootstrap
from dpone.contracts.mssql_tds_connection import TdsConnectionMaterial
from tests.test_mssql_sqlclient_restricted_writer_verify import verify_result
from tests.test_mssql_sqlclient_restricted_writer_verify_request import launch_request
from tests.test_mssql_tds_coordinator import PROCESS


class Channel:
    def __init__(self, *, fail_close=False):
        self.fail_close = fail_close
        self.close_count = 0

    def setblocking(self, value):
        assert value is False

    def shutdown(self, direction):
        del direction

    def close(self):
        self.close_count += 1
        if self.fail_close:
            raise RuntimeError("ambiguous channel close")


class Connection:
    def __init__(self, *, fail_close=False):
        self.fail_close = fail_close
        self.close_count = 0

    def close(self):
        self.close_count += 1
        if self.fail_close:
            raise RuntimeError("ambiguous close")


def arrange(monkeypatch, *, fail_close=False, fail_channel_close=False):
    startup = monotonic() + 2.0
    operation = startup + 2.0
    request = launch_request().request
    admission = b"admission"
    connection = Connection(fail_close=fail_close)
    writes = []
    channel = Channel(fail_close=fail_channel_close)
    launch = {
        "request": request,
        "startup_deadline": startup,
        "operation_deadline": operation,
        "admission_sha256": sha256(admission).hexdigest(),
        "profile": launch_request().profile,
    }
    monkeypatch.setattr(bootstrap.os, "getppid", lambda: 42)
    monkeypatch.setattr(bootstrap, "decode_verify_launch_request", lambda payload: launch)
    monkeypatch.setattr(bootstrap, "decode_connection_admission", lambda payload: (object(), launch["profile"]))
    monkeypatch.setattr(bootstrap.LinuxTdsProcess, "identify", lambda pid: PROCESS)
    monkeypatch.setattr(bootstrap.socket, "socket", lambda fileno: channel)
    reads = iter((b"private", b"authorization"))
    monkeypatch.setattr(bootstrap, "_read", lambda channel, deadline: next(reads))
    monkeypatch.setattr(
        bootstrap,
        "decode_verify_credentials",
        lambda private, **kwargs: (TdsConnectionMaterial("localhost", 1433, "db", "writer", "secret"), b"n" * 32),
    )
    monkeypatch.setattr(
        bootstrap,
        "TdsCoordinatorConnection",
        lambda build, profile: SimpleNamespace(connect=lambda *args, **kwargs: connection),
    )
    opening = object()
    monkeypatch.setattr(
        bootstrap,
        "SqlClientRestrictedWriterVerify",
        lambda *args: SimpleNamespace(
            open_writer_session=lambda *inner_args, **kwargs: opening,
            execute_authorized=lambda observed, **kwargs: verify_result() if observed is opening else None,
        ),
    )
    monkeypatch.setattr(bootstrap, "encode_verify_opening", lambda observed: b"opening" if observed is opening else b"")
    monkeypatch.setattr(
        bootstrap,
        "decode_probe_authorization",
        lambda payload, **kwargs: payload == b"authorization" and kwargs["opening_payload"] == b"opening",
    )
    monkeypatch.setattr(bootstrap, "encode_verify_child_result", lambda result: b"result")
    monkeypatch.setattr(bootstrap, "_write", lambda channel, payload, deadline: writes.append((payload, deadline)))
    arguments = dict(
        expected_parent_pid=42,
        startup_deadline=startup,
        operation_deadline=operation,
        channel_fd=7,
        admission=admission,
        public_request=b"public",
        implementation_sha256=request.implementation_sha256,
    )
    return connection, writes, arguments, launch, channel


def test_bootstrap_uses_startup_deadline_then_closes_sql_once_before_result(monkeypatch):
    connection, writes, arguments, _, channel = arrange(monkeypatch)
    assert bootstrap.run_restricted_writer_verify(**arguments) == 0
    assert connection.close_count == 1
    assert channel.close_count == 1
    assert writes[0][1] == arguments["startup_deadline"]
    assert writes[1] == (b"opening", arguments["operation_deadline"])
    assert writes[2] == (b"result", arguments["operation_deadline"])


def test_ambiguous_connection_close_is_never_retried_or_followed_by_result(monkeypatch):
    connection, writes, arguments, _, channel = arrange(monkeypatch, fail_close=True)
    assert bootstrap.run_restricted_writer_verify(**arguments) == 1
    assert connection.close_count == 1
    assert channel.close_count == 1
    assert len(writes) == 2


@pytest.mark.parametrize("field", ("startup_deadline", "admission_sha256"))
def test_bootstrap_rejects_substituted_startup_binding(monkeypatch, field):
    connection, writes, arguments, launch, channel = arrange(monkeypatch)
    launch[field] = "0" * 64 if field == "admission_sha256" else arguments[field] + 1.0
    assert bootstrap.run_restricted_writer_verify(**arguments) == 1
    assert connection.close_count == 0 and writes == []
    assert channel.close_count == 0


def test_ambiguous_result_channel_close_never_reports_success_or_retries(monkeypatch):
    connection, writes, arguments, _, channel = arrange(monkeypatch, fail_channel_close=True)
    assert bootstrap.run_restricted_writer_verify(**arguments) == 1
    assert connection.close_count == 1
    assert channel.close_count == 1
    assert writes[-1] == (b"result", arguments["operation_deadline"])
