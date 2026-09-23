"""One command and terminal/EOF enforcement at the process adapter."""

from dataclasses import replace

import pytest

from dpone.adapters.mssql_sqlclient_writer_observer_process import SqlClientWriterObserverProcessBackend
from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission
from dpone.contracts.mssql_sqlclient_writer_observer_wire import WriterObserverRequest
from dpone.contracts.mssql_tds_worker import TdsProcessIdentity
from tests.mssql_sqlclient_departure_v2_fixtures import sample


class _Process:
    identity = TdsProcessIdentity("a" * 64, "11111111-1111-4111-8111-111111111111", 123, 4)
    operation_deadline = 3.0
    channel = object()

    def assert_current(self):
        return None


def _request():
    own = sample().observer.authority
    admission = SqlClientObserverAdmission(own.server, own.database, own.login, own.transport)
    return WriterObserverRequest(
        attempt_sha256="a" * 64,
        launch_sha256="b" * 64,
        observer_admission=admission,
        target_admission=replace(
            admission,
            login=replace(
                admission.login, principal_id=9, name="writer", sid="09", original_name="writer", original_sid="09"
            ),
        ),
        operation_deadline_ns=3_000_000_000,
    )


def test_backend_rejects_replay_before_second_write(monkeypatch):
    backend = SqlClientWriterObserverProcessBackend(_Process(), _request())
    monkeypatch.setattr("dpone.adapters.mssql_sqlclient_writer_observer_process.require_quiet", lambda channel: None)
    monkeypatch.setattr(
        "dpone.adapters.mssql_sqlclient_writer_observer_process.write_frame",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("first write")),
    )
    with pytest.raises(RuntimeError, match="first write"):
        backend.observe_once(session_id=52, nonce=b"n" * 32, deadline=3.0)
    with pytest.raises(ValueError, match="writer_observer_process_unknown"):
        backend.observe_once(session_id=52, nonce=b"n" * 32, deadline=3.0)
