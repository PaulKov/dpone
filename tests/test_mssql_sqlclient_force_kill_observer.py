from __future__ import annotations

import signal
from types import SimpleNamespace

import pytest

from dpone.contracts.mssql_sqlclient_ipc import sqlclient_application_name
from tests.integration.mssql.test_mssql_sqlclient_route_live import _ActiveBulkCopyKiller


class _Connector:
    def __init__(self, response):
        self.response = response
        self.calls = []
        self.closed = False

    def get_records(self, query, params):
        self.calls.append((query, params))
        return self.response(params)

    def close(self):
        self.closed = True


def _request(attempt: str = "attempt-under-test"):
    return SimpleNamespace(
        attempt_id=attempt,
        grant_token_sha256="a" * 64,
        object_id=781,
        stage_id_sha256="b" * 64,
    )


def test_force_kill_observer_ignores_unrelated_qualifying_session() -> None:
    request = _request()
    unrelated = sqlclient_application_name("another-attempt", "a" * 64, 781, "b" * 64)
    connector = _Connector(lambda params: [(51, "BULK INSERT", 1, 1, 1)] if params[-1] == unrelated else [])
    killed = []
    observer = _ActiveBulkCopyKiller(
        connector_factory=lambda: connector,
        kill_process_group=lambda *args: killed.append(args),
        signal_process_group=lambda *_args: None,
        timeout_seconds=0.01,
        poll_seconds=0.001,
    )

    observer.start(9001, request)

    with pytest.raises(pytest.fail.Exception, match="force-kill synchronization failed"):
        observer.require_observation()
    assert killed == []
    assert connector.closed is True
    assert all(
        params[-1]
        == sqlclient_application_name(
            request.attempt_id,
            request.grant_token_sha256,
            request.object_id,
            request.stage_id_sha256,
        )
        for _query, params in connector.calls
    )


def test_force_kill_observer_binds_attempt_lock_and_exact_stage() -> None:
    request = _request()
    competing = _request("competing-attempt")
    expected = (
        request.object_id,
        request.object_id,
        sqlclient_application_name(
            request.attempt_id,
            request.grant_token_sha256,
            request.object_id,
            request.stage_id_sha256,
        ),
    )
    competing_expected = (
        competing.object_id,
        competing.object_id,
        sqlclient_application_name(
            competing.attempt_id,
            competing.grant_token_sha256,
            competing.object_id,
            competing.stage_id_sha256,
        ),
    )
    connector = _Connector(
        lambda params: [(52, "BULK INSERT", 1, 1, 1)] if params in {expected, competing_expected} else []
    )
    killed = []
    signals = []
    observer = _ActiveBulkCopyKiller(
        connector_factory=lambda: connector,
        kill_process_group=lambda *args: killed.append(args),
        signal_process_group=lambda *args: signals.append(args),
        timeout_seconds=0.1,
        poll_seconds=0.001,
    )

    observer.start(9002, request)
    observer.start(9003, competing)

    assert observer.require_observation() == {
        "bulk_copy_active": True,
        "transaction_active": True,
        "session_applock_held": True,
        "exact_stage_lock_held": True,
        "competing_writer_active": True,
        "competing_writer_ignored": True,
    }
    assert killed == [(9002, signal.SIGKILL)]
    assert signals == [
        (9002, signal.SIGSTOP),
        (9003, signal.SIGSTOP),
        (9002, signal.SIGCONT),
        (9003, signal.SIGCONT),
    ]
    assert connector.closed is True
    query = connector.calls[0][0]
    assert "l.resource_database_id=DB_ID()" in query
    assert "l.resource_type IN (N'OBJECT',N'HOBT')" in query
    assert "l.request_mode IN (N'BU',N'IX',N'X')" in query
    assert "COUNT_BIG(*)" in query
