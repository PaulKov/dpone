"""Exact-stage transaction proof for supervised native writers."""

from contextlib import nullcontext

import pytest

from dpone.adapters.mssql_native_guard import native_exact_stage_barrier, sqlclient_exact_stage_barrier
from dpone.contracts.mssql_sqlclient_ipc import applock_resource


class _Connector:
    def __init__(self, *, fail_lock=False, fail_rollback=False):
        self.events = []
        self.fail_lock = fail_lock
        self.fail_rollback = fail_rollback

    def bounded_query_timeout(self, seconds):
        self.events.append(("timeout", seconds))
        return nullcontext()

    def begin(self):
        self.events.append("begin")

    def get_records(self, sql, params=None):
        self.events.append((sql, params) if params is not None else sql)
        if self.fail_lock:
            raise TimeoutError("lock timeout")
        if "sp_getapplock" in sql or "sp_releaseapplock" in sql:
            return [(0,)]
        return []

    def commit_transaction(self):
        self.events.append("commit")

    def rollback(self):
        self.events.append("rollback")
        if self.fail_rollback:
            raise ConnectionError("private vendor detail")


def test_barrier_holds_one_transaction_across_both_identity_checks_and_digest():
    connector = _Connector()

    def identity():
        connector.events.append("identity")

    with native_exact_stage_barrier(connector, "[db].[stage].[owned]", timeout_seconds=3, assert_identity=identity):
        connector.events.append("digest")
    assert connector.events == [
        ("timeout", 3),
        "begin",
        "SELECT TOP (1) 1 FROM [db].[stage].[owned] WITH (TABLOCKX, HOLDLOCK)",
        "identity",
        "digest",
        "identity",
        "commit",
    ]


def test_barrier_timeout_and_replacement_rollback_without_authority():
    connector = _Connector(fail_lock=True)
    with (
        pytest.raises(TimeoutError),
        native_exact_stage_barrier(connector, "[db].[stage].[owned]", timeout_seconds=3, assert_identity=lambda: None),
    ):
        pytest.fail("must not enter")
    assert connector.events[-1] == "rollback"

    connector = _Connector()
    checks = 0

    def replacement():
        nonlocal checks
        checks += 1
        if checks == 2:
            raise ValueError("stage replaced")

    with (
        pytest.raises(ValueError, match="stage replaced"),
        native_exact_stage_barrier(connector, "[db].[stage].[owned]", timeout_seconds=3, assert_identity=replacement),
    ):
        connector.events.append("digest")
    assert connector.events[-1] == "rollback"
    assert "commit" not in connector.events


def test_barrier_rollback_failure_preserves_primary_error_with_redacted_note():
    connector = _Connector(fail_rollback=True)

    with pytest.raises(ValueError, match="primary digest failure") as caught:
        with native_exact_stage_barrier(
            connector,
            "[db].[stage].[owned]",
            timeout_seconds=3,
            assert_identity=lambda: None,
        ):
            raise ValueError("primary digest failure")

    assert caught.value.__notes__ == ["mssql_native.barrier_rollback_failed:builtins.ConnectionError"]
    assert "private vendor detail" not in caught.value.__notes__[0]


def test_sqlclient_barrier_acquires_same_session_lock_before_exact_stage_transaction():
    connector = _Connector()
    token = "a" * 64

    def identity():
        connector.events.append("identity")

    with sqlclient_exact_stage_barrier(
        connector,
        "[db].[stage].[owned]",
        grant_token_sha256=token,
        timeout_seconds=3,
        assert_identity=identity,
    ):
        connector.events.append("digest")

    resource = applock_resource(token)
    acquire = next(item for item in connector.events if isinstance(item, tuple) and "sp_getapplock" in item[0])
    release = connector.events[-1]
    assert "sp_getapplock" in acquire[0]
    assert acquire[1] == (resource, 3000)
    assert connector.events.index("begin") < connector.events.index("digest") < connector.events.index("commit")
    assert "sp_releaseapplock" in release[0]
    assert release[1] == (resource,)


def test_sqlclient_barrier_rejects_failed_applock_without_stage_access():
    class Refused(_Connector):
        def get_records(self, sql, params=None):
            self.events.append((sql, params) if params is not None else sql)
            if "sp_getapplock" in sql:
                return [(-1,)]
            return []

    connector = Refused()
    with pytest.raises(TimeoutError, match="sqlclient_writer_not_settled"):
        with sqlclient_exact_stage_barrier(
            connector,
            "[db].[stage].[owned]",
            grant_token_sha256="a" * 64,
            timeout_seconds=3,
            assert_identity=lambda: None,
        ):
            pytest.fail("must not enter")
    assert "begin" not in connector.events
