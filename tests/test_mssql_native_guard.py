"""Exact-stage transaction proof for supervised native writers."""

from contextlib import nullcontext

import pytest

from dpone.adapters.mssql_native_guard import native_exact_stage_barrier


class _Connector:
    def __init__(self, *, fail_lock=False):
        self.events = []
        self.fail_lock = fail_lock

    def bounded_query_timeout(self, seconds):
        self.events.append(("timeout", seconds))
        return nullcontext()

    def begin(self):
        self.events.append("begin")

    def get_records(self, sql):
        self.events.append(sql)
        if self.fail_lock:
            raise TimeoutError("lock timeout")
        return []

    def commit_transaction(self):
        self.events.append("commit")

    def rollback(self):
        self.events.append("rollback")


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
