"""Exact-stage retirement is repeatable across every durable crash boundary."""

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from dpone.runtime.mssql_native_target_local_recovery import retire_exact_owned_stage


class _Connector:
    def __init__(self, object_id=11):
        self.object_id = object_id
        self.calls = []

    @contextmanager
    def bounded_query_timeout(self, seconds):
        self.calls.append(("timeout", seconds))
        yield

    def begin(self):
        self.calls.append("begin")

    def get_records(self, query, params=None):
        if query == "SELECT OBJECT_ID(?)":
            return [(self.object_id,)]
        if "TABLOCKX, HOLDLOCK" in query:
            self.calls.append("table-lock")
            return []
        raise AssertionError(query)

    def execute_query(self, query):
        self.calls.append("drop")
        self.object_id = None

    def commit_transaction(self):
        self.calls.append("commit")

    def rollback(self):
        self.calls.append("rollback")


class _Importer:
    def __init__(self, connector, *, drift=None):
        self.connector = connector
        self.drift = drift
        self.calls = []

    @contextmanager
    def _mutation_scope(self, *args):
        self.calls.append("applock")
        yield

    def _assert_lease(self, lease):
        self.calls.append("lease")

    def table_name(self, plan, attempt):
        return "owned"

    def qualified(self, table):
        return "[db].[dbo].[owned]"

    def _assert_stage_identity(self, plan, attempt, table, object_id):
        self.calls.append("identity")
        if self.drift is not None:
            raise ValueError(f"mssql_native.stage_{self.drift}_changed")


def _receipt():
    return SimpleNamespace(
        attempt_id="a" * 64,
        stage_id="[db].[dbo].[owned]",
        consumed_part_evidence={"native_object_id": 11},
    )


def test_exact_retirement_replays_after_drop_before_durable_event():
    connector = _Connector()
    importer = _Importer(connector)
    retire_exact_owned_stage(importer, None, _receipt(), None, timeout_seconds=3)
    assert connector.object_id is None
    assert connector.calls == [("timeout", 3), "begin", "table-lock", "drop", "commit"]
    retire_exact_owned_stage(importer, None, _receipt(), None, timeout_seconds=3)
    assert connector.calls[-2:] == ["begin", "commit"]
    assert connector.calls.count("drop") == 1


@pytest.mark.parametrize("object_id,drift", [(12, None), (11, "owner"), (11, "schema")])
def test_exact_retirement_rejects_replacement_and_binding_drift(object_id, drift):
    connector = _Connector(object_id)
    with pytest.raises(ValueError):
        retire_exact_owned_stage(_Importer(connector, drift=drift), None, _receipt(), None, timeout_seconds=3)
    assert "drop" not in connector.calls
    assert "rollback" in connector.calls


def test_exact_retirement_rejects_stage_name_drift():
    receipt = _receipt()
    receipt.stage_id = "[db].[dbo].[replacement]"
    connector = _Connector()
    with pytest.raises(ValueError, match="stage_identity_mismatch"):
        retire_exact_owned_stage(_Importer(connector), None, receipt, None, timeout_seconds=3)
    assert connector.calls == []


def test_exact_retirement_timeout_rolls_back_without_drop():
    class TimedOutConnector(_Connector):
        def get_records(self, query, params=None):
            if "TABLOCKX, HOLDLOCK" in query:
                raise TimeoutError("stage lock unavailable")
            return super().get_records(query, params)

    connector = TimedOutConnector()
    with pytest.raises(TimeoutError, match="stage lock"):
        retire_exact_owned_stage(_Importer(connector), None, _receipt(), None, timeout_seconds=3)
    assert connector.object_id == 11
    assert "rollback" in connector.calls and "drop" not in connector.calls
