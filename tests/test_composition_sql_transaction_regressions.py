"""Offline SQL boundary regression checks; live commit visibility is separate."""

from types import SimpleNamespace

import pytest

from dpone.adapters.composition_clickhouse_dispatch_schema import DISPATCH_TABLES, dispatch_trigger_sql
from tests.integration.composition import mssql_transfer_fence_live_support as transfer


@pytest.mark.parametrize("table", [table.name for table in DISPATCH_TABLES])
def test_dispatch_requires_actual_transaction_lock_without_nested_count_assumption(table):
    sql = dispatch_trigger_sql("dpone_control", table)
    assert "XACT_STATE()<>1" in sql
    assert "APPLOCK_MODE(N'public'" in sql and "N'Transaction'" in sql and "N'Exclusive'" in sql
    assert "@@TRANCOUNT<2" not in sql
    assert "IF EXISTS (SELECT 1 FROM deleted)" in sql


def test_transfer_fixture_uses_dbapi_transaction_without_sql_mode_override(monkeypatch):
    calls = []
    worker = SimpleNamespace(autocommit=True)
    case = object.__new__(transfer.TransferGateCase)
    case.record = lambda *args: None

    def execute(connection, sql, *parameters):
        assert connection is worker and worker.autocommit is False
        calls.append(sql)
        if "@@TRANCOUNT" in sql:
            return ((1, 1, 2),)
        return ((0,),)

    monkeypatch.setattr(transfer, "execute", execute)
    case.begin_worker(worker)
    assert any("FROM [managed].[rows]" in sql for sql in calls)
    assert all("BEGIN TRANSACTION" not in sql and "SET IMPLICIT_TRANSACTIONS" not in sql for sql in calls)


@pytest.mark.parametrize("row", [(0, 0, 0), (2, 1, 2), (1, -1, 2)])
def test_transfer_fixture_rejects_unexpected_begin_state(monkeypatch, row):
    case = object.__new__(transfer.TransferGateCase)
    case.record = lambda *args: None
    monkeypatch.setattr(transfer, "execute", lambda connection, sql, *args: (row,) if "@@TRANCOUNT" in sql else ((0,),))
    with pytest.raises(AssertionError):
        case.begin_worker(SimpleNamespace(autocommit=True))


@pytest.mark.parametrize("rows", [(), ((1, 1, "secret"),), ((True, 1, 2),), ((1, 1, 2), (1, 1, 2))])
def test_transfer_transaction_diagnostics_reject_non_numeric_or_unbounded_shape(monkeypatch, rows):
    monkeypatch.setattr(transfer, "execute", lambda *args: rows)
    with pytest.raises(RuntimeError, match="transaction_observation"):
        transfer.observe_worker_transaction(object())
