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


@pytest.mark.parametrize("table", [table.name for table in DISPATCH_TABLES])
def test_dispatch_translates_only_missing_user_transaction_at_lock_observation(table):
    sql = dispatch_trigger_sql("dpone_control", table)
    guarded = sql.split("BEGIN TRY", 1)[1].split("END TRY", 1)[0]
    assert "SET @ledger_lock_mode=APPLOCK_MODE" in guarded
    assert "INSERT" not in guarded and "THROW" not in guarded
    caught = sql.split("BEGIN CATCH", 1)[1].split("END CATCH", 1)[0]
    assert "IF ERROR_NUMBER()=3918" in caught
    assert "THROW 51000, 'DPONE_COMPOSITION_DISPATCH_LOCK', 1;" in caught
    assert caught.strip().endswith("THROW;")
    assert "IF ISNULL(@ledger_lock_mode,N'NoLock')<>N'Exclusive'" in sql


@pytest.mark.parametrize("count", [0, 1])
def test_original_transaction_observed_independently_with_exact_identity(count):
    calls = []

    def target_sql(sql, identity):
        calls.append((sql, identity))
        return ((count,),)

    result = transfer.observe_original_transaction(SimpleNamespace(target_sql=target_sql), 123)
    assert result == {"transaction_id": 123, "active_session_count": count}
    assert calls == [("SELECT COUNT_BIG(*) FROM sys.dm_tran_session_transactions WHERE transaction_id=?;", 123)]


@pytest.mark.parametrize("rows", [(), ((True,),), ((2,),), ((0, 1),)])
def test_original_transaction_observation_rejects_ambiguous_shape(rows):
    case = SimpleNamespace(target_sql=lambda *args: rows)
    with pytest.raises(RuntimeError, match="original_transaction_observation"):
        transfer.observe_original_transaction(case, 123)


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
