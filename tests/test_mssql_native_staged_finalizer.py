"""The native lifecycle invokes the existing transaction and lost-ACK authority."""

from threading import Event
from types import SimpleNamespace

import pytest

from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.mssql_transaction_governance import MssqlTransactionAdmission
from dpone.manifest.mssql_native_policy import native_limits
from dpone.runtime.connectors.mssql_bulk import BcpOptions
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.sinks.load_result import AtomicCommitOutcome
from dpone.runtime.sinks.mssql_native_composition import compose_native_stage_context
from dpone.runtime.sinks.mssql_native_staged_load import MssqlNativeStagedLoadService, NativePreparedStage
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_finalizer import MssqlGenericCommitOutcomeUnknown
from tests.test_mssql_generic_transaction_governance import (
    _config,
    _finalizer,
    _FinalizerConnector,
    _FinalizerState,
    _operation,
    _source_lifecycle_receipt,
    _staging,
)
from tests.test_mssql_native_policy import config as native_config


class _LockSession:
    def __init__(self):
        self.held = False
        self.closed = False
        self.released = False

    def get_records(self, sql, params=None):
        assert not self.closed
        if sql == "SELECT DB_NAME()":
            return [("DWH",)]
        if "sp_getapplock" in sql:
            self.held = True
            return [(0,)]
        assert "sp_releaseapplock" in sql
        result = 0 if self.held else -999
        self.held = False
        self.released = result == 0
        return [(result,)]

    def close(self):
        self.held = False
        self.closed = True


class _PublicationConnector(_FinalizerConnector):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.target_lock = _LockSession()
        self.lock_sessions = []
        self.bound_queries = []

    def execute_query(self, sql, params=None):
        self.bound_queries.append((sql, params))
        return super().execute_query(sql, params)

    def open_session(self, *, application_name):
        session = _LockSession()
        self.lock_sessions.append(session)
        return session

    def get_records(self, sql, params=None, *, as_dict=False):
        if "sp_getapplock" in sql or "sp_releaseapplock" in sql:
            return self.target_lock.get_records(sql, params)
        return super().get_records(sql, params, as_dict=as_dict)

    def close(self):
        # Closing the publication connection discards its session-owned locks.
        self.target_lock = _LockSession()
        super().close()


@pytest.mark.parametrize("lost_ack,probe", [(False, False), (True, True), (True, False)])
def test_native_service_uses_transaction_receipt_and_fresh_probe(lost_ack, probe, tmp_path):
    connector = _PublicationConnector(commit_error=OSError("lost acknowledgement") if lost_ack else None)
    state = _FinalizerState(probe_after_commit=probe)
    events = []
    stage = _staging(2)
    stage.columns = ("value", "__dpone__loaded_at")
    stage.database, stage.schema, stage.table = "DWH", "stage", "prepared"
    prepared = NativePreparedStage(
        stage, MssqlTransactionAdmission(operation=_operation()), _source_lifecycle_receipt(), None, None
    )
    context = compose_native_stage_context(
        store=SimpleNamespace(assert_lease=lambda lease: None),
        plan=SimpleNamespace(run_id="synthetic"),
        lease=SimpleNamespace(target_id="target"),
        wire_contract=build_mssql_bcp_native_contract(schema=[("value", "int")], query="SELECT synthetic"),
        limits=native_limits(native_config()),
        work_dir=tmp_path,
        target_connector=connector,
        importer_connection=lambda: pytest.fail("publication must not open importers"),
        bcp_options_factory=BcpOptions,
        database="DWH",
        schema="dbo",
        row_source=lambda: pytest.fail("publication must not reopen the source"),
        journal_factory=lambda: None,
        cancelled=Event(),
        required_target_headroom_bytes=1024,
    )

    class Preparer:
        def stage(self, config, payload):
            return prepared

        def reverify(self, prepared):
            events.append("verified")

        def publication_scope(self, prepared):
            return context.preparation_scope()

        def cleanup(self, prepared):
            events.append("cleanup")

    strategy = SimpleNamespace(
        connector=connector,
        state_storage=state,
        transaction_finalizer_factory=lambda strategy, state: _finalizer(connector, state),
        _table_exists=lambda config: True,
        _target_name=lambda config: "[db].[dbo].[target]",
        _insert_from_staging_to_table=lambda *args, **kwargs: 2,
        _count_target=lambda config: 2,
    )
    sink = SimpleNamespace(_strategy_map={LoadStrategy.FULL_REFRESH: strategy})
    service = MssqlNativeStagedLoadService(sink, Preparer())
    config = _config()
    config.load_strategy = LoadStrategy.FULL_REFRESH
    handle = service.stage(config, object())
    if lost_ack and not probe:
        with pytest.raises(MssqlGenericCommitOutcomeUnknown):
            service.finalize(config, handle)
        with pytest.raises(RuntimeError, match="publication_unresolved"):
            service.abort(handle)
        assert "cleanup" not in events
    else:
        result = service.finalize(config, handle)
        expected = AtomicCommitOutcome.COMMITTED_AFTER_RECEIPT_PROBE if lost_ack else AtomicCommitOutcome.COMMITTED
        assert result.commit_outcome == expected
        assert result.commit_receipt_id == _operation().receipt_id
        assert result.staging_rows == 2
    assert connector.commits == 1
    assert connector.rollbacks == 0
    assert state.receipt_inserts == 1
    assert state.fresh_probes == int(lost_ack)
    assert connector.calls.count("TRUNCATE TABLE [db].[dbo].[target]") == 1
    clock_update = "UPDATE [DWH].[stage].[prepared] SET [__dpone__loaded_at] = ?"
    assert connector.calls.count(clock_update) == 1
    assert (
        connector.calls.index("SELECT SYSUTCDATETIME() AS loaded_at_utc")
        < connector.calls.index(clock_update)
        < connector.calls.index("TRUNCATE TABLE [db].[dbo].[target]")
    )
    assert state.persisted is not None
    assert (clock_update, (state.persisted.loaded_at_utc.replace(tzinfo=None),)) in connector.bound_queries
    assert len(connector.lock_sessions) == 1
    assert connector.lock_sessions[0].released
    assert connector.lock_sessions[0].closed
