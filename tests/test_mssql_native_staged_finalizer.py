"""The native lifecycle invokes the existing transaction and lost-ACK authority."""

from types import SimpleNamespace

import pytest

from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.mssql_transaction_governance import MssqlTransactionAdmission
from dpone.runtime.sinks.load_result import AtomicCommitOutcome
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


@pytest.mark.parametrize("lost_ack,probe", [(False, False), (True, True), (True, False)])
def test_native_service_uses_transaction_receipt_and_fresh_probe(lost_ack, probe):
    connector = _FinalizerConnector(commit_error=OSError("lost acknowledgement") if lost_ack else None)
    state = _FinalizerState(probe_after_commit=probe)
    events = []
    stage = _staging(2)
    prepared = NativePreparedStage(
        stage, MssqlTransactionAdmission(operation=_operation()), _source_lifecycle_receipt(), None, None
    )

    class Preparer:
        def stage(self, config, payload):
            return prepared

        def reverify(self, prepared):
            events.append("verified")

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
