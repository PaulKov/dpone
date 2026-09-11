"""Actual ledger policies on an offline DB-API model; no live certification."""

from dataclasses import FrozenInstanceError
from types import SimpleNamespace

import pytest

from dpone.adapters.composition_mssql_transaction_fence import MssqlCompositionTransactionFence
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.mssql_transaction_governance import MssqlTransactionAdmission
from dpone.runtime.sinks.load_result import AtomicCommitOutcome, LoadResult
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_finalizer import (
    MssqlGenericCommitOutcomeUnknown,
    MssqlGenericTransactionFinalizer,
)
from tests import composition_mssql_attempt_fixtures
from tests.composition_mssql_gate_helpers import SERVICE, attempt
from tests.composition_mssql_store_fault_model import Cursor
from tests.test_mssql_generic_transaction_governance import (
    _commit_evidence_for_staging,
    _config,
    _FinalizerConnector,
    _FinalizerState,
    _operation,
    _receipt,
    _source_lifecycle_receipt,
    _staging,
)

active = composition_mssql_attempt_fixtures.active


class TargetCursor(Cursor):
    def _select_or_mutate(self, sql, parameters):
        if sql.startswith("SELECT DB_NAME()"):
            return [(self.connection.database_name, self.connection.transaction_id)]
        return super()._select_or_mutate(sql, parameters)


class TargetConnector(_FinalizerConnector):
    def __init__(self, database, **kwargs):
        super().__init__(**kwargs)
        self.connection = database.connect()
        self.connection.database_name = "DWH"
        self.connection.cursor = lambda: TargetCursor(self.connection)

    def begin(self):
        self.connection.autocommit = False

    def rollback(self):
        super().rollback()
        self.connection.rollback()

    def commit_transaction(self):
        self.connection.commit()
        super().commit_transaction()


def setup(active, **kwargs):
    database, store = active
    store.admit_once(attempt())
    connector = TargetConnector(database, **kwargs)
    state = _FinalizerState()
    fence = MssqlCompositionTransactionFence(attempt(), SERVICE, "DWH")
    finalizer = MssqlGenericTransactionFinalizer(
        SimpleNamespace(connector=connector),
        object(),
        transaction_state=state,
        lock_acquirer=lambda *a, **k: None,
        target_lock_acquirer=lambda *a, **k: None,
        target_identity_assertion=lambda *a, **k: None,
        catalog_revalidator=lambda *a, **k: None,
        target_contract_validator=lambda *a, **k: None,
        composition_fence=fence,
    )
    return database, connector, state, finalizer


def run(finalizer, handler=lambda staging: LoadResult(inserted_rows=3, updated_rows=0, total_rows=3)):
    return finalizer.finalize(
        _config(),
        MssqlTransactionAdmission(operation=_operation()),
        handler,
        _staging(3),
        staging_rows=3,
        load_id="load-1",
        source_lifecycle_receipt=_source_lifecycle_receipt(),
    )


def test_binding_is_immutable():
    fence = MssqlCompositionTransactionFence(attempt(), SERVICE, "DWH")
    with pytest.raises(FrozenInstanceError):
        fence.target_database = "other"


def test_same_target_transaction_commits_once(active):
    database, connector, state, finalizer = setup(active)
    run(finalizer)
    assert connector.commits == state.receipt_inserts == 1
    assert connector.connection.commits == 1
    assert len(connector.connection.transaction_ids) == 1
    assert any("composition_operations" in sql for sql, _ in database.statements)


@pytest.mark.parametrize("fault", ["parent", "attempt", "epoch", "database", "missing"])
def test_protected_failure_prevents_business_and_receipt(active, fault):
    database, connector, state, finalizer = setup(active)
    data = database.data
    if fault == "parent":
        key = next(iter(data["owners"]))
        data["owners"][key] = (*data["owners"][key][:-1], "RETIRING")
    elif fault == "attempt":
        key = attempt().attempt_sha256
        data["operations"][key] = (*data["operations"][key][:6], "COMMIT_UNKNOWN", None, None, None)
    elif fault == "epoch":
        key = next(iter(data["domains"]))
        row = data["domains"][key]
        data["domains"][key] = (*row[:3], row[3] + 1, row[4])
    elif fault == "database":
        connector.connection.database_name = "elsewhere"
    else:
        data["operations"].clear()
    calls = []
    with pytest.raises(CompositionAdmissionError):
        run(finalizer, lambda staging: calls.append("business"))
    assert calls == [] and state.receipt_inserts == connector.commits == 0
    assert connector.rollbacks == 1


def test_terminal_recheck_rolls_back_before_receipt(active):
    _, connector, state, finalizer = setup(active)

    def business(staging):
        key = attempt().attempt_sha256
        data = connector.connection.data
        data["operations"][key] = (*data["operations"][key][:6], "FAILED", None, None, None)
        return LoadResult(inserted_rows=3, updated_rows=0, total_rows=3)

    with pytest.raises(CompositionAdmissionError):
        run(finalizer, business)
    assert state.receipt_inserts == connector.commits == 0
    assert connector.rollbacks == 1


def test_replay_still_requires_protected_active_parent(active):
    database, connector, state, finalizer = setup(active)
    key = next(iter(database.data["owners"]))
    database.data["owners"][key] = (*database.data["owners"][key][:-1], "RETIRING")
    with pytest.raises(CompositionAdmissionError):
        finalizer.replay_result(_receipt(_operation()))
    assert state.receipt_inserts == connector.commits == 0


@pytest.mark.parametrize("probe", [True, False])
def test_unknown_commit_preserves_existing_reconciliation(active, probe):
    _, connector, state, finalizer = setup(active, commit_error=RuntimeError("lost ACK"))
    state.probe_after_commit = probe
    if probe:
        result = run(finalizer)
        assert result.commit_outcome == AtomicCommitOutcome.COMMITTED_AFTER_RECEIPT_PROBE
    else:
        with pytest.raises(MssqlGenericCommitOutcomeUnknown):
            run(finalizer)
    assert connector.rollbacks == 0 and state.fresh_probes == 1


@pytest.mark.parametrize("fault", ["transaction", "database"])
def test_business_cannot_switch_transaction_or_database_before_receipt(active, fault):
    _, connector, state, finalizer = setup(active)

    def business(staging):
        if fault == "transaction":
            connector.connection.transaction_id += 1
        else:
            connector.connection.database_name = "elsewhere"
        return LoadResult(inserted_rows=3, updated_rows=0, total_rows=3)

    with pytest.raises(CompositionAdmissionError):
        run(finalizer, business)
    assert state.receipt_inserts == connector.commits == 0
    assert connector.rollbacks == 1


def test_valid_pretransaction_replay_is_read_only(active):
    _, connector, state, finalizer = setup(active)
    result = finalizer.replay_result(_receipt(_operation()))
    assert result.commit_outcome == AtomicCommitOutcome.REPLAY_SUPPRESSED
    assert state.receipt_inserts == connector.commits == 0
    assert connector.rollbacks == 1


def test_concurrent_receipt_replay_uses_existing_protected_transaction(active):
    _, connector, state, finalizer = setup(active)
    state.concurrent_receipt = _receipt(_operation(), payload_evidence=_commit_evidence_for_staging(3))
    result = run(finalizer, lambda staging: pytest.fail("replay ran business DML"))
    assert result.commit_outcome == AtomicCommitOutcome.REPLAY_SUPPRESSED
    assert len(connector.connection.transaction_ids) == 1
    assert state.receipt_inserts == connector.commits == 0
    assert connector.rollbacks == 1
