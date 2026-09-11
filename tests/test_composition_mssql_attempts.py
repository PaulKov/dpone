"""Actual attempt adapter over a transactional fault model, never SQL certification."""

from copy import deepcopy
from dataclasses import replace

import pytest

from dpone.adapters.composition_mssql_attempts import MssqlCompositionAttemptStore
from dpone.adapters.composition_mssql_store import MssqlCompositionActivationStore
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_persistence import encode_attempt_identity
from dpone.contracts.composition_proof import CompositionProofAuthority
from tests.composition_mssql_attempt_fixtures import active as active
from tests.composition_mssql_attempt_fixtures import set_receipt, store_proofs, terminal_proofs, writes
from tests.composition_mssql_gate_helpers import SERVICE, attempt
from tests.composition_mssql_store_fault_model import offline_gate as offline_gate
from tests.test_composition_activation_contract import digest

pytestmark = pytest.mark.usefixtures("offline_gate")


def test_unavailable_database_does_not_admit_or_echo_driver_secret():
    def unavailable():
        raise RuntimeError("driver-password-secret")

    with pytest.raises(CompositionAdmissionError) as caught:
        MssqlCompositionAttemptStore(unavailable, expected_service_id=SERVICE).admit_once(attempt())
    assert "driver-password-secret" not in str(caught.value)


def test_missing_global_lock_blocks_attempt_before_any_insert(active):
    database, store = active
    database.lock_result = -1
    with pytest.raises(CompositionAdmissionError, match="ledger_lock"):
        store.admit_once(attempt())
    assert database.connections[-1].rollbacks == 1 and writes(database) == []


def test_admission_reserves_full_attempt_then_fresh_reads_running(active):
    database, store = active
    value = attempt()
    result = store.admit_once(value)
    assert result.state == "RUNNING" and result.attempt == value
    assert len(database.connections) == 2 and all(c.commits == 1 for c in database.connections)
    assert len({c.transaction_ids[0] for c in database.connections}) == 2
    record = database.data["operations"][value.attempt_sha256]
    assert record[0] == record[4] == value.attempt_sha256 and record[5] == encode_attempt_identity(value)
    assert tuple((row[2], row[3]) for row in database.data["operation_domains"]) == value.guard_epochs
    assert not any("LOGIN" in sql for sql, _ in database.statements)


@pytest.mark.parametrize("state", ["RUNNING", "COMMIT_UNKNOWN", "SUCCEEDED", "FAILED"])
def test_any_exact_replay_rejects_before_insert(active, state):
    database, store = active
    store.admit_once(attempt())
    proofs = terminal_proofs(state) if state in {"SUCCEEDED", "FAILED"} else ()
    store_proofs(database, proofs)
    set_receipt(database, state, proofs)
    before = deepcopy(database.data)
    database.statements.clear()
    with pytest.raises(CompositionAdmissionError, match="attempt_replay"):
        store.admit_once(attempt())
    assert database.data == before and writes(database) == []


@pytest.mark.parametrize("state", ["RUNNING", "COMMIT_UNKNOWN"])
def test_entire_ledger_blocks_overlapping_distinct_attempt(active, state):
    database, store = active
    store.admit_once(attempt())
    set_receipt(database, state)
    database.statements.clear()
    with pytest.raises(CompositionAdmissionError, match="attempt_conflict"):
        store.admit_once(attempt(2))
    assert writes(database) == []
    assert any(
        "FROM [dpone_control].[composition_operations]" in sql
        and "ORDER BY operation_key" in sql
        and "WHERE" not in sql
        for sql, _ in database.statements
    )


def test_retiring_parent_rejects_admission(active):
    database, store = active
    MssqlCompositionActivationStore(database.connect, expected_service_id=SERVICE).begin_retirement(database.request)
    database.statements.clear()
    with pytest.raises(CompositionAdmissionError, match="occurrence_state"):
        store.admit_once(attempt())
    assert writes(database) == []


def test_stale_epoch_rejects_before_ledger_mutation(active):
    database, store = active
    value = attempt()
    stale = replace(value, guard_epochs=((value.guard_epochs[0][0], 2),))
    with pytest.raises(CompositionAdmissionError, match="attempt_guard_epochs"):
        store.admit_once(stale)
    assert writes(database) == []


def test_lost_admission_commit_ack_never_returns_execution_permit(active):
    database, store = active
    database.fail_commit = True
    with pytest.raises(CompositionAdmissionError, match="control_operation_unknown") as caught:
        store.admit_once(attempt())
    assert "synthetic-secret" not in str(caught.value)
    assert len(database.data["operations"]) == 1 and database.connections[-1].rollbacks == 1
    assert sum("INSERT INTO [dpone_control].[composition_operations]" in sql for sql in writes(database)) == 1


def test_attempt_partition_must_match_every_exact_guard(active):
    database, store = active
    store.admit_once(attempt())
    database.data["operation_domains"].clear()
    with pytest.raises(CompositionAdmissionError, match="attempt_partition"):
        store.read_exact(attempt())


def test_finalization_uses_actual_full_principal_proof_triplet(active):
    database, store = active
    store.admit_once(attempt())
    proofs = terminal_proofs()
    store_proofs(database, proofs)
    before = len(database.connections)
    result = store.finalize(attempt(), state="SUCCEEDED", outcome_evidence_sha256=proofs[-1].proof_sha256)
    assert result.state == "SUCCEEDED" and result.outcome_evidence_sha256 == proofs[-1].proof_sha256
    assert len(database.connections) == before + 2 and all(c.commits == 1 for c in database.connections[before:])
    assert database.gate_observations and all(row[0] == attempt() for row in database.gate_observations)


def test_valid_failed_outcome_cannot_be_promoted_to_success(active):
    _refused_finalization(active, "FAILED", "terminal_outcome_state")


def test_unrecorded_outcome_hash_cannot_terminalize_running_attempt(active):
    _refused_finalization(active, "unrecorded", "attempt_terminal_proof")


def test_foreign_principal_proof_does_not_cover_issued_journal(active):
    _refused_finalization(active, "foreign", "attempt_terminal_proof")


def _refused_finalization(active, failure, reason):
    database, store = active
    store.admit_once(attempt())
    principal = CompositionProofAuthority("mssql", SERVICE, "mssql-sid:" + "ff" * 16) if failure == "foreign" else None
    proofs = terminal_proofs("FAILED" if failure == "FAILED" else "SUCCEEDED", principal)
    store_proofs(database, proofs)
    outcome = digest("caller assertion") if failure == "unrecorded" else proofs[-1].proof_sha256
    database.statements.clear()
    before = deepcopy(database.data)
    with pytest.raises(CompositionAdmissionError, match=reason):
        store.finalize(attempt(), state="SUCCEEDED", outcome_evidence_sha256=outcome)
    assert database.data == before and writes(database) == []


def test_prior_terminal_without_protected_triplet_cannot_free_guard_for_new_attempt(active):
    database, store = active
    store.admit_once(attempt())
    set_receipt(database, "SUCCEEDED", terminal_proofs())
    store_proofs(database, ())
    database.statements.clear()
    with pytest.raises(CompositionAdmissionError, match="terminal_proof_identity"):
        store.admit_once(attempt(2))
    assert writes(database) == []


def test_unknown_reconciliation_is_explicit_and_requires_original_unknown_proof(active):
    database, store = active
    store.admit_once(attempt())
    old, new = terminal_proofs("COMMIT_UNKNOWN"), terminal_proofs()
    store_proofs(database, old)
    store.finalize(attempt(), state="COMMIT_UNKNOWN", outcome_evidence_sha256=old[-1].proof_sha256)
    store_proofs(database, new)
    result = store.reconcile_unknown(attempt(), state="SUCCEEDED", outcome_evidence_sha256=new[-1].proof_sha256)
    assert result.state == "SUCCEEDED" and result.outcome_evidence_sha256 == new[-1].proof_sha256
    assert (attempt().attempt_sha256, "OUTCOME", old[-1].proof_sha256) in database.data["proofs"]


def test_prior_terminal_full_proof_allows_fresh_attempt_without_reissuing_old_attempt(active):
    database, store = active
    store.admit_once(attempt())
    proofs = terminal_proofs()
    store_proofs(database, proofs)
    store.finalize(attempt(), state="SUCCEEDED", outcome_evidence_sha256=proofs[-1].proof_sha256)
    old = database.data["operations"][attempt().attempt_sha256]
    result = store.admit_once(attempt(2))
    assert result.state == "RUNNING" and result.attempt.try_number == 2
    assert len(database.data["operations"]) == 2 and database.data["operations"][attempt().attempt_sha256] == old


def test_unknown_cannot_be_resolved_by_ordinary_finalize(active):
    database, store = active
    store.admit_once(attempt())
    old, new = terminal_proofs("COMMIT_UNKNOWN"), terminal_proofs()
    store_proofs(database, old)
    set_receipt(database, "COMMIT_UNKNOWN", old)
    store_proofs(database, new)
    database.statements.clear()
    with pytest.raises(CompositionAdmissionError, match="attempt_terminal_replay"):
        store.finalize(attempt(), state="SUCCEEDED", outcome_evidence_sha256=new[-1].proof_sha256)
    assert writes(database) == []


def test_reconciliation_requires_protected_original_unknown_outcome(active):
    database, store = active
    store.admit_once(attempt())
    set_receipt(database, "COMMIT_UNKNOWN")
    database.statements.clear()
    with pytest.raises(CompositionAdmissionError, match="terminal_evidence"):
        store.reconcile_unknown(
            attempt(), state="SUCCEEDED", outcome_evidence_sha256=terminal_proofs()[-1].proof_sha256
        )
    assert writes(database) == []


@pytest.mark.parametrize("state", ["RUNNING", "SUCCEEDED", "FAILED"])
def test_reconciliation_does_not_change_nonunknown_attempts(active, state):
    database, store = active
    store.admit_once(attempt())
    proofs = terminal_proofs(state) if state != "RUNNING" else ()
    set_receipt(database, state, proofs)
    database.statements.clear()
    with pytest.raises(CompositionAdmissionError, match="attempt_unknown_required"):
        store.reconcile_unknown(
            attempt(), state="SUCCEEDED", outcome_evidence_sha256=terminal_proofs()[-1].proof_sha256
        )
    assert writes(database) == []
