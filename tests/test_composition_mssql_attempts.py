"""Concrete attempt-store replay, exact ledger and commit-uncertainty checks."""

from dataclasses import replace

import pytest

from dpone.adapters.composition_mssql_attempts import MssqlCompositionAttemptStore
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_persistence import encode_attempt_proof
from dpone.contracts.composition_proof import (
    CompositionAttemptProof,
    CompositionProofAuthority,
    composition_attempt_epoch_subject,
)
from tests.composition_mssql_gate_helpers import (
    SERVICE,
    Connection,
    Cursor,
    attempt,
    attempt_record,
    begin_steps,
    occurrence,
    parent_steps,
    partition_steps,
    receipt_steps,
)
from tests.test_composition_activation_contract import digest


def test_unavailable_database_does_not_admit_or_echo_driver_secret():
    def unavailable():
        raise RuntimeError("driver-password-secret")

    store = MssqlCompositionAttemptStore(unavailable, expected_service_id=SERVICE)
    with pytest.raises(CompositionAdmissionError) as caught:
        store.admit_once(attempt())
    assert "driver-password-secret" not in str(caught.value)


def test_missing_global_lock_blocks_attempt_before_any_insert():
    cursor = Cursor([("SET XACT_ABORT", []), ("sp_getapplock", [(-1,)])])
    connection = Connection(cursor)
    store = MssqlCompositionAttemptStore(lambda: connection, expected_service_id=SERVICE)
    with pytest.raises(CompositionAdmissionError, match="ledger_lock"):
        store.admit_once(attempt())
    assert connection.rolled_back
    assert not any("INSERT" in sql for sql, _ in cursor.calls)


def test_admission_reserves_full_attempt_then_fresh_reads_running():
    value = attempt()
    first = Cursor(
        begin_steps()
        + parent_steps()
        + [
            ("SELECT attempt_sha256", []),
            ("INSERT INTO [dpone_control].[composition_attempts]", []),
            ("INSERT INTO [dpone_control].[composition_attempt_domains]", []),
        ]
    )
    second = Cursor(begin_steps() + parent_steps() + receipt_steps())
    connections = [Connection(first), Connection(second)]
    pending = iter(connections)
    result = MssqlCompositionAttemptStore(lambda: next(pending), expected_service_id=SERVICE).admit_once(value)
    assert result.state == "RUNNING" and result.attempt == value
    assert all(connection.committed for connection in connections)
    assert not first.steps and not second.steps
    inserted = next(
        parameters for sql, parameters in first.calls if "INSERT INTO [dpone_control].[composition_attempts]" in sql
    )
    assert inserted[0] == value.attempt_sha256
    assert not any("LOGIN" in sql for sql, _ in first.calls)


@pytest.mark.parametrize("state", ["RUNNING", "COMMIT_UNKNOWN", "SUCCEEDED", "FAILED"])
def test_any_exact_replay_rejects_before_insert(state):
    from tests.test_composition_activation_contract import digest

    proofs = (
        tuple(digest(kind) for kind in ("closed", "quiescent", "outcome"))
        if state in {"SUCCEEDED", "FAILED"}
        else (None,) * 3
    )
    cursor = Cursor(
        begin_steps()
        + parent_steps()
        + [("SELECT attempt_sha256", [attempt_record(state=state, proofs=proofs)])]
        + partition_steps()
    )
    connection = Connection(cursor)
    with pytest.raises(CompositionAdmissionError, match="attempt_replay"):
        MssqlCompositionAttemptStore(lambda: connection, expected_service_id=SERVICE).admit_once(attempt())
    assert connection.rolled_back and not cursor.steps
    assert not any("INSERT" in sql for sql, _ in cursor.calls)


@pytest.mark.parametrize("state", ["RUNNING", "COMMIT_UNKNOWN"])
def test_entire_ledger_blocks_overlapping_distinct_attempt(state):
    cursor = Cursor(
        begin_steps() + parent_steps() + [("SELECT attempt_sha256", [attempt_record(state=state)])] + partition_steps()
    )
    with pytest.raises(CompositionAdmissionError, match="attempt_conflict"):
        MssqlCompositionAttemptStore(lambda: Connection(cursor), expected_service_id=SERVICE).admit_once(attempt(2))
    ledger_query = next(sql for sql, _ in cursor.calls if sql.startswith("SELECT attempt_sha256"))
    assert "WHERE" not in ledger_query


def test_retiring_parent_rejects_admission():
    cursor = Cursor(begin_steps() + parent_steps(occurrence("RETIRING")) + [("SELECT attempt_sha256", [])])
    with pytest.raises(CompositionAdmissionError, match="occurrence_state"):
        MssqlCompositionAttemptStore(lambda: Connection(cursor), expected_service_id=SERVICE).admit_once(attempt())
    assert not any("INSERT" in sql for sql, _ in cursor.calls)


def test_stale_epoch_rejects_before_ledger_mutation():
    value = attempt()
    stale = replace(value, guard_epochs=((value.guard_epochs[0][0], 2),))
    cursor = Cursor(begin_steps() + parent_steps())
    with pytest.raises(CompositionAdmissionError, match="attempt_guard_epochs"):
        MssqlCompositionAttemptStore(lambda: Connection(cursor), expected_service_id=SERVICE).admit_once(stale)
    assert not any("INSERT" in sql for sql, _ in cursor.calls)


def test_lost_admission_commit_ack_never_returns_execution_permit():
    cursor = Cursor(
        begin_steps() + parent_steps() + [("SELECT attempt_sha256", []), ("INSERT INTO", []), ("INSERT INTO", [])]
    )
    connection = Connection(cursor, commit_error=RuntimeError("password-driver-secret"))
    with pytest.raises(CompositionAdmissionError, match="control_operation_unknown") as caught:
        MssqlCompositionAttemptStore(lambda: connection, expected_service_id=SERVICE).admit_once(attempt())
    assert "password-driver-secret" not in str(caught.value)
    assert connection.rolled_back


def test_attempt_partition_must_match_every_exact_guard():
    steps = begin_steps() + parent_steps() + receipt_steps()
    steps[-1] = ("SELECT guard_id, fencing_epoch", [])
    with pytest.raises(CompositionAdmissionError, match="attempt_partition"):
        MssqlCompositionAttemptStore(lambda: Connection(Cursor(steps)), expected_service_id=SERVICE).read_exact(
            attempt()
        )


def terminal_proofs(outcome_state="SUCCEEDED", principal=None):
    value = attempt()
    authority = principal or CompositionProofAuthority("mssql", SERVICE, "mssql-sid:" + "61" * 16)
    return tuple(
        CompositionAttemptProof(
            kind,
            value.attempt_sha256,
            value.activation_request_sha256,
            composition_attempt_epoch_subject(value),
            (authority,),
            digest(kind),
            outcome_state if kind == "OUTCOME" else None,
        )
        for kind in ("CLOSED_GATES", "QUIESCENCE", "OUTCOME")
    )


def terminal_steps(proofs):
    authority = ("mssql", SERVICE, "mssql-sid:" + "61" * 16)
    selected = [("SELECT connector", [authority])]
    selected += [
        ("SELECT proof_sha256, proof_document", [(proof.proof_sha256, encode_attempt_proof(proof))]) for proof in proofs
    ]
    audited = [("SELECT connector", [authority])]
    audited += [
        (
            "SELECT activation_request_sha256",
            [(proof.activation_request_sha256, proof.guard_epochs_sha256, encode_attempt_proof(proof))],
        )
        for proof in proofs
    ]
    return selected + audited


def test_finalization_uses_actual_full_principal_proof_triplet():
    proofs = terminal_proofs()
    hashes = tuple(proof.proof_sha256 for proof in proofs)
    first = Cursor(
        begin_steps() + parent_steps() + receipt_steps() + terminal_steps(proofs) + [("UPDATE", [("SUCCEEDED",)])]
    )
    second = Cursor(
        begin_steps() + parent_steps() + receipt_steps(state="SUCCEEDED", proofs=hashes) + terminal_steps(proofs)[4:]
    )
    connections = [Connection(first), Connection(second)]
    pending = iter(connections)
    result = MssqlCompositionAttemptStore(lambda: next(pending), expected_service_id=SERVICE).finalize(
        attempt(), state="SUCCEEDED", outcome_evidence_sha256=hashes[-1]
    )
    assert result.state == "SUCCEEDED" and result.outcome_evidence_sha256 == hashes[-1]
    assert all(connection.committed for connection in connections)
    assert not first.steps and not second.steps


def test_valid_failed_outcome_cannot_be_promoted_to_success():
    proofs = terminal_proofs("FAILED")
    cursor = Cursor(begin_steps() + parent_steps() + receipt_steps() + terminal_steps(proofs))
    with pytest.raises(CompositionAdmissionError, match="terminal_outcome_state"):
        MssqlCompositionAttemptStore(lambda: Connection(cursor), expected_service_id=SERVICE).finalize(
            attempt(), state="SUCCEEDED", outcome_evidence_sha256=proofs[-1].proof_sha256
        )
    assert not any("UPDATE" in sql for sql, _ in cursor.calls)


def test_unrecorded_outcome_hash_cannot_terminalize_running_attempt():
    proofs = terminal_proofs()
    cursor = Cursor(begin_steps() + parent_steps() + receipt_steps() + terminal_steps(proofs)[:4])
    with pytest.raises(CompositionAdmissionError, match="attempt_terminal_proof"):
        MssqlCompositionAttemptStore(lambda: Connection(cursor), expected_service_id=SERVICE).finalize(
            attempt(), state="SUCCEEDED", outcome_evidence_sha256=digest("caller assertion")
        )
    assert not any("UPDATE" in sql for sql, _ in cursor.calls)


def test_foreign_principal_proof_does_not_cover_issued_journal():
    proofs = terminal_proofs(principal=CompositionProofAuthority("mssql", SERVICE, "mssql-sid:" + "ff" * 16))
    cursor = Cursor(begin_steps() + parent_steps() + receipt_steps() + terminal_steps(proofs)[:2])
    with pytest.raises(CompositionAdmissionError, match="attempt_terminal_proof"):
        MssqlCompositionAttemptStore(lambda: Connection(cursor), expected_service_id=SERVICE).finalize(
            attempt(), state="SUCCEEDED", outcome_evidence_sha256=proofs[-1].proof_sha256
        )
    assert not any("UPDATE" in sql for sql, _ in cursor.calls)


def test_prior_terminal_without_protected_triplet_cannot_free_guard_for_new_attempt():
    hashes = tuple(digest(kind) for kind in ("unbacked closed", "unbacked quiescence", "unbacked outcome"))
    cursor = Cursor(
        begin_steps()
        + parent_steps()
        + [("SELECT attempt_sha256", [attempt_record(state="SUCCEEDED", proofs=hashes)])]
        + partition_steps()
        + [
            ("SELECT connector", [("mssql", SERVICE, "mssql-sid:" + "61" * 16)]),
            ("SELECT activation_request_sha256", []),
        ]
    )
    with pytest.raises(CompositionAdmissionError, match="terminal_proof_identity"):
        MssqlCompositionAttemptStore(lambda: Connection(cursor), expected_service_id=SERVICE).admit_once(attempt(2))
    assert not any("INSERT" in sql for sql, _ in cursor.calls)


def test_unknown_reconciliation_is_explicit_and_requires_original_unknown_proof():
    old = terminal_proofs("COMMIT_UNKNOWN")
    new = terminal_proofs("SUCCEEDED")
    old_hashes = tuple(proof.proof_sha256 for proof in old)
    new_hashes = tuple(proof.proof_sha256 for proof in new)
    first = Cursor(
        begin_steps()
        + parent_steps()
        + receipt_steps(state="COMMIT_UNKNOWN", proofs=old_hashes)
        + terminal_steps(old)[4:]
        + terminal_steps(new)
        + [("UPDATE", [("SUCCEEDED",)])]
    )
    second = Cursor(
        begin_steps() + parent_steps() + receipt_steps(state="SUCCEEDED", proofs=new_hashes) + terminal_steps(new)[4:]
    )
    pending = iter([Connection(first), Connection(second)])
    result = MssqlCompositionAttemptStore(lambda: next(pending), expected_service_id=SERVICE).reconcile_unknown(
        attempt(), state="SUCCEEDED", outcome_evidence_sha256=new_hashes[-1]
    )
    assert result.state == "SUCCEEDED" and result.outcome_evidence_sha256 == new_hashes[-1]
    assert not first.steps and not second.steps


def test_prior_terminal_full_proof_allows_fresh_attempt_without_reissuing_old_attempt():
    proofs = terminal_proofs()
    hashes = tuple(proof.proof_sha256 for proof in proofs)
    first = Cursor(
        begin_steps()
        + parent_steps()
        + [("SELECT attempt_sha256", [attempt_record(state="SUCCEEDED", proofs=hashes)])]
        + partition_steps()
        + terminal_steps(proofs)[4:]
        + [("INSERT INTO", []), ("INSERT INTO", [])]
    )
    second = Cursor(begin_steps() + parent_steps() + receipt_steps(attempt(2)))
    pending = iter([Connection(first), Connection(second)])
    result = MssqlCompositionAttemptStore(lambda: next(pending), expected_service_id=SERVICE).admit_once(attempt(2))
    assert result.state == "RUNNING" and result.attempt.try_number == 2
    assert not first.steps and not second.steps


def test_unknown_cannot_be_resolved_by_ordinary_finalize():
    old = terminal_proofs("COMMIT_UNKNOWN")
    new = terminal_proofs("SUCCEEDED")
    cursor = Cursor(
        begin_steps()
        + parent_steps()
        + receipt_steps(state="COMMIT_UNKNOWN", proofs=tuple(proof.proof_sha256 for proof in old))
        + terminal_steps(new)
    )
    with pytest.raises(CompositionAdmissionError, match="attempt_terminal_replay"):
        MssqlCompositionAttemptStore(lambda: Connection(cursor), expected_service_id=SERVICE).finalize(
            attempt(), state="SUCCEEDED", outcome_evidence_sha256=new[-1].proof_sha256
        )
    assert not any("UPDATE" in sql for sql, _ in cursor.calls)


def test_reconciliation_requires_protected_original_unknown_outcome():
    cursor = Cursor(begin_steps() + parent_steps() + receipt_steps(state="COMMIT_UNKNOWN"))
    with pytest.raises(CompositionAdmissionError, match="terminal_evidence"):
        MssqlCompositionAttemptStore(lambda: Connection(cursor), expected_service_id=SERVICE).reconcile_unknown(
            attempt(), state="SUCCEEDED", outcome_evidence_sha256=terminal_proofs()[-1].proof_sha256
        )
    assert not any("UPDATE" in sql for sql, _ in cursor.calls)


@pytest.mark.parametrize("state", ["RUNNING", "SUCCEEDED", "FAILED"])
def test_reconciliation_does_not_change_nonunknown_attempts(state):
    proofs = tuple(proof.proof_sha256 for proof in terminal_proofs()) if state != "RUNNING" else (None, None, None)
    cursor = Cursor(begin_steps() + parent_steps() + receipt_steps(state=state, proofs=proofs))
    with pytest.raises(CompositionAdmissionError, match="attempt_unknown_required"):
        MssqlCompositionAttemptStore(lambda: Connection(cursor), expected_service_id=SERVICE).reconcile_unknown(
            attempt(), state="SUCCEEDED", outcome_evidence_sha256=terminal_proofs()[-1].proof_sha256
        )
    assert not any("UPDATE" in sql for sql, _ in cursor.calls)
