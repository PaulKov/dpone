"""Bounded original execution proofs inside the caller's pinned SQL transaction.

Proof hashes select immutable originals; they never replace complete issuance,
legacy decoding, outcome-state checks or the concrete MSSQL gate dependency.
Selection is valid before a RUNNING row is finalized. Historical callbacks
receive independently observed receipts and do not reobserve business activity.
"""

from __future__ import annotations

from collections.abc import Iterator

from dpone.adapters.composition_mssql_historical_gate import (
    _require_attempt,
    _rows_in,
    _table,
    require_historical_mssql_gate,
)
from dpone.adapters.composition_mssql_transaction import require_shared_transaction_in
from dpone.contracts.composition_activation import (
    CompositionActivationOccurrence,
    CompositionAdmissionError,
    require_digest,
)
from dpone.contracts.composition_attempt import (
    CompositionAttemptIdentity,
    CompositionAttemptReceipt,
    require_composition_attempt_scope,
)
from dpone.contracts.composition_persistence import decode_attempt_proof
from dpone.contracts.composition_proof import CompositionAttemptProof, CompositionProofAuthority
from dpone.ports.composition_sql import CompositionSqlContext

_KINDS = ("CLOSED_GATES", "QUIESCENCE", "OUTCOME")


def _issued_in(
    context: CompositionSqlContext,
    attempt: CompositionAttemptIdentity,
    *,
    expected_service_id: str,
    transaction_id: int,
) -> tuple[CompositionProofAuthority, ...]:
    rows = _rows_in(
        context,
        "SELECT TOP (8193) connector, LOWER(CONVERT(char(36), service_id)), principal_id "
        f"FROM {_table(context, 'issued_authorities')} WITH (HOLDLOCK) WHERE operation_key = ? "
        "ORDER BY connector, service_id, principal_id;",
        attempt.attempt_sha256,
        expected_service_id=expected_service_id,
        transaction_id=transaction_id,
    )
    if not 1 <= len(rows) <= 8192 or any(len(value) != 3 for value in rows):
        raise CompositionAdmissionError("terminal_issued_authorities")
    issued = tuple(sorted(CompositionProofAuthority(*value) for value in rows))
    if len(set(issued)) != len(issued):
        raise CompositionAdmissionError("terminal_issued_authorities")
    return issued


def _proofs_in(
    context: CompositionSqlContext, attempt: CompositionAttemptIdentity, kind: str, *, expected_service_id: str
) -> Iterator[CompositionAttemptProof]:
    transaction = require_shared_transaction_in(context, expected_service_id=expected_service_id)
    _require_attempt(attempt)
    if kind not in _KINDS:
        raise CompositionAdmissionError("attempt_proof_kind")
    prior: str | None = None
    while True:
        rows = _rows_in(
            context,
            "SELECT TOP (1) proof_sha256, operation_family, CASE WHEN DATALENGTH(proof_document) BETWEEN 1 AND 8388608 "
            f"THEN proof_document END FROM {_table(context, 'proofs')} WITH (HOLDLOCK) "
            "WHERE operation_key=? AND kind=? "
            + ("" if prior is None else "AND proof_sha256>? ")
            + "ORDER BY proof_sha256;",
            attempt.attempt_sha256,
            kind,
            *(() if prior is None else (prior,)),
            expected_service_id=expected_service_id,
            transaction_id=transaction,
        )
        if not rows:
            return
        if len(rows) != 1 or len(rows[0]) != 3 or rows[0][1] != "execution":
            raise CompositionAdmissionError("terminal_proof_identity")
        digest, _, document = rows[0]
        require_digest(digest)
        if prior is not None and digest <= prior:
            raise CompositionAdmissionError("terminal_proof_order")
        proof = decode_attempt_proof(document, digest).require_attempt(attempt)
        if proof.kind != kind:
            raise CompositionAdmissionError("attempt_proof_kind")
        prior = digest
        yield proof


def select_terminal_hashes_in(
    context: CompositionSqlContext, attempt: CompositionAttemptIdentity, outcome: str, *, expected_service_id: str
) -> tuple[str, str, str]:
    """Consume every original variant, retaining only the unambiguous selectors."""
    transaction = require_shared_transaction_in(context, expected_service_id=expected_service_id)
    _require_attempt(attempt)
    require_digest(outcome)
    issued = _issued_in(context, attempt, expected_service_id=expected_service_id, transaction_id=transaction)
    selected = []
    for kind in _KINDS:
        require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
        match = None
        for proof in _proofs_in(context, attempt, kind, expected_service_id=expected_service_id):
            require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
            if proof.authorities == issued and (kind != "OUTCOME" or proof.proof_sha256 == outcome):
                if match is not None:
                    raise CompositionAdmissionError("attempt_terminal_proof")
                match = proof.proof_sha256
        if match is None:
            raise CompositionAdmissionError("attempt_terminal_proof")
        selected.append(match)
    require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
    return selected[0], selected[1], selected[2]


def require_proofs_in(
    context: CompositionSqlContext,
    attempt: CompositionAttemptIdentity,
    services: set[tuple[str, str]],
    proof_hashes: tuple[str, str, str],
    *,
    expected_outcome_state: str,
    expected_service_id: str,
) -> None:
    """Validate originals for a proposed result, without requiring stored terminal state.

    The caller independently reopens owner/operation scope. A proposed finalization
    still has durable RUNNING state. A retained COMMIT_UNKNOWN triplet can be audited
    for reconciliation; this helper neither settles uncertainty nor releases ownership.
    """
    transaction = require_shared_transaction_in(context, expected_service_id=expected_service_id)
    _require_attempt(attempt)
    if type(proof_hashes) is not tuple or len(proof_hashes) != 3:
        raise CompositionAdmissionError("terminal_evidence")
    if expected_outcome_state not in ("SUCCEEDED", "FAILED", "COMMIT_UNKNOWN"):
        raise CompositionAdmissionError("terminal_outcome_state")
    issued = _issued_in(context, attempt, expected_service_id=expected_service_id, transaction_id=transaction)
    if {(value.connector, value.service_id) for value in issued} != services:
        raise CompositionAdmissionError("terminal_issued_authorities")
    selected = []
    for kind, digest in zip(_KINDS, proof_hashes, strict=True):
        require_digest(digest)
        rows = _rows_in(
            context,
            "SELECT TOP (2) operation_family, CASE WHEN DATALENGTH(proof_document) BETWEEN 1 AND 8388608 "
            f"THEN proof_document END FROM {_table(context, 'proofs')} WITH (HOLDLOCK) "
            "WHERE operation_key = ? AND kind = ? AND proof_sha256 = ?;",
            attempt.attempt_sha256,
            kind,
            digest,
            expected_service_id=expected_service_id,
            transaction_id=transaction,
        )
        if len(rows) != 1 or len(rows[0]) != 2 or rows[0][0] != "execution":
            raise CompositionAdmissionError("terminal_proof_identity")
        proof = decode_attempt_proof(rows[0][1], digest).require_attempt(attempt)
        if proof.kind != kind or proof.authorities != issued:
            raise CompositionAdmissionError("terminal_proof_authorities")
        if kind == "OUTCOME" and proof.outcome_state != expected_outcome_state:
            raise CompositionAdmissionError("terminal_outcome_state")
        selected.append(proof)
    if any(value.connector == "mssql" for value in issued):
        require_historical_mssql_gate(
            context, attempt, issued, selected[0], selected[1], expected_service_id=expected_service_id
        )
    require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)


def require_execution_terminal_in(
    context: CompositionSqlContext,
    occurrence: CompositionActivationOccurrence,
    receipt: CompositionAttemptReceipt,
    *,
    expected_service_id: str,
) -> None:
    """Execution-only callback; closure labels never release physical ownership."""
    transaction = require_shared_transaction_in(context, expected_service_id=expected_service_id)
    if type(occurrence) is not CompositionActivationOccurrence or type(receipt) is not CompositionAttemptReceipt:
        raise CompositionAdmissionError("terminal_attempt_identity")
    receipt.__post_init__()
    guards = require_composition_attempt_scope(occurrence, receipt.attempt)
    closed, quiescent, outcome = receipt.closed_gates_sha256, receipt.quiescence_sha256, receipt.outcome_evidence_sha256
    if closed is None or quiescent is None or outcome is None:
        raise CompositionAdmissionError("terminal_evidence")
    services = {
        (resource.connector, resource.service_id)
        for resource in occurrence.request.resources
        if resource.guard_id in guards
    }
    require_proofs_in(
        context,
        receipt.attempt,
        services,
        (closed, quiescent, outcome),
        expected_outcome_state=receipt.state,
        expected_service_id=expected_service_id,
    )
    require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
