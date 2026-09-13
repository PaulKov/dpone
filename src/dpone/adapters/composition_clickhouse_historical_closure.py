"""Reopen purpose closure originals without live observation or new authority.

Protected SQL custody establishes provenance; canonical retained observations
establish the exact historical subject. This reader neither retries a dispatch
nor rechecks mutable server activity after the original terminal observation.
"""

from __future__ import annotations

from dataclasses import dataclass

from dpone.adapters.composition_clickhouse_dispatch_queries import DispatchBinding, DispatchQueries, document_sha256
from dpone.adapters.composition_clickhouse_gate_queries import (
    ClickHouseGateBinding,
    ClickHouseGateQueries,
    ClickHouseLocalSupervisorObservation,
)
from dpone.adapters.composition_clickhouse_supervisor_enrollment import ClickHouseSupervisorEnrollment
from dpone.adapters.composition_clickhouse_supervisor_schema import require_clickhouse_supervisor_schema
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_persistence import (
    CompositionAttemptIdentity,
    CompositionAttemptProof,
    CompositionProofAuthority,
    composition_attempt_epoch_subject,
    encode_attempt_proof,
)
from dpone.contracts.composition_snapshot import SnapshotTarget
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object


def _require(value: bool) -> None:
    if not value:
        raise CompositionAdmissionError("clickhouse_historical_closure")


@dataclass(frozen=True, slots=True)
class PurposeClosureOriginals:
    """Exact singleton proofs sharing their retained CLOSED evidence original."""

    closed_gates: CompositionAttemptProof
    quiescence: CompositionAttemptProof
    evidence_document: bytes

    @property
    def proofs(self) -> tuple[CompositionAttemptProof, CompositionAttemptProof]:
        return self.closed_gates, self.quiescence


def _supervisor(q: ClickHouseGateQueries, document: bytes) -> ClickHouseLocalSupervisorObservation:
    body = strict_json_object(document)
    retained = body["supervisor"]
    _require(type(retained) is dict and set(retained) == {"evidence_sha256", "original"})
    original = retained["original"]
    fields = {
        name: original[name]
        for name in (
            "service_id",
            "database_uuid",
            "boot_id",
            "isolation_id",
            "enrollment_sha256",
            "network_namespace_id",
        )
    }
    observation = ClickHouseLocalSupervisorObservation(
        **fields,
        evidence_sha256=retained["evidence_sha256"],
        evidence_document=canonical_json_bytes(original),
    )
    q.check()
    require_clickhouse_supervisor_schema(q.ledger.cursor, q.ledger.schema)
    q.check()
    rows = q.rows(
        "SELECT TOP (2) LOWER(CONVERT(char(36),service_id)),LOWER(CONVERT(char(36),database_uuid)),"
        "LOWER(CONVERT(char(36),boot_id)),LOWER(CONVERT(char(36),isolation_id)),"
        "CASE WHEN DATALENGTH(enrollment_document) BETWEEN 1 AND 65536 THEN enrollment_document END "
        f"FROM {q.ledger.table('ch_supervisor_enrollments')} WITH (HOLDLOCK) WHERE enrollment_sha256=?;",
        observation.enrollment_sha256,
    )
    _require(len(rows) == 1 and len(rows[0]) == 5)
    enrollment = ClickHouseSupervisorEnrollment(observation.enrollment_sha256, rows[0][4])
    expected = enrollment.body
    _require(rows[0][:4] == tuple(expected[n] for n in ("service_id", "database_uuid", "boot_id", "isolation_id")))
    _require(
        all(
            canonical_json_bytes(original[n]) == canonical_json_bytes(expected[n])
            for n in ("service_id", "database_uuid", "boot_id", "isolation_id", "facts")
        )
    )
    _require(expected["target_enrollment_sha256"] == q.binding.target.enrollment_sha256)
    _require(observation.network_namespace_id == expected["facts"]["linux"]["network_namespace_id"])
    q.require_subject(observation)
    return observation


def read_purpose_closure_in(
    ledger: CompositionMssqlLedger,
    attempt: CompositionAttemptIdentity,
    target: SnapshotTarget,
    purpose: str,
) -> PurposeClosureOriginals:
    """Validate a complete retained purpose barrier in the supplied transaction.

    Absence, partial issuance, pending dispatch, nonlocal supervisor evidence,
    changed enrollment or proof bytes all reject. Historical terminal attempts
    use the dedicated read-only scope; no mutation scope is weakened. The
    returned proofs remain singleton purpose proofs, never aggregate authority.
    """
    try:
        _require(isinstance(ledger, CompositionMssqlLedger))
        _require(type(attempt) is CompositionAttemptIdentity and type(target) is SnapshotTarget)
        q = ClickHouseGateQueries(ledger, ClickHouseGateBinding(attempt, target, purpose))
        gate_id = q.binding_id()
        dq = DispatchQueries(ledger, DispatchBinding(attempt, target, gate_id))
        original_scope = dq.require_status_scope()
        result = _originals(q, dq, gate_id)
        _require(dq.require_status_scope() == original_scope)
        _require(_originals(q, dq, gate_id) == result)
        q.check()
        dq.check()
        return result
    except Exception:
        raise CompositionAdmissionError("clickhouse_historical_closure") from None


def _originals(q: ClickHouseGateQueries, dq: DispatchQueries, gate_id: str) -> PurposeClosureOriginals:
    _require(q.binding_id() == gate_id)
    document = q.event("CLOSED")
    _require(document is not None)
    assert document is not None
    observation = _supervisor(q, document)
    for phase in ("ENABLING", "READY", "CLOSING"):
        _require(
            q.event(phase) == canonical_json_bytes({"gate_key": q.binding.key, "phase": phase, "gate_id": gate_id})
        )
    dq.require_closing()
    links = dq.drained_links()
    expected = {
        "gate_key": q.binding.key,
        "gate_id": gate_id,
        "phase": "CLOSED",
        "supervisor": observation.to_dict(),
        "principal": {
            "user_id": gate_id,
            "username": q.binding.username,
            "host": None,
            "grants": (),
            "roles": (),
            "revoked": True,
        },
        "dispatch_terminals": links,
        "quiescence": "complete-dispatch-barrier-and-empty-server-work",
    }
    _require(document == canonical_json_bytes(expected))
    attempt = q.binding.attempt
    authorities = (CompositionProofAuthority("clickhouse", q.binding.target.service_id, "clickhouse-user:" + gate_id),)
    proofs = tuple(
        CompositionAttemptProof(
            kind,
            attempt.attempt_sha256,
            attempt.activation_request_sha256,
            composition_attempt_epoch_subject(attempt),
            authorities,
            document_sha256(document),
        )
        for kind in ("CLOSED_GATES", "QUIESCENCE")
    )
    for proof in proofs:
        rows = q.rows(
            "SELECT TOP (2) operation_family, CASE WHEN DATALENGTH(proof_document) BETWEEN 1 AND 8388608 "
            f"THEN proof_document END FROM {q.ledger.table('proofs')} WITH (HOLDLOCK) "
            "WHERE operation_key=? AND kind=? AND proof_sha256=?;",
            attempt.attempt_sha256,
            proof.kind,
            proof.proof_sha256,
        )
        _require(rows == (("execution", encode_attempt_proof(proof)),))
    pair = proofs[0], proofs[1]
    closure = dq.binding.closed_document(links, pair)
    _require(dq.phase("CLOSED") == (document_sha256(closure), closure))
    _require(dq.drained_links() == links)
    return PurposeClosureOriginals(*pair, document)
