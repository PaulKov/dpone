"""Bounded canonical UTF-8 documents for protected composition persistence.

SQL Server stores these bytes as VARBINARY rather than hashing NVARCHAR's
different UTF-16 encoding. A matching document is exact readback, not proof of
source authenticity or permission; callers still need a protected connection.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any
from uuid import UUID

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.composition_activation import (
    CompositionActivationOccurrence,
    CompositionActivationRequest,
    CompositionAdmissionError,
    CompositionOccurrenceContext,
    CompositionPhysicalResource,
    CompositionWorkloadAdmission,
    require_digest,
    require_ordered_unique,
    require_text,
)
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

if TYPE_CHECKING:
    pass


@dataclass(frozen=True, slots=True)
class CompositionAttemptIdentity:
    """One real scheduler attempt, pinned to the parent and selected workload.

    The parent request binds activation UUID, release, deployment and runtime
    context. Plan identity is separately supplied by the verified worker plan.
    ``try_number`` and ``map_index`` come from the actual Airflow task attempt.
    """

    activation_request_sha256: str
    workload_id: str
    constituent_id: str
    pack_sha256: str
    plan_sha256: str
    dag_run_id: str
    task_id: str
    try_number: int
    map_index: int
    guard_epochs: tuple[tuple[str, int], ...]

    def __post_init__(self) -> None:
        for value in (self.activation_request_sha256, self.pack_sha256, self.plan_sha256):
            require_digest(value)
        for value in (self.workload_id, self.dag_run_id, self.task_id):
            require_text(value)
        if self.constituent_id not in {"native", "standalone"}:
            raise CompositionAdmissionError("attempt_constituent")
        if (
            type(self.try_number) is not int
            or self.try_number < 1
            or type(self.map_index) is not int
            or (self.map_index < -1)
        ):
            raise CompositionAdmissionError("scheduler_attempt")
        if not isinstance(self.guard_epochs, tuple) or any(
            type(pair) is not tuple or len(pair) != 2 for pair in self.guard_epochs
        ):
            raise CompositionAdmissionError("attempt_guard_epochs")
        require_ordered_unique(tuple((guard for guard, _ in self.guard_epochs)))
        for guard, epoch in self.guard_epochs:
            require_digest(guard)
            if type(epoch) is not int or epoch <= 0:
                raise CompositionAdmissionError("attempt_guard_epochs")

    @property
    def attempt_sha256(self) -> str:
        """Canonical identity; changing a try creates a new fenced attempt."""
        return canonical_fingerprint({"schema": "dpone.composition-attempt.v1", **asdict(self)})


@dataclass(frozen=True, slots=True)
class CompositionAttemptReceipt:
    """Protected outcome with independent closed-gate and quiescence evidence.

    A quiescent COMMIT_UNKNOWN remains blocking: stopped sessions do not prove
    whether SQL effects committed. Only explicit reconciliation may resolve it.
    """

    attempt: CompositionAttemptIdentity
    state: str
    closed_gates_sha256: str | None = None
    quiescence_sha256: str | None = None
    outcome_evidence_sha256: str | None = None

    def __post_init__(self) -> None:
        self.attempt.__post_init__()
        if self.state not in {"RUNNING", "SUCCEEDED", "FAILED", "COMMIT_UNKNOWN"}:
            raise CompositionAdmissionError("attempt_state")
        evidence = (self.closed_gates_sha256, self.quiescence_sha256, self.outcome_evidence_sha256)
        if self.state in {"SUCCEEDED", "FAILED"} and any(value is None for value in evidence):
            raise CompositionAdmissionError("terminal_evidence")
        for value in evidence:
            if value is not None:
                require_digest(value)


def require_composition_attempt_scope(
    occurrence: CompositionActivationOccurrence, attempt: CompositionAttemptIdentity
) -> frozenset[str]:
    """Audit exact parent/workload/epoch scope without granting new admission."""
    occurrence.__post_init__()
    attempt.__post_init__()
    if attempt.activation_request_sha256 != occurrence.request.request_sha256:
        raise CompositionAdmissionError("attempt_parent")
    workloads = {row.workload_id: row for row in occurrence.request.workloads}
    workload = workloads.get(attempt.workload_id)
    if workload is None or (workload.constituent_id, workload.pack_sha256) != (
        attempt.constituent_id,
        attempt.pack_sha256,
    ):
        raise CompositionAdmissionError("attempt_workload")
    subjects = set(workload.write_subjects)
    guards = frozenset(
        row.guard_id for row in occurrence.request.resources if subjects.intersection(row.write_subjects)
    )
    expected = tuple(pair for pair in occurrence.receipt.guard_epochs if pair[0] in guards)
    if attempt.guard_epochs != expected:
        raise CompositionAdmissionError("attempt_guard_epochs")
    return guards


def require_composition_attempt_admission(
    occurrence: CompositionActivationOccurrence,
    attempt: CompositionAttemptIdentity,
    existing: tuple[CompositionAttemptReceipt, ...],
) -> None:
    """Reject stale parents, missing scopes, replays and overlapping unknown work.

    The caller must also bind ``plan_sha256`` to the actual verified runtime
    plan and supply the complete durable attempt ledger, including other
    occurrences holding any requested guard. No caller-provided subset is proof.
    """
    occurrence.require_state("ACTIVE")
    guards = require_composition_attempt_scope(occurrence, attempt)
    for receipt in existing:
        receipt.__post_init__()
        if receipt.attempt.attempt_sha256 == attempt.attempt_sha256:
            raise CompositionAdmissionError("attempt_replay")
        if receipt.state in {"RUNNING", "COMMIT_UNKNOWN"} and guards.intersection(
            (guard for guard, _ in receipt.attempt.guard_epochs)
        ):
            raise CompositionAdmissionError("attempt_conflict")


@dataclass(frozen=True, order=True, slots=True)
class CompositionProofAuthority:
    """One independently journaled principal on one protected backend service."""

    connector: str
    service_id: str
    principal_id: str

    def __post_init__(self) -> None:
        try:
            valid_service = str(UUID(self.service_id)) == self.service_id
            if self.connector == "mssql":
                valid_principal = re.fullmatch("mssql-sid:[0-9a-f]{32}", self.principal_id) is not None
            elif self.connector == "clickhouse":
                prefix, _, principal = self.principal_id.partition(":")
                valid_principal = prefix == "clickhouse-user" and str(UUID(principal)) == principal
            else:
                valid_principal = False
        except (ValueError, TypeError, AttributeError):
            valid_service = valid_principal = False
        if not valid_service or not valid_principal:
            raise CompositionAdmissionError("proof_authority")


def composition_attempt_epoch_subject(attempt: CompositionAttemptIdentity) -> str:
    """Bind the complete ordered attempt guard closure without changing its hash."""
    attempt.__post_init__()
    return canonical_fingerprint({"guard_epochs": attempt.guard_epochs})


@dataclass(frozen=True, slots=True)
class CompositionAttemptProof:
    """Canonical scope and producer-evidence subject for one protected proof."""

    kind: str
    attempt_sha256: str
    activation_request_sha256: str
    guard_epochs_sha256: str
    authorities: tuple[CompositionProofAuthority, ...]
    evidence_sha256: str
    outcome_state: str | None = None

    def __post_init__(self) -> None:
        if self.kind not in {"CLOSED_GATES", "QUIESCENCE", "OUTCOME"}:
            raise CompositionAdmissionError("proof_kind")
        if (
            self.kind == "OUTCOME"
            and self.outcome_state not in {"SUCCEEDED", "FAILED", "COMMIT_UNKNOWN"}
            or (self.kind != "OUTCOME" and self.outcome_state is not None)
        ):
            raise CompositionAdmissionError("proof_outcome_state")
        for value in (
            self.attempt_sha256,
            self.activation_request_sha256,
            self.guard_epochs_sha256,
            self.evidence_sha256,
        ):
            require_digest(value)
        if (
            type(self.authorities) is not tuple
            or not self.authorities
            or len(self.authorities) > 8192
            or any(type(value) is not CompositionProofAuthority for value in self.authorities)
        ):
            raise CompositionAdmissionError("proof_authority_closure")
        for authority in self.authorities:
            authority.__post_init__()
        if self.authorities != tuple(sorted(set(self.authorities))):
            raise CompositionAdmissionError("proof_authority_closure")

    def to_dict(self) -> dict[str, object]:
        """Return detached scope bytes for the protected proof producer."""
        return {"schema": "dpone.composition-attempt-proof.v1", **asdict(self)}

    @property
    def proof_sha256(self) -> str:
        return canonical_fingerprint(self.to_dict())

    def require_attempt(self, attempt: CompositionAttemptIdentity) -> CompositionAttemptProof:
        """Require exact attempt, parent and epoch closure before issuer comparison."""
        self.__post_init__()
        attempt.__post_init__()
        if (self.attempt_sha256, self.activation_request_sha256, self.guard_epochs_sha256) != (
            attempt.attempt_sha256,
            attempt.activation_request_sha256,
            composition_attempt_epoch_subject(attempt),
        ):
            raise CompositionAdmissionError("proof_attempt")
        return self


_MAX_DOCUMENT_BYTES = 8 * 1024 * 1024


def encode_physical_resource(resource: CompositionPhysicalResource) -> bytes:
    """Retain the legacy observation and write partition as exact original bytes.

    The caller validates the resource's own family. This shared serializer keeps
    the existing SQL adapter encoding without tightening legacy service values.
    """
    return canonical_json_bytes(asdict(resource))


def encode_activation_request(request: CompositionActivationRequest) -> bytes:
    """Return the canonical, detached bytes retained across catalog changes."""
    request.__post_init__()
    return _encode_document(request.to_dict())


def _encode_document(payload: dict[str, Any]) -> bytes:
    encoded = canonical_json_bytes(payload)
    if len(encoded) > _MAX_DOCUMENT_BYTES:
        raise CompositionAdmissionError("persistence_budget")
    return encoded


def decode_activation_request(document: bytes, expected_sha256: str) -> CompositionActivationRequest:
    """Reject changed, ambiguous, noncanonical or partial protected documents."""
    require_digest(expected_sha256)
    if type(document) is not bytes or len(document) > _MAX_DOCUMENT_BYTES:
        raise CompositionAdmissionError("persistence_document")
    try:
        body = strict_json_object(document)
        if not isinstance(body, dict) or set(body) != {
            "schema",
            "context",
            "source_subject_sha256",
            "workloads",
            "resources",
        }:
            raise CompositionAdmissionError("persistence_shape")
        if body["schema"] != "dpone.composition-activation-request.v1":
            raise CompositionAdmissionError("persistence_schema")
        request = CompositionActivationRequest(**activation_request_fields(body))
        if request.request_sha256 != expected_sha256 or encode_activation_request(request) != document:
            raise CompositionAdmissionError("persistence_identity")
        return request
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError, OverflowError):
        raise CompositionAdmissionError("persistence_readback") from None


def activation_request_fields(body: dict[str, Any]) -> dict[str, Any]:
    """Decode shared structural values after an explicit family checks its shape.

    This helper neither selects a document family nor verifies its hash or
    authority. Each closed reader must separately validate every outer member,
    construct its own request type and compare its original canonical bytes.
    """
    if any(type(body[key]) is not list or len(body[key]) > 8192 for key in ("workloads", "resources")):
        raise CompositionAdmissionError("persistence_closure")
    return {
        "context": CompositionOccurrenceContext(**body["context"]),
        "source_subject_sha256": body["source_subject_sha256"],
        "workloads": tuple(
            CompositionWorkloadAdmission(**dict(row, write_subjects=tuple(row["write_subjects"])))
            for row in body["workloads"]
        ),
        "resources": tuple(
            CompositionPhysicalResource(**dict(row, write_subjects=tuple(row["write_subjects"])))
            for row in body["resources"]
        ),
    }


def encode_attempt_identity(attempt: CompositionAttemptIdentity) -> bytes:
    """Persist existing attempt identity, including real scheduler coordinates."""
    attempt.__post_init__()
    return _encode_document({"schema": "dpone.composition-attempt.v1", **asdict(attempt)})


def decode_attempt_identity(document: bytes, expected_sha256: str) -> CompositionAttemptIdentity:
    """Reconstruct only canonical exact attempt bytes from protected storage."""
    require_digest(expected_sha256)
    try:
        body = _decode_document(document, "dpone.composition-attempt.v1")
        body["guard_epochs"] = tuple(tuple(pair) for pair in body["guard_epochs"])
        attempt = CompositionAttemptIdentity(**body)
        if attempt.attempt_sha256 != expected_sha256 or encode_attempt_identity(attempt) != document:
            raise CompositionAdmissionError("persistence_identity")
        return attempt
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError, OverflowError):
        raise CompositionAdmissionError("attempt_persistence_readback") from None


def encode_attempt_proof(proof: CompositionAttemptProof) -> bytes:
    """Retain the canonical producer scope; this function grants no authority."""
    proof.__post_init__()
    return _encode_document(proof.to_dict())


def decode_attempt_proof(document: bytes, expected_sha256: str) -> CompositionAttemptProof:
    """Reconstruct proof scope before independent issuance-journal comparison."""
    require_digest(expected_sha256)
    try:
        body = _decode_document(document, "dpone.composition-attempt-proof.v1")
        body["authorities"] = tuple(CompositionProofAuthority(**value) for value in body["authorities"])
        proof = CompositionAttemptProof(**body)
        if proof.proof_sha256 != expected_sha256 or encode_attempt_proof(proof) != document:
            raise CompositionAdmissionError("persistence_identity")
        return proof
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError, OverflowError):
        raise CompositionAdmissionError("proof_persistence_readback") from None


def _decode_document(document: bytes, schema: str) -> dict[str, Any]:
    if type(document) is not bytes or len(document) > _MAX_DOCUMENT_BYTES:
        raise CompositionAdmissionError("persistence_document")
    body = strict_json_object(document)
    if type(body) is not dict or body.pop("schema", None) != schema:
        raise CompositionAdmissionError("persistence_schema")
    return body


CompositionAttemptIdentity.__module__ = "dpone.contracts.composition_attempt"
CompositionAttemptReceipt.__module__ = "dpone.contracts.composition_attempt"
require_composition_attempt_scope.__module__ = "dpone.contracts.composition_attempt"
require_composition_attempt_admission.__module__ = "dpone.contracts.composition_attempt"
CompositionProofAuthority.__module__ = "dpone.contracts.composition_proof"
composition_attempt_epoch_subject.__module__ = "dpone.contracts.composition_proof"
CompositionAttemptProof.__module__ = "dpone.contracts.composition_proof"
