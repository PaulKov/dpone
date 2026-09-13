"""Exact transfer identity retained before issuing a cross-database SQL fence.

These values grant no authority. The protected registrar must reopen the source
plan, parent workload and issued SID before retaining their canonical originals.
Lease expiry is renewal metadata, deliberately excluded from stable identity.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from hashlib import sha256
from typing import Any
from uuid import UUID

from dpone.contracts.composition_attempt import CompositionAttemptIdentity
from dpone.contracts.composition_identity import CompositionAdmissionError, require_digest
from dpone.contracts.composition_persistence import decode_attempt_identity, encode_attempt_identity
from dpone.contracts.dbt_relation_writes import DbtRelationWrite, transfer_relation_write
from dpone.contracts.mssql_transaction_governance import (
    InvocationIdentity,
    MssqlAttemptRequest,
    MssqlGenericCommitReceipt,
    MssqlTransactionAttempt,
    MssqlTransactionOperation,
)
from dpone.contracts.source_physical_identity import SourcePhysicalIdentity
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object


def stable_operation_document(operation: MssqlTransactionOperation) -> bytes:
    """Encode every generic operation field except renewable lease expiration."""
    if type(operation) is not MssqlTransactionOperation:
        raise CompositionAdmissionError("transfer_operation")
    operation.__post_init__()
    operation.attempt.__post_init__()
    operation.attempt.request.__post_init__()
    operation.attempt.request.invocation.__post_init__()
    payload = asdict(operation)
    payload.pop("lease_expires_at_utc")
    payload["attempt"]["request"]["invocation"] = asdict(operation.attempt.request.invocation)
    for key in ("operation_key", "scope_hash", "owner_digest"):
        payload[key] = payload[key].hex()
    for key in ("target_identity", "route_fingerprint"):
        payload["attempt"]["request"][key] = payload["attempt"]["request"][key].hex()
    return canonical_json_bytes(payload)


@dataclass(frozen=True, slots=True)
class CompositionMssqlOperationBinding:
    """One protected original: attempt, invocation, physical target, plan and SID."""

    attempt: CompositionAttemptIdentity
    operation: MssqlTransactionOperation
    write: DbtRelationWrite
    mutation_plan_sha256: bytes
    issued_sid: bytes
    service_id: str
    control_database: str
    parent_document: bytes
    preplan_document_sha256: str | None = None

    def __post_init__(self) -> None:
        if type(self.attempt) is not CompositionAttemptIdentity or type(self.write) is not DbtRelationWrite:
            raise CompositionAdmissionError("transfer_binding")
        if self.preplan_document_sha256 is not None:
            require_digest(self.preplan_document_sha256)
        self.attempt.__post_init__()
        self.write.__post_init__()
        stable_operation_document(self.operation)
        request = self.operation.attempt.request
        if (
            self.write.connector != "mssql"
            or self.write.kind != "transfer"
            or self.write.role != "target"
            or (self.write.database, self.write.schema, self.write.relation)
            != (request.target_database, request.target_schema, request.target_table)
            or request.strategy != "full_refresh"
            or not self.operation.attempt.is_current_generation
        ):
            raise CompositionAdmissionError("transfer_write")
        if type(self.mutation_plan_sha256) is not bytes or len(self.mutation_plan_sha256) != 32:
            raise CompositionAdmissionError("transfer_mutation_plan")
        if type(self.issued_sid) is not bytes or len(self.issued_sid) != 16:
            raise CompositionAdmissionError("transfer_issued_sid")
        try:
            valid = str(UUID(self.service_id)) == self.service_id and UUID(self.service_id).int != 0
        except (ValueError, TypeError, AttributeError):
            valid = False
        if not valid:
            raise CompositionAdmissionError("control_service_id")
        if (
            type(self.control_database) is not str
            or not self.control_database
            or self.control_database == request.target_database
        ):
            raise CompositionAdmissionError("control_database")
        if (
            type(self.parent_document) is not bytes
            or not self.parent_document
            or "sha256:" + sha256(self.parent_document).hexdigest() != self.attempt.activation_request_sha256
        ):
            raise CompositionAdmissionError("transfer_parent_original")

    @classmethod
    def from_bytes(cls, document: bytes, attempt: CompositionAttemptIdentity) -> CompositionMssqlOperationBinding:
        """Decode only the original canonical registered binding, including operation."""
        try:
            body = strict_json_object(document)
            operation = decode_stable_operation_document(bytes.fromhex(body["operation_original"]))
            binding = cls(
                attempt,
                operation,
                DbtRelationWrite(**body["write"]),
                bytes.fromhex(body["mutation_plan_sha256"]),
                bytes.fromhex(body["issued_sid"]),
                body["service_id"],
                body["control_database"],
                bytes.fromhex(body["parent_original"]),
                body.get("preplan_document_sha256"),
            )
            if binding.document != document:
                raise ValueError("original")
            return binding
        except Exception:
            raise CompositionAdmissionError("transfer_binding_original") from None

    @property
    def document(self) -> bytes:
        """Canonical UTF-8 original compared byte-for-byte inside the SQL module."""
        self.__post_init__()
        return canonical_json_bytes(
            {
                "schema": "dpone.composition-mssql-operation-binding.v1"
                if self.preplan_document_sha256 is None
                else "dpone.composition-mssql-operation-binding.v2",
                **(
                    {}
                    if self.preplan_document_sha256 is None
                    else {"preplan_document_sha256": self.preplan_document_sha256}
                ),
                "attempt_original": encode_attempt_identity(self.attempt).hex(),
                "operation_original": stable_operation_document(self.operation).hex(),
                "write": asdict(self.write),
                "mutation_plan_sha256": self.mutation_plan_sha256.hex(),
                "issued_sid": self.issued_sid.hex(),
                "service_id": self.service_id,
                "control_database": self.control_database,
                "parent_original": self.parent_document.hex(),
            }
        )

    @property
    def digest(self) -> bytes:
        """Binary SHA256 of the exact original, without SQL text re-encoding."""
        return sha256(self.document).digest()

    def require_preplan(self) -> str:
        """Require the v2 trusted envelope; v1 remains readable without new authority."""
        if self.preplan_document_sha256 is None:
            raise CompositionAdmissionError("transfer_preplan_original")
        require_digest(self.preplan_document_sha256)
        return self.preplan_document_sha256

    def require_operation(self, operation: MssqlTransactionOperation, mutation_plan_sha256: bytes) -> None:
        """Reject changed invocation, coordinates, generation, owner, epoch or plan."""
        if stable_operation_document(operation) != stable_operation_document(self.operation):
            raise CompositionAdmissionError("transfer_operation_binding")
        if mutation_plan_sha256 != self.mutation_plan_sha256:
            raise CompositionAdmissionError("transfer_mutation_plan")

    def require_receipt(self, receipt: MssqlGenericCommitReceipt) -> None:
        """Replay consumes only a receipt for this entire previously bound write."""
        if type(receipt) is not MssqlGenericCommitReceipt:
            raise CompositionAdmissionError("transfer_receipt")
        receipt.__post_init__()
        operation = self.operation
        request = operation.attempt.request
        expected = (
            operation.receipt_id,
            operation.operation_key,
            operation.attempt.attempt_key,
            request.target_identity,
            operation.attempt.generation,
            operation.scope_hash,
            operation.epoch,
            operation.owner_digest,
            request.route_fingerprint,
            request.load_id,
            request.strategy,
            self.mutation_plan_sha256,
        )
        actual = (
            receipt.receipt_id,
            receipt.operation_key,
            receipt.attempt_key,
            receipt.target_identity,
            receipt.generation,
            receipt.scope_hash,
            receipt.operation_epoch,
            receipt.owner_digest,
            receipt.route_fingerprint,
            receipt.load_id,
            receipt.strategy,
            receipt.mutation_plan_sha256,
        )
        if actual != expected:
            raise CompositionAdmissionError("transfer_receipt_binding")


MAX_TRANSFER_PREPLAN_BYTES = 4 * 1024 * 1024
_PREPLAN_FIELDS = {
    "schema",
    "attempt_original",
    "operation_original",
    "write",
    "plan_sha256",
    "manifest_sha256",
    "preplan",
    "source_identity",
    "source_projection",
    "source_provenance_sha256",
    "target_identity",
    "route_fingerprint",
    "connection_observation",
}


def _closed_model(kind: Any, value: Any) -> Any:
    if type(value) is not dict or set(value) != {field.name for field in fields(kind)}:
        raise ValueError
    return kind(**value)


def decode_stable_operation_document(document: bytes) -> MssqlTransactionOperation:
    """Decode the same closed operation original for bindings and preplan envelopes.

    The roundtrip preserves canonical digest spelling and rejects renewable lease
    metadata. Callers retain their boundary-specific admission error vocabulary.
    """
    value = strict_json_object(document)
    raw, attempt = dict(value), dict(value["attempt"])
    request = dict(attempt["request"])
    request["invocation"] = _closed_model(InvocationIdentity, request["invocation"])
    for key in ("target_identity", "route_fingerprint"):
        request[key] = bytes.fromhex(request[key])
    attempt["request"] = _closed_model(MssqlAttemptRequest, request)
    raw["attempt"] = _closed_model(MssqlTransactionAttempt, attempt)
    for key in ("operation_key", "scope_hash", "owner_digest"):
        raw[key] = bytes.fromhex(raw[key])
    raw["lease_expires_at_utc"] = None
    result = _closed_model(MssqlTransactionOperation, raw)
    if stable_operation_document(result) != document:
        raise ValueError
    return result


def decode_transfer_preplan_subject(
    document: bytes, expected_sha256: bytes, *, attempt: CompositionAttemptIdentity
) -> tuple[dict[str, Any], DbtRelationWrite, MssqlTransactionOperation]:
    """Validate the bounded envelope's original identity, not runtime catalog models.

    Projection, schema-preplan and target reconstruction remain runtime work.
    Filesystem ownership and independently observed authority remain caller duties.
    """
    try:
        if (
            type(document) is not bytes
            or not 0 < len(document) <= MAX_TRANSFER_PREPLAN_BYTES
            or type(expected_sha256) is not bytes
            or len(expected_sha256) != 32
            or sha256(document).digest() != expected_sha256
        ):
            raise ValueError
        body = strict_json_object(document)
        if (
            set(body) != _PREPLAN_FIELDS
            or body["schema"] != "dpone.composition-transfer-preplan.v1"
            or canonical_json_bytes(body) != document
        ):
            raise ValueError
        original = canonical_json_bytes(body["attempt_original"])
        decode_attempt_identity(original, attempt.attempt_sha256)
        if original != encode_attempt_identity(attempt) or body["plan_sha256"] != attempt.plan_sha256:
            raise ValueError
        require_digest(body["manifest_sha256"])
        write = _closed_model(DbtRelationWrite, body["write"])
        if write.kind != "transfer" or write.connector != "mssql" or write.role != "target":
            raise ValueError
        identity = SourcePhysicalIdentity(**body["source_identity"])
        if identity.version not in {2, 3} or identity.to_dict() != body["source_identity"]:
            raise ValueError
        operation = decode_stable_operation_document(canonical_json_bytes(body["operation_original"]))
        if type(body["connection_observation"]) is not dict or not body["connection_observation"]:
            raise ValueError
        return body, write, operation
    except Exception:
        raise CompositionAdmissionError("transfer_preplan_original") from None


def require_transfer_manifest_write(
    attempt: CompositionAttemptIdentity, write: DbtRelationWrite, manifest: dict[str, Any]
) -> None:
    """Match the verified manifest to the exact transfer write before observation."""
    expected = transfer_relation_write(
        project_path=attempt.constituent_id,
        workflow_id=write.workflow_id,
        workload_id=attempt.workload_id,
        manifest=manifest,
    )
    if write != expected:
        raise CompositionAdmissionError("transfer_preplan_write")


def transfer_preplan_document(
    attempt: CompositionAttemptIdentity,
    operation: MssqlTransactionOperation,
    write: DbtRelationWrite,
    *,
    manifest_sha256: str,
    observations: dict[str, Any],
) -> bytes:
    """Encode the unchanged V1 envelope around detached runtime observations.

    Runtime owns observation/model serialization; this policy owns the versioned
    subject envelope. A document creates no authority or persistence permission.
    """
    subject_fields = {"schema", "attempt_original", "operation_original", "write", "plan_sha256", "manifest_sha256"}
    if set(observations) != _PREPLAN_FIELDS - subject_fields:
        raise ValueError("transfer_preplan_observation_fields")
    return canonical_json_bytes(
        {
            "schema": "dpone.composition-transfer-preplan.v1",
            "attempt_original": strict_json_object(encode_attempt_identity(attempt)),
            "operation_original": strict_json_object(stable_operation_document(operation)),
            "write": asdict(write),
            "plan_sha256": attempt.plan_sha256,
            "manifest_sha256": manifest_sha256,
            **observations,
        }
    )


def require_transfer_preplan_binding(
    body: dict[str, Any], binding: CompositionMssqlOperationBinding, mutation_plan_sha256: bytes
) -> None:
    """Compare retained subject originals with the protected registered binding."""
    if (
        canonical_json_bytes(body["attempt_original"]) != encode_attempt_identity(binding.attempt)
        or canonical_json_bytes(body["operation_original"]) != stable_operation_document(binding.operation)
        or body["write"] != asdict(binding.write)
        or mutation_plan_sha256 != binding.mutation_plan_sha256
    ):
        raise CompositionAdmissionError("transfer_preplan_binding")
