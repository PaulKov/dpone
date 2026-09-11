"""Exact transfer identity retained before issuing a cross-database SQL fence.

These values grant no authority. The protected registrar must reopen the source
plan, parent workload and issued SID before retaining their canonical originals.
Lease expiry is renewal metadata, deliberately excluded from stable identity.
"""

from dataclasses import asdict, dataclass
from hashlib import sha256
from uuid import UUID

from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_attempt import CompositionAttemptIdentity
from dpone.contracts.composition_persistence import encode_attempt_identity
from dpone.contracts.dbt_relation_writes import DbtRelationWrite
from dpone.contracts.mssql_transaction_governance import MssqlGenericCommitReceipt, MssqlTransactionOperation
from dpone.contracts.strict_json import canonical_json_bytes


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

    def __post_init__(self) -> None:
        if type(self.attempt) is not CompositionAttemptIdentity or type(self.write) is not DbtRelationWrite:
            raise CompositionAdmissionError("transfer_binding")
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

    @property
    def document(self) -> bytes:
        """Canonical UTF-8 original compared byte-for-byte inside the SQL module."""
        self.__post_init__()
        return canonical_json_bytes(
            {
                "schema": "dpone.composition-mssql-operation-binding.v1",
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
