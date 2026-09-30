"""Source-free payload metadata bound atomically to the source EOF receipt."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from dpone.runtime.extraction_lifecycle import ExtractionLifecycleReceipt
from dpone.runtime.sinks.mssql_native_recovery import mutation_snapshot, restore_mutation
from dpone.runtime.sinks.mssql_target_mutation_plan import MssqlTargetMutationPlan
from dpone.type_system.source_sink.provenance import SourceRelationDialect


@dataclass(frozen=True)
class NativeCompletedPayload:
    """A completed source description; deliberately exposes no row iterator."""

    schema: tuple[tuple[str, str], ...]
    relation_schema: tuple[tuple[str, str], ...] | None
    relation_dialect: Any
    mssql_transaction_admission: Any
    mssql_target_mutation_plan: Any
    lifecycle: ExtractionLifecycleReceipt
    relation_metadata: Any = None
    target_projection: Any = None

    def require_completed_extraction(self) -> ExtractionLifecycleReceipt:
        return self.lifecycle


def completion_metadata(payload: Any, *, recovery_bindings: Any = None) -> dict[str, Any]:
    lifecycle = asdict(payload.require_completed_extraction())
    metadata = {
        "version": 1,
        "source_relation_uuid": getattr(getattr(payload, "artifact", None), "source_relation_uuid", None),
        "schema": [list(column) for column in payload.schema],
        "relation_schema": None
        if payload.relation_schema is None
        else [list(column) for column in payload.relation_schema],
        "relation_dialect": getattr(payload.relation_dialect, "value", payload.relation_dialect),
        "lifecycle": {
            key: value.isoformat() if isinstance(value, datetime) else value for key, value in lifecycle.items()
        },
        "mutation": mutation_snapshot(
            payload.mssql_target_mutation_plan
            or MssqlTargetMutationPlan.from_admission(payload.mssql_transaction_admission)
        ),
        "operation_key": payload.mssql_transaction_admission.operation.operation_key.hex(),
        "generation": payload.mssql_transaction_admission.operation.attempt.generation,
    }
    if recovery_bindings is not None:
        from dpone.contracts.mssql_native_recovery_authority import build_mssql_native_recovery_authority

        metadata["recovery_authority_v1"] = build_mssql_native_recovery_authority(
            payload.mssql_transaction_admission,
            recovery_bindings,
        )
    return metadata


def restore_payload(context: Any, admission: Any) -> NativeCompletedPayload:
    metadata = context.journal_factory().completed_metadata()
    operation = admission.operation
    if metadata.get("version") != 1 or operation is None:
        raise ValueError("mssql_native.completed_payload_required")
    if (
        metadata["operation_key"] != operation.operation_key.hex()
        or metadata["generation"] != operation.attempt.generation
    ):
        raise ValueError("mssql_native.completed_operation_changed")
    lifecycle = dict(metadata["lifecycle"])
    for key in ("extraction_started_at", "extraction_completed_at", "snapshot_acquired_at"):
        lifecycle[key] = datetime.fromisoformat(lifecycle[key]) if lifecycle.get(key) is not None else None
    receipt = ExtractionLifecycleReceipt(**lifecycle)
    if not receipt.complete:
        raise ValueError("mssql_native.completed_source_required")
    return NativeCompletedPayload(
        tuple(tuple(column) for column in metadata["schema"]),
        None if metadata["relation_schema"] is None else tuple(tuple(column) for column in metadata["relation_schema"]),
        SourceRelationDialect(metadata["relation_dialect"]) if metadata["relation_dialect"] else None,
        admission,
        restore_mutation(metadata["mutation"]),
        receipt,
    )


def authenticate_recovery_admission(context: Any, admission: Any) -> Any:
    """Bind a freshly claimed target admission to the source-EOF authority."""

    identity = getattr(context, "verification_identity", None)
    if identity is None:
        return admission
    journal = context.journal_factory()
    if journal.completed() is None:
        return admission
    metadata = journal.completed_metadata()
    authority = metadata.get("recovery_authority_v1")
    bindings_factory = getattr(context, "recovery_bindings", None)
    if authority is None or not callable(bindings_factory):
        raise ValueError("mssql_native.recovery_authority_required")
    from dpone.contracts.mssql_native_recovery_authority import restore_mssql_native_recovery_admission

    original = restore_mssql_native_recovery_admission(
        authority,
        expected_bindings=bindings_factory(admission),
    )
    if _admission_identity(original) != _admission_identity(admission):
        raise ValueError("mssql_native.recovery_operation_changed")
    return admission


def _admission_identity(admission: Any) -> tuple[Any, ...]:
    operation = getattr(admission, "operation", None)
    receipt = getattr(admission, "replay_receipt", None)
    if operation is not None:
        attempt = operation.attempt
        return (
            operation.operation_key,
            attempt.attempt_key,
            attempt.target_identity,
            attempt.route_fingerprint,
            attempt.generation,
            operation.scope_hash,
            attempt.request.load_id,
        )
    if receipt is not None:
        return (
            receipt.operation_key,
            receipt.attempt_key,
            receipt.target_identity,
            receipt.route_fingerprint,
            receipt.generation,
            receipt.scope_hash,
            receipt.load_id,
        )
    raise ValueError("mssql_native.recovery_operation_required")
