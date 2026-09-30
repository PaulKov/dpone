"""Source-free payload metadata bound atomically to the source EOF receipt."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
from hashlib import sha256
from typing import Any

import dpone.contracts.clickhouse_raw_snapshot as raw_snapshot
from dpone.contracts.mssql_native_verification import native_completion_digests, ordered_native_receipts
from dpone.manifest.clickhouse_raw_snapshot_policy import native_source_snapshot_policy
from dpone.manifest.mssql_native_policy import native_window
from dpone.ports.mssql_native import NativeStageComplete, NativeVerificationIdentityV2
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
    artifact = getattr(payload, "artifact", None)
    profile = getattr(artifact, "raw_snapshot_profile", None)
    if profile is not None:
        binding, eof = getattr(artifact, "source_query_binding", None), getattr(artifact, "raw_snapshot_eof", None)
        if (
            not isinstance(binding, str)
            or not isinstance(profile, raw_snapshot.ClickHouseRawSnapshotProfileV1)
            or not isinstance(eof, raw_snapshot.ClickHouseRawSourceEofV1)
        ):
            raise ValueError("mssql_native.source_snapshot_extension_invalid")
        metadata["source_snapshot_v1"] = raw_snapshot.raw_snapshot_extension(binding, profile, eof)
    return metadata


def require_raw_snapshot_row_count(payload: Any, complete: NativeStageComplete) -> None:
    """Reject a source/stage count mismatch before preparation or publication."""
    artifact = getattr(payload, "artifact", None)
    if getattr(artifact, "raw_snapshot_profile", None) is None:
        return
    eof = getattr(artifact, "raw_snapshot_eof", None)
    if eof is None or type(complete.rows) is not int or eof.rows != complete.rows:
        raise ValueError("mssql_native.source_snapshot_row_count_mismatch")


def validate_raw_snapshot_recovery(
    config: Any, *, identity: NativeVerificationIdentityV2, projection: Mapping[str, Any], action: str
) -> None:
    """Authenticate raw EOF authority before runtime bindings or any target I/O.

    Pre-EOF settlement deliberately requires no source profile or source access.
    The caller retains the existing custody-aware permitted-action gate.
    """
    policy = native_source_snapshot_policy(config)
    binding = identity.plan.source_query_id
    metadata = projection.get("completion_metadata", {})
    has_extension = "source_snapshot_v1" in metadata
    raw_marker = binding.startswith("clickhouse.raw-query")
    if not raw_marker and not has_extension and policy.mode == "query_visible":
        return  # Historical opaque bindings and bytes retain their original reader.
    if raw_snapshot.raw_source_query_binding_version(binding) != 1 or policy.mode != "exact_raw_rows":
        raise ValueError("mssql_native.source_snapshot_binding_invalid")
    complete = projection.get("complete")
    if projection.get("phase") != "stage_complete":
        if has_extension or complete is not None or action not in {"inspect", "reconcile", "retire"}:
            raise ValueError("mssql_native.source_snapshot_eof_required")
        return
    if not isinstance(complete, dict):
        raise ValueError("mssql_native.source_snapshot_eof_required")
    if native_completion_digests(ordered_native_receipts(projection["chunks"]), metadata) != complete:
        raise ValueError("mssql_native.source_snapshot_completion_changed")
    profile, _eof = raw_snapshot.restore_raw_snapshot_extension(
        metadata.get("source_snapshot_v1"), binding=binding, completed_rows=complete["rows"]
    )
    window = native_window(config)
    window_value = None if window is None else (window.column, window.start.isoformat(), window.end.isoformat())
    authority = metadata.get("recovery_authority_v1", {})
    operation = authority.get("operation", {})
    route = operation.get("route_fingerprint")
    legacy = _legacy_digest((profile.relation_uuid, route, window_value))
    relation_schema = [[name, dtype] for name, dtype, _default in profile.ordered_schema]
    if (
        profile.replica_scope != policy.replica_scope
        or profile.window != window_value
        or _legacy_digest(window_value) != identity.plan.window_fingerprint
        or metadata.get("source_relation_uuid") != profile.relation_uuid
        or metadata.get("relation_schema") != relation_schema
        or raw_snapshot.raw_source_query_binding(legacy, profile) != binding
    ):
        raise ValueError("mssql_native.source_snapshot_binding_invalid")
    from dpone.runtime.etl.mssql_schema_preplan_support import schema_columns_sha256
    from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
    from dpone.runtime.schema_evolution_payload import columns_from_schema
    from dpone.type_system.source_sink.provenance import clickhouse_type_nullable

    columns = columns_from_schema([(name, dtype, clickhouse_type_nullable(dtype)) for name, dtype in relation_schema])
    try:
        wire = build_mssql_bcp_native_contract(schema=metadata["schema"], query=binding, target_format="mssql_native")
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("mssql_native.source_snapshot_schema_changed") from error
    if (
        schema_columns_sha256(columns).hex() != identity.plan.schema_fingerprint
        or wire.type_layout_hash != identity.plan.wire_fingerprint
    ):
        raise ValueError("mssql_native.source_snapshot_schema_changed")


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


def authenticate_recovery_admission(context: Any, admission: Any, *, config: Any = None) -> Any:
    """Bind a freshly claimed target admission to the source-EOF authority."""

    identity = getattr(context, "verification_identity", None)
    if identity is None:
        return admission
    journal = context.journal_factory()
    if config is not None and journal.data is not None:
        projection = journal.data
        validate_raw_snapshot_recovery(
            config,
            identity=identity,
            projection=projection,
            action="resume" if projection.get("phase") == "stage_complete" else "reconcile",
        )
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


def _legacy_digest(value: Any) -> str:
    """Reproduce the pre-existing source/window binding, including ASCII escapes."""
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()
