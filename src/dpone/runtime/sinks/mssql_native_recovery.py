"""Closed JSON representation of prepared native stages for source-free recovery."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from typing import Any

from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.consumed_payload_evidence import ConsumedPayloadEvidence, ConsumedPayloadPartEvidence
from dpone.runtime.extraction_lifecycle import ExtractionLifecycleReceipt
from dpone.runtime.sinks.mssql_native_staged_load import NativePreparedStage
from dpone.runtime.sinks.mssql_target_catalog_fingerprint import MssqlTargetCatalogExpectation
from dpone.runtime.sinks.mssql_target_mutation_plan import MssqlTargetMutationAction, MssqlTargetMutationPlan

_STAGE_FIELDS = (
    "schema",
    "table",
    "columns",
    "row_count",
    "database",
    "target_schema",
    "column_types",
    "target_column_types",
    "target_column_nullability",
    "target_column_collations",
    "wire_schema",
    "source_provenance_sha256",
    "typed_file_ingestion",
    "typed_file_row_hash_validation",
    "typed_file_deferred_native_evidence",
    "direct_native_staging",
    "typed_transport",
    "typed_batch_count",
)


def prepared_snapshot(prepared: NativePreparedStage, *, recovery: dict[str, Any]) -> dict[str, Any]:
    """Persist values only, without clients, credentials, callbacks or imports."""

    if prepared.target_projection is not None:
        raise ValueError("mssql_native.foreign_projection_not_admitted")
    operation = prepared.admission.operation
    if operation is None:
        raise ValueError("mssql_native.operation_required")
    lifecycle = asdict(prepared.source_lifecycle)
    lifecycle = {key: value.isoformat() if isinstance(value, datetime) else value for key, value in lifecycle.items()}
    plan = prepared.mutation_plan or MssqlTargetMutationPlan.from_admission(prepared.admission)
    stage = {name: getattr(prepared.staging, name) for name in _STAGE_FIELDS}
    stage["consumed_payload_evidence"] = asdict(prepared.staging.consumed_payload_evidence.require_complete())
    return {
        "version": 1,
        "operation_key": operation.operation_key.hex(),
        "generation": operation.attempt.generation,
        "target_identity": operation.attempt.target_identity.hex(),
        "load_id": operation.attempt.request.load_id,
        "stage": stage,
        "lifecycle": lifecycle,
        "mutation": mutation_snapshot(plan),
        "recovery": recovery,
        "interval": prepared.interval.to_dict() if prepared.interval is not None else None,
    }


def restore_prepared(
    snapshot: dict[str, Any],
    *,
    admission: Any,
    staging_manager: Any,
    interval: Any,
    resources: tuple[Any, ...],
) -> NativePreparedStage:
    """Rebind only an identical admitted operation, with a fresh owner epoch."""

    operation = admission.operation
    identity = operation if operation is not None else admission.replay_receipt
    if snapshot.get("version") != 1 or identity is None:
        raise ValueError("mssql_native.prepared_snapshot_invalid")
    generation = operation.attempt.generation if operation is not None else identity.generation
    target = operation.attempt.target_identity if operation is not None else identity.target_identity
    load_id = operation.attempt.request.load_id if operation is not None else identity.load_id
    if (
        snapshot["operation_key"] != identity.operation_key.hex()
        or snapshot["generation"] != generation
        or snapshot["target_identity"] != target.hex()
        or snapshot["load_id"] != load_id
        or snapshot["interval"] != (interval.to_dict() if interval is not None else None)
    ):
        raise ValueError("mssql_native.prepared_operation_changed")
    stage_values = dict(snapshot["stage"])
    evidence = dict(stage_values.pop("consumed_payload_evidence"))
    evidence["parts"] = tuple(ConsumedPayloadPartEvidence(**part) for part in evidence["parts"])
    stage_values["wire_schema"] = tuple(tuple(item) for item in stage_values["wire_schema"])
    stage_values["consumed_payload_evidence"] = ConsumedPayloadEvidence(**evidence).require_complete()
    stage = StagingTableArtifact(staging_manager=staging_manager, **stage_values)
    lifecycle = dict(snapshot["lifecycle"])
    for key in ("extraction_started_at", "extraction_completed_at", "snapshot_acquired_at"):
        lifecycle[key] = datetime.fromisoformat(lifecycle[key]) if lifecycle.get(key) is not None else None
    receipt = ExtractionLifecycleReceipt(**lifecycle)
    if not receipt.complete:
        raise ValueError("mssql_native.completed_source_required")
    plan = restore_mutation(snapshot["mutation"])
    return NativePreparedStage(stage, admission, receipt, plan, None, interval, resources)


def mutation_snapshot(plan: Any) -> dict[str, Any]:
    """The planner's exact authorized DDL and catalog fingerprints."""
    return {
        "target_identity": plan.target_identity.hex(),
        "target_database": plan.target_database,
        "target_schema": plan.target_schema,
        "target_table": plan.target_table,
        "actions": [asdict(action) for action in plan.actions],
        "expectations": [
            {
                "kind": item.kind,
                "before_sha256": item.before_sha256.hex(),
                "after_sha256": item.after_sha256.hex(),
                "representation": item.representation,
            }
            for item in plan.expectations
        ],
    }


def restore_mutation(snapshot: dict[str, Any]) -> MssqlTargetMutationPlan:
    values = dict(snapshot)
    values["target_identity"] = bytes.fromhex(values["target_identity"])
    values["actions"] = tuple(MssqlTargetMutationAction(**item) for item in values["actions"])
    values["expectations"] = tuple(
        MssqlTargetCatalogExpectation(
            item["kind"],
            bytes.fromhex(item["before_sha256"]),
            bytes.fromhex(item["after_sha256"]),
            item["representation"],
        )
        for item in values["expectations"]
    )
    return MssqlTargetMutationPlan(**values)
