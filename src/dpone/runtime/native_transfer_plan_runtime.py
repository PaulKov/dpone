"""Runtime checkpoint helpers for lazy native transfer plans."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.runtime.lineage.partition_resume import PartitionResumePlan
    from dpone.runtime.native_transfer_slicing import TransferSlice


import hashlib
import json
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from dpone.runtime.lineage.partition_checkpoint import (
    PartitionCheckpoint,
    PartitionCheckpointStatus,
    build_transfer_partition_id,
)
from dpone.runtime.lineage.partition_resume import PlannedTransferPartition
from dpone.runtime.native_transfer_artifacts import PartitionedTransferPlanArtifact
from dpone.runtime.native_transfer_row_authority import (
    authority_diagnostics,
    canonical_non_negative_int,
)


class NativeTransferPlanRuntimeAdapter:
    """Build resume/checkpoint state for lazy transfer plan artifacts."""

    @staticmethod
    def planned_partitions(
        load_config: Any,
        artifact: PartitionedTransferPlanArtifact,
    ) -> tuple[PlannedTransferPartition, ...]:
        return tuple(_planned_slice(load_config, artifact, item) for item in artifact.slices)

    @staticmethod
    def active_slices(
        load_config: Any,
        artifact: PartitionedTransferPlanArtifact,
        resume_plan: PartitionResumePlan,
    ) -> tuple[TransferSlice, ...]:
        if not resume_plan.retry:
            return ()
        if load_config.load_strategy.value == "full_refresh" and resume_plan.skip:
            return tuple(artifact.slices)
        retry_ids = {decision.partition.transfer_partition_id for decision in resume_plan.retry}
        return tuple(
            item
            for item in artifact.slices
            if _planned_slice(load_config, artifact, item).transfer_partition_id in retry_ids
        )

    @staticmethod
    def with_slices(
        artifact: PartitionedTransferPlanArtifact,
        slices: tuple[TransferSlice, ...],
    ) -> PartitionedTransferPlanArtifact:
        clone = PartitionedTransferPlanArtifact(
            slices=slices,
            columns=artifact.columns,
            exporter=artifact.exporter,
            resource_policy=artifact.resource_policy,
            stream_exporter=artifact.stream_exporter,
            transport_plan=artifact.transport_plan,
            estimated_rows=sum(item.estimated_rows or 0 for item in slices) or None,
            format=artifact.format,
            bulk_text_codec=artifact.bulk_text_codec,
            bulk_wire_contract=artifact.bulk_wire_contract,
            transfer_store=artifact.transfer_store,
            reusable_objects=artifact.reusable_objects,
        )
        for key in ("query_hash", "schema_hash", "source_table", "target_table", "strategy"):
            if hasattr(artifact, key):
                setattr(clone, key, getattr(artifact, key))
        return clone

    @staticmethod
    def checkpoints(
        load_config: Any,
        artifact: PartitionedTransferPlanArtifact,
        status: PartitionCheckpointStatus,
        *,
        error: str | None,
        transition_at: datetime | None = None,
        target_commit_guard: Mapping[str, Any] | None = None,
    ) -> tuple[PartitionCheckpoint, ...]:
        evidence = _evidence_by_slice(artifact)
        checkpoint_at = transition_at or datetime.now(
            timezone.utc  # noqa: UP017 - mypy config still targets Python 3.10.
        )
        return tuple(
            _checkpoint(
                load_config,
                artifact,
                item,
                status,
                error=error,
                evidence=evidence,
                transition_at=checkpoint_at,
                target_commit_guard=target_commit_guard,
            )
            for item in artifact.slices
        )


def _planned_slice(
    load_config: Any,
    artifact: PartitionedTransferPlanArtifact,
    item: TransferSlice,
) -> PlannedTransferPartition:
    bounds = _slice_bounds(item)
    query_hash = native_transfer_query_hash(artifact, load_config)
    schema_hash = native_transfer_schema_hash(artifact, load_config)
    source_table = native_transfer_source_table(artifact, load_config)
    target_table = native_transfer_target_table(artifact, load_config)
    strategy = native_transfer_strategy(artifact, load_config)
    return PlannedTransferPartition(
        transfer_partition_id=build_transfer_partition_id(
            source_table=source_table,
            target_table=target_table,
            strategy=strategy,
            query_hash=query_hash,
            schema_hash=schema_hash,
            partition_bounds=bounds,
        ),
        source_table=source_table,
        target_table=target_table,
        strategy=strategy,
        query_hash=query_hash,
        schema_hash=schema_hash,
        partition_bounds=bounds,
        artifact_sha256=_logical_artifact_hash(artifact, bounds, query_hash, schema_hash),
    )


def _checkpoint(
    load_config: Any,
    artifact: PartitionedTransferPlanArtifact,
    item: TransferSlice,
    status: PartitionCheckpointStatus,
    *,
    error: str | None,
    evidence: dict[tuple[int, int], dict[str, Any]],
    transition_at: datetime,
    target_commit_guard: Mapping[str, Any] | None,
) -> PartitionCheckpoint:
    planned = _planned_slice(load_config, artifact, item)
    item_evidence = evidence.get((item.partition_index, item.slice_index), {})
    source_rows = canonical_non_negative_int(item_evidence.get("rows_exported"))
    target_rows = canonical_non_negative_int(item_evidence.get("rows_loaded"))
    diagnostics: dict[str, Any] = {"artifact_sha256": planned.artifact_sha256}
    if item_evidence:
        diagnostics["transport"] = item_evidence.get("transport")
        diagnostics["bytes"] = item_evidence.get("bytes")
    if error:
        diagnostics["error"] = error
    if target_commit_guard is not None:
        diagnostics["target_commit_guard"] = dict(target_commit_guard)
    diagnostics = authority_diagnostics(
        source_rows=source_rows,
        target_rows=target_rows,
        base=diagnostics,
    )
    return PartitionCheckpoint(
        transfer_partition_id=planned.transfer_partition_id,
        status=status,
        query_hash=planned.query_hash,
        schema_hash=planned.schema_hash,
        source_table=planned.source_table,
        target_table=planned.target_table,
        partition_bounds=planned.partition_bounds,
        started_at=transition_at,
        completed_at=_completed_at(status, transition_at),
        rows_exported=source_rows,
        bytes_exported=_bytes(item_evidence),
        diagnostics=diagnostics,
    )


def _evidence_by_slice(artifact: PartitionedTransferPlanArtifact) -> dict[tuple[int, int], dict[str, Any]]:
    return {
        (int(item.get("partition_index", -1)), int(item.get("slice_index", -1))): item
        for item in artifact.slice_evidence
        if isinstance(item, dict)
    }


def _bytes(item_evidence: Mapping[str, Any]) -> int | None:
    return canonical_non_negative_int(item_evidence.get("bytes"))


def _slice_bounds(item: TransferSlice) -> dict[str, Any]:
    slice_info = item.to_dict()
    return {
        "partition_index": slice_info["partition_index"],
        "slice_index": slice_info["slice_index"],
        "lower": slice_info["lower_bound"],
        "upper": slice_info["upper_bound"],
        "include_upper": slice_info["include_upper"],
        "is_null_partition": slice_info["is_null_partition"],
    }


def native_transfer_query_hash(artifact: Any, load_config: Any) -> str:
    return str(
        getattr(artifact, "query_hash", None)
        or hash_native_transfer_identity(f"{load_config.source_schema}.{load_config.source_table}")
    )


def native_transfer_schema_hash(artifact: Any, load_config: Any) -> str:
    return str(
        getattr(artifact, "schema_hash", None)
        or hash_native_transfer_identity(f"{load_config.target_schema}.{load_config.target_table}")
    )


def native_transfer_source_table(artifact: Any, load_config: Any) -> str:
    return str(getattr(artifact, "source_table", None) or f"{load_config.source_schema}.{load_config.source_table}")


def native_transfer_target_table(artifact: Any, load_config: Any) -> str:
    return str(getattr(artifact, "target_table", None) or f"{load_config.target_schema}.{load_config.target_table}")


def native_transfer_strategy(artifact: Any, load_config: Any) -> str:
    return str(getattr(artifact, "strategy", None) or load_config.load_strategy.value)


def _logical_artifact_hash(
    artifact: PartitionedTransferPlanArtifact,
    bounds: dict[str, Any],
    query_hash: str,
    schema_hash: str,
) -> str:
    payload = {
        "bounds": bounds,
        "format": artifact.format,
        "query_hash": query_hash,
        "schema_hash": schema_hash,
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return f"sha256:{hashlib.sha256(encoded.encode('utf-8')).hexdigest()}"


def hash_native_transfer_identity(value: str) -> str:
    """Return the canonical fallback digest for native-transfer identity."""

    return f"sha256:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _completed_at(
    status: PartitionCheckpointStatus,
    transition_at: datetime,
) -> datetime | None:
    if status in {PartitionCheckpointStatus.COMMITTED, PartitionCheckpointStatus.FAILED}:
        return transition_at
    return None


__all__ = [
    "NativeTransferPlanRuntimeAdapter",
    "hash_native_transfer_identity",
    "native_transfer_query_hash",
    "native_transfer_schema_hash",
    "native_transfer_source_table",
    "native_transfer_strategy",
    "native_transfer_target_table",
]
