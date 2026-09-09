from __future__ import annotations

from pathlib import Path

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.lineage.partition_checkpoint_store import JsonlPartitionCheckpointStore
from dpone.runtime.native_transfer import NativeTransferRuntimeService
from dpone.runtime.native_transfer_artifacts import PartitionedTransferPlanArtifact
from dpone.runtime.native_transfer_execution import NativeTransferResourcePolicy
from dpone.runtime.native_transfer_slicing import TransferSlice
from dpone.runtime.sinks.load_payload import LoadPayload


def test_lazy_plan_quality_scope_uses_independent_export_and_load_rows(tmp_path: Path) -> None:
    artifact = _sparse_plan(tmp_path, source_rows=2)
    service = NativeTransferRuntimeService(
        checkpoint_store=JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    )
    context = service.prepare_before_load(
        load_config=_config(),
        payload=LoadPayload(artifact=artifact, schema=[("id", "int")]),
        run_id="sparse-range",
    )
    scope = service.quality_scope(context)
    assert scope is not None

    before_staging = scope.projected_snapshots(staged_rows=2)
    assert before_staging.source.row_count is None
    assert before_staging.target.row_count == 2

    loaded = context.payload.artifact.load_with(lambda _file: 2)
    snapshots = scope.projected_snapshots(staged_rows=loaded)

    assert snapshots.source.row_count == 2
    assert snapshots.target.row_count == 2
    assert snapshots.scope_summary.source_rows == 2
    assert snapshots.scope_summary.target_rows == 2


def test_lazy_plan_quality_scope_rejects_malformed_observed_rows(tmp_path: Path) -> None:
    artifact = _sparse_plan(tmp_path, source_rows=2)
    service = NativeTransferRuntimeService(
        checkpoint_store=JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    )
    context = service.prepare_before_load(
        load_config=_config(),
        payload=LoadPayload(artifact=artifact, schema=[("id", "int")]),
        run_id="malformed-evidence",
    )
    scope = service.quality_scope(context)
    assert scope is not None
    context.payload.artifact.slice_evidence.append(
        {
            "partition_index": 0,
            "slice_index": 0,
            "rows_exported": True,
            "rows_loaded": 2,
        }
    )

    snapshots = scope.projected_snapshots(staged_rows=2)

    assert snapshots.source.row_count is None
    assert snapshots.target.row_count == 2


def test_lazy_plan_quality_scope_keeps_mismatched_authorities(tmp_path: Path) -> None:
    artifact = _sparse_plan(tmp_path, source_rows=10)
    service = NativeTransferRuntimeService(
        checkpoint_store=JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    )
    context = service.prepare_before_load(
        load_config=_config(),
        payload=LoadPayload(artifact=artifact, schema=[("id", "int")]),
        run_id="mismatch-authorities",
    )
    scope = service.quality_scope(context)
    assert scope is not None
    loaded = context.payload.artifact.load_with(lambda _file: 9)
    snapshots = scope.projected_snapshots(staged_rows=loaded)

    assert snapshots.source.row_count == 10
    assert snapshots.target.row_count == 9


def _sparse_plan(tmp_path: Path, *, source_rows: int) -> PartitionedTransferPlanArtifact:
    def export(item: TransferSlice) -> FileExportArtifact:
        path = tmp_path / f"slice-{item.partition_index}-{item.slice_index}.tsv"
        path.write_text("\n".join(str(index) for index in range(source_rows)) + "\n", encoding="utf-8")
        artifact = FileExportArtifact(
            str(path),
            ["id"],
            format="mssql-delimited",
            estimated_rows=item.estimated_rows,
        )
        artifact.rows_exported = source_rows
        return artifact

    artifact = PartitionedTransferPlanArtifact(
        slices=(TransferSlice(0, 0, 0, 100, estimated_rows=100),),
        columns=["id"],
        exporter=export,
        resource_policy=NativeTransferResourcePolicy(max_active_files=1),
        estimated_rows=100,
    )
    artifact.query_hash = "query-sparse"
    artifact.schema_hash = "schema-v1"
    artifact.source_table = "dbo.orders"
    artifact.target_table = "analytics.orders"
    artifact.strategy = "incremental_append"
    return artifact


def _config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="dbo",
        source_table="orders",
        target_schema="analytics",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        options={"lineage": False, "native_transfer": {"enabled": True}},
    )
