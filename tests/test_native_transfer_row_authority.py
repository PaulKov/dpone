"""Independent native quality row authorities (SS-76)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.file_artifacts import FileExportArtifact, PartitionedFileExportArtifact
from dpone.runtime.lineage.partition_checkpoint import (
    PartitionCheckpoint,
    PartitionCheckpointStatus,
    build_transfer_partition_id,
)
from dpone.runtime.lineage.partition_checkpoint_store import JsonlPartitionCheckpointStore
from dpone.runtime.native_transfer import NativeTransferRuntimeService
from dpone.runtime.native_transfer_artifacts import PartitionedTransferPlanArtifact
from dpone.runtime.native_transfer_execution import NativeTransferResourcePolicy
from dpone.runtime.native_transfer_plan_runtime import NativeTransferPlanRuntimeAdapter
from dpone.runtime.native_transfer_quality_scope import NativeTransferQualityScope
from dpone.runtime.native_transfer_row_authority import ROW_COUNT_AUTHORITY
from dpone.runtime.native_transfer_slicing import TransferSlice
from dpone.runtime.sinks.load_payload import LoadPayload


@pytest.mark.parametrize(
    "bad_rows",
    [
        pytest.param(True, id="boolean"),
        pytest.param("2", id="string"),
        pytest.param(2.0, id="float"),
        pytest.param(-1, id="negative"),
        pytest.param({"rows": 2}, id="mapping"),
        pytest.param([2], id="list"),
        pytest.param(None, id="none"),
    ],
)
def test_plan_loader_rejects_malformed_target_row_count(tmp_path: Path, bad_rows: object) -> None:
    artifact = _plan(tmp_path, source_rows=10)

    with pytest.raises(RuntimeError, match="native_transfer_target_row_count_invalid"):
        artifact.load_with(lambda _file: bad_rows)  # type: ignore[arg-type, return-value]

    assert artifact.slice_evidence == []


def test_plan_records_independent_export_and_load_counts(tmp_path: Path) -> None:
    artifact = _plan(tmp_path, source_rows=10)

    loaded = artifact.load_with(lambda _file: 9)

    assert loaded == 9
    assert len(artifact.slice_evidence) == 1
    evidence = artifact.slice_evidence[0]
    assert evidence["rows_exported"] == 10
    assert evidence["rows_loaded"] == 9
    assert evidence["file_name"] == "slice-0-0.tsv"
    assert isinstance(evidence["bytes"], int) and evidence["bytes"] >= 0


def test_plan_quality_scope_keeps_source_and_target_independent(tmp_path: Path) -> None:
    artifact = _plan(tmp_path, source_rows=10)
    service = NativeTransferRuntimeService(
        checkpoint_store=JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    )
    context = service.prepare_before_load(
        load_config=_config(),
        payload=LoadPayload(artifact=artifact, schema=[("id", "int")]),
        run_id="independent-rows",
    )
    scope = service.quality_scope(context)
    assert scope is not None

    before = scope.projected_snapshots(staged_rows=9)
    assert before.source.row_count is None
    assert before.target.row_count == 9

    loaded = context.payload.artifact.load_with(lambda _file: 9)
    snapshots = scope.projected_snapshots(staged_rows=loaded)

    assert snapshots.source.row_count == 10
    assert snapshots.target.row_count == 9


def test_checkpoint_authority_marker_requires_both_counts(tmp_path: Path) -> None:
    artifact = _plan(tmp_path, source_rows=10)
    artifact.load_with(lambda _file: 10)
    checkpoints = NativeTransferPlanRuntimeAdapter.checkpoints(
        _config(),
        artifact,
        PartitionCheckpointStatus.COMMITTED,
        error=None,
        transition_at=datetime(2026, 7, 19, tzinfo=UTC),
    )

    assert len(checkpoints) == 1
    checkpoint = checkpoints[0]
    assert checkpoint.rows_exported == 10
    assert checkpoint.diagnostics["rows_loaded"] == 10
    assert checkpoint.diagnostics["row_count_authority"] == ROW_COUNT_AUTHORITY


def test_old_checkpoint_without_authority_cannot_certify_quality(tmp_path: Path) -> None:
    artifact = _file_artifact(tmp_path, index=0, rows_exported=5)
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    service = NativeTransferRuntimeService(checkpoint_store=store)
    store.upsert(
        PartitionCheckpoint(
            transfer_partition_id=artifact.transfer_partition_id,
            status=PartitionCheckpointStatus.COMMITTED,
            query_hash=artifact.query_hash,
            schema_hash=artifact.schema_hash,
            source_table=artifact.source_table,
            target_table=artifact.target_table,
            partition_bounds=artifact.partition_bounds,
            started_at=datetime(2026, 7, 19, tzinfo=UTC),
            completed_at=datetime(2026, 7, 19, 0, 1, tzinfo=UTC),
            rows_exported=5,
            bytes_exported=Path(artifact.file_path).stat().st_size,
            diagnostics={"artifact_sha256": service.checksum_service.checksum(artifact)},
        )
    )
    context = service.prepare_before_load(
        load_config=_config(),
        payload=LoadPayload(
            artifact=PartitionedFileExportArtifact([artifact], ["id"]),
            schema=[("id", "int")],
        ),
        run_id="legacy-checkpoint",
    )

    snapshots = NativeTransferQualityScope.from_context(context).resume_snapshots()

    assert snapshots.source.row_count is None
    assert snapshots.target.row_count is None


def test_trusted_checkpoints_sum_source_and_target_separately(tmp_path: Path) -> None:
    first = _file_artifact(tmp_path, index=0, rows_exported=10)
    second = _file_artifact(tmp_path, index=1, rows_exported=4)
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    service = NativeTransferRuntimeService(checkpoint_store=store)
    for artifact, loaded in ((first, 10), (second, 3)):
        store.upsert(
            PartitionCheckpoint(
                transfer_partition_id=artifact.transfer_partition_id,
                status=PartitionCheckpointStatus.COMMITTED,
                query_hash=artifact.query_hash,
                schema_hash=artifact.schema_hash,
                source_table=artifact.source_table,
                target_table=artifact.target_table,
                partition_bounds=artifact.partition_bounds,
                started_at=datetime(2026, 7, 19, tzinfo=UTC),
                completed_at=datetime(2026, 7, 19, 0, 1, tzinfo=UTC),
                rows_exported=artifact.rows_exported,
                bytes_exported=Path(artifact.file_path).stat().st_size,
                diagnostics={
                    "artifact_sha256": service.checksum_service.checksum(artifact),
                    "rows_loaded": loaded,
                    "row_count_authority": ROW_COUNT_AUTHORITY,
                },
            )
        )
    context = service.prepare_before_load(
        load_config=_config(),
        payload=LoadPayload(
            artifact=PartitionedFileExportArtifact([first, second], ["id"]),
            schema=[("id", "int")],
        ),
        run_id="trusted-split",
    )

    snapshots = NativeTransferQualityScope.from_context(context).resume_snapshots()

    assert snapshots.source.row_count == 14
    assert snapshots.target.row_count == 13


def test_file_partition_quality_ignores_estimated_rows(tmp_path: Path) -> None:
    artifact = _file_artifact(tmp_path, index=0, rows_exported=None, estimated_rows=100)
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    service = NativeTransferRuntimeService(checkpoint_store=store)
    context = service.prepare_before_load(
        load_config=_config(),
        payload=LoadPayload(
            artifact=PartitionedFileExportArtifact([artifact], ["id"]),
            schema=[("id", "int")],
        ),
        run_id="estimate-only",
    )

    snapshots = NativeTransferQualityScope.from_context(context).projected_snapshots(staged_rows=2)

    assert snapshots.source.row_count is None
    assert snapshots.target.row_count == 2


def _plan(tmp_path: Path, *, source_rows: int) -> PartitionedTransferPlanArtifact:
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
    artifact.query_hash = "query-authority"
    artifact.schema_hash = "schema-v1"
    artifact.source_table = "dbo.orders"
    artifact.target_table = "analytics.orders"
    artifact.strategy = "incremental_append"
    return artifact


def _file_artifact(
    tmp_path: Path,
    *,
    index: int,
    rows_exported: int | None,
    estimated_rows: int | None = None,
) -> FileExportArtifact:
    path = tmp_path / f"part_{index}.tsv"
    path.write_text("1\n", encoding="utf-8")
    artifact = FileExportArtifact(
        str(path),
        ["id"],
        format="mssql-delimited",
        estimated_rows=estimated_rows if estimated_rows is not None else rows_exported,
    )
    bounds = {"index": index, "lower": index, "upper": index + 1}
    artifact.partition_bounds = bounds
    artifact.query_hash = "query-a"
    artifact.schema_hash = "schema-a"
    artifact.source_table = "dbo.orders"
    artifact.target_table = "analytics.orders"
    artifact.strategy = "incremental_append"
    artifact.transfer_partition_id = build_transfer_partition_id(
        source_table=artifact.source_table,
        target_table=artifact.target_table,
        strategy=artifact.strategy,
        query_hash=artifact.query_hash,
        schema_hash=artifact.schema_hash,
        partition_bounds=bounds,
    )
    if rows_exported is not None:
        artifact.rows_exported = rows_exported
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
