from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifacts import FileExportArtifact, PartitionedFileExportArtifact
from dpone.runtime.lineage.partition_checkpoint import (
    PartitionCheckpoint,
    PartitionCheckpointStatus,
    build_transfer_partition_id,
)
from dpone.runtime.lineage.partition_checkpoint_store import JsonlPartitionCheckpointStore
from dpone.runtime.native_transfer import NativeTransferRuntimeService
from dpone.runtime.native_transfer_artifacts import PartitionedTransferPlanArtifact
from dpone.runtime.native_transfer_execution import NativeTransferResourcePolicy
from dpone.runtime.native_transfer_quality_scope import NativeTransferQualityScope
from dpone.runtime.native_transfer_row_authority import ROW_COUNT_AUTHORITY
from dpone.runtime.native_transfer_slicing import TransferSlice
from dpone.runtime.sinks.base import LoadPayload, LoadResult


def _cfg(strategy: LoadStrategy = LoadStrategy.INCREMENTAL_APPEND) -> LoadConfig:
    return LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="analytics",
        target_table="orders",
        load_strategy=strategy,
        options={"lineage": False, "native_transfer": {"require_artifact_checksum": True}},
    )


def _artifact(
    tmp_path: Path,
    index: int,
    *,
    body: str = "1\talpha\n",
    estimated_rows: object = 1,
    rows_exported: int | None = None,
    rows_loaded: int | None = None,
) -> FileExportArtifact:
    file_path = tmp_path / f"part_{index}.tsv"
    file_path.write_text(body, encoding="utf-8")
    artifact = FileExportArtifact(
        str(file_path),
        ["id", "name"],
        format="mssql-delimited",
        estimated_rows=estimated_rows,  # type: ignore[arg-type]
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
    if rows_loaded is not None:
        artifact.rows_loaded = rows_loaded
    return artifact


def _checkpoint(
    artifact: FileExportArtifact,
    status: PartitionCheckpointStatus,
    *,
    artifact_sha256: str | None,
    rows_loaded: int | None = None,
    with_authority: bool = False,
) -> PartitionCheckpoint:
    diagnostics: dict[str, object] = {}
    if artifact_sha256:
        diagnostics["artifact_sha256"] = artifact_sha256
    source_rows = getattr(artifact, "rows_exported", None)
    if (
        source_rows is None
        and isinstance(artifact.estimated_rows, int)
        and not isinstance(artifact.estimated_rows, bool)
    ):
        source_rows = artifact.estimated_rows
    target_rows = rows_loaded
    if target_rows is None:
        target_rows = getattr(artifact, "rows_loaded", None)
    if with_authority and isinstance(source_rows, int) and isinstance(target_rows, int):
        diagnostics["rows_loaded"] = target_rows
        diagnostics["row_count_authority"] = ROW_COUNT_AUTHORITY
    return PartitionCheckpoint(
        transfer_partition_id=artifact.transfer_partition_id,
        status=status,
        query_hash=artifact.query_hash,
        schema_hash=artifact.schema_hash,
        source_table=artifact.source_table,
        target_table=artifact.target_table,
        partition_bounds=artifact.partition_bounds,
        started_at=datetime(2026, 6, 9, tzinfo=UTC),
        completed_at=datetime(2026, 6, 9, 0, 1, tzinfo=UTC),
        rows_exported=source_rows,
        bytes_exported=Path(artifact.file_path).stat().st_size,
        diagnostics=diagnostics,
    )


def test_runtime_service_filters_committed_partitions_and_commits_retry_partitions(tmp_path: Path) -> None:
    committed = _artifact(tmp_path, 0)
    retry = _artifact(tmp_path, 1)
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    service = NativeTransferRuntimeService(checkpoint_store=store, artifact_dir=tmp_path / "reports")

    committed_checksum = service.checksum_service.checksum(committed)
    store.upsert(_checkpoint(committed, PartitionCheckpointStatus.COMMITTED, artifact_sha256=committed_checksum))
    payload = LoadPayload(
        artifact=PartitionedFileExportArtifact([committed, retry], ["id", "name"], max_workers=2),
        schema=[("id", "int"), ("name", "text")],
    )

    context = service.prepare_before_load(load_config=_cfg(), payload=payload, run_id="run-1")

    assert context.should_skip_load is False
    assert context.resume_plan.summary == {"skip": 1, "retry": 1}
    assert [Path(part.file_path).name for part in context.payload.artifact.partitions] == ["part_1.tsv"]
    assert store.summary()["exported"] == 1
    assert not list((tmp_path / "reports").glob("native_transfer_runtime_run-1*.json"))

    result = service.commit_checkpoints(
        context,
        LoadResult(inserted_rows=1, updated_rows=0, total_rows=2, staging_rows=1),
    )

    assert store.summary()["committed"] == 2
    assert not list((tmp_path / "reports").glob("native_transfer_runtime_run-1*.json"))
    service.publish_success_report(context, result)
    assert list((tmp_path / "reports").glob("native_transfer_runtime_run-1*.json"))


def test_runtime_service_skips_load_when_every_partition_is_already_committed(tmp_path: Path) -> None:
    parts = [_artifact(tmp_path, 0), _artifact(tmp_path, 1)]
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    service = NativeTransferRuntimeService(checkpoint_store=store, artifact_dir=tmp_path / "reports")
    for artifact in parts:
        store.upsert(
            _checkpoint(
                artifact,
                PartitionCheckpointStatus.COMMITTED,
                artifact_sha256=service.checksum_service.checksum(artifact),
            )
        )
    payload = LoadPayload(
        artifact=PartitionedFileExportArtifact(parts, ["id", "name"], max_workers=2),
        schema=[("id", "int"), ("name", "text")],
    )

    context = service.prepare_before_load(load_config=_cfg(LoadStrategy.FULL_REFRESH), payload=payload, run_id="run-2")

    assert context.should_skip_load is True
    assert context.skipped_load_result().reconciliation_metrics["native_transfer_resume"]["summary"] == {
        "skip": 2,
        "retry": 0,
    }
    assert not list((tmp_path / "reports").glob("native_transfer_runtime_run-2*.json"))


def test_native_quality_scope_combines_active_and_matching_committed_rows(tmp_path: Path) -> None:
    committed = _artifact(tmp_path, 0, estimated_rows=5, rows_exported=5, rows_loaded=5)
    active = _artifact(tmp_path, 1, estimated_rows=3, rows_exported=3)
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    service = NativeTransferRuntimeService(checkpoint_store=store)
    store.upsert(
        _checkpoint(
            committed,
            PartitionCheckpointStatus.COMMITTED,
            artifact_sha256=service.checksum_service.checksum(committed),
            with_authority=True,
        )
    )
    context = service.prepare_before_load(
        load_config=_cfg(),
        payload=LoadPayload(
            artifact=PartitionedFileExportArtifact([committed, active], ["id", "name"]),
            schema=[("id", "int"), ("name", "text")],
        ),
        run_id="logical-partial",
    )

    snapshots = NativeTransferQualityScope.from_context(context).projected_snapshots(staged_rows=3)

    assert snapshots.source.row_count == 8
    assert snapshots.target.row_count == 8
    assert snapshots.source.typed_hash is None
    assert snapshots.target.typed_hash is None
    summary = snapshots.scope_summary.to_jsonable()
    assert summary["planned_count"] == 2
    assert summary["active_count"] == 1
    assert summary["skipped_committed_count"] == 1
    assert summary["reactivated_count"] == 0
    assert summary["source_rows"] == 8
    assert summary["target_rows"] == 8
    assert str(summary["digest"]).startswith("sha256:")
    serialized = json.dumps(summary, sort_keys=True)
    assert committed.transfer_partition_id not in serialized
    assert active.transfer_partition_id not in serialized
    assert str(tmp_path) not in serialized
    assert "partition_bounds" not in serialized


def test_mixed_full_refresh_reactivation_counts_each_partition_once(tmp_path: Path) -> None:
    reactivated = _artifact(tmp_path, 0, estimated_rows=5, rows_exported=5)
    retry = _artifact(tmp_path, 1, estimated_rows=3, rows_exported=3)
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    service = NativeTransferRuntimeService(checkpoint_store=store)
    old_checkpoint = _checkpoint(
        reactivated,
        PartitionCheckpointStatus.COMMITTED,
        artifact_sha256=service.checksum_service.checksum(reactivated),
        rows_loaded=5,
        with_authority=True,
    )
    store.upsert(replace(old_checkpoint, rows_exported=101))
    context = service.prepare_before_load(
        load_config=_cfg(LoadStrategy.FULL_REFRESH),
        payload=LoadPayload(
            artifact=PartitionedFileExportArtifact([reactivated, retry], ["id", "name"]),
            schema=[("id", "int"), ("name", "text")],
        ),
        run_id="logical-full-refresh",
    )

    snapshots = NativeTransferQualityScope.from_context(context).projected_snapshots(staged_rows=8)

    # Full-refresh reactivates the skipped partition, so both active exports are counted
    # once on the source side and the staged target remains independent.
    assert snapshots.source.row_count == 8
    assert snapshots.target.row_count == 8
    summary = snapshots.scope_summary.to_jsonable()
    assert summary["planned_count"] == 2
    assert summary["active_count"] == 2
    assert summary["skipped_committed_count"] == 1
    assert summary["reactivated_count"] == 1


@pytest.mark.parametrize(
    "row_count",
    [
        pytest.param(None, id="missing"),
        pytest.param(True, id="boolean"),
        pytest.param("5", id="string"),
        pytest.param(-1, id="negative"),
    ],
)
def test_native_resume_quality_scope_fails_closed_for_invalid_checkpoint_rows(
    tmp_path: Path,
    row_count: object,
) -> None:
    artifact = _artifact(tmp_path, 0)
    service = NativeTransferRuntimeService()
    checkpoint = replace(
        _checkpoint(
            artifact,
            PartitionCheckpointStatus.COMMITTED,
            artifact_sha256=service.checksum_service.checksum(artifact),
        ),
        rows_exported=row_count,
    )
    store = _MemoryCheckpointStore((checkpoint,))
    service = NativeTransferRuntimeService(checkpoint_store=store)
    context = service.prepare_before_load(
        load_config=_cfg(),
        payload=LoadPayload(
            artifact=PartitionedFileExportArtifact([artifact], ["id", "name"]),
            schema=[("id", "int"), ("name", "text")],
        ),
        run_id="logical-invalid-resume",
    )

    snapshots = NativeTransferQualityScope.from_context(context).resume_snapshots()

    assert snapshots.source.row_count is None
    assert snapshots.target.row_count is None


@pytest.mark.parametrize(
    "row_count",
    [
        pytest.param(None, id="missing"),
        pytest.param(True, id="boolean"),
        pytest.param("5", id="string"),
        pytest.param(-1, id="negative"),
    ],
)
def test_native_projected_quality_scope_fails_closed_for_invalid_active_rows(
    tmp_path: Path,
    row_count: object,
) -> None:
    artifact = _artifact(tmp_path, 0, estimated_rows=row_count)
    store = _MemoryCheckpointStore(())
    service = NativeTransferRuntimeService(checkpoint_store=store)
    context = service.prepare_before_load(
        load_config=_cfg(),
        payload=LoadPayload(
            artifact=PartitionedFileExportArtifact([artifact], ["id", "name"]),
            schema=[("id", "int"), ("name", "text")],
        ),
        run_id="logical-invalid-active",
    )

    snapshots = NativeTransferQualityScope.from_context(context).projected_snapshots(staged_rows=0)

    assert snapshots.source.row_count is None
    assert snapshots.target.row_count == 0


def test_native_committed_quality_scope_rejects_stale_matching_identity(tmp_path: Path) -> None:
    artifact = _artifact(tmp_path, 0)
    service = NativeTransferRuntimeService()
    matching = _checkpoint(
        artifact,
        PartitionCheckpointStatus.COMMITTED,
        artifact_sha256=service.checksum_service.checksum(artifact),
    )
    planning_store = _MemoryCheckpointStore((matching,))
    service = NativeTransferRuntimeService(checkpoint_store=planning_store)
    context = service.prepare_before_load(
        load_config=_cfg(),
        payload=LoadPayload(
            artifact=PartitionedFileExportArtifact([artifact], ["id", "name"]),
            schema=[("id", "int"), ("name", "text")],
        ),
        run_id="logical-stale",
    )
    stale_store = _MemoryCheckpointStore((replace(matching, query_hash="stale-query"),))

    snapshots = NativeTransferQualityScope.from_context(context).committed_snapshots(stale_store)

    assert snapshots.source.row_count is None
    assert snapshots.target.row_count is None


def test_runtime_service_retries_when_committed_checksum_changed(tmp_path: Path) -> None:
    artifact = _artifact(tmp_path, 0)
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    service = NativeTransferRuntimeService(checkpoint_store=store, artifact_dir=tmp_path / "reports")
    store.upsert(_checkpoint(artifact, PartitionCheckpointStatus.COMMITTED, artifact_sha256="sha256:old"))
    payload = LoadPayload(
        artifact=PartitionedFileExportArtifact([artifact], ["id", "name"]),
        schema=[("id", "int"), ("name", "text")],
    )

    context = service.prepare_before_load(load_config=_cfg(), payload=payload, run_id="run-3")

    assert context.should_skip_load is False
    assert context.resume_plan.summary == {"skip": 0, "retry": 1}
    assert context.resume_plan.retry[0].reason == "artifact_checksum_changed"


def test_runtime_service_marks_retry_partitions_failed_on_load_error(tmp_path: Path) -> None:
    artifact = _artifact(tmp_path, 0)
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    service = NativeTransferRuntimeService(checkpoint_store=store, artifact_dir=tmp_path / "reports")
    payload = LoadPayload(
        artifact=PartitionedFileExportArtifact([artifact], ["id", "name"]),
        schema=[("id", "int"), ("name", "text")],
    )
    context = service.prepare_before_load(load_config=_cfg(), payload=payload, run_id="run-4")

    service.mark_failed(context, RuntimeError("password=do-not-record"))

    latest = store.list_latest()
    assert [checkpoint.status for checkpoint in latest] == [PartitionCheckpointStatus.FAILED]
    assert latest[0].completed_at is not None
    assert latest[0].diagnostics["error"] == "native_transfer_failed"
    assert "do-not-record" not in str(latest[0].diagnostics)


def test_runtime_service_never_downgrades_committed_partition_after_secondary_failure(tmp_path: Path) -> None:
    artifact = _artifact(tmp_path, 0)
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    service = NativeTransferRuntimeService(checkpoint_store=store, artifact_dir=tmp_path / "reports")
    context = service.prepare_before_load(
        load_config=_cfg(),
        payload=LoadPayload(
            artifact=PartitionedFileExportArtifact([artifact], ["id", "name"]),
            schema=[("id", "int"), ("name", "text")],
        ),
        run_id="run-terminal",
    )

    result = service.commit_checkpoints(
        context,
        LoadResult(inserted_rows=1, updated_rows=0, total_rows=1, staging_rows=1),
    )
    service.mark_failed(context, RuntimeError("token=do-not-record"))

    [latest] = store.list_latest()
    assert latest.status is PartitionCheckpointStatus.COMMITTED
    assert "do-not-record" not in str(latest.diagnostics)
    service.publish_success_report(context, result)
    assert list((tmp_path / "reports").glob("native_transfer_runtime_run-terminal*.json"))


def test_runtime_report_json_is_absent_when_authoritative_publish_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime import native_transfer_report

    artifact = _artifact(tmp_path, 0)
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    service = NativeTransferRuntimeService(checkpoint_store=store, artifact_dir=tmp_path / "reports")
    context = service.prepare_before_load(
        load_config=_cfg(),
        payload=LoadPayload(
            artifact=PartitionedFileExportArtifact([artifact], ["id", "name"]),
            schema=[("id", "int"), ("name", "text")],
        ),
        run_id="run-report-failure",
    )
    result = service.commit_checkpoints(
        context,
        LoadResult(inserted_rows=1, updated_rows=0, total_rows=1, staging_rows=1),
    )
    real_replace = native_transfer_report.os.replace

    def fail_json_replace(source: Path, destination: Path) -> None:
        if Path(destination).suffix == ".json":
            raise OSError("authoritative publish failed")
        real_replace(source, destination)

    monkeypatch.setattr(native_transfer_report.os, "replace", fail_json_replace)

    with pytest.raises(OSError, match="authoritative publish failed"):
        service.publish_success_report(context, result)

    assert not list((tmp_path / "reports").glob("native_transfer_runtime_run-report-failure*.json"))
    assert store.list_latest()[0].status is PartitionCheckpointStatus.COMMITTED


def test_compatibility_mark_committed_checkpoints_before_terminal_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime import native_transfer_report

    artifact = _artifact(tmp_path, 0)
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    reports = tmp_path / "reports"
    service = NativeTransferRuntimeService(
        checkpoint_store=store,
        artifact_dir=reports,
    )
    payload = LoadPayload(
        artifact=PartitionedFileExportArtifact([artifact], ["id", "name"]),
        schema=[("id", "int"), ("name", "text")],
    )
    context = service.prepare_before_load(
        load_config=_cfg(),
        payload=payload,
        run_id="run-compat-report-failure",
    )
    real_replace = native_transfer_report.os.replace

    def fail_json_replace(source: Path, destination: Path) -> None:
        if Path(destination).suffix == ".json":
            raise OSError("terminal report publish failed")
        real_replace(source, destination)

    monkeypatch.setattr(
        native_transfer_report.os,
        "replace",
        fail_json_replace,
    )

    with pytest.raises(OSError, match="terminal report publish failed"):
        service.mark_committed(
            context,
            LoadResult(
                inserted_rows=1,
                updated_rows=0,
                total_rows=1,
                staging_rows=1,
            ),
        )

    [checkpoint] = store.list_latest()
    assert checkpoint.status is PartitionCheckpointStatus.COMMITTED
    assert not list(reports.glob("native_transfer_runtime_*.json"))

    monkeypatch.setattr(native_transfer_report.os, "replace", real_replace)
    replay = service.prepare_before_load(
        load_config=_cfg(),
        payload=payload,
        run_id="run-compat-report-repair",
    )

    assert replay.should_skip_load is True
    service.publish_success_report(replay, replay.skipped_load_result())
    assert list(reports.glob("native_transfer_runtime_run-compat-report-repair*.json"))


def test_runtime_service_checkpoints_lazy_transfer_plan_slices(tmp_path: Path) -> None:
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    service = NativeTransferRuntimeService(checkpoint_store=store, artifact_dir=tmp_path / "reports")
    plan = _plan_artifact(tmp_path)
    context = service.prepare_before_load(
        load_config=_cfg(),
        payload=LoadPayload(artifact=plan, schema=[("id", "int"), ("name", "text")]),
        run_id="run-plan-1",
    )

    service.before_target_mutation(context)
    assert {item.diagnostics["target_commit_guard"]["phase"] for item in store.list_latest()} == {"armed"}
    loaded = context.payload.artifact.load_with(lambda artifact: artifact.estimated_rows)
    result = service.commit_checkpoints(
        context,
        LoadResult(inserted_rows=loaded, updated_rows=0, total_rows=loaded),
    )
    service.publish_success_report(context, result)

    assert store.summary()["committed"] == 2
    assert all(item.diagnostics["artifact_sha256"].startswith("sha256:") for item in store.list_latest())
    assert {item.diagnostics["target_commit_guard"]["phase"] for item in store.list_latest()} == {
        "checkpoint_committed"
    }

    second = service.prepare_before_load(
        load_config=_cfg(),
        payload=LoadPayload(artifact=_plan_artifact(tmp_path), schema=[("id", "int"), ("name", "text")]),
        run_id="run-plan-2",
    )

    assert second.should_skip_load is True
    assert second.resume_plan.summary == {"skip": 2, "retry": 0}
    assert not list((tmp_path / "reports").glob("native_transfer_runtime_run-plan-2*.json"))
    service.publish_success_report(second, second.skipped_load_result())
    assert list((tmp_path / "reports").glob("native_transfer_runtime_run-plan-2*.json"))


def _plan_artifact(tmp_path: Path) -> PartitionedTransferPlanArtifact:
    def export(item: TransferSlice) -> FileExportArtifact:
        path = tmp_path / f"plan_{item.partition_index}_{item.slice_index}.tsv"
        path.write_text("1\talpha\n", encoding="utf-8")
        return FileExportArtifact(
            str(path),
            ["id", "name"],
            format="mssql-delimited",
            estimated_rows=item.estimated_rows,
        )

    artifact = PartitionedTransferPlanArtifact(
        slices=(
            TransferSlice(0, 0, 0, 10, estimated_rows=1),
            TransferSlice(1, 0, 10, 20, estimated_rows=1),
        ),
        columns=["id", "name"],
        exporter=export,
        resource_policy=NativeTransferResourcePolicy(max_active_files=1),
        format="mssql-delimited",
    )
    artifact.query_hash = "query-a"
    artifact.schema_hash = "schema-a"
    artifact.source_table = "dbo.orders"
    artifact.target_table = "analytics.orders"
    artifact.strategy = "incremental_append"
    return artifact


class _MemoryCheckpointStore:
    """Preserve untrusted row values exactly for fail-closed boundary tests."""

    def __init__(self, checkpoints: tuple[PartitionCheckpoint, ...]) -> None:
        self._latest = {item.transfer_partition_id: item for item in checkpoints}

    def upsert(self, checkpoint: PartitionCheckpoint) -> None:
        self._latest[checkpoint.transfer_partition_id] = checkpoint

    def upsert_many(self, checkpoints: tuple[PartitionCheckpoint, ...]) -> None:
        for checkpoint in checkpoints:
            self.upsert(checkpoint)

    def list_latest(self) -> tuple[PartitionCheckpoint, ...]:
        return tuple(self._latest.values())

    def safe_to_skip(self, *, query_hash: str, schema_hash: str) -> tuple[PartitionCheckpoint, ...]:
        return tuple(
            checkpoint
            for checkpoint in self._latest.values()
            if checkpoint.can_skip(query_hash=query_hash, schema_hash=schema_hash)
        )

    def summary(self) -> dict[str, int]:
        counts = {status.value: 0 for status in PartitionCheckpointStatus}
        for checkpoint in self._latest.values():
            counts[checkpoint.status.value] += 1
        return counts
