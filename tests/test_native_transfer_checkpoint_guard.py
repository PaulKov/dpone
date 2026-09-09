from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.file_artifacts import FileExportArtifact, PartitionedFileExportArtifact
from dpone.runtime.lineage.partition_checkpoint import (
    PartitionCheckpoint,
    PartitionCheckpointStatus,
    build_transfer_partition_id,
)
from dpone.runtime.lineage.partition_resume import NativeTransferTargetCommitGuardError
from dpone.runtime.native_transfer import NativeTransferRuntimeService
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult

_BATCH_ID = f"sha256:{'a' * 64}"
_SCHEMA_VERSION = "dpone.native_transfer.target_commit_guard.v1"
_MISSING = object()


def _guard_payload(phase: str) -> dict[str, str]:
    return {
        "schema_version": _SCHEMA_VERSION,
        "checkpoint_batch_id": _BATCH_ID,
        "phase": phase,
        "recovery": "manual_reconciliation_required",
        "strategy": "incremental_append",
    }


def test_surviving_partial_armed_batch_blocks_retry_before_target(tmp_path: Path) -> None:
    artifacts = tuple(_artifact(tmp_path, index) for index in range(2))
    store = _PartialArmStore()
    service = NativeTransferRuntimeService(checkpoint_store=store)
    context = _context(service, artifacts, run_id="guard-partial")
    target_calls = 0

    with pytest.raises(_ArmWriteError) as raised:
        service.before_target_mutation(context)
        target_calls += 1

    assert raised.value is store.error
    assert target_calls == 0
    assert any(_guard(checkpoint).get("phase") == "armed" for checkpoint in store.list_latest())

    retry_artifacts = tuple(_artifact(tmp_path, index, query_hash="query-b") for index in range(2))
    with pytest.raises(NativeTransferTargetCommitGuardError) as retry:
        _context(
            NativeTransferRuntimeService(checkpoint_store=store),
            retry_artifacts,
            run_id="guard-retry",
        )

    assert retry.value.code == "native_transfer_manual_reconciliation_required"


@pytest.mark.parametrize(
    ("status", "phase", "expected_code"),
    [
        (
            PartitionCheckpointStatus.EXPORTED,
            "armed",
            "native_transfer_manual_reconciliation_required",
        ),
        (
            PartitionCheckpointStatus.FAILED,
            "checkpoint_incomplete",
            "native_transfer_manual_reconciliation_required",
        ),
        (
            PartitionCheckpointStatus.FAILED,
            "target_outcome_unknown",
            "native_transfer_manual_reconciliation_required",
        ),
    ],
)
def test_unresolved_route_guards_fail_closed(
    tmp_path: Path,
    status: PartitionCheckpointStatus,
    phase: str,
    expected_code: str,
) -> None:
    artifact = _artifact(tmp_path, 0)
    store = _MemoryCheckpointStore((_checkpoint(artifact, status=status, guard=_guard_payload(phase)),))

    with pytest.raises(NativeTransferTargetCommitGuardError) as raised:
        _context(
            NativeTransferRuntimeService(checkpoint_store=store),
            (artifact,),
            run_id="guard-unresolved",
        )

    assert raised.value.code == expected_code
    assert str(raised.value) == expected_code


def test_route_guard_scan_ignores_query_schema_and_partition_identity_changes(
    tmp_path: Path,
) -> None:
    previous = _artifact(tmp_path, 0)
    store = _MemoryCheckpointStore(
        (
            _checkpoint(
                previous,
                status=PartitionCheckpointStatus.EXPORTED,
                guard=_guard_payload("armed"),
            ),
        )
    )
    changed = _artifact(
        tmp_path,
        7,
        query_hash="query-changed",
        schema_hash="schema-changed",
    )

    with pytest.raises(NativeTransferTargetCommitGuardError) as raised:
        _context(
            NativeTransferRuntimeService(checkpoint_store=store),
            (changed,),
            run_id="guard-identity-change",
        )

    assert raised.value.code == "native_transfer_manual_reconciliation_required"


def test_documented_guard_without_strategy_still_blocks_same_table_route(
    tmp_path: Path,
) -> None:
    artifact = _artifact(tmp_path, 0)
    guard = _guard_payload("armed")
    guard.pop("strategy")
    store = _MemoryCheckpointStore(
        (
            _checkpoint(
                artifact,
                status=PartitionCheckpointStatus.EXPORTED,
                guard=guard,
            ),
        )
    )

    with pytest.raises(NativeTransferTargetCommitGuardError) as raised:
        _context(
            NativeTransferRuntimeService(checkpoint_store=store),
            (artifact,),
            run_id="guard-documented-shape",
        )

    assert raised.value.code == "native_transfer_manual_reconciliation_required"


def test_guard_for_another_strategy_does_not_block_current_route(tmp_path: Path) -> None:
    artifact = _artifact(tmp_path, 0)
    guard = _guard_payload("armed")
    guard["strategy"] = "incremental_merge"
    store = _MemoryCheckpointStore(
        (
            _checkpoint(
                artifact,
                status=PartitionCheckpointStatus.EXPORTED,
                guard=guard,
            ),
        )
    )

    context = _context(
        NativeTransferRuntimeService(checkpoint_store=store),
        (artifact,),
        run_id="guard-other-strategy",
    )

    assert context.resume_plan.summary == {"skip": 0, "retry": 1}


@pytest.mark.parametrize(
    ("guard", "status"),
    [
        pytest.param(None, PartitionCheckpointStatus.EXPORTED, id="null"),
        pytest.param(
            {
                "schema_version": "dpone.native_transfer.target_commit_guard.v2",
                "checkpoint_batch_id": _BATCH_ID,
                "phase": "armed",
                "recovery": "manual_reconciliation_required",
            },
            PartitionCheckpointStatus.EXPORTED,
            id="unknown-schema",
        ),
        pytest.param(
            {
                "schema_version": _SCHEMA_VERSION,
                "checkpoint_batch_id": "sha256:not-a-digest",
                "phase": "armed",
                "recovery": "manual_reconciliation_required",
            },
            PartitionCheckpointStatus.EXPORTED,
            id="malformed-batch-id",
        ),
        pytest.param(
            {
                "schema_version": _SCHEMA_VERSION,
                "checkpoint_batch_id": _BATCH_ID,
                "phase": "unsupported",
                "recovery": "manual_reconciliation_required",
            },
            PartitionCheckpointStatus.EXPORTED,
            id="unsupported-phase",
        ),
        pytest.param(
            _guard_payload("checkpoint_committed"),
            PartitionCheckpointStatus.FAILED,
            id="status-phase-mismatch",
        ),
    ],
)
def test_malformed_or_unknown_guards_fail_closed_without_echoing_values(
    tmp_path: Path,
    guard: object,
    status: PartitionCheckpointStatus,
) -> None:
    artifact = _artifact(tmp_path, 0)
    checkpoint = _checkpoint(artifact, status=status, guard=guard)
    checkpoint.diagnostics["unsafe"] = "password=do-not-echo"
    store = _MemoryCheckpointStore((checkpoint,))

    with pytest.raises(NativeTransferTargetCommitGuardError) as raised:
        _context(
            NativeTransferRuntimeService(checkpoint_store=store),
            (artifact,),
            run_id="guard-invalid",
        )

    assert raised.value.code == "native_transfer_target_commit_guard_invalid"
    assert "do-not-echo" not in str(raised.value)


@pytest.mark.parametrize(
    ("status", "guard"),
    [
        pytest.param(PartitionCheckpointStatus.FAILED, _guard_payload("failed_before_target"), id="safe-failure"),
        pytest.param(PartitionCheckpointStatus.EXPORTED, _MISSING, id="legacy"),
    ],
)
def test_failed_before_target_and_legacy_checkpoints_remain_retryable(
    tmp_path: Path,
    status: PartitionCheckpointStatus,
    guard: object,
) -> None:
    artifact = _artifact(tmp_path, 0)
    store = _MemoryCheckpointStore((_checkpoint(artifact, status=status, guard=guard),))

    context = _context(
        NativeTransferRuntimeService(checkpoint_store=store),
        (artifact,),
        run_id="guard-safe-retry",
    )

    assert context.resume_plan.summary == {"skip": 0, "retry": 1}
    assert context.active_artifacts == (artifact,)


def test_guard_and_commit_batches_share_identity_and_use_one_timestamp_each(
    tmp_path: Path,
) -> None:
    armed_at = datetime(2026, 7, 18, 12, 0, tzinfo=UTC)
    committed_at = datetime(2026, 7, 18, 12, 1, tzinfo=UTC)
    transitions = iter((armed_at, committed_at))
    artifacts = tuple(_artifact(tmp_path, index) for index in range(2))
    store = _MemoryCheckpointStore(())
    service = NativeTransferRuntimeService(
        checkpoint_store=store,
        clock=lambda: next(transitions),
    )
    context = _context(service, artifacts, run_id="guard-timestamps")

    service.before_target_mutation(context)
    armed = store.list_latest()
    batch_ids = {_guard(checkpoint)["checkpoint_batch_id"] for checkpoint in armed}

    assert len(batch_ids) == 1
    assert all(checkpoint.started_at == armed_at for checkpoint in armed)
    assert all(checkpoint.completed_at is None for checkpoint in armed)

    result = service.record_target_success(
        context,
        LoadResult(inserted_rows=2, updated_rows=0, total_rows=2),
    )
    assert context.terminal_state.target_returned_success is True
    assert store.list_latest() == armed

    service.commit_checkpoints(context, result)
    committed = store.list_latest()

    assert {_guard(checkpoint)["checkpoint_batch_id"] for checkpoint in committed} == batch_ids
    assert {_guard(checkpoint)["phase"] for checkpoint in committed} == {"checkpoint_committed"}
    assert all(checkpoint.started_at == committed_at for checkpoint in committed)
    assert all(checkpoint.completed_at == committed_at for checkpoint in committed)


def test_target_exception_is_marked_unknown_with_safe_diagnostics(tmp_path: Path) -> None:
    artifact = _artifact(tmp_path, 0)
    store = _MemoryCheckpointStore(())
    service = NativeTransferRuntimeService(checkpoint_store=store)
    context = _context(service, (artifact,), run_id="guard-target-error")
    primary = RuntimeError("password=must-not-be-persisted")

    service.before_target_mutation(context)
    service.mark_failed(context, primary)

    [checkpoint] = store.list_latest()
    assert checkpoint.status is PartitionCheckpointStatus.FAILED
    assert _guard(checkpoint)["phase"] == "target_outcome_unknown"
    assert checkpoint.diagnostics["error"] == "COMMIT_UNKNOWN"
    assert "must-not-be-persisted" not in repr(checkpoint.diagnostics)


class _ArmWriteError(RuntimeError):
    pass


class _MemoryCheckpointStore:
    def __init__(self, checkpoints: tuple[PartitionCheckpoint, ...]) -> None:
        self._latest = {checkpoint.transfer_partition_id: checkpoint for checkpoint in checkpoints}

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


class _PartialArmStore(_MemoryCheckpointStore):
    def __init__(self) -> None:
        super().__init__(())
        self.error = _ArmWriteError("armed batch write failed")
        self._failed = False

    def upsert_many(self, checkpoints: tuple[PartitionCheckpoint, ...]) -> None:
        is_armed = bool(checkpoints) and all(_guard(checkpoint).get("phase") == "armed" for checkpoint in checkpoints)
        if is_armed and not self._failed:
            self._failed = True
            self.upsert(checkpoints[0])
            raise self.error
        super().upsert_many(checkpoints)


def _context(
    service: NativeTransferRuntimeService,
    artifacts: tuple[FileExportArtifact, ...],
    *,
    run_id: str,
) -> Any:
    return service.prepare_before_load(
        load_config=_load_config(),
        payload=LoadPayload(
            artifact=PartitionedFileExportArtifact(artifacts, ["id"]),
            schema=[("id", "int")],
        ),
        run_id=run_id,
    )


def _load_config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="analytics",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        options={
            "lineage": False,
            "native_transfer": {"require_artifact_checksum": True},
        },
    )


def _artifact(
    tmp_path: Path,
    index: int,
    *,
    query_hash: str = "query-a",
    schema_hash: str = "schema-a",
) -> FileExportArtifact:
    file_path = tmp_path / f"guard-partition-{index}-{query_hash}.tsv"
    file_path.write_text(f"{index}\n", encoding="utf-8")
    artifact = FileExportArtifact(
        str(file_path),
        ["id"],
        format="mssql-delimited",
        estimated_rows=1,
    )
    bounds = {"index": index, "lower": index, "upper": index + 1}
    artifact.partition_bounds = bounds
    artifact.query_hash = query_hash
    artifact.schema_hash = schema_hash
    artifact.source_table = "dbo.orders"
    artifact.target_table = "analytics.orders"
    artifact.strategy = "incremental_append"
    artifact.transfer_partition_id = build_transfer_partition_id(
        source_table=artifact.source_table,
        target_table=artifact.target_table,
        strategy=artifact.strategy,
        query_hash=query_hash,
        schema_hash=schema_hash,
        partition_bounds=bounds,
    )
    return artifact


def _checkpoint(
    artifact: FileExportArtifact,
    *,
    status: PartitionCheckpointStatus,
    guard: object,
) -> PartitionCheckpoint:
    diagnostics: dict[str, Any] = {
        "artifact_sha256": f"sha256:{'b' * 64}",
    }
    if guard is not _MISSING:
        diagnostics["target_commit_guard"] = guard
    return PartitionCheckpoint(
        transfer_partition_id=artifact.transfer_partition_id,
        status=status,
        query_hash=artifact.query_hash,
        schema_hash=artifact.schema_hash,
        source_table=artifact.source_table,
        target_table=artifact.target_table,
        partition_bounds=artifact.partition_bounds,
        rows_exported=artifact.estimated_rows,
        bytes_exported=Path(artifact.file_path).stat().st_size,
        diagnostics=diagnostics,
    )


def _guard(checkpoint: PartitionCheckpoint) -> dict[str, Any]:
    value = checkpoint.diagnostics.get("target_commit_guard")
    return value if isinstance(value, dict) else {}
