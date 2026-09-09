from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.file_artifacts import FileExportArtifact, PartitionedFileExportArtifact
from dpone.runtime.lineage.partition_checkpoint import (
    PartitionCheckpoint,
    PartitionCheckpointStatus,
    build_transfer_partition_id,
)
from dpone.runtime.native_transfer import NativeTransferRuntimeService
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult


class _PartialCommitError(RuntimeError):
    pass


class _PartialCommitStore:
    """Persist the first commit candidate, expose a durable decoy, then fail."""

    def __init__(
        self,
        *,
        second_durable_view: str,
        reconciliation_read_error: Exception | None = None,
        incomplete_write_error: Exception | None = None,
    ) -> None:
        self._latest: dict[str, PartitionCheckpoint] = {}
        self._failed_commit_batch = False
        self._second_durable_view = second_durable_view
        self._reconciliation_read_error = reconciliation_read_error
        self._incomplete_write_error = incomplete_write_error
        self._reconciliation_read_failed = False
        self.error = _PartialCommitError("password=must-not-be-persisted")

    def upsert(self, checkpoint: PartitionCheckpoint) -> None:
        self._latest[checkpoint.transfer_partition_id] = checkpoint

    def upsert_many(self, checkpoints: tuple[PartitionCheckpoint, ...]) -> None:
        is_commit_batch = checkpoints and all(
            checkpoint.status is PartitionCheckpointStatus.COMMITTED for checkpoint in checkpoints
        )
        if is_commit_batch and not self._failed_commit_batch:
            self._failed_commit_batch = True
            assert len(checkpoints) == 2
            self.upsert(checkpoints[0])
            self._set_second_durable_view(checkpoints[1])
            raise self.error
        is_incomplete_batch = checkpoints and all(
            _guard_phase(checkpoint) == "checkpoint_incomplete" for checkpoint in checkpoints
        )
        if is_incomplete_batch and self._incomplete_write_error is not None:
            raise self._incomplete_write_error
        for checkpoint in checkpoints:
            self.upsert(checkpoint)

    def list_latest(self) -> tuple[PartitionCheckpoint, ...]:
        if (
            self._failed_commit_batch
            and self._reconciliation_read_error is not None
            and not self._reconciliation_read_failed
        ):
            self._reconciliation_read_failed = True
            raise self._reconciliation_read_error
        return tuple(self._latest.values())

    def safe_to_skip(
        self,
        *,
        query_hash: str,
        schema_hash: str,
    ) -> tuple[PartitionCheckpoint, ...]:
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

    def _set_second_durable_view(self, candidate: PartitionCheckpoint) -> None:
        if self._second_durable_view == "not_persisted":
            return
        if self._second_durable_view == "mismatched":
            self.upsert(
                replace(
                    candidate,
                    diagnostics={"artifact_sha256": "sha256:mismatched"},
                )
            )
            return
        if self._second_durable_view == "stale":
            stale_at = datetime(2020, 1, 1, tzinfo=UTC)
            self.upsert(
                replace(
                    candidate,
                    started_at=stale_at,
                    completed_at=stale_at,
                )
            )
            return
        raise AssertionError(f"unexpected durable view: {self._second_durable_view}")


@pytest.mark.parametrize(
    "second_durable_view",
    [
        pytest.param("not_persisted", id="write-did-not-persist"),
        pytest.param("mismatched", id="durable-identity-mismatch"),
        pytest.param("stale", id="durable-prior-attempt"),
    ],
)
def test_partial_commit_batch_reconciles_only_exact_current_candidates(
    tmp_path: Path,
    second_durable_view: str,
) -> None:
    artifacts = tuple(_artifact(tmp_path, index) for index in range(2))
    store = _PartialCommitStore(second_durable_view=second_durable_view)
    service = NativeTransferRuntimeService(checkpoint_store=store)
    context = service.prepare_before_load(
        load_config=_load_config(),
        payload=LoadPayload(
            artifact=PartitionedFileExportArtifact(artifacts, ["id"]),
            schema=[("id", "int")],
        ),
        run_id="partial-checkpoint-batch",
    )
    partition_ids = tuple(artifact.transfer_partition_id for artifact in artifacts)
    context = replace(
        context,
        reactivated_partition_ids=frozenset(partition_ids),
    )
    service.before_target_mutation(context)

    with pytest.raises(_PartialCommitError) as raised:
        service.commit_checkpoints(
            context,
            LoadResult(inserted_rows=2, updated_rows=0, total_rows=2),
        )

    committed_after_error = set(context.terminal_state.committed_partition_ids)
    assert raised.value is store.error

    service.mark_failed(context, raised.value)

    latest = {checkpoint.transfer_partition_id: checkpoint for checkpoint in store.list_latest()}
    assert (
        committed_after_error,
        latest[partition_ids[0]].status,
        latest[partition_ids[1]].status,
    ) == (
        {partition_ids[0]},
        PartitionCheckpointStatus.COMMITTED,
        PartitionCheckpointStatus.FAILED,
    )
    assert latest[partition_ids[1]].diagnostics["error"] == "native_transfer_failed"
    assert latest[partition_ids[1]].diagnostics["target_commit_guard"]["phase"] == "checkpoint_incomplete"
    assert "must-not-be-persisted" not in repr(tuple(latest.values()))


def test_partial_commit_reconciliation_read_failure_preserves_primary_exception(
    tmp_path: Path,
) -> None:
    secondary = RuntimeError("reconciliation read failed")
    store = _PartialCommitStore(
        second_durable_view="not_persisted",
        reconciliation_read_error=secondary,
    )
    service, context, artifacts = _prepared_context(tmp_path, store)
    service.before_target_mutation(context)

    with pytest.raises(_PartialCommitError) as raised:
        service.commit_checkpoints(
            context,
            LoadResult(inserted_rows=2, updated_rows=0, total_rows=2),
        )

    assert raised.value is store.error
    latest = {checkpoint.transfer_partition_id: checkpoint for checkpoint in store.list_latest()}
    assert latest[artifacts[0].transfer_partition_id].status is PartitionCheckpointStatus.COMMITTED
    assert _guard_phase(latest[artifacts[1].transfer_partition_id]) == "armed"


def test_partial_commit_incomplete_marker_failure_preserves_primary_exception(
    tmp_path: Path,
) -> None:
    secondary = RuntimeError("incomplete marker write failed")
    store = _PartialCommitStore(
        second_durable_view="not_persisted",
        incomplete_write_error=secondary,
    )
    service, context, artifacts = _prepared_context(tmp_path, store)
    service.before_target_mutation(context)

    with pytest.raises(_PartialCommitError) as raised:
        service.commit_checkpoints(
            context,
            LoadResult(inserted_rows=2, updated_rows=0, total_rows=2),
        )

    assert raised.value is store.error
    latest = {checkpoint.transfer_partition_id: checkpoint for checkpoint in store.list_latest()}
    assert latest[artifacts[0].transfer_partition_id].status is PartitionCheckpointStatus.COMMITTED
    assert _guard_phase(latest[artifacts[1].transfer_partition_id]) == "armed"


def _prepared_context(
    tmp_path: Path,
    store: _PartialCommitStore,
) -> tuple[
    NativeTransferRuntimeService,
    object,
    tuple[FileExportArtifact, ...],
]:
    artifacts = tuple(_artifact(tmp_path, index) for index in range(2))
    service = NativeTransferRuntimeService(checkpoint_store=store)
    context = service.prepare_before_load(
        load_config=_load_config(),
        payload=LoadPayload(
            artifact=PartitionedFileExportArtifact(artifacts, ["id"]),
            schema=[("id", "int")],
        ),
        run_id="partial-checkpoint-secondary-failure",
    )
    return service, context, artifacts


def _guard_phase(checkpoint: PartitionCheckpoint) -> str | None:
    guard = checkpoint.diagnostics.get("target_commit_guard")
    return guard.get("phase") if isinstance(guard, dict) else None


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


def _artifact(tmp_path: Path, index: int) -> FileExportArtifact:
    file_path = tmp_path / f"partition-{index}.tsv"
    file_path.write_text(f"{index}\n", encoding="utf-8")
    artifact = FileExportArtifact(
        str(file_path),
        ["id"],
        format="mssql-delimited",
        estimated_rows=1,
    )
    bounds = {"index": index, "lower": index, "upper": index + 1}
    artifact.partition_bounds = bounds
    artifact.query_hash = "query-current"
    artifact.schema_hash = "schema-current"
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
    return artifact
