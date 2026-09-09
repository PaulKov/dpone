"""Runtime resume/checkpoint service for native partitioned transfers.

The service is intentionally connector-neutral. It only understands
``PartitionedFileExportArtifact`` plus the storage-neutral checkpoint protocol;
source and sink adapters keep owning vendor-specific export/load/finalization.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig
    from dpone.runtime.file_artifacts import FileExportArtifact
    from dpone.runtime.lineage.partition_checkpoint_store import PartitionCheckpointStore


from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.file_artifacts import PartitionedFileExportArtifact
from dpone.runtime.lineage.artifact_checksum import ArtifactChecksumService
from dpone.runtime.lineage.partition_checkpoint import build_transfer_partition_id
from dpone.runtime.lineage.partition_resume import PartitionResumePlan, PartitionResumePlanner, PlannedTransferPartition
from dpone.runtime.native_transfer_artifacts import PartitionedTransferPlanArtifact
from dpone.runtime.native_transfer_checkpoint_lifecycle import (
    NativeTransferCheckpointLifecycle,
    safe_failure_code,
)
from dpone.runtime.native_transfer_plan_runtime import (
    NativeTransferPlanRuntimeAdapter,
    native_transfer_query_hash,
    native_transfer_schema_hash,
    native_transfer_source_table,
    native_transfer_strategy,
    native_transfer_target_table,
)
from dpone.runtime.native_transfer_report import (
    publish_native_transfer_commit_unknown_report,
    publish_native_transfer_runtime_report,
)
from dpone.runtime.native_transfer_terminal_policy import (
    CommitUnknownError,
    NativeTransferTerminalState,
    PreCheckpointQualityScope,
    classify_native_transfer_commit_unknown,
    native_transfer_quality_scope,
    record_native_transfer_target_success,
)
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult


@dataclass(frozen=True)
class NativeTransferRuntimeContext:
    """Per-run native transfer checkpoint context."""

    enabled: bool
    load_config: LoadConfig
    run_id: str
    payload: LoadPayload
    resume_plan: PartitionResumePlan
    active_artifacts: tuple[Any, ...]
    checkpoint_store: PartitionCheckpointStore | None
    artifact_dir: Path | None = None
    reactivated_partition_ids: frozenset[str] = frozenset()
    terminal_state: NativeTransferTerminalState = field(
        default_factory=NativeTransferTerminalState,
        compare=False,
        repr=False,
    )

    @property
    def should_skip_load(self) -> bool:
        return self.enabled and bool(self.resume_plan.skip) and not self.active_artifacts

    def skipped_load_result(self) -> LoadResult:
        metrics = {"native_transfer_resume": self.runtime_metrics()}
        return LoadResult(inserted_rows=0, updated_rows=0, total_rows=0, staging_rows=0, reconciliation_metrics=metrics)

    def runtime_metrics(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "summary": self.resume_plan.summary,
            "decisions": [decision.to_dict() for decision in self.resume_plan.decisions],
        }


class NativeTransferRuntimeService:
    """Coordinate partition checkpoints around an existing source/sink load."""

    def __init__(
        self,
        *,
        checkpoint_store: PartitionCheckpointStore | None = None,
        planner: PartitionResumePlanner | None = None,
        checksum_service: ArtifactChecksumService | None = None,
        artifact_dir: str | Path | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.checkpoint_store = checkpoint_store
        self.planner = planner
        self.checksum_service = checksum_service or ArtifactChecksumService()
        self._checkpoint_lifecycle = NativeTransferCheckpointLifecycle(
            checkpoint_store=checkpoint_store,
            checksum_service=self.checksum_service,
            clock=clock,
        )
        self.artifact_dir = Path(artifact_dir) if artifact_dir else None
        self._schema_ready = False

    def prepare_before_load(
        self,
        *,
        load_config: LoadConfig,
        payload: LoadPayload,
        run_id: str,
    ) -> NativeTransferRuntimeContext:
        artifact = payload.artifact
        if not self._is_enabled(load_config, artifact):
            return NativeTransferRuntimeContext(
                enabled=False,
                load_config=load_config,
                run_id=run_id,
                payload=payload,
                resume_plan=PartitionResumePlan(()),
                active_artifacts=(),
                checkpoint_store=self.checkpoint_store,
                artifact_dir=self._artifact_dir(load_config),
            )

        self._ensure_schema()
        if isinstance(payload.artifact, PartitionedTransferPlanArtifact):
            return self._prepare_plan_context(load_config=load_config, payload=payload, run_id=run_id)
        artifact = cast(PartitionedFileExportArtifact, payload.artifact)
        partitions = tuple(artifact.partitions)
        planned = tuple(self._planned_partition(load_config, item, index) for index, item in enumerate(partitions))
        require_checksum = bool((load_config.options or {}).get("native_transfer", {}).get("require_artifact_checksum"))
        planner = self.planner or PartitionResumePlanner(require_artifact_checksum=require_checksum)
        resume_plan = planner.plan(planned, self.checkpoint_store)  # type: ignore[arg-type]
        active_artifacts = self._active_artifacts(load_config, partitions, resume_plan)
        filtered_payload = payload
        if len(active_artifacts) != len(partitions):
            skipped = tuple(item for item in partitions if item not in active_artifacts)
            for skipped_artifact in skipped:
                skipped_artifact.cleanup()
            filtered_payload = payload.rebind(
                artifact=PartitionedFileExportArtifact(
                    active_artifacts,
                    artifact.columns,
                    max_workers=artifact.max_workers,
                    estimated_rows=sum(item.estimated_rows or 0 for item in active_artifacts) or None,
                ),
                schema=payload.schema,
            )
        self._checkpoint_lifecycle.mark_exported(active_artifacts)
        context = NativeTransferRuntimeContext(
            enabled=True,
            load_config=load_config,
            run_id=run_id,
            payload=filtered_payload,
            resume_plan=resume_plan,
            active_artifacts=active_artifacts,
            checkpoint_store=self.checkpoint_store,
            artifact_dir=self._artifact_dir(load_config),
            reactivated_partition_ids=_reactivated_partition_ids(load_config, resume_plan),
        )
        return context

    def before_target_mutation(
        self,
        context: NativeTransferRuntimeContext,
    ) -> None:
        """Durably arm every active partition before the sink can be invoked."""

        if not context.enabled or not context.active_artifacts:
            return
        if not context.terminal_state.guard_armed:
            self._checkpoint_lifecycle.arm(context)
        context.terminal_state.target_invocation_started = True

    def record_target_success(self, context: NativeTransferRuntimeContext, load_result: LoadResult) -> LoadResult:
        """Record process-local target success without advancing checkpoints."""

        if not context.enabled:
            return load_result
        if context.active_artifacts and not context.terminal_state.guard_armed:
            self.before_target_mutation(context)
        record_native_transfer_target_success(context, load_result)
        return self._with_runtime_metrics(load_result, context)

    def commit_checkpoints(self, context: NativeTransferRuntimeContext, load_result: LoadResult) -> LoadResult:
        """Persist target acceptance before terminal passed evidence is published."""

        if not context.enabled:
            return load_result
        load_result = self.record_target_success(context, load_result)
        committed = self._checkpoint_lifecycle.commit(context)
        context.terminal_state.committed_partition_ids.update(committed)
        return self._with_runtime_metrics(load_result, context)

    def quality_scope(
        self,
        context: NativeTransferRuntimeContext,
    ) -> PreCheckpointQualityScope | None:
        """Return a logical quality scope only for checkpointed native work."""

        if not context.enabled:
            return None
        return native_transfer_quality_scope(context)

    def publish_success_report(
        self,
        context: NativeTransferRuntimeContext,
        load_result: LoadResult,
    ) -> LoadResult:
        """Publish native-transfer acceptance only after required evidence succeeds."""

        if not context.enabled:
            return load_result
        result = self._with_runtime_metrics(load_result, context)
        self._write_report(context, result)
        return result

    def publish_commit_unknown_report(
        self,
        context: NativeTransferRuntimeContext,
        error: CommitUnknownError,
    ) -> None:
        """Invalidate optimistic success evidence after incomplete checkpointing."""

        if context.artifact_dir is None:
            return
        publish_native_transfer_commit_unknown_report(
            context.artifact_dir,
            run_id=context.run_id,
            error=error,
        )

    def mark_committed(self, context: NativeTransferRuntimeContext, load_result: LoadResult) -> LoadResult:
        """Compatibility facade preserving the historical combined operation."""

        result = self.record_target_success(context, load_result)
        try:
            result = self.commit_checkpoints(context, result)
        except Exception:
            if commit_unknown := classify_native_transfer_commit_unknown(context):
                self._publish_commit_unknown_preserving_primary(context, commit_unknown)
            raise
        return self.publish_success_report(context, result)

    def mark_failed(
        self,
        context: NativeTransferRuntimeContext,
        exc: Exception,
        *,
        safe_error_code: str | None = None,
    ) -> None:
        del exc
        if not context.enabled or context.terminal_state.target_returned_success:
            return
        commit_unknown = classify_native_transfer_commit_unknown(context)
        error_code = commit_unknown.code if commit_unknown is not None else safe_failure_code(safe_error_code)
        phase = "target_outcome_unknown" if context.terminal_state.target_invocation_started else "failed_before_target"
        self._checkpoint_lifecycle.mark_failed(
            context,
            error=error_code,
            phase=phase,
        )

    def commit_unknown_error(self, context: NativeTransferRuntimeContext) -> CommitUnknownError | None:
        """Return a terminal failure only while target durability is unproven."""

        return classify_native_transfer_commit_unknown(context)

    def _is_enabled(self, load_config: LoadConfig, artifact: Any) -> bool:
        options = (load_config.options or {}).get("native_transfer", {})
        if options is False or options.get("enabled", True) is False:
            return False
        return self.checkpoint_store is not None and isinstance(
            artifact, (PartitionedFileExportArtifact, PartitionedTransferPlanArtifact)
        )

    def _prepare_plan_context(
        self,
        *,
        load_config: LoadConfig,
        payload: LoadPayload,
        run_id: str,
    ) -> NativeTransferRuntimeContext:
        artifact = cast(PartitionedTransferPlanArtifact, payload.artifact)
        planned = NativeTransferPlanRuntimeAdapter.planned_partitions(load_config, artifact)
        require_checksum = bool((load_config.options or {}).get("native_transfer", {}).get("require_artifact_checksum"))
        planner = self.planner or PartitionResumePlanner(require_artifact_checksum=require_checksum)
        resume_plan = planner.plan(planned, self.checkpoint_store)  # type: ignore[arg-type]
        active_slices = NativeTransferPlanRuntimeAdapter.active_slices(load_config, artifact, resume_plan)
        filtered_payload = payload
        if len(active_slices) != len(artifact.slices):
            filtered_payload = payload.rebind(
                artifact=NativeTransferPlanRuntimeAdapter.with_slices(artifact, active_slices),
                schema=payload.schema,
            )
        context = NativeTransferRuntimeContext(
            enabled=True,
            load_config=load_config,
            run_id=run_id,
            payload=filtered_payload,
            resume_plan=resume_plan,
            active_artifacts=active_slices,
            checkpoint_store=self.checkpoint_store,
            artifact_dir=self._artifact_dir(load_config),
            reactivated_partition_ids=_reactivated_partition_ids(load_config, resume_plan),
        )
        return context

    def _active_artifacts(
        self,
        load_config: LoadConfig,
        partitions: tuple[FileExportArtifact, ...],
        resume_plan: PartitionResumePlan,
    ) -> tuple[FileExportArtifact, ...]:
        if not resume_plan.retry:
            return ()
        if load_config.load_strategy is LoadStrategy.FULL_REFRESH and resume_plan.skip:
            return partitions
        retry_ids = {decision.partition.transfer_partition_id for decision in resume_plan.retry}
        return tuple(item for item in partitions if self._artifact_partition_id(load_config, item) in retry_ids)

    def _planned_partition(
        self,
        load_config: LoadConfig,
        artifact: FileExportArtifact,
        index: int,
    ) -> PlannedTransferPartition:
        bounds = _partition_bounds(artifact, index)
        query_hash = native_transfer_query_hash(artifact, load_config)
        schema_hash = native_transfer_schema_hash(artifact, load_config)
        source_table = native_transfer_source_table(artifact, load_config)
        target_table = native_transfer_target_table(artifact, load_config)
        strategy = native_transfer_strategy(artifact, load_config)
        transfer_partition_id = getattr(artifact, "transfer_partition_id", None) or build_transfer_partition_id(
            source_table=source_table,
            target_table=target_table,
            strategy=strategy,
            query_hash=query_hash,
            schema_hash=schema_hash,
            partition_bounds=bounds,
        )
        artifact_sha256 = self.checksum_service.checksum(artifact)
        setattr(artifact, "artifact_sha256", artifact_sha256)
        return PlannedTransferPartition(
            transfer_partition_id=str(transfer_partition_id),
            source_table=source_table,
            target_table=target_table,
            strategy=strategy,
            query_hash=query_hash,
            schema_hash=schema_hash,
            partition_bounds=bounds,
            artifact_sha256=artifact_sha256,
        )

    def _artifact_partition_id(self, load_config: LoadConfig, artifact: FileExportArtifact) -> str:
        return self._planned_partition(load_config, artifact, 0).transfer_partition_id

    def _with_runtime_metrics(self, load_result: LoadResult, context: NativeTransferRuntimeContext) -> LoadResult:
        from dataclasses import replace

        metrics = {**(load_result.reconciliation_metrics or {}), "native_transfer_resume": context.runtime_metrics()}
        return replace(load_result, reconciliation_metrics=metrics)

    def _ensure_schema(self) -> None:
        if self._schema_ready or self.checkpoint_store is None:
            return
        ensure_schema = getattr(self.checkpoint_store, "ensure_schema", None)
        if callable(ensure_schema):
            ensure_schema()
        self._schema_ready = True

    def _artifact_dir(self, load_config: LoadConfig) -> Path | None:
        if self.artifact_dir:
            return self.artifact_dir
        evidence = (load_config.options or {}).get("runtime_evidence") or {}
        output_dir = evidence.get("output_dir") or (load_config.options or {}).get("artifact_dir")
        return Path(output_dir) if output_dir else None

    def _write_report(self, context: NativeTransferRuntimeContext, load_result: LoadResult) -> None:
        artifact_dir = context.artifact_dir
        if artifact_dir is None:
            return
        publish_native_transfer_runtime_report(
            artifact_dir,
            run_id=context.run_id,
            context=context,
            load_result=load_result,
        )

    def _publish_commit_unknown_preserving_primary(
        self,
        context: NativeTransferRuntimeContext,
        error: CommitUnknownError,
    ) -> None:
        try:
            self.publish_commit_unknown_report(context, error)
        except Exception:
            error.add_note("terminal COMMIT_UNKNOWN evidence could not be persisted")


def _partition_bounds(artifact: FileExportArtifact, index: int) -> dict[str, Any]:
    return dict(getattr(artifact, "partition_bounds", None) or {"index": index})


def _reactivated_partition_ids(
    load_config: LoadConfig,
    resume_plan: PartitionResumePlan,
) -> frozenset[str]:
    if load_config.load_strategy is not LoadStrategy.FULL_REFRESH or not resume_plan.skip or not resume_plan.retry:
        return frozenset()
    return frozenset(decision.partition.transfer_partition_id for decision in resume_plan.skip)
