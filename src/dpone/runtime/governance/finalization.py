"""Pre-finalize governance coordinator for staged sink loads."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dpone.runtime.governance.ports import SinkSideLineageProjector
    from dpone.runtime.governance.quality_execution import QualityGateExecution


from collections.abc import Callable
from datetime import datetime, timezone

from dpone.contracts.quality_failure import QualityGateReceiptError
from dpone.runtime import columnar_range_quality_bridge as range_quality
from dpone.runtime.governance.acceptance_metrics import (
    AcceptanceMetricPolicy,
    AcceptanceMetricsRecorder,
)
from dpone.runtime.governance.finalization_support import (
    capture_acceptance,
    classify_post_commit_cleanup_failure,
    completed_acceptance_metrics,
    prepare_acceptance,
    preserve_completed_replay_truth,
    staged_probe_result,
    validated_staged_rows,
    with_governance_metrics,
)
from dpone.runtime.governance.ports import (
    NoopSinkSideLineageProjector,
    abort_staged_load_preserving_primary,
    finalize_staged_load_after_validation,
    staged_load_commit_unknown_details,
    staged_load_failure_details,
    staged_load_handle_details,
    validate_staged_load_if_supported,
)
from dpone.runtime.governance.quality_execution import quality_gate_report_evidence
from dpone.runtime.governance.service import LoadGovernanceService, QualityGateFailure, QualityGateReceipt
from dpone.runtime.lineage.options import LineageOptions


class LoadGovernanceFinalizationCoordinator:
    """Run lineage and quality gates before target finalization."""

    def __init__(
        self,
        governance_service: LoadGovernanceService | None = None,
        metric_recorder: AcceptanceMetricsRecorder | None = None,
    ) -> None:
        self._governance_service = governance_service or LoadGovernanceService()
        self._metric_recorder = metric_recorder or AcceptanceMetricsRecorder()

    def load(
        self,
        *,
        source: Any | None = None,
        sink: Any,
        load_config: Any,
        payload: Any,
        extract_result: Any,
        load_record: Any,
        projector: SinkSideLineageProjector | None = None,
        before_target_mutation: Callable[[], None] | None = None,
        on_target_committed: Callable[[Any], Any] | None = None,
        quality_scope: Any | None = None,
        quality_execution: QualityGateExecution | None = None,
    ) -> Any:
        acceptance_policy = (
            quality_execution.snapshot.acceptance_policy
            if quality_execution is not None
            else AcceptanceMetricPolicy.from_load_config(load_config)
        )
        quality_policy = (
            quality_execution.snapshot.gate_policy
            if quality_execution is not None
            else self._governance_service.validate_quality_config(load_config=load_config)
        )
        if quality_execution is not None:
            quality_execution.select_boundary("pre_commit", load_config=load_config)
        replay_session = getattr(quality_execution, "replay_session", None)
        replay_evidence = None
        stage_started = _utc_now()
        handle = sink.stage_payload(load_config, payload)
        projected = None
        lifecycle_load_config = load_config
        lifecycle_handle = handle
        failure: Exception | None = None
        quality_started: datetime | None = None
        target_state = "pre_target"
        acceptance_failure_recorded = False
        failure_evidence_recorded = False
        try:
            validated_staged_rows(handle)
            self._record(
                load_record,
                "staged",
                "succeeded",
                started_at=stage_started,
                details=staged_load_handle_details(handle),
            )
            lineage_options = LineageOptions.from_config((getattr(load_config, "options", {}) or {}).get("lineage"))
            selected_projector: SinkSideLineageProjector = (
                projector or getattr(sink, "lineage_projector", None) or NoopSinkSideLineageProjector()
            )
            projection_started = _utc_now()
            projected = selected_projector.project(
                load_config=load_config,
                handle=handle,
                lineage_options=lineage_options,
                load_record=load_record,
            )
            lifecycle_handle = projected.handle
            self._record(
                load_record,
                "lineage_projected",
                "succeeded",
                started_at=projection_started,
                details=projected.to_jsonable(),
            )
            quality_started = _utc_now()
            staged_result = staged_probe_result(projected.handle)
            quality_snapshots = (
                quality_scope.projected_snapshots(staged_result.staging_rows) if quality_scope is not None else None
            )
            scope_summary = quality_snapshots.scope_summary.to_jsonable() if quality_snapshots is not None else None
            quality_receipt: QualityGateReceipt | None
            quality_receipt, quality_evidence = self._governance_service.evaluate_quality_gate_receipt(
                load_config=load_config,
                extract_result=extract_result,
                load_result=staged_result,
                boundary="pre_commit",
                scope_summary=scope_summary,
                source_snapshot=quality_snapshots.source if quality_snapshots is not None else None,
                target_snapshot=quality_snapshots.target if quality_snapshots is not None else None,
                quality_execution=quality_execution,
            )
            assert quality_receipt is not None
            self._record(
                load_record,
                "quality_checked",
                "succeeded",
                started_at=quality_started,
                details=quality_evidence,
            )
            try:
                acceptance_run = prepare_acceptance(
                    self._metric_recorder,
                    self._governance_service,
                    policy=acceptance_policy,
                    load_config=load_config,
                    replay_session=replay_session,
                    staged_handle=projected.handle,
                    extract_result=extract_result,
                    source=source,
                    sink=sink,
                    load_record=load_record,
                )
            except Exception:
                acceptance_failure_recorded = True
                raise
            if quality_execution is not None:
                quality_execution.assert_current(load_config=load_config)
            else:
                quality_receipt = self._governance_service.validate_quality_gate_receipt(
                    load_config=load_config,
                    receipt=quality_receipt,
                )
                assert quality_receipt is not None
            range_quality.advance_range_governed_quality(projected.handle, quality_receipt, quality_evidence)
            validation_receipt = validate_staged_load_if_supported(sink, load_config, projected.handle)
            if validation_receipt.validated:
                _token, lifecycle_load_config, lifecycle_handle = validation_receipt.frozen_inputs(
                    sink=sink,
                    load_config=load_config,
                )
            finalize_started = _utc_now()
            if before_target_mutation is not None:
                before_target_mutation()
            if replay_session is not None:
                replay_session.prepare(
                    config=load_config,
                    handle=projected.handle,
                    extract_result=extract_result,
                    receipt=quality_receipt,
                    acceptance=acceptance_run,
                )
            target_state = "target_invoked"
            load_result = finalize_staged_load_after_validation(
                sink,
                load_config,
                validation_receipt,
            )
            if quality_execution is not None:
                quality_execution.assert_current(load_config=load_config)
            if on_target_committed is not None:
                load_result = on_target_committed(load_result)
            target_state = "target_confirmed"
            try:
                capture_acceptance(
                    self._metric_recorder,
                    self._governance_service,
                    acceptance_run,
                    sides=() if replay_session is not None else ("target",),
                    boundary="post_commit",
                    source=source,
                    sink=sink,
                    load_config=lifecycle_load_config,
                    extract_result=extract_result,
                    staged_handle=lifecycle_handle,
                    load_record=load_record,
                )
            except Exception:
                acceptance_failure_recorded = True
                raise
            if quality_execution is not None:
                quality_execution.assert_current(load_config=load_config)
            if replay_session is not None:
                replay_evidence = replay_session.finish_original(load_config, receipt=quality_receipt)
            try:
                self._record(
                    load_record,
                    "finalized",
                    "succeeded",
                    started_at=finalize_started,
                    details=range_quality.range_load_result_details(load_result, lifecycle_handle),
                )
            except Exception:
                self._record_preserving_primary(
                    load_record,
                    "load_governance_failed",
                    "failed",
                    started_at=_utc_now(),
                    error_message="finalized_evidence_failed",
                    details={
                        "failure_boundary": "post_commit",
                        "error_code": "finalized_evidence_failed",
                    },
                )
                failure_evidence_recorded = True
                raise
        except Exception as exc:
            preserve_completed_replay_truth(exc, replay_evidence, quality_execution)
            failure = exc
            failure_details = None
            if target_state == "pre_target":
                cleanup_status = abort_staged_load_preserving_primary(sink, lifecycle_handle)
                failure_details = staged_load_failure_details(
                    exc,
                    lifecycle_handle,
                    cleanup_status=cleanup_status,
                )
                setattr(exc, "details", failure_details)
            elif target_state == "target_invoked":
                failure_details = staged_load_commit_unknown_details(exc, lifecycle_handle)
                setattr(exc, "details", failure_details)
            if isinstance(exc, QualityGateFailure) and quality_started is not None:
                self._record_preserving_primary(
                    load_record,
                    "quality_checked",
                    "failed",
                    started_at=quality_started,
                    error_message=str(exc),
                    details=quality_gate_report_evidence(exc.report, quality_policy),
                )
            if isinstance(exc, QualityGateReceiptError):
                self._record_preserving_primary(
                    load_record,
                    "load_governance_failed",
                    "failed",
                    started_at=_utc_now(),
                    error_message=exc.code,
                    details={
                        "failure_boundary": "pre_commit",
                        "error_code": exc.code,
                    },
                )
                failure_evidence_recorded = True
            if not acceptance_failure_recorded and not failure_evidence_recorded:
                self._record_preserving_primary(
                    load_record,
                    "load_governance_failed",
                    "failed",
                    started_at=_utc_now(),
                    error_message=f"{target_state} governance failed: {exc}",
                    details=failure_details,
                )
            raise
        finally:
            if target_state == "target_confirmed" and (cleanup := getattr(sink, "cleanup_staged_load", None)):
                try:
                    cleanup(lifecycle_handle)
                except Exception as cleanup_error:
                    classified = classify_post_commit_cleanup_failure(cleanup_error, lifecycle_handle, failure)
                    preserve_completed_replay_truth(classified, replay_evidence, quality_execution)
                    self._record_preserving_primary(
                        load_record,
                        "load_governance_failed",
                        "failed",
                        started_at=_utc_now(),
                        error_message="staged_cleanup_failed",
                        details=classified.details,
                    )
                    if failure is None:
                        raise classified from cleanup_error
                terminal_recorder = self._record if failure is None else None
                range_quality.record_terminal_range_evidence(terminal_recorder, load_record, lifecycle_handle)

        return with_governance_metrics(
            load_result,
            projected,
            quality_receipt,
            quality_evidence,
            completed_acceptance_metrics(acceptance_run, load_result, replay_evidence),
        )

    def _record(
        self,
        load_record: Any,
        step_id: str,
        status: str,
        *,
        started_at: datetime,
        error_message: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self._governance_service.record_load_step(
            load_record=load_record,
            step_id=step_id,
            phase="load_governance",
            kind="target_preparation",
            status=status,
            started_at=started_at,
            error_message=error_message,
            details=details,
        )

    def _record_preserving_primary(
        self,
        load_record: Any,
        step_id: str,
        status: str,
        *,
        started_at: datetime,
        error_message: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        try:
            self._record(
                load_record,
                step_id,
                status,
                started_at=started_at,
                error_message=error_message,
                details=details,
            )
        except Exception:
            return


# Keep timezone.utc until the oldest supported Python typing exposes datetime.UTC.
def _utc_now() -> datetime:
    return datetime.now(timezone.utc)  # noqa: UP017 - current Python target lacks datetime.UTC typing.


__all__ = ["LoadGovernanceFinalizationCoordinator"]
