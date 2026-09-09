"""Acceptance coordination for non-staged and native resume-only load paths."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.contracts.quality_failure import QualityFailureBoundary
    from dpone.runtime.governance.acceptance_metrics import AcceptanceMetricRun
    from dpone.runtime.governance.quality_execution import QualityGateExecution


from collections.abc import Callable, Mapping
from typing import Any

from dpone.contracts.quality_failure import (
    QualityGateFailureOutcome,
    QualityGateReceiptError,
    QualityGateReceiptMismatch,
)
from dpone.governance.quality import (
    QualityGatePolicy,
    quality_gate_contract,
    quality_gate_policy_fingerprint,
)
from dpone.runtime.governance.acceptance_metrics import (
    AcceptanceEvidenceContext,
    AcceptanceMetricPolicy,
    AcceptanceMetricsRecorder,
)
from dpone.runtime.governance.legacy_acceptance_result import (
    mark_blocking_post_commit_failure,
    quality_scope_summary,
    with_acceptance_metrics,
)
from dpone.runtime.governance.quality_execution import quality_gate_report_evidence
from dpone.runtime.governance.quality_receipt import validate_quality_gate_receipt
from dpone.runtime.governance.service import (
    LoadGovernanceService,
    QualityGateFailure,
    QualityGateReceipt,
)

_PHYSICAL_SIDES = ("source", "target")


class LegacyLoadGovernanceCoordinator:
    """Apply one acceptance policy around legacy target mutation boundaries."""

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
        source: Any | None,
        sink: Any,
        load_config: Any,
        payload: Any,
        extract_result: Any,
        load_record: Any,
        before_target_mutation: Callable[[], None] | None = None,
        on_target_committed: Callable[[Any], Any] | None = None,
        quality_scope: Any | None = None,
        quality_execution: QualityGateExecution | None = None,
    ) -> Any:
        """Capture source before ``sink.load`` and target after its successful return."""

        quality_policy = (
            quality_execution.snapshot.gate_policy
            if quality_execution is not None
            else self._quality_policy(load_config)
        )
        acceptance_policy = (
            quality_execution.snapshot.acceptance_policy
            if quality_execution is not None
            else AcceptanceMetricPolicy.from_load_config(load_config)
        )
        if quality_execution is not None:
            quality_execution.select_boundary("post_commit", load_config=load_config)
        run = self._start(
            source=source,
            sink=sink,
            load_config=load_config,
            payload=payload,
            extract_result=extract_result,
            load_record=load_record,
            boundary="pre_commit",
            policy=acceptance_policy,
        )
        self._capture(
            run,
            side="source",
            boundary="pre_commit",
            source=source,
            sink=sink,
            load_config=load_config,
            payload=payload,
            extract_result=extract_result,
            load_record=load_record,
        )
        self._ensure_policy_unchanged(load_config, quality_policy, quality_execution)
        if before_target_mutation is not None:
            before_target_mutation()
        load_result = sink.load(load_config, payload)
        checkpoint_committed = False
        try:
            self._ensure_policy_unchanged(load_config, quality_policy, quality_execution)
            if on_target_committed is not None:
                load_result = on_target_committed(load_result)
                checkpoint_committed = quality_scope is not None
            self._ensure_policy_unchanged(load_config, quality_policy, quality_execution)
            self._capture(
                run,
                side="target",
                boundary="post_commit",
                source=source,
                sink=sink,
                load_config=load_config,
                payload=payload,
                extract_result=extract_result,
                load_record=load_record,
            )
            self._ensure_policy_unchanged(load_config, quality_policy, quality_execution)
            quality_snapshots = quality_scope.committed_snapshots() if quality_scope is not None else None
            scope_summary = quality_scope_summary(quality_snapshots)
            quality_receipt, quality_evidence = self._evaluate_quality(
                load_config=load_config,
                extract_result=extract_result,
                load_result=load_result,
                quality_snapshots=quality_snapshots,
                quality_execution=quality_execution,
                boundary="post_commit",
                scope_summary=scope_summary,
                policy=quality_policy,
            )
        except QualityGateFailure as exc:
            raise QualityGateFailure(
                exc.report,
                outcome=QualityGateFailureOutcome.from_post_commit_result(
                    load_result,
                    native=quality_scope is not None,
                    checkpoint_committed=checkpoint_committed,
                ),
            ) from exc
        except QualityGateReceiptError as exc:
            raise exc.with_outcome(
                QualityGateFailureOutcome.from_post_commit_result(
                    load_result,
                    native=quality_scope is not None,
                    checkpoint_committed=checkpoint_committed,
                )
            ) from exc
        except Exception as exc:
            if acceptance_policy.enabled and acceptance_policy.mode == "required":
                mark_blocking_post_commit_failure(exc)
            raise
        return with_acceptance_metrics(
            load_result,
            run,
            governance_finalization="legacy_post_finalize",
            quality_receipt=quality_receipt,
            quality_evidence=quality_evidence,
            scope_summary=scope_summary,
        )

    def validate_resume_only(
        self,
        *,
        source: Any | None,
        sink: Any,
        load_config: Any,
        payload: Any,
        extract_result: Any,
        load_record: Any,
        load_result: Any,
        quality_scope: Any | None = None,
        quality_execution: QualityGateExecution | None = None,
    ) -> Any:
        """Evaluate acceptance without mutating a target or native checkpoint."""

        quality_policy = (
            quality_execution.snapshot.gate_policy
            if quality_execution is not None
            else self._quality_policy(load_config)
        )
        acceptance_policy = (
            quality_execution.snapshot.acceptance_policy
            if quality_execution is not None
            else AcceptanceMetricPolicy.from_load_config(load_config)
        )
        if quality_execution is not None:
            quality_execution.select_boundary("resume_validation", load_config=load_config)
        run = self._start(
            source=source,
            sink=sink,
            load_config=load_config,
            payload=payload,
            extract_result=extract_result,
            load_record=load_record,
            boundary="resume_validation",
            policy=acceptance_policy,
        )
        for side in _PHYSICAL_SIDES:
            self._capture(
                run,
                side=side,
                boundary="resume_validation",
                source=source,
                sink=sink,
                load_config=load_config,
                payload=payload,
                extract_result=extract_result,
                load_record=load_record,
            )
        try:
            self._ensure_policy_unchanged(load_config, quality_policy, quality_execution)
            quality_snapshots = quality_scope.resume_snapshots() if quality_scope is not None else None
            scope_summary = quality_scope_summary(quality_snapshots)
            quality_receipt, quality_evidence = self._evaluate_quality(
                load_config=load_config,
                extract_result=extract_result,
                load_result=load_result,
                quality_snapshots=quality_snapshots,
                quality_execution=quality_execution,
                boundary="resume_validation",
                scope_summary=scope_summary,
                policy=quality_policy,
            )
        except QualityGateFailure as exc:
            raise QualityGateFailure(
                exc.report,
                outcome=QualityGateFailureOutcome.from_resume_result(
                    load_result,
                    native=quality_scope is not None,
                ),
            ) from exc
        except QualityGateReceiptError as exc:
            raise exc.with_outcome(
                QualityGateFailureOutcome.from_resume_result(
                    load_result,
                    native=quality_scope is not None,
                )
            ) from exc
        return with_acceptance_metrics(
            load_result,
            run,
            quality_receipt=quality_receipt,
            quality_evidence=quality_evidence,
            scope_summary=scope_summary,
        )

    def _evaluate_quality(
        self,
        *,
        load_config: Any,
        extract_result: Any,
        load_result: Any,
        quality_snapshots: Any | None,
        quality_execution: QualityGateExecution | None,
        boundary: QualityFailureBoundary,
        scope_summary: dict[str, object] | None,
        policy: QualityGatePolicy,
    ) -> tuple[QualityGateReceipt | None, dict[str, object] | None]:
        evaluate = getattr(self._governance_service, "evaluate_quality_gate_receipt", None)
        if callable(evaluate):
            return evaluate(
                load_config=load_config,
                extract_result=extract_result,
                load_result=load_result,
                boundary=boundary,
                scope_summary=scope_summary,
                source_snapshot=quality_snapshots.source if quality_snapshots is not None else None,
                target_snapshot=quality_snapshots.target if quality_snapshots is not None else None,
                quality_execution=quality_execution,
            )
        report = self._run_quality_gates(
            load_config=load_config,
            extract_result=extract_result,
            load_result=load_result,
            quality_snapshots=quality_snapshots,
        )
        receipt = validate_quality_gate_receipt(
            QualityGateReceipt(report, boundary, scope_summary) if report is not None else None,
            policy=policy,
        )
        return receipt, quality_gate_report_evidence(report, policy) if report is not None else None

    def _run_quality_gates(
        self,
        *,
        load_config: Any,
        extract_result: Any,
        load_result: Any,
        quality_snapshots: Any | None,
    ) -> Any | None:
        run_quality_gates = getattr(self._governance_service, "run_quality_gates", None)
        if not callable(run_quality_gates):
            return None
        snapshot_arguments = {}
        if quality_snapshots is not None:
            snapshot_arguments = {
                "source_snapshot": quality_snapshots.source,
                "target_snapshot": quality_snapshots.target,
            }
        return run_quality_gates(
            load_config=load_config,
            extract_result=extract_result,
            load_result=load_result,
            **snapshot_arguments,
        )

    def _ensure_policy_unchanged(
        self,
        load_config: Any,
        expected: QualityGatePolicy,
        quality_execution: QualityGateExecution | None,
    ) -> None:
        current = self._quality_policy(load_config)
        if quality_gate_policy_fingerprint(current) != quality_gate_policy_fingerprint(
            expected
        ) or quality_gate_contract(current) != quality_gate_contract(expected):
            raise QualityGateReceiptMismatch
        if quality_execution is not None:
            quality_execution.assert_current(load_config=load_config)

    def _quality_policy(self, load_config: Any) -> QualityGatePolicy:
        validate = getattr(self._governance_service, "validate_quality_config", None)
        if callable(validate):
            return validate(load_config=load_config)
        AcceptanceMetricPolicy.from_load_config(load_config)
        options = getattr(load_config, "options", None)
        quality = options.get("quality") if isinstance(options, Mapping) else None
        return QualityGatePolicy.from_config(quality)

    def _start(
        self,
        *,
        source: Any | None,
        sink: Any,
        load_config: Any,
        payload: Any,
        extract_result: Any,
        load_record: Any,
        boundary: str,
        policy: AcceptanceMetricPolicy,
    ) -> AcceptanceMetricRun:
        return self._metric_recorder.start(
            policy=policy,
            physical_sides=_PHYSICAL_SIDES,
            payload_schema=payload.schema,
            schemas_by_side={
                "source": getattr(extract_result, "schema", ()) or (),
                "target": payload.schema,
            },
            source=source,
            sink=sink,
            evidence=AcceptanceEvidenceContext(load_record, self._governance_service, boundary),
        )

    def _capture(
        self,
        run: AcceptanceMetricRun,
        *,
        side: str,
        boundary: str,
        source: Any | None,
        sink: Any,
        load_config: Any,
        payload: Any,
        extract_result: Any,
        load_record: Any,
    ) -> None:
        self._metric_recorder.capture(
            run,
            side=side,
            load_config=load_config,
            extract_result=extract_result,
            payload_schema=payload.schema,
            source=source,
            sink=sink,
            evidence=AcceptanceEvidenceContext(load_record, self._governance_service, boundary),
        )


__all__ = ["LegacyLoadGovernanceCoordinator"]
