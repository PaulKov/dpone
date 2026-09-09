"""Runtime facade for source preparation hooks and load governance."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.contracts.quality_failure import QualityFailureBoundary
    from dpone.governance.hooks import LoadStepAuditStorage
    from dpone.governance.quality import QualityGateReport, QualityProbeSnapshot


from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from dpone.contracts.quality_failure import (
    QualityGateFailure as _QualityGateFailure,
)
from dpone.governance.hooks import (
    HookExecutionContext,
    HookGraph,
    HookGraphRunner,
    HookPhaseEvidence,
    LoadStepAuditRecord,
)
from dpone.governance.quality import QualityGatePolicy, QualityGateRunner
from dpone.governance.sql_hooks import SqlHookProvider
from dpone.runtime.governance.acceptance_metrics import AcceptanceMetricPolicy
from dpone.runtime.governance.hook_graph_runtime import (
    connectors,
    filter_airflow_separate_hooks,
    hooks_config,
    manifest_dir,
    quality_config,
    repo_root,
)
from dpone.runtime.governance.quality_execution import (
    QualityExecutionSnapshot,
    QualityGateExecution,
    quality_gate_report_evidence,
)
from dpone.runtime.governance.quality_probe_snapshots import (
    source_snapshot as build_source_snapshot,
)
from dpone.runtime.governance.quality_probe_snapshots import (
    target_snapshot as build_target_snapshot,
)
from dpone.runtime.governance.quality_receipt import (
    QualityGateReceipt,
)
from dpone.runtime.governance.quality_receipt import (
    StagedQualityGateReceipt as _StagedQualityGateReceipt,
)
from dpone.runtime.governance.quality_receipt import (
    report_from_quality_gate_receipt as _report_from_quality_gate_receipt,
)
from dpone.runtime.governance.quality_receipt import (
    report_from_staged_quality_receipt as _report_from_staged_quality_receipt,
)
from dpone.runtime.governance.quality_receipt import (
    validate_quality_gate_receipt as _validate_quality_gate_receipt,
)

_UTC = timezone.utc  # noqa: UP017 - keep mypy-compatible timezone alias for current target.
StagedQualityGateReceipt = _StagedQualityGateReceipt
QualityGateFailure = _QualityGateFailure
report_from_quality_gate_receipt = _report_from_quality_gate_receipt
report_from_staged_quality_receipt = _report_from_staged_quality_receipt


class LoadGovernanceService:
    """Execute manifest-defined governance hooks through connector-neutral ports."""

    def __init__(
        self,
        *,
        audit_storage: LoadStepAuditStorage | None = None,
        quality_runner: object | None = None,
    ) -> None:
        self._audit_storage = audit_storage
        self._quality_runner = quality_runner if quality_runner is not None else QualityGateRunner()

    @property
    def audit_storage_configured(self) -> bool:
        return self._audit_storage is not None

    def set_default_audit_storage(self, audit_storage: LoadStepAuditStorage) -> None:
        if self._audit_storage is None:
            self._audit_storage = audit_storage
            return
        set_delegate = getattr(self._audit_storage, "set_default_delegate", None)
        if callable(set_delegate):
            set_delegate(audit_storage)

    def run_pre_hooks(
        self,
        *,
        load_config: Any,
        source: Any,
        sink: Any,
        load_record: Any,
        process_name: str | None,
    ) -> HookPhaseEvidence:
        return self._run_phase(
            load_config=load_config,
            source=source,
            sink=sink,
            load_record=load_record,
            process_name=process_name,
            phase="pre_hook",
        )

    def run_quality_gates(
        self,
        *,
        load_config: Any,
        extract_result: Any,
        load_result: Any,
        source_snapshot: QualityProbeSnapshot | None = None,
        target_snapshot: QualityProbeSnapshot | None = None,
        quality_execution: QualityGateExecution | None = None,
        boundary: QualityFailureBoundary = "pre_commit",
        scope_summary: Mapping[str, object] | None = None,
    ) -> QualityGateReport:
        source_value, target_value = self.quality_probe_snapshots(
            extract_result=extract_result,
            load_result=load_result,
            source_snapshot=source_snapshot,
            target_snapshot=target_snapshot,
        )
        if quality_execution is not None:
            return quality_execution.evaluate(
                load_config=load_config,
                boundary=boundary,
                source_snapshot=source_value,
                target_snapshot=target_value,
                scope_summary=scope_summary,
            ).report
        snapshot = QualityExecutionSnapshot.from_load_config(load_config)
        execution = self.create_quality_gate_execution(
            snapshot=snapshot,
            run_id="compatibility-run",
            load_id="compatibility-load",
        )
        execution.select_boundary("pre_commit", load_config=load_config)
        receipt = execution.evaluate(
            load_config=load_config,
            boundary="pre_commit",
            source_snapshot=source_value,
            target_snapshot=target_value,
        )
        return receipt.report

    def create_quality_gate_execution(
        self,
        *,
        snapshot: QualityExecutionSnapshot,
        run_id: str,
        load_id: str,
    ) -> QualityGateExecution:
        """Bind one injected runner to one immutable run/load snapshot."""

        return QualityGateExecution(snapshot, run_id=run_id, load_id=load_id, runner=self._quality_runner)

    def quality_probe_snapshots(
        self,
        *,
        extract_result: Any,
        load_result: Any,
        source_snapshot: QualityProbeSnapshot | None = None,
        target_snapshot: QualityProbeSnapshot | None = None,
    ) -> tuple[QualityProbeSnapshot, QualityProbeSnapshot]:
        """Build canonical source/target inputs for one quality evaluation."""

        return (
            source_snapshot or build_source_snapshot(extract_result),
            target_snapshot or build_target_snapshot(load_result),
        )

    def evaluate_quality_gate_receipt(
        self,
        *,
        load_config: Any,
        extract_result: Any,
        load_result: Any,
        boundary: QualityFailureBoundary,
        scope_summary: Mapping[str, object] | None = None,
        source_snapshot: QualityProbeSnapshot | None = None,
        target_snapshot: QualityProbeSnapshot | None = None,
        quality_execution: QualityGateExecution | None = None,
    ) -> tuple[QualityGateReceipt, dict[str, object]]:
        """Evaluate through the selected authority and return bounded evidence."""

        if quality_execution is None:
            report = self.run_quality_gates(
                load_config=load_config,
                extract_result=extract_result,
                load_result=load_result,
                source_snapshot=source_snapshot,
                target_snapshot=target_snapshot,
            )
            receipt = self.validate_quality_gate_receipt(
                load_config=load_config,
                receipt=QualityGateReceipt(report, boundary, scope_summary),
            )
            assert receipt is not None
            policy = self.validate_quality_config(load_config=load_config)
            return receipt, quality_gate_report_evidence(report, policy)
        report = self.run_quality_gates(
            load_config=load_config,
            extract_result=extract_result,
            load_result=load_result,
            source_snapshot=source_snapshot,
            target_snapshot=target_snapshot,
            quality_execution=quality_execution,
            boundary=boundary,
            scope_summary=scope_summary,
        )
        receipt = quality_execution.receipt_for_report(report, load_config=load_config)
        return (
            receipt,
            quality_execution.evidence_projection(receipt, load_config=load_config),
        )

    def validate_quality_config(self, *, load_config: Any) -> QualityGatePolicy:
        """Validate quality authoring before runtime side effects begin."""

        AcceptanceMetricPolicy.from_load_config(load_config)
        return QualityGatePolicy.from_config(quality_config(load_config))

    def validate_quality_gate_receipt(
        self,
        *,
        load_config: Any,
        receipt: object,
    ) -> QualityGateReceipt | None:
        """Purely validate coordinator evidence against the current policy."""

        policy = self.validate_quality_config(load_config=load_config)
        return _validate_quality_gate_receipt(receipt, policy=policy)

    def run_post_hooks(
        self,
        *,
        load_config: Any,
        source: Any,
        sink: Any,
        load_record: Any,
        process_name: str | None,
    ) -> HookPhaseEvidence:
        return self._run_phase(
            load_config=load_config,
            source=source,
            sink=sink,
            load_record=load_record,
            process_name=process_name,
            phase="post_hook",
        )

    def record_load_step(
        self,
        *,
        load_record: Any,
        step_id: str,
        phase: str,
        kind: str,
        status: str,
        started_at: datetime | None = None,
        error_message: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        if self._audit_storage is None:
            return
        self._audit_storage.record_step(
            LoadStepAuditRecord(
                run_id=str(getattr(load_record, "run_id", "")),
                load_id=str(getattr(load_record, "load_id", "")),
                step_id=step_id,
                phase=phase,
                kind=kind,
                status=status,
                started_at=started_at or _utc_now(),
                finished_at=_utc_now() if status != "running" else None,
                error_message=error_message,
                details=details or {},
            )
        )

    def record_acceptance_snapshot(
        self,
        *,
        load_record: Any,
        snapshot: Any,
        boundary: str,
    ) -> None:
        """Publish one snapshot and classify an evidence-write failure safely."""

        try:
            self.record_load_step(
                load_record=load_record,
                step_id=f"{snapshot.side}_metrics_captured",
                phase="load_governance",
                kind="data_quality_evidence",
                status="warning" if snapshot.warnings else "succeeded",
                details=snapshot.to_jsonable(),
            )
        except Exception:
            self.record_acceptance_failure(
                load_record=load_record,
                boundary=boundary,
                side=snapshot.side,
                code=f"{snapshot.side}_acceptance_metric_evidence_failed",
            )
            raise

    def record_acceptance_failure(
        self,
        *,
        load_record: Any,
        boundary: str,
        side: str,
        code: str,
    ) -> None:
        """Best-effort safe evidence that never replaces the primary failure."""

        try:
            self.record_load_step(
                load_record=load_record,
                step_id="load_governance_failed",
                phase="load_governance",
                kind="data_quality_evidence",
                status="failed",
                error_message=code,
                details={
                    "failure_boundary": boundary,
                    "acceptance_side": side,
                    "error_code": code,
                },
            )
        except Exception:
            return

    def _run_phase(
        self,
        *,
        load_config: Any,
        source: Any,
        sink: Any,
        load_record: Any,
        process_name: str | None,
        phase: str,
    ) -> HookPhaseEvidence:
        graph = HookGraph.from_config(
            hooks_config(load_config),
            manifest_dir=manifest_dir(load_config),
            repo_root=repo_root(load_config),
        )
        graph = filter_airflow_separate_hooks(graph, phase=phase)
        if not graph.phase(phase):
            return HookPhaseEvidence(phase=phase, steps=())
        runner = HookGraphRunner(
            providers={"sql": SqlHookProvider(connectors(source=source, sink=sink))},
            audit_storage=self._audit_storage,
        )
        return runner.run_phase(
            graph,
            phase,
            HookExecutionContext(
                run_id=str(getattr(load_record, "run_id", "")),
                load_id=str(getattr(load_record, "load_id", "")),
                process_name=process_name,
                phase=phase,
            ),
        )


def _utc_now() -> datetime:
    return datetime.now(_UTC)
