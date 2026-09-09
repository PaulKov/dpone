"""CDC resync execution through an injected sink applier."""

from __future__ import annotations

from pathlib import Path

from dpone.runtime.cdc.base import CDCBatch, CDCChange, CDCOperation
from dpone.runtime.cdc.retention_models import CdcResyncAction, CdcResyncExecutionReport, load_resync_plan
from dpone.runtime.cdc.runtime_models import CdcApplyReceipt, CdcRuntimeStream
from dpone.runtime.cdc.runtime_ports import CdcSinkApplier


class CdcResyncExecutionService:
    """Apply bounded resync actions without mutating CDC offsets."""

    def __init__(self, *, sink_applier: CdcSinkApplier) -> None:
        self._sink_applier = sink_applier

    def execute(
        self,
        *,
        output_dir: str | Path,
        resync_plan_json: str | Path,
        stream: CdcRuntimeStream,
        max_actions: int | None = None,
    ) -> CdcResyncExecutionReport:
        directory = Path(output_dir)
        plan = load_resync_plan(resync_plan_json, stream=stream)
        actions = tuple(action for action in plan.actions if action.replayable)
        if max_actions is not None:
            actions = actions[:max_actions]
        if not actions:
            return _report(
                stream=stream,
                output_dir=directory,
                actions=actions,
                sink_receipt=None,
                passed=True,
                blockers=tuple(),
                warnings=("cdc_resync.no_actions",),
            ).write()
        batch = CDCBatch(
            changes=tuple(
                _change(stream=stream, action=action, sequence=index) for index, action in enumerate(actions, 1)
            ),
            next_offset=None,
            high_watermark=actions[-1].action_id,
        )
        receipt = self._sink_applier.apply(stream=stream, batch=batch)
        blockers = tuple(receipt.blockers)
        if not receipt.passed:
            blockers += ("cdc_resync.sink_apply_failed",)
        if not receipt.durable:
            blockers += ("cdc_resync.sink_not_durable",)
        return _report(
            stream=stream,
            output_dir=directory,
            actions=actions,
            sink_receipt=receipt,
            passed=receipt.passed and receipt.durable and not blockers,
            blockers=blockers,
            warnings=tuple(receipt.warnings),
        ).write()


def _change(*, stream: CdcRuntimeStream, action: CdcResyncAction, sequence: int) -> CDCChange:
    operation = CDCOperation.DELETE if action.operation == "delete" else CDCOperation.INSERT
    return CDCChange(
        operation=operation,
        data=action.payload,
        position=action.action_id,
        source_schema=stream.source_schema,
        source_table=stream.source_table,
        sequence=sequence,
        before=None,
        metadata={
            "resync_action_id": action.action_id,
            "resync_reason": action.reason,
            "resync_operation": action.operation,
        },
    )


def _report(
    *,
    stream: CdcRuntimeStream,
    output_dir: Path,
    actions: tuple[CdcResyncAction, ...],
    sink_receipt: CdcApplyReceipt | None,
    passed: bool,
    blockers: tuple[str, ...],
    warnings: tuple[str, ...],
) -> CdcResyncExecutionReport:
    return CdcResyncExecutionReport(
        stream=stream,
        resync_actions=len(actions),
        committed=False,
        passed=passed,
        blockers=blockers,
        warnings=warnings,
        sink_receipt=sink_receipt,
        output_dir=str(output_dir),
        json_path=str(output_dir / "cdc_resync_execution.json"),
        markdown_path=str(output_dir / "cdc_resync_execution.md"),
    )


__all__ = ["CdcResyncExecutionService"]
