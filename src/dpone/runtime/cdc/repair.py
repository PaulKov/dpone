"""CDC repair execution through an injected sink applier."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from dpone.runtime.cdc.base import CDCBatch, CDCChange, CDCOperation
from dpone.runtime.cdc.compare_models import CdcRepairAction, load_repair_plan
from dpone.runtime.cdc.runtime_models import CdcApplyReceipt, CdcRuntimeStream
from dpone.runtime.cdc.runtime_ports import CdcSinkApplier

REPAIR_EXECUTION_SCHEMA_VERSION = "dpone.cdc_repair_execution.v1"


@dataclass(frozen=True, slots=True)
class CdcRepairExecutionReport:
    """Stable report for one repair execution run."""

    stream: CdcRuntimeStream
    repair_actions: int
    committed: bool
    passed: bool
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    sink_receipt: CdcApplyReceipt | None
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": REPAIR_EXECUTION_SCHEMA_VERSION,
            "stream": self.stream.to_dict(),
            "repair_actions": self.repair_actions,
            "committed": self.committed,
            "passed": self.passed,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "sink_receipt": self.sink_receipt.to_dict() if self.sink_receipt else None,
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# CDC repair execution",
            "",
            f"- Pipeline: `{self.stream.pipeline_name}`",
            f"- Route: `{self.stream.route_id}`",
            f"- Stream: `{self.stream.stream_id}`",
            f"- Passed: `{self.passed}`",
            f"- Committed: `{self.committed}`",
            f"- Repair actions: `{self.repair_actions}`",
            "",
            "## Blockers",
            "",
        ]
        lines.extend(f"- `{item}`" for item in self.blockers) if self.blockers else lines.append("- none")
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- `{item}`" for item in self.warnings) if self.warnings else lines.append("- none")
        lines.append("")
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")


class CdcRepairExecutionService:
    """Apply bounded repair actions without mutating CDC offsets."""

    def __init__(self, *, sink_applier: CdcSinkApplier) -> None:
        self._sink_applier = sink_applier

    def execute(
        self,
        *,
        output_dir: str | Path,
        repair_plan_json: str | Path,
        stream: CdcRuntimeStream,
        max_actions: int | None = None,
    ) -> CdcRepairExecutionReport:
        directory = Path(output_dir)
        json_path = directory / "cdc_repair_execution.json"
        markdown_path = directory / "cdc_repair_execution.md"
        plan = load_repair_plan(repair_plan_json, stream=stream)
        actions = tuple(action for action in plan.actions if action.replayable)
        if max_actions is not None:
            actions = actions[:max_actions]
        if not actions:
            report = _report(
                stream=stream,
                output_dir=directory,
                json_path=json_path,
                markdown_path=markdown_path,
                actions=actions,
                sink_receipt=None,
                passed=True,
                blockers=tuple(),
                warnings=("cdc_repair.no_actions",),
            )
            report.write()
            return report
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
            blockers += ("cdc_repair.sink_apply_failed",)
        if not receipt.durable:
            blockers += ("cdc_repair.sink_not_durable",)
        report = _report(
            stream=stream,
            output_dir=directory,
            json_path=json_path,
            markdown_path=markdown_path,
            actions=actions,
            sink_receipt=receipt,
            passed=receipt.passed and receipt.durable and not blockers,
            blockers=blockers,
            warnings=tuple(receipt.warnings),
        )
        report.write()
        return report


def _change(*, stream: CdcRuntimeStream, action: CdcRepairAction, sequence: int) -> CDCChange:
    return CDCChange(
        operation=CDCOperation(action.operation),
        data=action.payload,
        position=action.action_id,
        source_schema=stream.source_schema,
        source_table=stream.source_table,
        sequence=sequence,
        before=action.target_payload,
        metadata={
            "repair_action_id": action.action_id,
            "repair_kind": action.kind,
            "repair_reason": action.reason,
        },
    )


def _report(
    *,
    stream: CdcRuntimeStream,
    output_dir: Path,
    json_path: Path,
    markdown_path: Path,
    actions: tuple[CdcRepairAction, ...],
    sink_receipt: CdcApplyReceipt | None,
    passed: bool,
    blockers: tuple[str, ...],
    warnings: tuple[str, ...],
) -> CdcRepairExecutionReport:
    return CdcRepairExecutionReport(
        stream=stream,
        repair_actions=len(actions),
        committed=False,
        passed=passed,
        blockers=blockers,
        warnings=warnings,
        sink_receipt=sink_receipt,
        output_dir=str(output_dir),
        json_path=str(json_path),
        markdown_path=str(markdown_path),
    )


__all__ = ["CdcRepairExecutionReport", "CdcRepairExecutionService"]
