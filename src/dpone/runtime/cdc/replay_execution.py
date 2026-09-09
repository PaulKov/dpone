"""CDC poison quarantine replay execution."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.runtime.cdc.base import CDCBatch, CDCChange
from dpone.runtime.cdc.poison_models import change_from_dict, load_poison_quarantine
from dpone.runtime.cdc.runtime_models import CdcApplyReceipt, CdcRuntimeStream
from dpone.runtime.cdc.runtime_ports import CdcSinkApplier

REPLAY_EXECUTION_SCHEMA_VERSION = "dpone.cdc_replay_execution.v1"


@dataclass(frozen=True, slots=True)
class CdcReplayExecutionReport:
    """Stable report for one bounded replay execution."""

    stream: CdcRuntimeStream
    replayed_events: int
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
            "schema_version": REPLAY_EXECUTION_SCHEMA_VERSION,
            "stream": self.stream.to_dict(),
            "replayed_events": self.replayed_events,
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
            "# CDC replay execution",
            "",
            f"- Pipeline: `{self.stream.pipeline_name}`",
            f"- Stream: `{self.stream.stream_id}`",
            f"- Passed: `{self.passed}`",
            f"- Replayed events: `{self.replayed_events}`",
            f"- Committed offsets: `{self.committed}`",
            "",
            "## Blockers",
            "",
        ]
        lines.extend(f"- `{item}`" for item in self.blockers) if self.blockers else lines.append("- none")
        lines.append("")
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")


class CdcReplayExecutionService:
    """Apply quarantined CDC events without mutating source offsets."""

    def __init__(self, *, sink_applier: CdcSinkApplier) -> None:
        self._sink_applier = sink_applier

    def execute(
        self,
        *,
        output_dir: str | Path,
        quarantine_json: str | Path,
        stream: CdcRuntimeStream,
        max_events: int | None = None,
    ) -> CdcReplayExecutionReport:
        directory = Path(output_dir)
        changes = tuple(_changes(load_poison_quarantine(quarantine_json), max_events=max_events))
        blockers: list[str] = []
        warnings: list[str] = []
        receipt: CdcApplyReceipt | None = None
        if not changes:
            blockers.append("cdc_replay.no_replayable_events")
        else:
            batch = CDCBatch(changes=changes, next_offset=None, high_watermark=changes[-1].position)
            receipt = self._sink_applier.apply(stream=stream, batch=batch)
            blockers.extend(receipt.blockers)
            warnings.extend(receipt.warnings)
            if not receipt.passed and not receipt.blockers:
                blockers.append("cdc_replay.sink_apply_failed")
            if not receipt.durable:
                blockers.append("cdc_replay.sink_not_durable")
        report = CdcReplayExecutionReport(
            stream=stream,
            replayed_events=len(changes),
            committed=False,
            passed=not blockers,
            blockers=tuple(dict.fromkeys(blockers)),
            warnings=tuple(dict.fromkeys(warnings)),
            sink_receipt=receipt,
            output_dir=str(directory),
            json_path=str(directory / "cdc_replay_execution.json"),
            markdown_path=str(directory / "cdc_replay_execution.md"),
        )
        report.write()
        return report


def _changes(payload: Mapping[str, Any], *, max_events: int | None) -> Sequence[CDCChange]:
    records = payload.get("records", [])
    if not isinstance(records, list):
        raise ValueError("CDC poison quarantine records must be an array")
    changes: list[CDCChange] = []
    for record in records:
        item = _mapping(record)
        if item.get("replayable", True) is False:
            continue
        changes.append(change_from_dict(_mapping(item.get("change"))))
        if max_events is not None and len(changes) >= max_events:
            break
    return tuple(changes)


def _mapping(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    raise ValueError("CDC replay quarantine records must be objects")


__all__ = ["CdcReplayExecutionReport", "CdcReplayExecutionService", "REPLAY_EXECUTION_SCHEMA_VERSION"]
