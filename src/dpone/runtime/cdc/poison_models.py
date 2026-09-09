"""CDC poison-event quarantine value objects."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from dpone.runtime.cdc.base import CDCChange, CDCOperation

if TYPE_CHECKING:
    from dpone.runtime.cdc.runtime_models import CdcRuntimeStream

POISON_QUARANTINE_SCHEMA_VERSION = "dpone.cdc_poison_quarantine.v1"
PoisonMode = Literal["fail_closed", "quarantine_and_continue"]
PoisonAction = Literal["quarantine"]


@dataclass(frozen=True, slots=True)
class CdcPoisonRecord:
    """Normalized record for one quarantined CDC event."""

    record_id: str
    event_id: str
    reason: str
    action: PoisonAction
    replayable: bool
    summary: str
    change: CDCChange

    def to_dict(self) -> dict[str, object]:
        return {
            "action": self.action,
            "change": change_to_dict(self.change),
            "event_id": self.event_id,
            "reason": self.reason,
            "record_id": self.record_id,
            "replayable": self.replayable,
            "summary": self.summary,
        }


@dataclass(frozen=True, slots=True)
class CdcPoisonDecision:
    """Result of classifying a bounded CDC batch."""

    clean_changes: tuple[CDCChange, ...]
    records: tuple[CdcPoisonRecord, ...]

    @property
    def has_poison(self) -> bool:
        return bool(self.records)


@dataclass(frozen=True, slots=True)
class CdcPoisonQuarantineReport:
    """Stable JSON/Markdown report for one runtime poison quarantine write."""

    stream: CdcRuntimeStream
    records: tuple[CdcPoisonRecord, ...]
    output_dir: str
    json_path: str
    markdown_path: str

    @property
    def record_count(self) -> int:
        return len(self.records)

    @property
    def passed(self) -> bool:
        return True

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": POISON_QUARANTINE_SCHEMA_VERSION,
            "stream": self.stream.to_dict(),
            "record_count": self.record_count,
            "records": [record.to_dict() for record in self.records],
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# CDC poison quarantine",
            "",
            f"- Pipeline: `{self.stream.pipeline_name}`",
            f"- Stream: `{self.stream.stream_id}`",
            f"- Records: `{self.record_count}`",
            "",
            "## Records",
            "",
        ]
        if not self.records:
            lines.append("- none")
        else:
            lines.extend(
                f"- `{record.reason}` `{record.change.position}` `{record.event_id}`" for record in self.records
            )
        lines.append("")
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")


def change_to_dict(change: CDCChange) -> dict[str, object]:
    return {
        "before": dict(change.before or {}),
        "data": dict(change.data),
        "metadata": dict(change.metadata),
        "operation": change.operation.value,
        "position": change.position,
        "sequence": change.sequence,
        "source_schema": change.source_schema,
        "source_table": change.source_table,
        "transaction_id": change.transaction_id,
    }


def change_from_dict(payload: Mapping[str, Any]) -> CDCChange:
    before = payload.get("before")
    return CDCChange(
        operation=CDCOperation(str(payload["operation"]).strip().lower()),
        data=_mapping(payload.get("data")),
        position=str(payload["position"]),
        source_schema=str(payload["source_schema"]),
        source_table=str(payload["source_table"]),
        transaction_id=str(payload["transaction_id"]) if payload.get("transaction_id") is not None else None,
        sequence=payload.get("sequence"),
        before=_mapping(before) if before else None,
        metadata=_mapping(payload.get("metadata", {})),
    )


def load_poison_quarantine(path: str | Path) -> Mapping[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return _mapping(payload)


def _mapping(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    raise ValueError("CDC poison quarantine payload fields must be objects")


__all__ = [
    "CdcPoisonDecision",
    "CdcPoisonQuarantineReport",
    "CdcPoisonRecord",
    "POISON_QUARANTINE_SCHEMA_VERSION",
    "PoisonAction",
    "PoisonMode",
    "change_from_dict",
    "change_to_dict",
    "load_poison_quarantine",
]
