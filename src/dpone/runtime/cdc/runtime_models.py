"""CDC runtime orchestration value objects."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.readiness.cdc import CDCBackend, CDCOffset


import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from dpone.runtime.cdc.poison_models import CdcPoisonQuarantineReport, PoisonMode

SCHEMA_VERSION = "dpone.cdc_runtime_run.v1"


@dataclass(frozen=True, slots=True)
class CdcRuntimeStream:
    """Canonical runtime identity for one source -> sink CDC loop."""

    pipeline_name: str
    source: str
    sink: str
    backend: CDCBackend
    source_schema: str
    source_table: str
    target_dataset: str
    unique_key: tuple[str, ...]
    strategy: str = "cdc"

    @property
    def source_dataset(self) -> str:
        return f"{self.source_schema}.{self.source_table}"

    @property
    def route_id(self) -> str:
        return f"{_token(self.source)}_to_{_token(self.sink)}__{_token(self.strategy)}"

    @property
    def route_colon_id(self) -> str:
        return f"{_token(self.source)}:{_token(self.sink)}:{_token(self.strategy)}"

    @property
    def stream_id(self) -> str:
        return f"{self.route_id}__{_dataset_token(self.source_dataset)}__{_dataset_token(self.target_dataset)}"

    def to_dict(self) -> dict[str, object]:
        return {
            "pipeline_name": self.pipeline_name,
            "source": self.source,
            "sink": self.sink,
            "strategy": self.strategy,
            "backend": self.backend.value,
            "source_schema": self.source_schema,
            "source_table": self.source_table,
            "source_dataset": self.source_dataset,
            "target_dataset": self.target_dataset,
            "unique_key": list(self.unique_key),
            "route_id": self.route_id,
            "route_colon_id": self.route_colon_id,
            "stream_id": self.stream_id,
        }


@dataclass(frozen=True, slots=True)
class CdcRuntimePolicy:
    """Bounded runtime policy for one orchestrator tick."""

    max_changes: int = 10000
    require_idempotent: bool = True
    commit_empty_batches: bool = False
    poison_mode: PoisonMode = "fail_closed"


@dataclass(frozen=True, slots=True)
class CdcApplyReceipt:
    """Sink-side apply result consumed by the runtime orchestrator."""

    passed: bool
    durable: bool
    rows_applied: int
    rows_deleted: int
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    metrics: Mapping[str, object]
    artifact_uri: str

    def to_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "durable": self.durable,
            "rows_applied": self.rows_applied,
            "rows_deleted": self.rows_deleted,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "metrics": dict(self.metrics),
            "artifact_uri": self.artifact_uri,
        }


@dataclass(frozen=True, slots=True)
class CdcRuntimeRunReport:
    """Stable JSON/Markdown contract for one CDC runtime tick."""

    stream: CdcRuntimeStream
    start_offset: CDCOffset | None
    next_offset: CDCOffset | None
    high_watermark: str | None
    rows_read: int
    rows_applied: int
    rows_deleted: int
    committed: bool
    passed: bool
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    metrics: Mapping[str, object]
    sink_receipt: CdcApplyReceipt | None
    output_dir: str
    json_path: str
    markdown_path: str
    poison_quarantine: CdcPoisonQuarantineReport | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "stream": self.stream.to_dict(),
            "start_offset": _offset_dict(self.start_offset),
            "next_offset": _offset_dict(self.next_offset),
            "high_watermark": self.high_watermark,
            "rows_read": self.rows_read,
            "rows_applied": self.rows_applied,
            "rows_deleted": self.rows_deleted,
            "committed": self.committed,
            "passed": self.passed,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "metrics": dict(self.metrics),
            "sink_receipt": self.sink_receipt.to_dict() if self.sink_receipt else None,
            "poison_quarantine": self.poison_quarantine.to_dict() if self.poison_quarantine else None,
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# CDC runtime run",
            "",
            f"- Pipeline: `{self.stream.pipeline_name}`",
            f"- Route: `{self.stream.route_id}`",
            f"- Stream: `{self.stream.stream_id}`",
            f"- Passed: `{self.passed}`",
            f"- Committed: `{self.committed}`",
            f"- Rows read: `{self.rows_read}`",
            f"- Rows applied: `{self.rows_applied}`",
            f"- Rows deleted: `{self.rows_deleted}`",
            "",
            "## Offsets",
            "",
            f"- Start: `{self.start_offset.token if self.start_offset else ''}`",
            f"- Next: `{self.next_offset.token if self.next_offset else ''}`",
            f"- High watermark: `{self.high_watermark or ''}`",
            "",
            "## Blockers",
            "",
        ]
        lines.extend(f"- `{item}`" for item in self.blockers) if self.blockers else lines.append("- none")
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- `{item}`" for item in self.warnings) if self.warnings else lines.append("- none")
        lines.extend(["", "## Metrics", ""])
        lines.extend(f"- `{name}`: `{value}`" for name, value in self.metrics.items())
        lines.append("")
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")


def _offset_dict(offset: CDCOffset | None) -> dict[str, object] | None:
    return offset.to_state() if offset else None


def _token(value: str) -> str:
    return value.strip().lower().replace("-", "_").replace(" ", "_")


def _dataset_token(value: str) -> str:
    return _token(value).replace(".", "_")
