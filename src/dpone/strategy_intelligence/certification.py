from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from dpone.strategy_intelligence.models import StrategyCertificationEntry
from dpone.strategy_intelligence.native_paths import NativeFastPathCatalog

_SOURCES = ("postgres", "mssql", "mysql", "clickhouse", "rest", "kafka")
_SINKS = ("mssql", "postgres", "clickhouse", "bigquery", "kafka")
_STRATEGIES = (
    "full_refresh",
    "incremental_append",
    "incremental_merge",
    "replace",
    "partition_replace",
    "snapshot_diff",
    "scd2",
    "cdc_apply",
    "backfill",
)
_PARTITION_SINKS = {"mssql", "postgres", "clickhouse", "bigquery"}
_DB_SINKS = {"mssql", "postgres", "clickhouse", "bigquery"}


class StrategyCertificationMatrix:
    def __init__(self, entries: tuple[StrategyCertificationEntry, ...]) -> None:
        self._entries = entries

    @property
    def entries(self) -> tuple[StrategyCertificationEntry, ...]:
        return self._entries

    def lookup(self, source_type: str, sink_type: str, strategy_mode: str) -> StrategyCertificationEntry:
        key = (source_type.lower(), sink_type.lower(), strategy_mode.lower())
        for entry in self._entries:
            if (entry.source_type, entry.sink_type, entry.strategy_mode) == key:
                return entry
        return StrategyCertificationEntry(
            source_type=key[0],
            sink_type=key[1],
            strategy_mode=key[2],
            status="not_supported",
            native_fast_path="streaming_rows",
            required_evidence=(),
        )

    def to_dict(self) -> dict[str, object]:
        return {"entries": [entry.to_dict() for entry in self._entries]}


class StrategyCertificationMatrixService:
    """Build the strategy certification matrix used by docs and manual gates."""

    def __init__(self, native_paths: NativeFastPathCatalog | None = None) -> None:
        self._native_paths = native_paths or NativeFastPathCatalog()

    def build(self) -> StrategyCertificationMatrix:
        entries: list[StrategyCertificationEntry] = []
        for source in _SOURCES:
            for sink in _SINKS:
                for strategy in _STRATEGIES:
                    entries.append(self._entry(source, sink, strategy))
        return StrategyCertificationMatrix(tuple(entries))

    def _entry(self, source: str, sink: str, strategy: str) -> StrategyCertificationEntry:
        if source == "clickhouse" and sink == "mssql":
            status = {
                "full_refresh": "manual_live_gate",
                "replace": "manual_live_gate",
                "partition_replace": "manual_live_gate",
                "backfill": "manual_live_gate",
            }.get(strategy, "not_supported")
        elif sink == "kafka" and strategy == "partition_replace":
            status = "not_supported"
        elif strategy == "partition_replace" and sink in _PARTITION_SINKS:
            status = "manual_live_gate"
        elif strategy in {"scd2", "snapshot_diff"} and sink in _DB_SINKS:
            status = "manual_live_gate"
        elif strategy == "cdc_apply" and (source in {"postgres", "mssql", "kafka"}) and sink in _SINKS:
            status = "manual_live_gate"
        elif strategy in {"full_refresh", "incremental_append", "incremental_merge", "replace", "backfill"}:
            status = "contract_gate"
        else:
            status = "not_supported"
        path = self._native_paths.resolve(source, sink).path_id
        evidence = (
            "manifest_example",
            "strategy_plan",
            "wide_120_column_gate",
            "quality_reconciliation",
            "run_artifact",
        )
        return StrategyCertificationEntry(
            source_type=source,
            sink_type=sink,
            strategy_mode=strategy,
            status=status,
            native_fast_path=path,
            required_evidence=evidence if status != "not_supported" else (),
        )


@dataclass(frozen=True, slots=True)
class StrategyCertificationArtifact:
    json_path: Path
    markdown_path: Path


class StrategyCertificationArtifactWriter:
    """Write strategy certification matrix evidence artifacts."""

    def __init__(self, base_dir: str | Path = "test_artifacts/strategies") -> None:
        self._base_dir = Path(base_dir)

    def write(self, matrix: StrategyCertificationMatrix) -> StrategyCertificationArtifact:
        self._base_dir.mkdir(parents=True, exist_ok=True)
        json_path = self._base_dir / "strategy_certification_matrix.json"
        markdown_path = self._base_dir / "strategy_certification_matrix.md"
        payload = matrix.to_dict()
        json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        markdown_path.write_text(_render_matrix_markdown(matrix), encoding="utf-8")
        return StrategyCertificationArtifact(json_path=json_path, markdown_path=markdown_path)


def _render_matrix_markdown(matrix: StrategyCertificationMatrix) -> str:
    lines = [
        "# dpone strategy certification matrix",
        "",
        "| Source | Sink | Strategy | Status | Native fast path |",
        "| --- | --- | --- | --- | --- |",
    ]
    for entry in matrix.entries:
        lines.append(
            f"| `{entry.source_type}` | `{entry.sink_type}` | `{entry.strategy_mode}` | "
            f"`{entry.status}` | `{entry.native_fast_path}` |"
        )
    return "\n".join(lines) + "\n"
