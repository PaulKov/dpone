"""Native nested spill fast-path planning."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from dpone.runtime.file_artifacts import FileExportArtifact


@dataclass(frozen=True, slots=True)
class NestedSpillFastPathDecision:
    sink_type: str
    spill_format: str
    artifact_kind: str
    native_route: str | None
    reason: str

    @property
    def uses_file_artifact(self) -> bool:
        return self.artifact_kind == "file"


class NestedSpillFastPathPlanner:
    """Choose whether a spilled nested table can be handed to sink-native loaders."""

    def plan(self, *, sink_type: str | None, spill_format: str, enabled: bool = True) -> NestedSpillFastPathDecision:
        normalized_sink = _normalize_sink(sink_type)
        normalized_format = str(spill_format or "jsonl").lower()
        if not enabled or normalized_sink is None:
            return _streaming(normalized_sink, normalized_format, "native fast path disabled or sink_type missing")
        route = _ROUTES.get((normalized_sink, normalized_format))
        if route is None:
            return _streaming(normalized_sink, normalized_format, "sink does not support this spill format natively")
        return NestedSpillFastPathDecision(
            sink_type=normalized_sink,
            spill_format=normalized_format,
            artifact_kind="file",
            native_route=route,
            reason="sink supports this native spill format",
        )


class NestedSpillArtifactFactory:
    """Build sink-facing artifacts from spilled nested table files."""

    def file_artifact(
        self,
        *,
        path: Path,
        schema: Sequence[tuple[str, str]],
        spill_format: str,
        estimated_rows: int | None,
    ) -> FileExportArtifact:
        return FileExportArtifact(
            str(path),
            [column for column, _column_type in schema],
            format=_artifact_format(spill_format),
            estimated_rows=estimated_rows,
        )


def _streaming(sink_type: str | None, spill_format: str, reason: str) -> NestedSpillFastPathDecision:
    return NestedSpillFastPathDecision(
        sink_type=sink_type or "unknown",
        spill_format=spill_format,
        artifact_kind="streaming",
        native_route=None,
        reason=reason,
    )


def _normalize_sink(sink_type: str | None) -> str | None:
    if sink_type is None:
        return None
    normalized = str(sink_type).strip().lower()
    aliases = {"sqlserver": "mssql", "ms_sql": "mssql", "postgresql": "postgres", "ch": "clickhouse"}
    return aliases.get(normalized, normalized) if normalized else None


def _artifact_format(spill_format: str) -> str:
    return "jsonl" if spill_format == "json_each_row" else spill_format


# File routes remain disabled until each exact writer/loader wire contract has
# retained end-to-end certification. Portable typed streaming is the safe default.
_ROUTES: dict[tuple[str, str], str] = {}
