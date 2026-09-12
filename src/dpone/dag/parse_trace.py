"""Parse-time trace helpers (Step 19: post-parse provenance).

This module provides a tiny, dependency-light tracing API that can be used by
ETLProcessConfig.from_dict() to record *how* raw config mappings are normalized
into runtime dataclasses (LoadConfig, DependencyConfig, etc.).

Design goals
------------
- Zero impact on production logic when tracing is disabled.
- Keep it JSON/YAML friendly (records are serializable).
- Avoid tight coupling with CLI/UX code.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol


class LoadConfigParseTracer(Protocol):
    def record(
        self,
        *,
        kind: str,
        target: str,
        value: object,
        sources: tuple[str, ...],
        operation: str,
        details: dict[str, object] | None = None,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class ParseTraceRecord:
    """A single trace record produced during normalization."""

    kind: str
    """Record category, e.g. 'etl.field', 'load_config.field', 'dependency', 'option.key'."""

    target: str
    """Normalized (post-parse) dot-path, e.g. 'load_config.target_schema', 'dependencies[0].path'."""

    value: Any
    """Final normalized value."""

    sources: tuple[str, ...] = ()
    """Raw config dot-path(s) used to compute `value` (best-effort)."""

    operation: str = "copy"
    """Normalization operation: copy | default | merge | parse_enum | resolve_path | derive | inject | ..."""

    details: dict[str, Any] = field(default_factory=dict)
    """Extra structured context (safe to serialize)."""


@dataclass(frozen=True, slots=True)
class ParseTrace:
    base_path: str | None
    records: tuple[ParseTraceRecord, ...]
    warnings: tuple[str, ...] = ()


class ParseTracer:
    """A small mutable collector used during parsing."""

    def __init__(self, *, base_path: Path | None = None) -> None:
        self._base_path = str(base_path) if base_path else None
        self._records: list[ParseTraceRecord] = []
        self._warnings: list[str] = []

    @property
    def base_path(self) -> str | None:
        return self._base_path

    def warn(self, msg: str) -> None:
        s = str(msg).strip()
        if s:
            self._warnings.append(s)

    def record(
        self,
        *,
        kind: str,
        target: str,
        value: Any,
        sources: Sequence[str] = (),
        operation: str = "copy",
        details: dict[str, Any] | None = None,
    ) -> None:
        self._records.append(
            ParseTraceRecord(
                kind=str(kind),
                target=str(target),
                value=value,
                sources=tuple(str(s) for s in sources if str(s)),
                operation=str(operation),
                details=dict(details or {}),
            )
        )

    def extend(self, records: Iterable[ParseTraceRecord]) -> None:
        self._records.extend(list(records))

    def to_trace(self) -> ParseTrace:
        return ParseTrace(
            base_path=self._base_path,
            records=tuple(self._records),
            warnings=tuple(self._warnings),
        )


def record_load_fields(
    *,
    parse_tracer: LoadConfigParseTracer,
    source_options: dict[str, Any],
    sink_options: dict[str, Any],
    strategy_cfg: dict[str, Any],
    sink_custom_predicate: Any,
) -> None:
    parse_tracer.record(
        kind="load_config.field",
        target="load_config.unique_key",
        value=source_options.get("unique_key"),
        sources=("source.options.unique_key",),
        operation="copy" if "unique_key" in source_options else "default",
    )
    parse_tracer.record(
        kind="load_config.field",
        target="load_config.custom_predicate",
        value=sink_custom_predicate,
        sources=("sink.strategy.custom_predicate",),
        operation="copy" if "custom_predicate" in strategy_cfg else "default",
    )
    parse_tracer.record(
        kind="load_config.field",
        target="load_config.batch_size",
        value=source_options.get("batch_size", 10000),
        sources=("source.options.batch_size",),
        operation="default" if "batch_size" not in source_options else "copy",
    )
    parse_tracer.record(
        kind="load_config.field",
        target="load_config.log_sample_rows",
        value=sink_options.get("log_sample_rows", 5),
        sources=("sink.options.log_sample_rows",),
        operation="default" if "log_sample_rows" not in sink_options else "copy",
    )
    parse_tracer.record(
        kind="load_config.field",
        target="load_config.export_format",
        value=source_options.get("export_format", "csv"),
        sources=("source.options.export_format",),
        operation="default" if "export_format" not in source_options else "copy",
    )
    parse_tracer.record(
        kind="load_config.field",
        target="load_config.compress_export",
        value=source_options.get("compress_export", False),
        sources=("source.options.compress_export",),
        operation="default" if "compress_export" not in source_options else "copy",
    )


def record_runtime_contract_fields(
    *,
    config: Mapping[str, Any],
    reconciliation: bool,
    tech_schema: Any,
    parse_tracer: LoadConfigParseTracer | None,
) -> None:
    """Record canonical top-level contract fields at their parse boundary."""

    if not parse_tracer:
        return
    parse_tracer.record(
        kind="load_config.field",
        target="load_config.reconciliation",
        value=reconciliation,
        sources=("reconciliation",),
        operation="default" if "reconciliation" not in config else "copy",
    )
    parse_tracer.record(
        kind="load_config.field",
        target="load_config.tech_schema",
        value=tech_schema,
        sources=("tech_schema",),
        operation="default" if "tech_schema" not in config else "copy",
    )
