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

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


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
