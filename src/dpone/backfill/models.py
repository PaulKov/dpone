"""Backfill chunk configuration models.

The chunk spec is declared in the manifest under ``sink.strategy.backfill``:

.. code-block:: yaml

    sink:
      strategy:
        mode: backfill
        backfill:
          inner_mode: partition_replace
          chunk:
            column: business_date
            from: "2025-01-01"
            to: "2025-12-31"
            step: 1d
          parallel_workers: 4
          max_chunks: 1000

Window semantics
----------------
- ``date`` / ``timestamp`` kinds produce half-open windows
  ``column >= start AND column < end`` so adjacent chunks never overlap and a
  re-run of one chunk replaces exactly its own slice (idempotent replays).
- ``integer`` kind produces inclusive windows
  ``column >= start AND column <= end`` matching the route-refresh contract.
- For ``date`` kind the configured ``to`` value is inclusive (a human writing
  ``to: 2025-12-31`` expects that day loaded), so the final exclusive boundary
  is ``to + 1 day``.
- ``uuid`` divides the complete UUID keyspace into ``buckets`` deterministic
  indexable ranges. It needs no data-dependent ``min``/``max`` discovery and
  therefore cannot miss rows inserted around an initial-load preflight.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from dpone.contracts.portable_relation_scope import (
    PortableRangeScope,
    portable_scope_contract,
    portable_scope_sha256,
)

SUPPORTED_CHUNK_KINDS = ("date", "timestamp", "integer", "uuid")
SUPPORTED_INNER_MODES = (
    "partition_replace",
    "replace",
    "incremental_append",
    "incremental_merge",
    "full_refresh",
)
DEFAULT_INNER_MODE = "partition_replace"
DEFAULT_MAX_CHUNKS = 1000

_STEP_PATTERN = re.compile(r"^(?P<count>\d+)\s*(?P<unit>h|d|w|mo|y)$", re.IGNORECASE)
_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass(frozen=True, slots=True)
class BackfillStep:
    """Parsed chunk step: either temporal (``unit`` set) or integer."""

    count: int
    unit: str | None = None

    @property
    def is_temporal(self) -> bool:
        return self.unit is not None


@dataclass(frozen=True, slots=True)
class BackfillChunkSpec:
    """Validated chunk window declaration from manifest options."""

    column: str
    start: str
    end: str
    step: str
    kind: str
    max_chunks: int = DEFAULT_MAX_CHUNKS
    buckets: int | None = None

    def fingerprint_parts(self) -> tuple[str, ...]:
        return (self.column, self.start, self.end, self.step, self.kind, str(self.buckets or ""))

    def to_jsonable(self) -> dict[str, Any]:
        """Return the exact public authoring shape used by policy identity."""

        if self.kind == "uuid":
            return {"column": self.column, "kind": self.kind, "buckets": self.buckets}
        return {
            "column": self.column,
            "from": self.start,
            "to": self.end,
            "step": self.step,
            "kind": self.kind,
        }


@dataclass(frozen=True, slots=True)
class BackfillChunk:
    """One deterministic chunk of a backfill plan.

    ``end`` is exclusive for ``date``/``timestamp`` kinds and inclusive for
    ``integer`` kind.  ``portable_scope`` is the canonical authority; SQL is
    rendered independently by each admitted source/target dialect.
    """

    index: int
    start: str
    end: str
    portable_scope: PortableRangeScope
    idempotency_key: str

    @property
    def portable_scope_sha256(self) -> str:
        """Return the canonical AST identity without rendered SQL."""

        return portable_scope_sha256(self.portable_scope).hex()

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "start": self.start,
            "end": self.end,
            "portable_scope": portable_scope_contract(self.portable_scope),
            "portable_scope_sha256": self.portable_scope_sha256,
            "idempotency_key": self.idempotency_key,
        }


def chunk_spec_from_options(backfill_options: Mapping[str, Any] | None) -> BackfillChunkSpec | None:
    """Parse ``options.backfill.chunk`` into a validated spec.

    Returns ``None`` when no chunk block is configured (single-shot backfill
    delegation stays available for backward compatibility).
    Raises ``ValueError`` with a self-service message on invalid input.
    """

    options = {} if backfill_options is None else backfill_options
    if not isinstance(options, Mapping):
        raise ValueError("backfill must be an object")
    if "chunk" not in options:
        return None
    raw_chunk = options.get("chunk")
    if not isinstance(raw_chunk, Mapping):
        raise ValueError("backfill.chunk must be a mapping with column/from/to/step keys")
    if not raw_chunk:
        raise ValueError("backfill.chunk must be a non-empty object")

    column = _required_chunk_column(raw_chunk)
    kind = _required_or_inferred_chunk_kind(raw_chunk)
    if kind == "uuid":
        return _uuid_chunk_spec(options, raw_chunk, column=column)

    start = _required_chunk_boundary(raw_chunk, "from")
    end = _required_chunk_boundary(raw_chunk, "to")
    step = _required_chunk_boundary(raw_chunk, "step")
    missing = [name for name, value in (("column", column), ("from", start), ("to", end), ("step", step)) if not value]
    if missing:
        raise ValueError("backfill.chunk requires keys: " + ", ".join(missing))

    if "kind" not in raw_chunk:
        kind = _infer_kind(start)
    if kind not in SUPPORTED_CHUNK_KINDS:
        raise ValueError(f"backfill.chunk.kind must be one of: {', '.join(SUPPORTED_CHUNK_KINDS)} (got {kind!r})")

    parse_step(step, kind=kind)
    _validate_boundaries(start=start, end=end, kind=kind)

    max_chunks = options["max_chunks"] if "max_chunks" in options else DEFAULT_MAX_CHUNKS
    if type(max_chunks) is not int:
        raise ValueError("backfill.max_chunks must be an integer")
    if max_chunks < 1:
        raise ValueError("backfill.max_chunks must be >= 1")

    return BackfillChunkSpec(column=column, start=start, end=end, step=step, kind=kind, max_chunks=max_chunks)


def inner_mode_from_options(backfill_options: Mapping[str, Any] | None) -> str:
    """Return the validated inner load mode for a backfill configuration."""

    options = {} if backfill_options is None else backfill_options
    if not isinstance(options, Mapping):
        raise ValueError("backfill must be an object")
    if "inner_mode" not in options:
        return DEFAULT_INNER_MODE
    raw_mode = options.get("inner_mode")
    if not isinstance(raw_mode, str) or not raw_mode.strip():
        raise ValueError("backfill.inner_mode must be a non-empty string")
    inner_mode = raw_mode.strip().lower()
    if inner_mode not in SUPPORTED_INNER_MODES:
        raise ValueError("backfill.inner_mode must be one of: " + ", ".join(sorted(SUPPORTED_INNER_MODES)))
    return inner_mode


def parallel_workers_from_options(backfill_options: Mapping[str, Any] | None) -> int:
    """Return the validated ``parallel_workers`` setting (default 1)."""

    options = {} if backfill_options is None else backfill_options
    if not isinstance(options, Mapping):
        raise ValueError("backfill must be an object")
    raw_workers = options.get("parallel_workers")
    if "parallel_workers" not in options:
        return 1
    if type(raw_workers) is not int:
        raise ValueError("backfill.parallel_workers must be an integer")
    workers = raw_workers
    if workers < 1:
        raise ValueError("backfill.parallel_workers must be >= 1")
    return workers


def parse_step(step: str, *, kind: str) -> BackfillStep:
    """Parse a chunk step token (``1d``, ``6h``, ``1w``, ``1mo``, ``1y`` or int)."""

    if kind == "uuid":
        raise ValueError("backfill.chunk.kind=uuid uses buckets instead of step")
    token = step.strip().lower()
    match = _STEP_PATTERN.match(token)
    if match:
        if kind == "integer":
            raise ValueError(f"backfill.chunk.step {step!r} is temporal but chunk kind is integer")
        unit = match.group("unit").lower()
        if kind == "date" and unit == "h":
            raise ValueError("backfill.chunk.step in hours requires kind: timestamp")
        return BackfillStep(count=int(match.group("count")), unit=unit)
    try:
        count = int(token)
    except ValueError as exc:
        raise ValueError(
            f"backfill.chunk.step {step!r} is not supported; use Nh/Nd/Nw/Nmo/Ny or an integer step"
        ) from exc
    if kind != "integer":
        raise ValueError(f"backfill.chunk.step {step!r} is an integer step but chunk kind is {kind}")
    if count < 1:
        raise ValueError("backfill.chunk.step must be >= 1")
    return BackfillStep(count=count, unit=None)


def parse_date_boundary(value: str) -> date:
    return date.fromisoformat(value)


def parse_timestamp_boundary(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _infer_kind(start: str) -> str:
    if _DATE_PATTERN.match(start):
        return "date"
    try:
        int(start)
    except ValueError:
        pass
    else:
        return "integer"
    try:
        datetime.fromisoformat(start)
    except ValueError:
        return ""
    return "timestamp"


def _required_or_inferred_chunk_kind(raw_chunk: Mapping[str, Any]) -> str:
    if "kind" not in raw_chunk:
        return ""
    raw_kind = raw_chunk.get("kind")
    if not isinstance(raw_kind, str) or not raw_kind.strip():
        raise ValueError("backfill.chunk.kind must be a non-empty string")
    kind = raw_kind.strip().lower()
    if kind not in SUPPORTED_CHUNK_KINDS:
        raise ValueError(f"backfill.chunk.kind must be one of: {', '.join(SUPPORTED_CHUNK_KINDS)} (got {kind!r})")
    return kind


def _uuid_chunk_spec(
    options: Mapping[str, Any],
    raw_chunk: Mapping[str, Any],
    *,
    column: str,
) -> BackfillChunkSpec:
    unsupported = sorted(str(key) for key in raw_chunk if key in {"from", "to", "step"})
    if unsupported:
        raise ValueError("backfill.chunk.kind=uuid uses buckets and forbids: " + ", ".join(unsupported))
    raw_buckets = raw_chunk.get("buckets")
    if type(raw_buckets) is not int or raw_buckets < 1:
        raise ValueError("backfill.chunk.buckets must be an integer >= 1 for kind=uuid")
    max_chunks = options["max_chunks"] if "max_chunks" in options else DEFAULT_MAX_CHUNKS
    if type(max_chunks) is not int or max_chunks < 1:
        raise ValueError("backfill.max_chunks must be an integer >= 1")
    if raw_buckets > max_chunks:
        raise ValueError(
            f"backfill would produce {raw_buckets} chunks, above max_chunks={max_chunks}; "
            "increase backfill.max_chunks or use fewer UUID buckets"
        )
    return BackfillChunkSpec(
        column=column,
        start="00000000-0000-0000-0000-000000000000",
        end="ffffffff-ffff-ffff-ffff-ffffffffffff",
        step=str(raw_buckets),
        kind="uuid",
        max_chunks=max_chunks,
        buckets=raw_buckets,
    )


def _required_chunk_column(raw_chunk: Mapping[str, Any]) -> str:
    if "column" not in raw_chunk:
        return ""
    value = raw_chunk["column"]
    if not isinstance(value, str) or not value.strip():
        raise ValueError("backfill.chunk.column must be a non-empty string")
    return value.strip()


def _required_chunk_boundary(raw_chunk: Mapping[str, Any], key: str) -> str:
    if key not in raw_chunk:
        return ""
    value = raw_chunk[key]
    if isinstance(value, bool) or not isinstance(value, (str, int)) or not str(value).strip():
        raise ValueError(f"backfill.chunk.{key} must be a string or integer")
    return str(value).strip()


def _validate_boundaries(*, start: str, end: str, kind: str) -> None:
    try:
        if kind == "integer":
            inverted = int(start) > int(end)
        elif kind == "date":
            inverted = parse_date_boundary(start) > parse_date_boundary(end)
        else:
            inverted = parse_timestamp_boundary(start) > parse_timestamp_boundary(end)
    except ValueError as exc:
        raise ValueError(f"backfill.chunk boundaries are not valid for kind={kind}: {exc}") from exc
    if inverted:
        raise ValueError("backfill.chunk.from must be <= backfill.chunk.to")


__all__ = [
    "DEFAULT_INNER_MODE",
    "DEFAULT_MAX_CHUNKS",
    "SUPPORTED_CHUNK_KINDS",
    "SUPPORTED_INNER_MODES",
    "BackfillChunk",
    "BackfillChunkSpec",
    "BackfillStep",
    "chunk_spec_from_options",
    "inner_mode_from_options",
    "parallel_workers_from_options",
    "parse_date_boundary",
    "parse_step",
    "parse_timestamp_boundary",
]
