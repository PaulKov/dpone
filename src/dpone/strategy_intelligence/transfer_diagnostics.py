"""Reusable diagnostics for native source-to-sink transfers.

The functions here intentionally use duck typing over artifact protocols so
they can describe file-backed artifacts from any source/sink pair without
depending on concrete runtime connectors.
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import Any, TypeVar

T = TypeVar("T")

_BYTES_IN_MIB = 1024 * 1024


@dataclass(frozen=True)
class PhaseMetric:
    """Timing metric for one transfer phase."""

    name: str
    rows: int
    seconds: float
    rows_per_second: float

    @classmethod
    def from_elapsed(cls, name: str, rows: int, seconds: float) -> PhaseMetric:
        rounded_seconds = round(seconds, 3)
        return cls(
            name=name,
            rows=int(rows),
            seconds=rounded_seconds,
            rows_per_second=round(rows / seconds, 2) if seconds > 0 else 0.0,
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def measure_phase(name: str, rows: int, fn: Callable[[], T]) -> tuple[T, PhaseMetric]:
    """Execute ``fn`` and return its value with a phase metric."""

    started = time.perf_counter()
    value = fn()
    seconds = time.perf_counter() - started
    return value, PhaseMetric.from_elapsed(name, rows, seconds)


def artifact_diagnostics(artifact: Any, *, phase_seconds: float | None = None) -> dict[str, Any]:
    """Describe file-backed artifact size and partition skew.

    The returned dictionary is JSON-friendly and stable for run artifacts.
    Missing files are represented with ``exists=false`` and zero bytes instead
    of raising, because diagnostics should not mask the primary transfer error.
    """

    partitions = list(getattr(artifact, "partitions", None) or [])
    if not partitions:
        partitions = [artifact] if getattr(artifact, "file_path", None) else []

    partition_diagnostics = [_partition_diagnostic(index, partition) for index, partition in enumerate(partitions)]
    total_bytes = sum(int(item["bytes"]) for item in partition_diagnostics)
    estimated_rows = _safe_int(getattr(artifact, "estimated_rows", None))
    row_estimates = [
        int(item["estimated_rows"]) for item in partition_diagnostics if item["estimated_rows"] is not None
    ]
    slowest_partition = max(partition_diagnostics, key=lambda item: item.get("estimated_rows") or 0, default=None)
    partition_row_skew = (max(row_estimates) - min(row_estimates)) if row_estimates else 0
    mb_per_second = None
    if phase_seconds and phase_seconds > 0:
        mb_per_second = round((total_bytes / _BYTES_IN_MIB) / phase_seconds, 6)

    return {
        "artifact_type": type(artifact).__name__,
        "partition_count": len(partition_diagnostics),
        "estimated_rows": estimated_rows,
        "total_bytes": total_bytes,
        "total_mb": round(total_bytes / _BYTES_IN_MIB, 3),
        "mb_per_second": mb_per_second,
        "partition_row_skew": partition_row_skew,
        "slowest_partition": slowest_partition,
        "partitions": partition_diagnostics,
    }


def _partition_diagnostic(index: int, partition: Any) -> dict[str, Any]:
    file_path = str(getattr(partition, "file_path", "") or "")
    exists = bool(file_path and os.path.exists(file_path))
    file_size = os.path.getsize(file_path) if exists else 0
    return {
        "index": index,
        "artifact_type": type(partition).__name__,
        "file_path": file_path,
        "exists": exists,
        "bytes": file_size,
        "mb": round(file_size / _BYTES_IN_MIB, 3),
        "estimated_rows": _safe_int(getattr(partition, "estimated_rows", None)),
    }


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
