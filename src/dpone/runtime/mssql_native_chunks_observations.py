"""Observed phase concurrency; requested workers are never measured evidence."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def summarize_native_phases(observations: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    """Report phase span, worker-active time, peak overlap and effective parallelism.

    Durations overlap across phases and must not be summed into elapsed runtime.
    Failed imports count as active work. An absent phase has no available ratio.
    """
    result = {}
    for phase in ("encode", "import_verify"):
        selected = [row for row in observations if row["phase"] == phase]
        if not selected:
            result[phase] = dict(
                operations=0,
                failed_operations=0,
                wall_seconds=0.0,
                worker_active_seconds=0.0,
                peak_workers=0,
                effective_parallelism=None,
            )
            continue
        wall = max(row["end"] for row in selected) - min(row["start"] for row in selected)
        active = sum(row["end"] - row["start"] for row in selected)
        events = sorted((row[key], delta) for row in selected for key, delta in [("start", 1), ("end", -1)])
        current, peak = 0, 0
        for _, delta in events:
            current += delta
            peak = max(peak, current)
        result[phase] = dict(
            operations=len(selected),
            failed_operations=sum(row.get("outcome") == "failed" for row in selected),
            wall_seconds=wall,
            worker_active_seconds=active,
            peak_workers=peak,
            effective_parallelism=None if wall <= 0 else active / wall,
        )
    return result
