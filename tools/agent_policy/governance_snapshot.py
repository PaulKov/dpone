"""Bounded stable governance snapshot reads for SS-46 A/B/C."""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from typing import Any, NamedTuple

MIN_GAP_SECONDS = 5.0


class SnapshotBlocker(NamedTuple):
    code: str
    message: str


class StableSnapshot(NamedTuple):
    label: str
    projection: dict[str, Any]
    started_at: float
    completed_at: float
    read_count: int
    snapshot_json: str


def capture_stable_projection(
    *,
    label: str,
    reader: Callable[[], dict[str, Any]],
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    min_gap_seconds: float = MIN_GAP_SECONDS,
) -> tuple[StableSnapshot | None, tuple[SnapshotBlocker, ...]]:
    """Read a projection twice with a minimum gap; require identical canonical JSON."""

    started = clock()
    first = reader()
    first_json = _canonical_json(first)
    sleeper(min_gap_seconds)
    second = reader()
    second_json = _canonical_json(second)
    completed = clock()
    if first_json != second_json:
        return None, (
            SnapshotBlocker(
                "GOVERNANCE_SNAPSHOT_UNSTABLE",
                f"Governance snapshot {label} changed between bounded reads.",
            ),
        )
    if completed - started < min_gap_seconds:
        return None, (
            SnapshotBlocker(
                "GOVERNANCE_SNAPSHOT_GAP_TOO_SHORT",
                f"Governance snapshot {label} reads were closer than {min_gap_seconds} seconds.",
            ),
        )
    return (
        StableSnapshot(label, second, started, completed, 2, second_json),
        (),
    )


def protected_base_ancestry_ok(
    base_a: str, base_b: str, base_c: str, *, is_ancestor: Callable[[str, str], bool]
) -> bool:
    """Return True when base_A ≤ base_B ≤ base_C under the provided ancestry predicate."""

    return is_ancestor(base_a, base_b) and is_ancestor(base_b, base_c)


def _canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
