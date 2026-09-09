"""Quality gates for route-capability certification runs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

REQUIRED_LINEAGE_COLUMNS = (
    "__dpone__run_id",
    "__dpone__load_id",
    "__dpone__loaded_at",
    "__dpone__extracted_at",
)


class RouteQualityGate:
    """Compare source and certification target metrics without owning extraction."""

    def evaluate(
        self,
        *,
        route_id: str,
        source_quality: Mapping[str, object],
        target_quality: Mapping[str, object],
        cleanup: Mapping[str, object],
    ) -> dict[str, object]:
        checks = {
            "row_count": _equality_check(source_quality, target_quality, "row_count"),
            "null_counts": _equality_check(source_quality, target_quality, "null_counts"),
            "distinct_counts": _equality_check(source_quality, target_quality, "distinct_counts"),
            "typed_hash": _equality_check(source_quality, target_quality, "typed_hash"),
            "lineage_columns": _lineage_check(target_quality),
            "cleanup": _cleanup_check(cleanup, target_quality),
        }
        blockers = _blockers(route_id, checks)
        return {
            "route_id": route_id,
            "passed": not blockers,
            "checks": checks,
            "blockers": blockers,
        }


def _equality_check(
    source_quality: Mapping[str, object],
    target_quality: Mapping[str, object],
    key: str,
) -> dict[str, object]:
    source_value = source_quality.get(key)
    target_value = target_quality.get(key)
    return {
        "passed": source_value == target_value,
        "source": source_value,
        "target": target_value,
    }


def _lineage_check(target_quality: Mapping[str, object]) -> dict[str, object]:
    present = tuple(str(item) for item in _sequence(target_quality.get("lineage_columns")))
    missing = [column for column in REQUIRED_LINEAGE_COLUMNS if column not in present]
    return {"passed": not missing, "present": list(present), "missing": missing}


def _cleanup_check(cleanup: Mapping[str, object], target_quality: Mapping[str, object]) -> dict[str, object]:
    stale = [str(item) for item in _sequence(cleanup.get("stale_artifacts"))]
    if not stale:
        target_cleanup = target_quality.get("cleanup")
        if isinstance(target_cleanup, Mapping):
            stale = [str(item) for item in _sequence(target_cleanup.get("stale_artifacts"))]
    return {"passed": not stale, "stale_artifacts": stale}


def _blockers(route_id: str, checks: Mapping[str, Mapping[str, object]]) -> list[str]:
    blockers: list[str] = []
    for name, check in checks.items():
        if bool(check.get("passed")):
            continue
        suffix = "missing" if name == "lineage_columns" else "mismatch"
        blockers.append(f"{route_id}.{name}_{suffix}")
    return blockers


def _sequence(value: object) -> Sequence[Any]:
    return value if isinstance(value, (list, tuple)) else ()


__all__ = ["REQUIRED_LINEAGE_COLUMNS", "RouteQualityGate"]
