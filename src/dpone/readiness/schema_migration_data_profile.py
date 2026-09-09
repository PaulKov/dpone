"""Provider-neutral data profiling for schema migration rehearsal fixtures."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dpone.readiness.migration_control import stable_fingerprint
from dpone.readiness.schema_migration_row_hash import typed_rows_hash

DATA_PROFILE_SCHEMA = "dpone.schema_migration_data_profile.v1"


@dataclass(frozen=True, slots=True)
class MigrationDataProfileAnalyzer:
    """Computes deterministic fixture quality profiles from JSON-like rows."""

    def profile(
        self,
        *,
        rows: Sequence[Mapping[str, Any]],
        pack_id: str,
        fixture_build_id: str,
        stage: str,
        key_columns: Sequence[str] = (),
        hierarchy: Mapping[str, Any] | None = None,
        checks: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        enabled = _enabled_checks(checks)
        normalized_rows = tuple(_row(row) for row in rows)
        blockers = _quality_blockers(
            rows=normalized_rows,
            key_columns=tuple(key_columns),
            hierarchy=hierarchy or {},
            checks=enabled,
        )
        metrics = _metrics(rows=normalized_rows, key_columns=tuple(key_columns), checks=enabled)
        status = "blocked" if blockers else "profiled"
        payload: dict[str, Any] = {
            "schema_version": DATA_PROFILE_SCHEMA,
            "pack_id": pack_id,
            "fixture_build_id": fixture_build_id,
            "stage": stage,
            "status": status,
            "checks": enabled,
            "metrics": metrics,
            "blockers": list(blockers),
            "warnings": [],
        }
        payload["profile_id"] = stable_fingerprint(
            {
                "pack_id": pack_id,
                "fixture_build_id": fixture_build_id,
                "stage": stage,
                "checks": enabled,
                "metrics": metrics,
                "blockers": blockers,
            }
        )
        return payload


def _enabled_checks(raw: Mapping[str, Any] | None) -> dict[str, bool]:
    defaults = {
        "row_count": True,
        "typed_hash": True,
        "null_distribution": True,
        "distinct_count": True,
        "min_max": True,
        "duplicate_key": True,
        "null_key": True,
        "nested_parent_child": True,
    }
    if raw is None:
        return defaults
    return {key: bool(raw.get(key, default)) for key, default in defaults.items()}


def _metrics(
    *,
    rows: Sequence[dict[str, Any]],
    key_columns: Sequence[str],
    checks: Mapping[str, bool],
) -> dict[str, Any]:
    columns = sorted({column for row in rows for column in row})
    metrics: dict[str, Any] = {"row_count": len(rows), "column_count": len(columns)}
    if checks.get("typed_hash"):
        metrics["typed_hash"] = typed_rows_hash(rows, key_columns)
    if checks.get("null_distribution"):
        metrics["null_distribution"] = {column: sum(1 for row in rows if row.get(column) is None) for column in columns}
    if checks.get("distinct_count"):
        metrics["distinct_count"] = {column: len({_json_key(row.get(column)) for row in rows}) for column in columns}
    if checks.get("min_max"):
        metrics["min_max"] = _min_max(rows, columns)
    return metrics


def _quality_blockers(
    *,
    rows: Sequence[dict[str, Any]],
    key_columns: Sequence[str],
    hierarchy: Mapping[str, Any],
    checks: Mapping[str, bool],
) -> tuple[str, ...]:
    blockers: list[str] = []
    if key_columns and checks.get("duplicate_key"):
        blockers.extend(_duplicate_key_blockers(rows, key_columns))
    if key_columns and checks.get("null_key"):
        blockers.extend(_null_key_blockers(rows, key_columns))
    if checks.get("nested_parent_child"):
        blockers.extend(_hierarchy_blockers(rows, hierarchy))
    return tuple(dict.fromkeys(blockers))


def _duplicate_key_blockers(rows: Sequence[dict[str, Any]], key_columns: Sequence[str]) -> list[str]:
    counts = Counter(tuple(row.get(column) for column in key_columns) for row in rows)
    duplicate = [key for key, count in counts.items() if count > 1 and all(item is not None for item in key)]
    return [f"schema_migration_profile.duplicate_key:{','.join(key_columns)}"] if duplicate else []


def _null_key_blockers(rows: Sequence[dict[str, Any]], key_columns: Sequence[str]) -> list[str]:
    return [
        f"schema_migration_profile.null_key:{column}"
        for column in key_columns
        if any(row.get(column) is None for row in rows)
    ]


def _hierarchy_blockers(rows: Sequence[dict[str, Any]], hierarchy: Mapping[str, Any]) -> list[str]:
    parent_column = str(hierarchy.get("parent_column", "")).strip()
    child_column = str(hierarchy.get("child_column", "")).strip()
    if not parent_column or not child_column:
        return []
    child_values = {row.get(child_column) for row in rows if row.get(child_column) is not None}
    orphans = [
        row.get(parent_column)
        for row in rows
        if row.get(parent_column) is not None and row.get(parent_column) not in child_values
    ]
    return [f"schema_migration_profile.orphan_child:{parent_column}"] if orphans else []


def _min_max(rows: Sequence[dict[str, Any]], columns: Sequence[str]) -> dict[str, dict[str, Any]]:
    values: dict[str, dict[str, Any]] = {}
    for column in columns:
        raw_values = [row.get(column) for row in rows if row.get(column) is not None]
        comparable = [value for value in raw_values if isinstance(value, str | int | float | bool)]
        if not comparable:
            continue
        try:
            values[column] = {"min": min(comparable), "max": max(comparable)}
        except TypeError:
            values[column] = {"min": min(map(str, comparable)), "max": max(map(str, comparable))}
    return values


def _row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): _jsonable(value) for key, value in row.items()}


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    if value is None or isinstance(value, str | int | float | bool):
        return value
    return str(value)


def _json_key(value: Any) -> str:
    return json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


__all__ = ["DATA_PROFILE_SCHEMA", "MigrationDataProfileAnalyzer", "typed_rows_hash"]
