"""Deterministic synthetic datasets for route conformance."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from decimal import Decimal

from dpone.ops.routes.conformance_models import (
    RouteConformanceColumn,
    RouteConformanceDataset,
    RouteConformanceDatasetProfile,
)


class SyntheticRouteDatasetFactory:
    """Generate deterministic route-agnostic source fixtures."""

    def generate(self, profile: RouteConformanceDatasetProfile) -> RouteConformanceDataset:
        columns = _columns(profile)
        rows = tuple(_row(index, columns, include_nested=profile.include_nested) for index in range(profile.row_count))
        evolution = _schema_evolution_plan(profile)
        return RouteConformanceDataset(
            profile=profile,
            columns=columns,
            rows=rows,
            fingerprint=_fingerprint(columns, rows, evolution),
            schema_evolution_plan=evolution,
        )


def _columns(profile: RouteConformanceDatasetProfile) -> tuple[RouteConformanceColumn, ...]:
    base = [
        RouteConformanceColumn("id", "integer", "bigint", False, 0, primary_key=True),
        RouteConformanceColumn("parent_id", "integer", "bigint", True, 1, parent_key=True),
        RouteConformanceColumn("updated_at", "timestamp", "datetime2(6)", False, 2),
        RouteConformanceColumn("text_001", "text", "nvarchar(max)", True, 3),
        RouteConformanceColumn("decimal_001", "decimal", "decimal(38,10)", False, 4),
        RouteConformanceColumn("bool_001", "boolean", "bit", False, 5),
        RouteConformanceColumn("date_001", "date", "date", False, 6),
        RouteConformanceColumn("binary_001", "binary", "varbinary(max)", True, 7),
        RouteConformanceColumn("json_001", "json", "nvarchar(max)", True, 8),
    ]
    if profile.include_nested:
        base.append(RouteConformanceColumn("nested_payload", "json", "nvarchar(max)", True, len(base)))
    while len(base) < profile.column_count:
        base.append(_generated_column(len(base)))
    return tuple(base[: profile.column_count])


def _generated_column(ordinal: int) -> RouteConformanceColumn:
    family = ordinal % 6
    suffix = f"{ordinal:03d}"
    if family == 0:
        return RouteConformanceColumn(f"int_{suffix}", "integer", "bigint", False, ordinal)
    if family == 1:
        return RouteConformanceColumn(f"decimal_{suffix}", "decimal", "decimal(38,10)", False, ordinal)
    if family == 2:
        return RouteConformanceColumn(f"text_{suffix}", "text", "nvarchar(max)", True, ordinal)
    if family == 3:
        return RouteConformanceColumn(f"timestamp_{suffix}", "timestamp", "datetime2(6)", False, ordinal)
    if family == 4:
        return RouteConformanceColumn(f"bool_{suffix}", "boolean", "bit", False, ordinal)
    return RouteConformanceColumn(f"json_{suffix}", "json", "nvarchar(max)", True, ordinal)


def _row(index: int, columns: Sequence[RouteConformanceColumn], *, include_nested: bool) -> Mapping[str, object]:
    row_id = index + 1
    return {column.name: _value(column, row_id=row_id, include_nested=include_nested) for column in columns}


def _value(column: RouteConformanceColumn, *, row_id: int, include_nested: bool) -> object:
    if column.name == "id":
        return row_id
    if column.name == "parent_id":
        return None if row_id == 1 else max(1, row_id // 2)
    if column.name == "updated_at" or column.logical_type == "timestamp":
        return f"2026-06-{(row_id % 28) + 1:02d}T12:{row_id % 60:02d}:00.000000"
    if column.logical_type == "integer":
        return row_id * 10 + column.ordinal
    if column.logical_type == "decimal":
        return str(Decimal(row_id * 1000 + column.ordinal) / Decimal("100.0000000000"))
    if column.logical_type == "boolean":
        return (row_id + column.ordinal) % 2 == 0
    if column.logical_type == "date":
        return f"2026-06-{(row_id % 28) + 1:02d}"
    if column.logical_type == "binary":
        return hashlib.sha256(f"{row_id}:{column.name}".encode()).hexdigest()[:24]
    if column.name == "nested_payload" and include_nested:
        return {"id": row_id, "parent_id": None if row_id == 1 else max(1, row_id // 2), "items": [row_id, row_id + 1]}
    if column.logical_type == "json":
        return {"row": row_id, "column": column.name, "even": row_id % 2 == 0}
    return f"{column.name}:{row_id:06d}"


def _schema_evolution_plan(profile: RouteConformanceDatasetProfile) -> Mapping[str, object]:
    if not profile.include_schema_evolution:
        return {"enabled": False, "operations": []}
    return {
        "enabled": True,
        "operations": ["add_column", "widen_decimal", "make_nullable"],
        "expected_contracts": {
            "add_column": "nullable column with deterministic default",
            "widen_decimal": "precision increases without value drift",
            "make_nullable": "nullable relaxation only",
        },
    }


def _fingerprint(
    columns: Sequence[RouteConformanceColumn],
    rows: Sequence[Mapping[str, object]],
    evolution: Mapping[str, object],
) -> str:
    payload = {
        "columns": [column.to_dict() for column in columns],
        "rows": [dict(row) for row in rows],
        "schema_evolution": dict(evolution),
    }
    data = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


__all__ = ["SyntheticRouteDatasetFactory"]
