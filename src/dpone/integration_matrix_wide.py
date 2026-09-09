"""Wide mock row generation for integration matrix tests."""

from __future__ import annotations

from typing import Any

from dpone.integration_matrix_constants import MatrixRow


def _mock_row(row_id: int, *, source: str, variant: str, op: str | None = None) -> MatrixRow:
    business_date = (
        "2026-06-03" if variant.startswith(("source", "incremental", "xmin", "cdc", "replace")) else "2026-06-02"
    )
    row: MatrixRow = {
        "id": row_id,
        "business_date": business_date,
        "value": f"{variant}_{row_id}",
        "updated_at": f"2026-06-03T{row_id % 24:02d}:{row_id % 60:02d}:00Z",
        "source_family": source,
        "mock_variant": variant,
    }
    if op:
        row["__dpone__op"] = op
    row.update(_wide_type_payload(row_id=row_id, source=source))
    return row


def _wide_type_payload(*, row_id: int, source: str) -> MatrixRow:
    payload: MatrixRow = {}
    type_families = (
        "bool",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "numeric",
        "decimal",
        "float32",
        "float64",
        "money",
        "date",
        "time",
        "timestamp",
        "timestamptz",
        "datetime2",
        "datetimeoffset",
        "uuid",
        "char",
        "varchar",
        "text",
        "nchar",
        "nvarchar",
        "binary",
        "varbinary",
        "json",
        "jsonb",
        "xml",
        "array_int",
        "array_text",
        "range_int",
        "range_ts",
        "enum",
        "domain",
        "composite",
        "point",
        "line",
        "polygon",
        "inet",
        "cidr",
        "macaddr",
        "bit",
        "varbit",
        "tsvector",
        "tsquery",
        "pg_lsn",
        "oid",
        "rowversion",
        "sql_variant",
        "hierarchyid",
        "geometry",
        "geography",
        "low_cardinality",
        "nullable",
        "map",
        "tuple",
        "nested",
        "ipv4",
        "ipv6",
    )
    for index in range(1, 121):
        family = type_families[(index - 1) % len(type_families)]
        value = None if _is_sparse_null(row_id=row_id, index=index) else _wide_value(family, row_id=row_id, index=index)
        payload[f"wide_{source}_{index:03d}_{family}"] = value
    return payload


def _is_sparse_null(*, row_id: int, index: int) -> bool:
    bucket = index % 10
    if bucket == 0:
        return row_id % 10 != 0
    if bucket in {1, 2}:
        return row_id % 2 == 0
    if bucket in {3, 4, 5}:
        return row_id % 4 == 0
    return False


def _wide_value(family: str, *, row_id: int, index: int) -> Any:
    if family in {"bool", "nullable"}:
        return (row_id + index) % 2 == 0
    if family.startswith("int") or family.startswith("uint") or family in {"oid", "rowversion"}:
        return row_id * 1000 + index
    if family in {"numeric", "decimal", "money", "float32", "float64"}:
        return f"{row_id}.{index:04d}"
    if family == "date":
        return "2026-06-03"
    if family == "time":
        return "10:00:00"
    if family in {"timestamp", "timestamptz", "datetime2", "datetimeoffset"}:
        return "2026-06-03T10:00:00Z"
    if family == "uuid":
        return f"00000000-0000-0000-{row_id % 10000:04d}-{index:012d}"
    if family in {"binary", "varbinary"}:
        return f"0x{row_id:02x}{index:02x}"
    if family in {"json", "jsonb", "map", "tuple", "nested", "composite", "sql_variant"}:
        return {"row_id": row_id, "index": index, "family": family}
    if family.startswith("array"):
        return [row_id, index, family]
    if family.startswith("range"):
        return f"[{row_id},{row_id + index})"
    if family in {"point", "geometry", "geography"}:
        return f"POINT({row_id} {index})"
    if family in {"line", "polygon"}:
        return f"{family.upper()}({row_id},{index})"
    if family == "inet":
        return f"192.0.2.{(row_id + index) % 255}"
    if family == "cidr":
        return "192.0.2.0/24"
    if family == "macaddr":
        return f"02:00:00:00:{row_id % 100:02d}:{index % 100:02d}"
    if family in {"bit", "varbit"}:
        return "101010"
    if family == "pg_lsn":
        return f"0/{row_id + index:X}"
    return f"{family}_{row_id}_{index}"


__all__ = [
    "_mock_row",
    "_wide_type_payload",
    "_is_sparse_null",
    "_wide_value",
]
