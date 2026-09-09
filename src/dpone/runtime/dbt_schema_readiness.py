"""Fail-closed dbt relation contract gate before sink staging."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dpone.contracts.mssql_type_contract import mssql_logical_type


class DbtSchemaReadinessError(RuntimeError):
    """Stable schema-drift failure without row values."""

    code = "DPONE_DBT_SCHEMA_DRIFT"


@dataclass(frozen=True, slots=True)
class DbtSchemaReadinessReport:
    expected_columns: tuple[str, ...]
    observed_columns: tuple[str, ...]
    checked_types: int
    checked_nullability: int


class DbtSchemaReadinessGate:
    """Compare one observed source relation with its compiled dbt contract."""

    def validate(
        self,
        *,
        readiness: Mapping[str, Any],
        schema_contract: Mapping[str, Any],
        source_database: str,
        source_schema: str,
        source_table: str,
        observed_schema: Sequence[tuple[str, str]],
    ) -> DbtSchemaReadinessReport:
        if readiness.get("enabled") is not True:
            raise DbtSchemaReadinessError("dbt schema readiness must be explicitly enabled")
        relation = _mapping(readiness.get("source_relation"))
        if (
            str(relation.get("database") or "") != source_database
            or str(relation.get("schema") or "") != source_schema
            or str(relation.get("name") or "") != source_table
        ):
            raise DbtSchemaReadinessError("resolved source relation differs from the compiled dbt relation")
        expected_order = _string_tuple(readiness.get("column_order"))
        observed_order = tuple(str(name) for name, _ in observed_schema)
        if not expected_order or observed_order != expected_order:
            raise DbtSchemaReadinessError("source columns differ from the compiled dbt order")
        columns = _mapping(schema_contract.get("columns"))
        if set(columns) != set(expected_order):
            raise DbtSchemaReadinessError("compiled schema contract and dbt column order disagree")
        required_constraints = _mapping(readiness.get("required_constraints"))
        for name, observed_type in observed_schema:
            expected = _mapping(columns.get(name))
            observed = _observed_type(observed_type)
            if not _compatible(expected, observed):
                raise DbtSchemaReadinessError(f"source column type differs from the dbt contract: {name}")
            expected_nullable = expected.get("nullable")
            if not isinstance(expected_nullable, bool) or observed.nullable != expected_nullable:
                raise DbtSchemaReadinessError(f"source column nullability differs from the dbt contract: {name}")
            constraints = _string_tuple(required_constraints.get(name))
            unsupported = set(constraints) - {"not_null"}
            if unsupported:
                raise DbtSchemaReadinessError(f"source constraint cannot be proven safely: {name}")
            if "not_null" in constraints and observed.nullable:
                raise DbtSchemaReadinessError(f"source required constraint differs from the dbt contract: {name}")
        return DbtSchemaReadinessReport(
            expected_order,
            observed_order,
            len(observed_schema),
            len(observed_schema),
        )


@dataclass(frozen=True, slots=True)
class _ObservedType:
    family: str
    precision: int | None
    scale: int | None
    nullable: bool


def _compatible(expected: Mapping[str, Any], observed: _ObservedType) -> bool:
    expected_family = _type_family(str(expected.get("type") or ""))
    if not expected_family or expected_family != observed.family:
        return False
    expected_precision = _optional_int(expected.get("precision"))
    expected_scale = _optional_int(expected.get("scale"))
    return (expected_precision is None or expected_precision == observed.precision) and (
        expected_scale is None or expected_scale == observed.scale
    )


def _observed_type(value: str) -> _ObservedType:
    normalized = value.strip().lower()
    nullable = normalized.endswith(" nullable")
    if nullable:
        normalized = normalized[: -len(" nullable")].rstrip()
    try:
        logical = mssql_logical_type(normalized)
    except ValueError as exc:
        raise DbtSchemaReadinessError("source column type has no safe canonical mapping") from exc
    return _ObservedType(
        family=str(logical["type"]),
        precision=_optional_int(logical.get("precision")),
        scale=_optional_int(logical.get("scale")),
        nullable=nullable,
    )


def _type_family(normalized: str) -> str:
    normalized = normalized.strip().lower()
    if normalized in {"integer", "int", "smallint", "tinyint"}:
        return "integer"
    if normalized in {"bigint"}:
        return "bigint"
    if normalized in {"decimal", "numeric"} or re.match(r"^(?:decimal|numeric)\(", normalized):
        return "decimal"
    if normalized in {"date"}:
        return "date"
    if normalized in {
        "timestamp with time zone",
        "timestamptz",
        "datetimeoffset",
    }:
        return "timestamp with time zone"
    if normalized.startswith(("datetime", "timestamp")):
        return "timestamp"
    if normalized in {"string", "text"} or normalized.startswith(("char", "varchar", "nchar", "nvarchar")):
        return "string"
    if normalized in {"boolean", "bool", "bit"}:
        return "boolean"
    if normalized in {"uuid", "uniqueidentifier"}:
        return "uuid"
    if normalized in {"binary", "varbinary", "image", "rowversion"}:
        return "binary"
    if normalized in {"float32", "real"}:
        return "float32"
    if normalized in {"float64", "float", "double", "double precision"}:
        return "float64"
    if normalized == "time" or normalized.startswith("time("):
        return "time"
    return normalized


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _string_tuple(value: object) -> tuple[str, ...]:
    if not isinstance(value, list | tuple) or any(not isinstance(item, str) or not item for item in value):
        return ()
    return tuple(value)


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise DbtSchemaReadinessError("compiled dbt type parameters are invalid")
    return value


__all__ = [
    "DbtSchemaReadinessError",
    "DbtSchemaReadinessGate",
    "DbtSchemaReadinessReport",
]
