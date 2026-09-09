"""Exact ClickHouse target-schema proof for wide MSSQL route certification."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any

from dpone.runtime.support.type_mapping.mssql_clickhouse import (
    MssqlClickHouseTypeMapper,
    MssqlClickHouseTypePolicy,
)


@dataclass(frozen=True, slots=True)
class ClickHouseTargetSchemaMetrics:
    """Expected and observed ordered target schema with an exact mismatch count."""

    column_count: int
    mismatch_count: int
    expected_sha256: str
    observed_sha256: str


def clickhouse_target_schema_metrics(
    *,
    mssql: Any,
    clickhouse: Any,
    source_schema: str,
    source_table: str,
    target_database: str,
    target_table: str,
    type_fidelity: object | None,
) -> ClickHouseTargetSchemaMetrics:
    """Compare the actual table with the exact policy-derived target schema."""

    source = tuple(mssql.fetch_schema(source_schema, source_table))
    expected = _expected_schema(source, type_fidelity)
    observed = tuple(
        (str(row[0]), _normalize_type(str(row[1])))
        for row in clickhouse.get_records(f"DESCRIBE TABLE {_identifier(target_database)}.{_identifier(target_table)}")
    )
    mismatch_count = abs(len(expected) - len(observed)) + sum(
        left != right for left, right in zip(expected, observed, strict=False)
    )
    return ClickHouseTargetSchemaMetrics(
        column_count=len(observed),
        mismatch_count=mismatch_count,
        expected_sha256=_schema_sha256(expected),
        observed_sha256=_schema_sha256(observed),
    )


def expected_clickhouse_schema_sha256(
    *,
    mssql: Any,
    source_schema: str,
    source_table: str,
    type_fidelity: object | None,
) -> str:
    """Derive the trusted target-schema digest without reading the target."""

    source = tuple(mssql.fetch_schema(source_schema, source_table))
    return _schema_sha256(_expected_schema(source, type_fidelity))


def _expected_schema(
    source: tuple[tuple[str, str], ...],
    type_fidelity: object | None,
) -> tuple[tuple[str, str], ...]:
    mapper = MssqlClickHouseTypeMapper(MssqlClickHouseTypePolicy.from_config(type_fidelity))
    decisions = mapper.resolve_schema(source)
    return tuple((name, _normalize_type(decisions[name].clickhouse_type)) for name, _ in source)


def _normalize_type(value: str) -> str:
    return re.sub(r"\s+", "", value.strip())


def _schema_sha256(value: tuple[tuple[str, str], ...]) -> str:
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _identifier(value: str) -> str:
    return "`" + str(value).replace("`", "``") + "`"


__all__ = [
    "ClickHouseTargetSchemaMetrics",
    "clickhouse_target_schema_metrics",
    "expected_clickhouse_schema_sha256",
]
