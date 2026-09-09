"""Runtime payload helpers for type-matrix certification."""

from __future__ import annotations

from typing import Any

from dpone.type_system.source_sink.certification_models import (
    DecisionCategory,
    TypeCertificationCase,
    TypeCertificationSuite,
)
from dpone.type_system.source_sink.clickhouse_mssql import ClickHouseMssqlTypeMapper
from dpone.type_system.source_sink.mssql_bigquery import MSSQLBigQueryTypeMapper
from dpone.type_system.source_sink.mssql_clickhouse import (
    MssqlClickHouseMatrixMapper,
    MssqlClickHouseMatrixPolicy,
)
from dpone.type_system.source_sink.mysql_bigquery import MySQLBigQueryTypeMapper
from dpone.type_system.source_sink.mysql_clickhouse import MySQLClickHouseTypeMapper
from dpone.type_system.source_sink.mysql_mssql import MySQLMssqlTypeMapper
from dpone.type_system.source_sink.mysql_postgres import MySQLPostgresTypeMapper
from dpone.type_system.source_sink.postgres_bigquery import PostgresBigQueryTypeMapper
from dpone.type_system.source_sink.postgres_mssql import PostgresMssqlTypeMapper


def assert_certification_suite_matches_runtime_decisions(suite: TypeCertificationSuite) -> None:
    """Assert that certification fixtures still match pair-specific runtime mappers."""

    mismatches: list[str] = []
    for case in suite.cases:
        entry = case_matrix_entry(case)
        if entry["target_type"] != case.expected_target_type:
            mismatches.append(
                f"{case.source}->{case.sink} {case.name}: expected target {case.expected_target_type}, "
                f"runtime resolved {entry['target_type']}"
            )
        if entry["schema_evolution_compatible"] != case.compatible:
            mismatches.append(f"{case.source}->{case.sink} {case.name}: compatibility drift")
        if entry["decision_category"] != case.decision_category:
            mismatches.append(f"{case.source}->{case.sink} {case.name}: decision category drift")
    if mismatches:
        raise AssertionError("\n".join(mismatches))


def matrix_entry_for_mssql_clickhouse(
    *,
    column: str | None,
    source_type: str,
    policy: MssqlClickHouseMatrixPolicy | None = None,
) -> dict[str, Any]:
    decision = MssqlClickHouseMatrixMapper(policy or MssqlClickHouseMatrixPolicy()).resolve(source_type)
    return {
        "column": column,
        "source_type": source_type,
        "canonical_type": _canonical_mssql_type(source_type),
        "target_type": decision.target_type,
        "native_transport": decision.native_transport,
        "schema_evolution_compatible": decision.schema_evolution_compatible,
        "lossless": decision.lossless,
        "nullable_behavior": _nullable_behavior(source_type, decision.target_type),
        "requires_explicit_contract": decision.requires_explicit_contract,
        "decision_category": _category(decision.requires_explicit_contract),
        "decision_source": "source_metadata",
        "reason": decision.reason,
    }


def matrix_entry_for_postgres_mssql(*, column: str | None, source_type: str) -> dict[str, Any]:
    decision = PostgresMssqlTypeMapper().resolve(source_type)
    return {
        "column": column,
        "source_type": decision.source_type,
        "canonical_type": _canonical_postgres_type(source_type),
        "target_type": decision.target_type,
        "native_transport": decision.transfer_representation,
        "schema_evolution_compatible": decision.compatible,
        "lossless": True,
        "nullable_behavior": _nullable_behavior(source_type, decision.target_type),
        "requires_explicit_contract": decision.requires_explicit_contract,
        "decision_category": _category(decision.requires_explicit_contract),
        "decision_source": "source_metadata",
        "reason": decision.warning or "mapped by postgres_to_mssql_native_v2",
    }


def matrix_entry_for_mysql_mssql(*, column: str | None, source_type: str) -> dict[str, Any]:
    decision = MySQLMssqlTypeMapper().resolve(source_type)
    return {
        "column": column,
        "source_type": decision.source_type,
        "canonical_type": _canonical_mysql_type(source_type),
        "target_type": decision.target_type,
        "native_transport": decision.transfer_representation,
        "schema_evolution_compatible": decision.compatible,
        "lossless": decision.lossless and not decision.requires_explicit_contract,
        "nullable_behavior": _nullable_behavior(source_type, decision.target_type),
        "requires_explicit_contract": decision.requires_explicit_contract,
        "decision_category": _category(decision.requires_explicit_contract),
        "decision_source": "source_metadata",
        "reason": decision.warning or "mapped by mysql_to_mssql_native_v1",
    }


def matrix_entry_for_mysql_postgres(*, column: str | None, source_type: str) -> dict[str, Any]:
    decision = MySQLPostgresTypeMapper().resolve(source_type)
    return {
        "column": column,
        "source_type": decision.source_type,
        "canonical_type": _canonical_mysql_type(source_type),
        "target_type": decision.target_type,
        "native_transport": decision.transfer_representation,
        "schema_evolution_compatible": decision.compatible,
        "lossless": decision.lossless and not decision.requires_explicit_contract,
        "nullable_behavior": _nullable_behavior(source_type, decision.target_type),
        "requires_explicit_contract": decision.requires_explicit_contract,
        "decision_category": _category(decision.requires_explicit_contract),
        "decision_source": "source_metadata",
        "reason": decision.warning or "mapped by mysql_to_postgres_native_v1",
    }


def matrix_entry_for_mysql_clickhouse(*, column: str | None, source_type: str) -> dict[str, Any]:
    decision = MySQLClickHouseTypeMapper().resolve(source_type)
    return {
        "column": column,
        "source_type": decision.source_type,
        "canonical_type": _canonical_mysql_type(source_type),
        "target_type": decision.target_type,
        "native_transport": decision.transfer_representation,
        "schema_evolution_compatible": decision.compatible,
        "lossless": decision.lossless and not decision.requires_explicit_contract,
        "nullable_behavior": _nullable_behavior(source_type, decision.target_type),
        "requires_explicit_contract": decision.requires_explicit_contract,
        "decision_category": _category(decision.requires_explicit_contract),
        "decision_source": "source_metadata",
        "reason": decision.warning or "mapped by mysql_to_clickhouse_analytics_v1",
    }


def matrix_entry_for_mysql_bigquery(*, column: str | None, source_type: str) -> dict[str, Any]:
    decision = MySQLBigQueryTypeMapper().resolve(source_type)
    return {
        "column": column,
        "source_type": decision.source_type,
        "canonical_type": _canonical_mysql_type(source_type),
        "target_type": decision.target_type,
        "native_transport": decision.transfer_representation,
        "schema_evolution_compatible": decision.compatible,
        "lossless": decision.lossless and not decision.requires_explicit_contract,
        "nullable_behavior": _nullable_behavior(source_type, decision.target_type),
        "requires_explicit_contract": decision.requires_explicit_contract,
        "decision_category": _category(decision.requires_explicit_contract),
        "decision_source": "source_metadata",
        "reason": decision.warning or "mapped by mysql_to_bigquery_analytics_v1",
    }


def matrix_entry_for_postgres_bigquery(*, column: str | None, source_type: str) -> dict[str, Any]:
    decision = PostgresBigQueryTypeMapper().resolve(source_type)
    return {
        "column": column,
        "source_type": decision.source_type,
        "canonical_type": _canonical_postgres_type(source_type),
        "target_type": decision.target_type,
        "native_transport": decision.transfer_representation,
        "schema_evolution_compatible": decision.compatible,
        "lossless": decision.lossless and not decision.requires_explicit_contract,
        "nullable_behavior": _nullable_behavior(source_type, decision.target_type),
        "requires_explicit_contract": decision.requires_explicit_contract,
        "decision_category": _category(decision.requires_explicit_contract),
        "decision_source": "source_metadata",
        "reason": decision.warning or "mapped by postgres_to_bigquery_analytics_v1",
    }


def matrix_entry_for_mssql_bigquery(*, column: str | None, source_type: str) -> dict[str, Any]:
    decision = MSSQLBigQueryTypeMapper().resolve(source_type)
    return {
        "column": column,
        "source_type": decision.source_type,
        "canonical_type": _canonical_mssql_type(source_type),
        "target_type": decision.target_type,
        "native_transport": decision.transfer_representation,
        "schema_evolution_compatible": decision.compatible,
        "lossless": decision.lossless and not decision.requires_explicit_contract,
        "nullable_behavior": _nullable_behavior(source_type, decision.target_type),
        "requires_explicit_contract": decision.requires_explicit_contract,
        "decision_category": _category(decision.requires_explicit_contract),
        "decision_source": "source_metadata",
        "reason": decision.warning or "mapped by mssql_to_bigquery_analytics_v1",
    }


def matrix_entry_for_clickhouse_mssql(*, column: str | None, source_type: str) -> dict[str, Any]:
    decision = ClickHouseMssqlTypeMapper().resolve(source_type)
    return {
        "column": column,
        "source_type": decision.source_type,
        "canonical_type": _canonical_clickhouse_type(source_type),
        "target_type": decision.target_type,
        "native_transport": decision.native_transport,
        "schema_evolution_compatible": decision.compatible,
        "lossless": decision.lossless,
        "nullable_behavior": _nullable_behavior(source_type, decision.target_type),
        "requires_explicit_contract": decision.requires_explicit_contract,
        "decision_category": _category(decision.requires_explicit_contract),
        "decision_source": "source_metadata",
        "reason": decision.warning or "mapped by clickhouse_to_mssql_landing_v1",
    }


def case_matrix_entry(case: TypeCertificationCase) -> dict[str, Any]:
    if case.source == "mssql" and case.sink == "clickhouse":
        return matrix_entry_for_mssql_clickhouse(
            column=case.name,
            source_type=case.source_spec,
            policy=MssqlClickHouseMatrixPolicy.from_config(dict(case.type_fidelity or {})),
        )
    if case.source == "postgres" and case.sink == "mssql":
        return matrix_entry_for_postgres_mssql(column=case.name, source_type=case.source_spec)
    if case.source == "mysql" and case.sink == "mssql":
        return matrix_entry_for_mysql_mssql(column=case.name, source_type=case.source_spec)
    if case.source == "mysql" and case.sink == "postgres":
        return matrix_entry_for_mysql_postgres(column=case.name, source_type=case.source_spec)
    if case.source == "mysql" and case.sink == "clickhouse":
        return matrix_entry_for_mysql_clickhouse(column=case.name, source_type=case.source_spec)
    if case.source == "mysql" and case.sink == "bigquery":
        return matrix_entry_for_mysql_bigquery(column=case.name, source_type=case.source_spec)
    if case.source == "mssql" and case.sink == "bigquery":
        return matrix_entry_for_mssql_bigquery(column=case.name, source_type=case.source_spec)
    if case.source == "clickhouse" and case.sink == "mssql":
        return matrix_entry_for_clickhouse_mssql(column=case.name, source_type=case.source_spec)
    raise ValueError(f"unsupported certification route {case.source} -> {case.sink}")


def _category(requires_explicit_contract: bool) -> DecisionCategory:
    return "incompatible_requires_policy" if requires_explicit_contract else "auto_inferred"


def _nullable_behavior(source_type: str, target_type: str) -> str:
    if "nullable" in source_type.lower() or target_type.startswith("Nullable("):
        return "source NULL is emitted as the sink-native NULL representation for nullable targets"
    return "non-null source values are emitted directly"


def _canonical_mssql_type(source_type: str) -> str:
    normalized = source_type.lower()
    if "sql_variant" in normalized:
        return "variant"
    if "datetimeoffset" in normalized:
        return "offset_timestamp"
    if "datetime" in normalized or "smalldatetime" in normalized:
        return "timestamp"
    if normalized.replace(" nullable", "").strip() == "date":
        return "date"
    if "time" in normalized:
        return "time"
    if "uniqueidentifier" in normalized:
        return "uuid"
    if (
        "binary" in normalized
        or "rowversion" in normalized
        or normalized.replace(" nullable", "").strip() == "timestamp"
    ):
        return "binary"
    if "decimal" in normalized or "numeric" in normalized or "money" in normalized:
        return "decimal"
    if any(token in normalized for token in ("float", "real")):
        return "float"
    if "bit" in normalized:
        return "boolean"
    if any(token in normalized for token in ("int", "bigint", "smallint", "tinyint")):
        return "integer"
    return "string"


def _canonical_postgres_type(source_type: str) -> str:
    normalized = source_type.lower()
    if "[]" in normalized:
        return "array"
    if "range" in normalized:
        return "range"
    if "json" in normalized:
        return "json"
    if "timestamp with time zone" in normalized or normalized == "timestamptz":
        return "offset_timestamp"
    if "timestamp" in normalized:
        return "timestamp"
    if normalized == "date":
        return "date"
    if normalized.startswith("time"):
        return "time"
    if "uuid" in normalized:
        return "uuid"
    if "bytea" in normalized:
        return "binary"
    if "numeric" in normalized or "decimal" in normalized:
        return "decimal"
    if "bool" in normalized:
        return "boolean"
    if any(token in normalized for token in ("smallint", "integer", "bigint", "serial")):
        return "integer"
    if normalized in {"text", "varchar", "character varying"} or "char" in normalized:
        return "string"
    return "enum"


def _canonical_mysql_type(source_type: str) -> str:
    normalized = source_type.lower()
    if normalized.startswith(("enum(", "set(")):
        return "enum"
    if "json" in normalized:
        return "json"
    if "datetime" in normalized or "timestamp" in normalized:
        return "timestamp"
    if normalized.startswith("date") or normalized == "year":
        return "date"
    if normalized.startswith("time"):
        return "time"
    if "decimal" in normalized or "numeric" in normalized:
        return "decimal"
    if any(token in normalized for token in ("float", "double")):
        return "float"
    if "bool" in normalized or normalized in {"bit", "tinyint(1)"}:
        return "boolean"
    if any(token in normalized for token in ("blob", "binary", "varbinary")):
        return "binary"
    if any(token in normalized for token in ("tinyint", "smallint", "mediumint", "int", "bigint")):
        return "integer"
    return "string"


def _canonical_clickhouse_type(source_type: str) -> str:
    normalized = source_type.lower()
    if normalized.startswith(("array(", "map(", "tuple(", "nested(")):
        return "complex"
    if "datetime64" in normalized or normalized.startswith("datetime"):
        return "timestamp"
    if normalized.startswith(("date", "nullable(date")):
        return "date"
    if normalized.startswith(("decimal", "nullable(decimal")):
        return "decimal"
    if normalized.startswith(("float", "nullable(float")):
        return "float"
    if "bool" in normalized:
        return "boolean"
    if "uuid" in normalized:
        return "uuid"
    if "int" in normalized:
        return "integer"
    if normalized.startswith(("enum", "nullable(enum")):
        return "enum"
    if normalized.startswith(("fixedstring", "nullable(fixedstring")):
        return "binary_or_fixed_text"
    return "string"


__all__ = [
    "assert_certification_suite_matches_runtime_decisions",
    "case_matrix_entry",
    "matrix_entry_for_clickhouse_mssql",
    "matrix_entry_for_mssql_clickhouse",
    "matrix_entry_for_mysql_bigquery",
    "matrix_entry_for_mysql_clickhouse",
    "matrix_entry_for_mysql_mssql",
    "matrix_entry_for_mysql_postgres",
    "matrix_entry_for_postgres_bigquery",
    "matrix_entry_for_postgres_mssql",
]
