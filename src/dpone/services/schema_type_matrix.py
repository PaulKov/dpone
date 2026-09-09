"""Source -> sink type-matrix explanations for operator UX."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.services.schema_type_matrix_mssql_bigquery import build_mssql_bigquery_matrix
from dpone.services.schema_type_matrix_mysql_bigquery import build_mysql_bigquery_matrix
from dpone.services.schema_type_matrix_postgres_bigquery import build_postgres_bigquery_matrix
from dpone.type_system.source_sink.certification import (
    matrix_entry_for_clickhouse_mssql,
    matrix_entry_for_mssql_clickhouse,
    matrix_entry_for_mysql_clickhouse,
    matrix_entry_for_mysql_mssql,
    matrix_entry_for_mysql_postgres,
    matrix_entry_for_postgres_mssql,
)
from dpone.type_system.source_sink.mssql_clickhouse import MssqlClickHouseMatrixPolicy
from dpone.type_system.source_sink.profiles import PairTypeProfile, build_default_profiles


class PairTypeMatrixService:
    """Build explainable source -> sink type mapping matrices."""

    def __init__(self, profiles: Mapping[tuple[str, str], PairTypeProfile] | None = None) -> None:
        self._profiles = dict(profiles or build_default_profiles())

    def available_pairs(self) -> tuple[tuple[str, str], ...]:
        specialized = {
            ("mssql", "clickhouse"),
            ("mssql", "bigquery"),
            ("postgres", "mssql"),
            ("postgres", "bigquery"),
            ("mysql", "mssql"),
            ("mysql", "postgres"),
            ("mysql", "clickhouse"),
            ("mysql", "bigquery"),
        }
        return tuple(sorted(specialized | set(self._profiles)))

    def build(
        self,
        *,
        source: str,
        sink: str,
        source_types: Sequence[str] | None = None,
        type_fidelity: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        source_key = source.strip().lower()
        sink_key = sink.strip().lower()
        if source_key == "mssql" and sink_key == "clickhouse":
            return self._mssql_clickhouse(source_key, sink_key, source_types, type_fidelity)
        if source_key == "postgres" and sink_key == "mssql":
            return self._postgres_mssql(source_key, sink_key, source_types)
        if source_key == "mysql" and sink_key == "mssql":
            return self._mysql_mssql(source_key, sink_key, source_types)
        if source_key == "mysql" and sink_key == "postgres":
            return self._mysql_postgres(source_key, sink_key, source_types)
        if source_key == "mysql" and sink_key == "clickhouse":
            return self._mysql_clickhouse(source_key, sink_key, source_types)
        if source_key == "mysql" and sink_key == "bigquery":
            return build_mysql_bigquery_matrix(source_key, sink_key, source_types, split_source_spec=_split_source_spec)
        if source_key == "postgres" and sink_key == "bigquery":
            return build_postgres_bigquery_matrix(
                source_key, sink_key, source_types, split_source_spec=_split_source_spec
            )
        if source_key == "mssql" and sink_key == "bigquery":
            return build_mssql_bigquery_matrix(source_key, sink_key, source_types, split_source_spec=_split_source_spec)
        if source_key == "clickhouse" and sink_key == "mssql":
            return self._clickhouse_mssql(source_key, sink_key, source_types)
        if profile := self._profiles.get((source_key, sink_key)):
            return self._profile_matrix(profile, source_types)
        raise ValueError(f"type matrix is not available for {source_key} -> {sink_key}")

    def _profile_matrix(self, profile: PairTypeProfile, source_types: Sequence[str] | None) -> dict[str, Any]:
        entries = []
        for source_spec in source_types or profile.default_source_types:
            column, source_type = _split_source_spec(source_spec)
            decision = profile.resolve(source_type)
            entries.append(
                {
                    "column": column,
                    "source_type": decision.source_type,
                    "canonical_type": _canonical_generic_type(decision.source_type),
                    "target_type": decision.target_type,
                    "native_transport": decision.native_transport,
                    "schema_evolution_compatible": decision.compatible,
                    "lossless": decision.lossless,
                    "nullable_behavior": _nullable_behavior(source_type, decision.target_type),
                    "requires_explicit_contract": decision.requires_explicit_contract,
                    "decision_category": "incompatible_requires_policy"
                    if decision.requires_explicit_contract
                    else "auto_inferred",
                    "decision_source": "source_metadata",
                    "reason": decision.warning or f"mapped by {profile.profile}",
                }
            )
        return {
            "source": profile.source,
            "sink": profile.sink,
            "profile": profile.profile,
            "entries": entries,
            "runbook": profile.runbook,
        }

    def _mssql_clickhouse(
        self,
        source: str,
        sink: str,
        source_types: Sequence[str] | None,
        type_fidelity: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        policy = MssqlClickHouseMatrixPolicy.from_config(dict(type_fidelity or {}))
        entries = []
        for source_spec in source_types or _DEFAULT_MSSQL_CLICKHOUSE_TYPES:
            column, source_type = _split_source_spec(source_spec)
            entries.append(matrix_entry_for_mssql_clickhouse(column=column, source_type=source_type, policy=policy))
        return {
            "source": source,
            "sink": sink,
            "profile": "mssql_to_clickhouse_lossless_v2",
            "type_fidelity": policy.to_dict(),
            "entries": entries,
            "certification": {
                "suite": "mssql_to_clickhouse",
                "artifact_names": [
                    "type_matrix_decisions.json",
                    "physical_ddl_plan.sql",
                    "schema_evolution_rerun.json",
                    "typed_reconciliation.json",
                    "live_fixture_summary.md",
                ],
            },
            "runbook": "docs/source-sink/mssql-to-clickhouse.md#runbook-false-schema-evolution-type-changes",
        }

    def _postgres_mssql(
        self,
        source: str,
        sink: str,
        source_types: Sequence[str] | None,
    ) -> dict[str, Any]:
        entries = []
        for source_spec in source_types or _DEFAULT_POSTGRES_MSSQL_TYPES:
            column, source_type = _split_source_spec(source_spec)
            entries.append(matrix_entry_for_postgres_mssql(column=column, source_type=source_type))
        return {
            "source": source,
            "sink": sink,
            "profile": "postgres_to_mssql_native_v2",
            "entries": entries,
            "certification": {
                "suite": "postgres_to_mssql",
                "artifact_names": [
                    "type_matrix_decisions.json",
                    "physical_ddl_plan.sql",
                    "schema_evolution_rerun.json",
                    "typed_reconciliation.json",
                    "live_fixture_summary.md",
                ],
            },
            "runbook": "docs/source-sink/postgres-to-mssql.md#schema-evolution-and-type-mapping",
        }

    def _mysql_mssql(
        self,
        source: str,
        sink: str,
        source_types: Sequence[str] | None,
    ) -> dict[str, Any]:
        entries = []
        for source_spec in source_types or _DEFAULT_MYSQL_MSSQL_TYPES:
            column, source_type = _split_source_spec(source_spec)
            entries.append(matrix_entry_for_mysql_mssql(column=column, source_type=source_type))
        return {
            "source": source,
            "sink": sink,
            "profile": "mysql_to_mssql_native_v1",
            "entries": entries,
            "certification": {
                "suite": "mysql_to_mssql",
                "artifact_names": [
                    "type_matrix_decisions.json",
                    "physical_ddl_plan.sql",
                    "schema_evolution_rerun.json",
                    "typed_reconciliation.json",
                    "live_fixture_summary.md",
                ],
            },
            "runbook": "docs/source-sink/mysql-to-mssql.md#schema-evolution-and-type-mapping",
        }

    def _mysql_postgres(
        self,
        source: str,
        sink: str,
        source_types: Sequence[str] | None,
    ) -> dict[str, Any]:
        entries = []
        for source_spec in source_types or _DEFAULT_MYSQL_POSTGRES_TYPES:
            column, source_type = _split_source_spec(source_spec)
            entries.append(matrix_entry_for_mysql_postgres(column=column, source_type=source_type))
        return {
            "source": source,
            "sink": sink,
            "profile": "mysql_to_postgres_native_v1",
            "entries": entries,
            "certification": {
                "suite": "mysql_to_postgres",
                "artifact_names": [
                    "type_matrix_decisions.json",
                    "physical_ddl_plan.sql",
                    "schema_evolution_rerun.json",
                    "typed_reconciliation.json",
                    "live_fixture_summary.md",
                ],
            },
            "runbook": "docs/source-sink/mysql-to-postgres.md#schema-evolution-and-type-mapping",
        }

    def _mysql_clickhouse(
        self,
        source: str,
        sink: str,
        source_types: Sequence[str] | None,
    ) -> dict[str, Any]:
        entries = []
        for source_spec in source_types or _DEFAULT_MYSQL_CLICKHOUSE_TYPES:
            column, source_type = _split_source_spec(source_spec)
            entries.append(matrix_entry_for_mysql_clickhouse(column=column, source_type=source_type))
        return {
            "source": source,
            "sink": sink,
            "profile": "mysql_to_clickhouse_analytics_v1",
            "entries": entries,
            "certification": {
                "suite": "mysql_to_clickhouse",
                "artifact_names": [
                    "type_matrix_decisions.json",
                    "physical_ddl_plan.sql",
                    "schema_evolution_rerun.json",
                    "typed_reconciliation.json",
                    "live_fixture_summary.md",
                ],
            },
            "runbook": "docs/source-sink/mysql-to-clickhouse.md#schema-evolution-and-type-mapping",
        }

    def _clickhouse_mssql(
        self,
        source: str,
        sink: str,
        source_types: Sequence[str] | None,
    ) -> dict[str, Any]:
        entries = []
        for source_spec in source_types or _DEFAULT_CLICKHOUSE_MSSQL_TYPES:
            column, source_type = _split_source_spec(source_spec)
            entries.append(matrix_entry_for_clickhouse_mssql(column=column, source_type=source_type))
        return {
            "source": source,
            "sink": sink,
            "profile": "clickhouse_to_mssql_landing_v1",
            "entries": entries,
            "certification": {
                "suite": "clickhouse_to_mssql",
                "artifact_names": [
                    "type_matrix_decisions.json",
                    "physical_ddl_plan.sql",
                    "schema_evolution_rerun.json",
                    "typed_reconciliation.json",
                    "live_fixture_summary.md",
                ],
            },
            "runbook": "docs/source-sink/clickhouse-to-mssql.md#type-matrix-certification",
        }


def _split_source_spec(source_spec: str) -> tuple[str | None, str]:
    raw = str(source_spec).strip()
    if ":" not in raw:
        return None, raw
    column, source_type = raw.split(":", 1)
    normalized_column = column.strip() or None
    source_type = source_type.strip()
    return normalized_column, source_type


def _nullable_behavior(source_type: str, target_type: str) -> str:
    if "nullable" in source_type.lower() or target_type.startswith("Nullable("):
        return "MSSQL NULL is emitted as ClickHouse TSV NULL marker for nullable targets"
    return "non-null source values are emitted directly"


def _canonical_generic_type(source_type: str) -> str:
    normalized = source_type.lower()
    if "json" in normalized:
        return "json"
    if "date" in normalized and "time" not in normalized:
        return "date"
    if "time" in normalized or "timestamp" in normalized or "datetime" in normalized:
        return "timestamp"
    if "decimal" in normalized or "numeric" in normalized:
        return "decimal"
    if "int" in normalized:
        return "integer"
    if "bool" in normalized or "bit" in normalized:
        return "boolean"
    if "binary" in normalized or "byte" in normalized:
        return "binary"
    return "string"


_DEFAULT_MSSQL_CLICKHOUSE_TYPES: tuple[str, ...] = (
    "bigint",
    "int nullable",
    "decimal(18,4)",
    "money",
    "bit nullable",
    "uniqueidentifier",
    "date",
    "datetime",
    "datetime2(7) nullable",
    "smalldatetime",
    "datetimeoffset(7)",
    "time(7)",
    "nvarchar(510) nullable",
    "varbinary(max)",
    "rowversion",
)

_DEFAULT_MYSQL_MSSQL_TYPES: tuple[str, ...] = (
    "tinyint",
    "smallint",
    "int",
    "bigint",
    "decimal(18,4)",
    "tinyint(1)",
    "float",
    "double",
    "date",
    "datetime",
    "timestamp",
    "time",
    "varchar(255)",
    "text",
    "json",
    "blob",
)

_DEFAULT_MYSQL_POSTGRES_TYPES: tuple[str, ...] = (
    "tinyint",
    "smallint",
    "int",
    "bigint",
    "decimal(18,4)",
    "tinyint(1)",
    "float",
    "double",
    "date",
    "datetime",
    "timestamp",
    "time",
    "varchar(255)",
    "text",
    "json",
    "blob",
)

_DEFAULT_MYSQL_CLICKHOUSE_TYPES: tuple[str, ...] = (
    "tinyint",
    "smallint",
    "int",
    "bigint",
    "decimal(18,4)",
    "tinyint(1)",
    "float",
    "double",
    "date",
    "datetime",
    "timestamp",
    "time",
    "varchar(255)",
    "text",
    "json",
    "blob",
)

_DEFAULT_POSTGRES_MSSQL_TYPES: tuple[str, ...] = (
    "smallint",
    "integer",
    "bigint",
    "numeric(18,4)",
    "boolean",
    "uuid",
    "bytea",
    "date",
    "timestamp without time zone",
    "timestamp with time zone",
    "jsonb",
    "text[]",
    "my_enum",
)

_DEFAULT_CLICKHOUSE_MSSQL_TYPES: tuple[str, ...] = (
    "UInt8",
    "Int32",
    "Nullable(UInt64)",
    "Float64",
    "Bool",
    "Nullable(String)",
    "UUID",
    "Date",
    "DateTime64(3, 'Europe/Moscow')",
    "Decimal(18,4)",
    "DateTime64(9)",
    "Decimal(76,4)",
    "Array(String)",
    "Enum8('a' = 1)",
)


__all__ = ["PairTypeMatrixService"]
