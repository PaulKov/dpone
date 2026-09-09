"""Target-specific type resolution."""

from __future__ import annotations

from collections.abc import Mapping

from dpone.readiness.physical_design_models import LowCardinalityOptions, PhysicalDesignOptions, ResolvedTargetColumn
from dpone.runtime.sinks.clickhouse_nullability_policy import (
    ClickHouseNullabilityOptions,
    ClickHouseNullabilityPolicy,
    nullable_clickhouse_type,
)
from dpone.runtime.support.temporal_fidelity import TemporalFidelityPolicy
from dpone.type_system.models import ColumnProfile, InferredColumn


class TargetTypeResolver:
    """Resolve one logical column to a sink-specific target type."""

    def __init__(self, temporal_policy: TemporalFidelityPolicy | None = None) -> None:
        self.temporal_policy = temporal_policy or TemporalFidelityPolicy()

    def resolve(
        self,
        *,
        sink_type: str,
        column: InferredColumn,
        options: PhysicalDesignOptions,
        profile: ColumnProfile | None = None,
    ) -> ResolvedTargetColumn:
        normalized_sink = _normalize_sink(sink_type)
        override = _target_override(options, column.name, normalized_sink)
        if override:
            return ResolvedTargetColumn(
                name=column.name,
                logical_type=column.logical_type,
                target_type=override,
                nullable=column.nullable,
                decision_source="physical_override",
                reason=f"explicit physical target type for {normalized_sink}",
                decision_category="explicit_physical_override",
            )
        temporal_policy = self.temporal_policy.for_column(column.name)
        temporal = _temporal_target_type(normalized_sink, column, temporal_policy)
        if temporal:
            target_type = temporal
            reason = f"temporal fidelity {temporal_policy.offset_timestamp_mode}"
            decision_source = "type_fidelity"
            return ResolvedTargetColumn(
                name=column.name,
                logical_type=column.logical_type,
                target_type=target_type,
                nullable=column.nullable,
                decision_source=decision_source,
                reason=reason,
                decision_category=_category_for(decision_source),
            )
        target_type = _default_target_type(normalized_sink, column)
        reason = f"{normalized_sink} mapping from {column.decision_source}"
        decision_source = column.decision_source
        if normalized_sink == "clickhouse":
            target_type, low_reason = _maybe_low_cardinality(
                target_type,
                column=column,
                profile=profile,
                options=options.low_cardinality(),
            )
            if low_reason:
                reason = low_reason
                decision_source = "physical_design"
            if _explicit_clickhouse_nullable(column):
                target_type = nullable_clickhouse_type(target_type)
            nullability = ClickHouseNullabilityPolicy().resolve_options(
                options=_clickhouse_nullability_options(options),
                column=column.name,
                mapped_type=target_type,
            )
            if nullability.decision_source == "physical_design":
                reason = nullability.reason
                decision_source = "physical_design"
            target_type = nullability.target_type
        return ResolvedTargetColumn(
            name=column.name,
            logical_type=column.logical_type,
            target_type=target_type,
            nullable=column.nullable,
            decision_source=decision_source,
            reason=reason,
            decision_category=_category_for(decision_source),
        )


def _normalize_sink(sink_type: str) -> str:
    normalized = str(sink_type).strip().lower()
    if normalized in {"sqlserver", "sql_server"}:
        return "mssql"
    if normalized in {"bq", "google_bigquery"}:
        return "bigquery"
    return normalized


def _target_override(options: PhysicalDesignOptions, column: str, sink_type: str) -> str | None:
    override = options.columns.get(column) or options.columns.get(column.lower())
    if not override:
        return None
    return override.target_type.get(sink_type)


def _default_target_type(sink_type: str, column: InferredColumn) -> str:
    logical = column.logical_type.lower()
    if sink_type == "mssql":
        return _mssql_type(column, logical)
    if sink_type == "postgres":
        return _postgres_type(column, logical)
    if sink_type == "clickhouse":
        return _clickhouse_type(column, logical)
    if sink_type == "bigquery":
        return _bigquery_type(column, logical)
    if sink_type == "kafka":
        return _kafka_type(column, logical)
    return "string"


def _temporal_target_type(
    sink_type: str,
    column: InferredColumn,
    policy: TemporalFidelityPolicy,
) -> str | None:
    if column.logical_type.lower() != "timestamp" or not column.timezone:
        return None
    mode = policy.offset_timestamp_mode
    if mode == "preserve_text" or policy.malformed == "preserve_text":
        return {
            "mssql": "nvarchar(max)",
            "postgres": "text",
            "clickhouse": "String",
            "bigquery": "STRING",
            "kafka": "string",
        }.get(sink_type, "string")
    if sink_type == "clickhouse":
        timezone = policy.target_timezone if mode == "fixed_timezone" else "UTC"
        return f"DateTime64(6, '{timezone}')"
    if sink_type == "mssql":
        return "datetimeoffset" if mode == "preserve_offset" else "datetime2"
    if sink_type == "postgres":
        return "timestamptz"
    if sink_type == "bigquery":
        return "TIMESTAMP"
    if sink_type == "kafka":
        return "timestamp"
    return None


def _mssql_type(column: InferredColumn, logical: str) -> str:
    if logical in {"integer", "bigint"}:
        return "bigint"
    if logical == "decimal":
        return f"decimal({column.precision or 38},{column.scale or 9})"
    if logical == "float":
        return "float"
    if logical == "boolean":
        return "bit"
    if logical == "timestamp":
        return "datetimeoffset" if column.timezone else "datetime2"
    if logical == "date":
        return "date"
    if logical == "time":
        return "time"
    if logical == "binary":
        return "varbinary(max)"
    return "nvarchar(max)"


def _postgres_type(column: InferredColumn, logical: str) -> str:
    if logical in {"integer", "bigint"}:
        return "bigint"
    if logical == "decimal":
        return f"numeric({column.precision or 38},{column.scale or 9})"
    if logical == "float":
        return "double precision"
    if logical == "boolean":
        return "boolean"
    if logical == "timestamp":
        return "timestamptz" if column.timezone else "timestamp"
    if logical == "date":
        return "date"
    if logical == "time":
        return "time"
    if logical == "json":
        return "jsonb"
    if logical == "binary":
        return "bytea"
    return "text"


def _clickhouse_type(column: InferredColumn, logical: str) -> str:
    if logical in {"integer", "bigint"}:
        return "Int64"
    if logical == "decimal":
        return f"Decimal({column.precision or 38},{column.scale or 9})"
    if logical == "float":
        return "Float64"
    if logical == "boolean":
        return "Bool"
    if logical == "timestamp":
        return "DateTime64(6)"
    if logical == "date":
        return "Date"
    if logical == "time":
        return "String"
    if logical == "array":
        return "String"
    return "String"


def _bigquery_type(column: InferredColumn, logical: str) -> str:
    if logical in {"integer", "bigint"}:
        return "INT64"
    if logical == "decimal":
        precision = column.precision or 38
        scale = column.scale or 9
        return "BIGNUMERIC" if precision > 38 or scale > 9 else "NUMERIC"
    if logical == "float":
        return "FLOAT64"
    if logical == "boolean":
        return "BOOL"
    if logical == "timestamp":
        return "TIMESTAMP"
    if logical == "date":
        return "DATE"
    if logical == "time":
        return "TIME"
    if logical == "json":
        return "JSON"
    if logical == "binary":
        return "BYTES"
    return "STRING"


def _kafka_type(column: InferredColumn, logical: str) -> str:
    del column
    return {
        "integer": "int64",
        "bigint": "int64",
        "decimal": "decimal",
        "float": "double",
        "boolean": "boolean",
        "timestamp": "timestamp",
        "date": "date",
        "json": "json",
        "binary": "bytes",
        "array": "array",
    }.get(logical, "string")


def _category_for(decision_source: str) -> str:
    if decision_source == "physical_override":
        return "explicit_physical_override"
    if decision_source == "schema_contract":
        return "explicit_logical_contract"
    if decision_source == "safe_fallback":
        return "quarantine_required"
    return "auto_inferred"


def _maybe_low_cardinality(
    target_type: str,
    *,
    column: InferredColumn,
    profile: ColumnProfile | None,
    options: LowCardinalityOptions,
) -> tuple[str, str | None]:
    if target_type != "String" or column.logical_type.lower() != "string":
        return target_type, None
    mode = options.mode
    if mode == "off":
        return target_type, None
    configured = {item.lower() for item in options.columns}
    column_configured = column.name.lower() in configured
    if mode == "preserve":
        return target_type, None
    if mode == "explicit" and column_configured:
        return "LowCardinality(String)", "explicit LowCardinality column"
    if mode == "force" and column_configured:
        return "LowCardinality(String)", "forced LowCardinality column"
    if mode != "auto" or profile is None:
        return target_type, None
    if _is_low_cardinality(profile, options) and not _looks_like_identifier(column.name):
        return "LowCardinality(String)", "low cardinality profile selected LowCardinality(String)"
    return target_type, None


def _is_low_cardinality(profile: ColumnProfile, options: LowCardinalityOptions) -> bool:
    return (
        profile.distinct_count <= options.max_distinct_values and profile.distinct_ratio <= options.max_distinct_ratio
    )


def _looks_like_identifier(column_name: str) -> bool:
    lowered = column_name.lower()
    return any(token in lowered for token in ("id", "uuid", "email", "url", "hash", "token"))


def _clickhouse_nullability_options(options: PhysicalDesignOptions) -> ClickHouseNullabilityOptions:
    clickhouse = options.storage.get("clickhouse", {})
    if not isinstance(clickhouse, Mapping):
        return ClickHouseNullabilityOptions()
    return ClickHouseNullabilityOptions.from_options({"physical_design": {"storage": {"clickhouse": dict(clickhouse)}}})


def _explicit_clickhouse_nullable(column: InferredColumn) -> bool:
    if not column.nullable:
        return False
    reason = column.reason.lower()
    return " nullable" in reason or "nullable(" in reason
