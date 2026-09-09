from __future__ import annotations

from typing import Any

from dpone.runtime.support.temporal_fidelity import (
    TemporalFidelityPolicy,
    is_offset_timestamp_type,
    offset_minutes_column_name,
    temporal_policy_warnings,
)
from dpone.runtime.support.type_mapping.mssql_clickhouse import (
    MssqlClickHouseTypeMapper,
    MssqlClickHouseTypePolicy,
)


def build_native_transfer_type_fidelity(
    *,
    source_type: str,
    sink_type: str,
    source_options: dict[str, Any],
    sink_options: dict[str, Any],
) -> dict[str, Any]:
    schema = _schema_from_options(source_options)
    if source_type.lower() != "mssql" or sink_type.lower() != "clickhouse":
        return _generic_temporal_fidelity(
            source_type=source_type,
            sink_type=sink_type,
            source_options=source_options,
            sink_options=sink_options,
            schema=schema,
        )
    if not schema:
        return {
            "profile": "mssql_to_clickhouse_lossless_v1",
            "available": False,
            "columns": {},
        }
    policy = MssqlClickHouseTypePolicy.from_config(
        _merge_options(
            _dict_option(source_options, "type_fidelity"),
            _dict_option(sink_options, "type_fidelity"),
        )
    )
    decisions = MssqlClickHouseTypeMapper(policy).resolve_schema(schema)
    generated_columns = _generated_temporal_columns(schema, policy.temporal)
    return {
        "profile": "mssql_to_clickhouse_lossless_v1",
        "available": True,
        "policy": policy.to_dict(),
        "warnings": list(
            dict.fromkeys(
                [
                    *policy.warnings,
                    *temporal_policy_warnings(policy.temporal, generated_columns=tuple(generated_columns.values())),
                ]
            )
        ),
        "columns": {name: decision.to_dict() for name, decision in decisions.items()},
        "generated_columns": generated_columns,
        "lossless": all(decision.lossless for decision in decisions.values()),
    }


def _schema_from_options(source_options: dict[str, Any]) -> list[tuple[str, str]]:
    raw = source_options.get("source_schema") or source_options.get("columns") or ()
    schema: list[tuple[str, str]] = []
    for item in raw:
        if isinstance(item, dict):
            name = item.get("name")
            source_type = item.get("type") or item.get("source_type")
            if name and source_type:
                schema.append((str(name), str(source_type)))
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            schema.append((str(item[0]), str(item[1])))
    return schema


def _generic_temporal_fidelity(
    *,
    source_type: str,
    sink_type: str,
    source_options: dict[str, Any],
    sink_options: dict[str, Any],
    schema: list[tuple[str, str]],
) -> dict[str, Any]:
    policy = TemporalFidelityPolicy.from_config(
        _merge_options(
            _dict_option(source_options, "type_fidelity"),
            _dict_option(sink_options, "type_fidelity"),
        )
    )
    columns = {
        column: _generic_temporal_column_decision(column, dtype, sink_type, policy.for_column(column))
        for column, dtype in schema
        if is_offset_timestamp_type(dtype)
    }
    generated_columns = _generated_temporal_columns(schema, policy)
    if not columns and not policy.warnings:
        return {}
    return {
        "profile": "temporal_offset_timestamp_v1",
        "available": bool(columns),
        "policy": {"temporal": {"offset_timestamp": policy.to_dict()}},
        "warnings": list(temporal_policy_warnings(policy, generated_columns=tuple(generated_columns.values()))),
        "columns": columns,
        "generated_columns": generated_columns,
        "lossless": policy.offset_preserving,
    }


def _generic_temporal_column_decision(
    column: str,
    source_type: str,
    sink_type: str,
    policy: TemporalFidelityPolicy,
) -> dict[str, Any]:
    generated = offset_minutes_column_name(column) if policy.preserves_offset_column else None
    return {
        "source_type": source_type,
        "target_type": _generic_temporal_target_type(sink_type, policy),
        "mode": policy.offset_timestamp_mode,
        "timezone": policy.target_timezone,
        "malformed": policy.malformed,
        "lossless": policy.offset_preserving,
        "generated_column": generated,
        "reason": _generic_temporal_reason(policy),
    }


def _generic_temporal_target_type(sink_type: str, policy: TemporalFidelityPolicy) -> str:
    sink = sink_type.lower()
    if policy.offset_timestamp_mode == "preserve_text" or policy.malformed == "preserve_text":
        return {
            "mssql": "nvarchar(max)",
            "sqlserver": "nvarchar(max)",
            "postgres": "text",
            "postgresql": "text",
            "clickhouse": "String",
            "bigquery": "STRING",
            "kafka": "string",
        }.get(sink, "string")
    if sink == "clickhouse":
        timezone = policy.target_timezone if policy.offset_timestamp_mode == "fixed_timezone" else "UTC"
        return f"DateTime64(6, '{timezone}')"
    return {
        "mssql": "datetimeoffset(7)" if policy.offset_timestamp_mode == "preserve_offset" else "datetime2(7)",
        "sqlserver": "datetimeoffset(7)" if policy.offset_timestamp_mode == "preserve_offset" else "datetime2(7)",
        "postgres": "timestamptz",
        "postgresql": "timestamptz",
        "bigquery": "TIMESTAMP",
        "kafka": "timestamp",
    }.get(sink, "timestamp")


def _generic_temporal_reason(policy: TemporalFidelityPolicy) -> str:
    if policy.offset_timestamp_mode == "utc_instant":
        return "stores the instant normalized to UTC; original source offset is not preserved"
    if policy.offset_timestamp_mode == "fixed_timezone":
        return "stores the instant converted to the configured business timezone"
    if policy.offset_timestamp_mode == "preserve_offset":
        return "stores UTC instant plus generated offset-minutes companion column"
    return "stores the original offset timestamp as raw text"


def _generated_temporal_columns(
    schema: list[tuple[str, str]],
    policy: TemporalFidelityPolicy,
) -> dict[str, str]:
    if not policy.preserves_offset_column:
        return {
            column: offset_minutes_column_name(column)
            for column, dtype in schema
            if is_offset_timestamp_type(dtype) and policy.for_column(column).preserves_offset_column
        }
    return {
        column: offset_minutes_column_name(column)
        for column, dtype in schema
        if is_offset_timestamp_type(dtype) and policy.for_column(column).preserves_offset_column
    }


def _merge_options(*options: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for option in options:
        merged.update(option)
    return merged


def _dict_option(options: dict[str, Any], key: str) -> dict[str, Any]:
    value = options.get(key)
    return dict(value) if isinstance(value, dict) else {}
