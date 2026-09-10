"""Admission and resource policy for the opt-in single-query native route."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.contracts.mssql_native_chunks import NativeChunkLimits
from dpone.contracts.rolling_window import FrozenRollingWindow, RollingWindowSpec


def _native(config: Any) -> Mapping[str, Any]:
    options = getattr(config, "options", {}) or {}
    value = options.get("native_transfer", {})
    if value is False or value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError("mssql_native.invalid_options")
    return value


def native_requested(config: Any) -> bool:
    """Recognize either explicit marker so a malformed request cannot fall back."""
    value = _native(config)
    wire = value.get("wire", {})
    execution = value.get("execution", {})
    chunking = execution.get("chunking", {}) if isinstance(execution, Mapping) else {}
    return (isinstance(wire, Mapping) and wire.get("binary_format") == "mssql_native") or (
        isinstance(chunking, Mapping) and chunking.get("mode") == "bounded_stream"
    )


def native_limits(config: Any) -> NativeChunkLimits:
    """Parse the closed public byte policy without coercion or clamping."""
    execution = _native(config).get("execution")
    if not isinstance(execution, Mapping):
        raise ValueError("mssql_native.execution_required")
    chunking, chunks = execution.get("chunking"), execution.get("native_chunks")
    if not isinstance(chunking, Mapping) or set(chunking) - {"mode", "parallelism", "checkpointing"}:
        raise ValueError("mssql_native.chunking_invalid")
    if chunking.get("mode") != "bounded_stream" or chunking.get("checkpointing", "resumable") != "resumable":
        raise ValueError("mssql_native.chunking_invalid")
    allowed = set(NativeChunkLimits.__dataclass_fields__) - {"parallelism"}
    if not isinstance(chunks, Mapping) or set(chunks) - allowed:
        raise ValueError("mssql_native.native_chunks_invalid")
    if not {"max_total_encoded_bytes", "stage_allocated_bytes_stop_threshold"}.issubset(chunks):
        raise ValueError("mssql_native.capacity_limits_required")
    return NativeChunkLimits(**dict(chunks), parallelism=chunking.get("parallelism", 1))


def native_window(config: Any) -> FrozenRollingWindow | None:
    """Freeze authored scope against the supplied execution interval, never wall time."""
    strategy = getattr(config.load_strategy, "value", config.load_strategy)
    options = config.options or {}
    raw = options.get("mssql_native_window")
    if strategy == "full_refresh":
        if raw is not None:
            raise ValueError("mssql_native.full_refresh_window_forbidden")
        return None
    if strategy != "partition_replace" or raw is None:
        raise ValueError("mssql_native.explicit_window_required")
    interval = options.get("interval")
    if not isinstance(interval, Mapping):
        raise ValueError("mssql_native.window_interval_required")
    return RollingWindowSpec.from_mapping(raw).freeze({"data_interval_end": interval.get("interval_end")})


def validate_native_config(config: Any) -> None:
    """Reject unsupported routes and source policies before connector row I/O."""
    value = _native(config)
    wire = value.get("wire")
    if (
        not isinstance(wire, Mapping)
        or wire.get("mode") != "typed_binary"
        or wire.get("binary_format") != "mssql_native"
    ):
        raise ValueError("mssql_native.wire_required")
    options = config.options or {}
    if options.get("source_type") != "clickhouse" or options.get("sink_type") != "mssql":
        raise ValueError("mssql_native.route_unsupported")
    native_limits(config)
    native_window(config)
    unsupported = (
        "query",
        "columns",
        "export_to_gcs",
        "source_custom_predicate",
        "custom_predicate",
        "partition_by",
        "date_from",
        "date_to",
        "with_dedup",
        "dedup_expression",
        "source_materialization",
        "pre_sql",
        "post_sql",
        "hooks",
        "pre_hooks",
        "post_hooks",
        "backfill",
        "cdc",
        "scd2",
        "diff",
    )
    for scope in (options, options.get("source_options", {})):
        if not isinstance(scope, Mapping):
            raise ValueError("mssql_native.source_options_invalid")
        if any(scope.get(key) not in (None, False, "") for key in unsupported):
            raise ValueError("mssql_native.source_policy_unsupported")
    if getattr(config, "with_dedup", False) or getattr(config, "custom_predicate", None):
        raise ValueError("mssql_native.additional_scope_unsupported")
