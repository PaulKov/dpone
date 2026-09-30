"""Admission and resource policy for the opt-in single-query native route."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.contracts.mssql_native_chunks import NativeChunkLimits
from dpone.contracts.mssql_native_verification import NativeImportBackend, NativeVerificationBackend
from dpone.contracts.rolling_window import FrozenRollingWindow, RollingWindowSpec
from dpone.manifest.clickhouse_raw_snapshot_policy import native_source_snapshot_policy


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
    return (
        "source_snapshot" in value
        or (isinstance(wire, Mapping) and wire.get("binary_format") == "mssql_native")
        or (isinstance(chunking, Mapping) and chunking.get("mode") == "bounded_stream")
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
    for name in ("encoding_parallelism", "import_parallelism"):
        if name in chunks and chunks[name] is None:
            raise ValueError(f"mssql_native.invalid_limit:{name}")
    return NativeChunkLimits(**dict(chunks), parallelism=chunking.get("parallelism", 1))


def native_verification_backend(config: Any) -> NativeVerificationBackend:
    """Resolve the closed verifier selector without materializing an authored default."""
    execution = _native(config).get("execution")
    if not isinstance(execution, Mapping):
        raise ValueError("mssql_native.execution_required")
    if "verification_backend" not in execution:
        return NativeVerificationBackend.PYTHON_READBACK
    value = execution["verification_backend"]
    if value is None:
        raise ValueError("mssql_native.invalid_verification_backend")
    return NativeVerificationBackend.parse(value)


def native_import_backend(config: Any) -> NativeImportBackend:
    """Resolve the closed writer selector while keeping omission on BCP."""
    execution = _native(config).get("execution")
    if not isinstance(execution, Mapping):
        raise ValueError("mssql_native.execution_required")
    if "import_backend" not in execution:
        return NativeImportBackend.BCP
    value = execution["import_backend"]
    if value is None:
        raise ValueError("mssql_native.invalid_import_backend")
    return NativeImportBackend.parse(value)


def native_sqlclient_layout_version(config: Any) -> int:
    """Resolve the explicit SqlClient physical layout, preserving v1 on omission."""
    execution = _native(config).get("execution")
    if not isinstance(execution, Mapping):
        raise ValueError("mssql_native.execution_required")
    value = execution.get("layout_version", 1)
    if type(value) is not int or value not in {1, 2}:
        raise ValueError("mssql_native.invalid_layout_version")
    return value


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
    verifier = native_verification_backend(config)
    importer = native_import_backend(config)
    layout_version = native_sqlclient_layout_version(config)
    if (
        native_source_snapshot_policy(config).mode == "exact_raw_rows"
        and verifier is not NativeVerificationBackend.TARGET_LOCAL
    ):
        raise ValueError("mssql_native.raw_snapshot_requires_target_local_verification")
    if importer is NativeImportBackend.MSSQL_SQLCLIENT and verifier is not NativeVerificationBackend.TARGET_LOCAL:
        raise ValueError("mssql_native.sqlclient_requires_target_local_verification")
    execution = value.get("execution")
    if (
        isinstance(execution, Mapping)
        and "layout_version" in execution
        and importer is not NativeImportBackend.MSSQL_SQLCLIENT
    ):
        raise ValueError("mssql_native.layout_requires_sqlclient")
    if layout_version == 2 and importer is not NativeImportBackend.MSSQL_SQLCLIENT:
        raise ValueError("mssql_native.layout_requires_sqlclient")
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
