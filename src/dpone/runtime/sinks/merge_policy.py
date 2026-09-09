"""Shared incremental merge and partition replace policy contracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any


class MergePolicy:
    """Stable public policy names for ``strategy.mode: incremental_merge``."""

    AUTO = "auto"
    UPDATE_INSERT = "update_insert"
    DELETE_INSERT = "delete_insert"
    SHADOW_SWAP = "shadow_swap"
    LIGHTWEIGHT_DELETE_INSERT = "lightweight_delete_insert"
    MUTATION_DELETE_INSERT = "mutation_delete_insert"
    EVENT_UPSERT = "event_upsert"


DEFAULT_MERGE_POLICY_BY_SINK: Mapping[str, str] = {
    "mssql": MergePolicy.DELETE_INSERT,
    "postgres": MergePolicy.DELETE_INSERT,
    "bigquery": MergePolicy.DELETE_INSERT,
    "clickhouse": MergePolicy.LIGHTWEIGHT_DELETE_INSERT,
    "kafka": MergePolicy.EVENT_UPSERT,
}

SUPPORTED_MERGE_POLICIES_BY_SINK: Mapping[str, tuple[str, ...]] = {
    "mssql": (MergePolicy.DELETE_INSERT, MergePolicy.SHADOW_SWAP),
    "postgres": (MergePolicy.DELETE_INSERT, MergePolicy.SHADOW_SWAP),
    "bigquery": (MergePolicy.DELETE_INSERT, MergePolicy.SHADOW_SWAP),
    "clickhouse": (
        MergePolicy.LIGHTWEIGHT_DELETE_INSERT,
        MergePolicy.SHADOW_SWAP,
        MergePolicy.MUTATION_DELETE_INSERT,
    ),
    "kafka": (MergePolicy.EVENT_UPSERT,),
}

NON_RECOMMENDED_POLICIES: frozenset[tuple[str, str]] = frozenset({("clickhouse", MergePolicy.MUTATION_DELETE_INSERT)})

DB_PARTITION_REPLACE_SINKS: frozenset[str] = frozenset({"mssql", "postgres", "bigquery", "clickhouse"})


@dataclass(frozen=True, slots=True)
class PartitionReplaceConfig:
    """Validated public config for ``strategy.mode: partition_replace``."""

    column: str
    value_expression: str | None = None
    values_from_staging: bool = True
    max_partitions_per_run: int = 64
    native: bool = True
    native_mode: str = "auto"
    require_native: bool = False


def resolve_merge_policy(load_config: Any, sink: str) -> str:
    """Resolve ``merge_policy: auto`` into a concrete sink-specific policy.

    The function is intentionally side-effect free so CLI plan, runtime sinks, and
    tests can use the same policy diagnostics.
    """

    sink_key = _normalize_sink(sink)
    requested = _strategy_value(load_config, "merge_policy", default=MergePolicy.AUTO)
    policy = _normalize_policy(requested)
    if policy == MergePolicy.AUTO:
        policy = DEFAULT_MERGE_POLICY_BY_SINK[sink_key]

    supported = SUPPORTED_MERGE_POLICIES_BY_SINK.get(sink_key, ())
    if policy not in supported:
        raise ValueError(
            f"merge_policy={policy!r} is not supported for sink.type={sink_key!r}. "
            f"Supported policies: {', '.join(supported) or 'none'}."
        )

    if (sink_key, policy) in NON_RECOMMENDED_POLICIES and not _allow_non_recommended_policy(load_config):
        raise ValueError(
            f"merge_policy={policy!r} for sink.type={sink_key!r} is non-recommended and must be explicitly enabled with "
            "sink.strategy.allow_non_recommended_policy: true. Prefer lightweight_delete_insert or shadow_swap."
        )

    return policy


def validate_duplicate_policy(load_config: Any) -> str:
    """Validate the v1 duplicate policy contract.

    v1 keeps duplicate handling deterministic: staging duplicates fail before the
    final target mutation. Future policies such as latest_by can be added without
    changing sink finalizers.
    """

    policy = str(_strategy_value(load_config, "duplicate_policy", default="fail") or "fail").strip().lower()
    if policy != "fail":
        raise ValueError(f"duplicate_policy={policy!r} is not supported yet. v1 supports duplicate_policy='fail' only.")
    return policy


def unique_key_columns(unique_key: str | Sequence[str] | None) -> tuple[str, ...]:
    if unique_key is None:
        return ()
    if isinstance(unique_key, str):
        columns = tuple(part.strip() for part in unique_key.split(",") if part.strip())
    else:
        columns = tuple(str(part).strip() for part in unique_key if str(part).strip())
    return columns


def require_unique_key(load_config: Any, *, strategy_name: str = "incremental_merge") -> tuple[str, ...]:
    columns = unique_key_columns(getattr(load_config, "unique_key", None))
    if not columns:
        raise ValueError(f"{strategy_name} requires sink.strategy.unique_key")
    return columns


def validate_partition_replace(load_config: Any, sink: str) -> PartitionReplaceConfig:
    sink_key = _normalize_sink(sink)
    if sink_key not in DB_PARTITION_REPLACE_SINKS:
        raise ValueError(
            f"partition_replace is not supported for sink.type={sink_key!r}. "
            "Use full_refresh, incremental_append, incremental_merge, or replace for this sink."
        )

    raw = getattr(load_config, "partition", None) or _strategy_value(load_config, "partition", default={}) or {}
    if not isinstance(raw, Mapping):
        raise ValueError("sink.strategy.partition must be an object")

    column = str(raw.get("column") or "").strip()
    if not column:
        raise ValueError("partition_replace requires sink.strategy.partition.column")
    value_expression = _partition_value_expression(raw.get("value_expression") or raw.get("expression"))

    max_partitions = int(raw.get("max_partitions_per_run", 64) or 64)
    if max_partitions < 1:
        raise ValueError("partition.max_partitions_per_run must be >= 1")

    native = bool(raw.get("native", True))
    native_mode = str(raw.get("native_mode") or raw.get("finalization") or "auto").strip().lower()
    if native_mode in {"off", "disabled", "disable", "fallback"}:
        native = False
        native_mode = "fallback"
    if native_mode in {"required", "require", "native_required"}:
        native = True
        native_mode = "required"
    if native_mode not in {"auto", "required", "fallback"}:
        raise ValueError("partition.native_mode must be one of: auto, required, fallback")

    return PartitionReplaceConfig(
        column=column,
        value_expression=value_expression,
        values_from_staging=bool(raw.get("values_from_staging", True)),
        max_partitions_per_run=max_partitions,
        native=native,
        native_mode=native_mode,
        require_native=bool(raw.get("require_native", False)) or native_mode == "required",
    )


def _partition_value_expression(value: Any) -> str | None:
    if value is None:
        return None
    expression = str(value).strip()
    if not expression:
        return None
    forbidden = (";", "--", "/*", "*/")
    if any(token in expression for token in forbidden):
        raise ValueError("partition.value_expression must be a single read-only SQL expression")
    return expression


def _strategy_value(load_config: Any, key: str, *, default: Any = None) -> Any:
    direct = getattr(load_config, key, None)
    if direct is not None:
        return direct
    options = getattr(load_config, "options", {}) or {}
    return options.get(key, default)


def _allow_non_recommended_policy(load_config: Any) -> bool:
    value = _strategy_value(load_config, "allow_non_recommended_policy", default=False)
    return bool(value)


def _normalize_sink(sink: str) -> str:
    normalized = str(sink).strip().lower().replace("-", "_")
    aliases = {"sqlserver": "mssql", "sql_server": "mssql", "bq": "bigquery"}
    return aliases.get(normalized, normalized)


def _normalize_policy(value: Any) -> str:
    if value is None:
        return MergePolicy.AUTO
    normalized = str(value).strip().lower().replace("-", "_")
    aliases = {
        "default": MergePolicy.AUTO,
        "auto": MergePolicy.AUTO,
        "update_insert": MergePolicy.UPDATE_INSERT,
        "update+insert": MergePolicy.UPDATE_INSERT,
        "delete_insert": MergePolicy.DELETE_INSERT,
        "delete+insert": MergePolicy.DELETE_INSERT,
        "delete_insert_merge": MergePolicy.DELETE_INSERT,
        "shadow_swap": MergePolicy.SHADOW_SWAP,
        "exchange": MergePolicy.SHADOW_SWAP,
        "lightweight_delete_insert": MergePolicy.LIGHTWEIGHT_DELETE_INSERT,
        "lightweight_delete+insert": MergePolicy.LIGHTWEIGHT_DELETE_INSERT,
        "mutation_delete_insert": MergePolicy.MUTATION_DELETE_INSERT,
        "alter_delete_insert": MergePolicy.MUTATION_DELETE_INSERT,
        "event_upsert": MergePolicy.EVENT_UPSERT,
        "upsert_events": MergePolicy.EVENT_UPSERT,
    }
    if normalized not in aliases:
        raise ValueError(f"Unknown merge_policy={value!r}")
    return aliases[normalized]
