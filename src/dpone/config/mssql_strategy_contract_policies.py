"""Per-strategy MSSQL policy normalizers used by plan and runtime."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.config.mssql_strategy_contract_models import (
    BackfillPolicy,
    FullRefreshPolicy,
    IncrementalAppendPolicy,
    IncrementalMergePolicy,
    MSSQLLoadStrategyContract,
    PartitionReplacePolicy,
    SCD2Policy,
    SnapshotDiffPolicy,
)
from dpone.config.mssql_strategy_contract_validation import (
    blocked,
    blocked_from,
    load_strategy,
    options,
    reject_irrelevant_direct_options,
    reject_irrelevant_strategy_sections,
    reject_unknown_keys,
    section,
    strict_bool,
    unique_key,
)
from dpone.config.source_scope_contract import SourceScopeContractError, resolve_source_scope
from dpone.contracts.connector_declarations import canonical_endpoint_type
from dpone.contracts.sink_dialect import is_mssql_dialect


def incremental_merge_policy(load_config: Any) -> IncrementalMergePolicy:
    keys = unique_key(load_config)
    if not keys:
        blocked("mssql.strategy.incremental_merge.unique_key", "incremental_merge requires unique_key")
    requested = str(getattr(load_config, "merge_policy", None) or options(load_config).get("merge_policy") or "auto")
    aliases = {
        "auto": "delete_insert",
        "update_insert": "update_insert",
        "delete_insert": "delete_insert",
        "shadow_swap": "shadow_swap",
    }
    if requested not in aliases:
        blocked(
            "mssql.strategy.incremental_merge.merge_policy",
            f"merge_policy={requested!r} is unsupported; supported: auto, update_insert, delete_insert, shadow_swap",
        )
    if requested == "shadow_swap":
        blocked(
            "mssql.strategy.incremental_merge.shadow_swap_physical_preservation",
            "shadow_swap cannot preserve every existing secondary index, constraint, permission, and "
            "supported physical object; use update_insert or delete_insert",
        )
    duplicate = str(
        getattr(load_config, "duplicate_policy", None) or options(load_config).get("duplicate_policy") or "fail"
    )
    if duplicate != "fail":
        blocked(
            "mssql.strategy.incremental_merge.duplicate_policy",
            f"duplicate_policy={duplicate!r} is unsupported; supported: fail",
        )
    if bool(getattr(load_config, "allow_non_recommended_policy", False)):
        blocked(
            "mssql.strategy.incremental_merge.allow_non_recommended_policy",
            "allow_non_recommended_policy is a ClickHouse-only option",
        )
    if getattr(load_config, "mutations_sync", None) is not None:
        blocked(
            "mssql.strategy.incremental_merge.mutations_sync",
            "mutations_sync is a ClickHouse-only option",
        )
    return IncrementalMergePolicy(
        merge_policy=aliases[requested],
        duplicate_policy=duplicate,
        unique_key=keys,
    )


def partition_replace_policy(load_config: Any) -> PartitionReplacePolicy:
    raw = getattr(load_config, "partition", None) or options(load_config).get("partition") or {}
    if not isinstance(raw, Mapping):
        blocked("mssql.strategy.partition_replace.partition", "partition must be an object")
    allowed = {
        "column",
        "value_expression",
        "values_from_staging",
        "max_partitions_per_run",
        "native",
        "native_mode",
        "require_native",
        "partition_function",
        "switch_out_schema",
        "switch_out_table_template",
    }
    reject_unknown_keys("mssql.strategy.partition_replace.partition", raw, allowed)
    column = str(raw.get("column") or "").strip()
    if not column:
        blocked("mssql.strategy.partition_replace.column", "partition.column is required")
    values_from_staging = strict_bool(raw.get("values_from_staging", True), "partition.values_from_staging")
    if not values_from_staging:
        blocked(
            "mssql.strategy.partition_replace.values_from_staging",
            "MSSQL requires values_from_staging=true because no external partition-value list is declared",
        )
    if str(raw.get("value_expression") or "").strip():
        blocked(
            "mssql.strategy.partition_replace.value_expression",
            "MSSQL fallback does not yet support expression-derived partition identities",
        )
    maximum = _positive_integer(
        raw.get("max_partitions_per_run", 64),
        blocker="mssql.strategy.partition_replace.max_partitions_per_run",
        field="max_partitions_per_run",
    )
    native = strict_bool(raw.get("native", True), "partition.native")
    native_mode = str(raw.get("native_mode") or "auto").strip().lower()
    if native_mode not in {"auto", "required", "fallback"}:
        blocked("mssql.strategy.partition_replace.native_mode", f"unknown native_mode={native_mode!r}")
    require_native = strict_bool(raw.get("require_native", False), "partition.require_native")
    hints = [
        key
        for key in ("partition_function", "switch_out_schema", "switch_out_table_template")
        if raw.get(key) not in (None, "")
    ]
    if hints or native_mode == "required" or require_native:
        blocked(
            "mssql.strategy.partition_replace.native_switch",
            "native SQL Server partition SWITCH is not route-live certified; use native_mode=fallback",
        )
    effective_native = native and native_mode == "auto"
    effective_mode = "auto" if effective_native else "fallback"
    return PartitionReplacePolicy(
        column=column,
        value_expression=None,
        values_from_staging=values_from_staging,
        max_partitions_per_run=maximum,
        native=effective_native,
        native_mode=effective_mode,
        require_native=False,
    )


def snapshot_diff_policy(load_config: Any) -> SnapshotDiffPolicy:
    raw = section(load_config, "diff")
    reject_unknown_keys("mssql.strategy.snapshot_diff.diff", raw, {"compare", "delete_policy"})
    compare = str(raw.get("compare") or "row_hash")
    if compare not in {"row_hash", "all_columns"}:
        blocked("mssql.strategy.snapshot_diff.compare", f"unknown compare={compare!r}")
    delete_policy = str(raw.get("delete_policy") or "hard_delete")
    if delete_policy not in {"ignore", "hard_delete", "soft_delete"}:
        blocked("mssql.strategy.snapshot_diff.delete_policy", f"unknown delete_policy={delete_policy!r}")
    keys = unique_key(load_config)
    if not keys:
        blocked("mssql.strategy.snapshot_diff.unique_key", "snapshot_diff requires unique_key")
    _reject_unbound_source_scope(
        load_config,
        destructive=delete_policy != "ignore",
        blocker="mssql.strategy.snapshot_diff.source_custom_predicate",
    )
    return SnapshotDiffPolicy(compare=compare, delete_policy=delete_policy, unique_key=keys)


def scd2_policy(load_config: Any) -> SCD2Policy:
    raw = section(load_config, "scd2")
    reject_unknown_keys(
        "mssql.strategy.scd2.scd2",
        raw,
        {
            "valid_from_column",
            "valid_to_column",
            "current_flag_column",
            "row_hash_column",
            "delete_policy",
        },
    )
    defaults = {
        "valid_from_column": "__dpone__valid_from_at",
        "valid_to_column": "__dpone__valid_to_at",
        "current_flag_column": "__dpone__is_current",
        "row_hash_column": "__dpone__row_hash",
    }
    for key, default in defaults.items():
        actual = str(raw.get(key) or default)
        if actual != default:
            blocked(
                f"mssql.strategy.scd2.{key}",
                f"custom {key} is not supported by the metadata enricher; use {default}",
            )
    delete_policy = str(raw.get("delete_policy") or "expire")
    if delete_policy == "hard_delete_not_supported":
        blocked(
            "mssql.strategy.scd2.delete_policy",
            "hard_delete_not_supported is a declaration marker, not an executable MSSQL policy",
        )
    if delete_policy not in {"expire", "ignore"}:
        blocked("mssql.strategy.scd2.delete_policy", f"unknown delete_policy={delete_policy!r}")
    keys = unique_key(load_config)
    if not keys:
        blocked("mssql.strategy.scd2.unique_key", "scd2 requires unique_key")
    _validate_scd2_physical_primary_key(
        load_config,
        business_key=keys,
        valid_from_column=defaults["valid_from_column"],
    )
    _reject_unbound_source_scope(
        load_config,
        destructive=delete_policy == "expire",
        blocker="mssql.strategy.scd2.source_custom_predicate",
    )
    return SCD2Policy(
        delete_policy=delete_policy,
        unique_key=keys,
        valid_from_column=defaults["valid_from_column"],
        valid_to_column=defaults["valid_to_column"],
        current_flag_column=defaults["current_flag_column"],
        row_hash_column=defaults["row_hash_column"],
    )


def _validate_scd2_physical_primary_key(
    load_config: Any,
    *,
    business_key: tuple[str, ...],
    valid_from_column: str,
) -> None:
    """Reject a physical key that cannot represent SCD2 history."""

    physical = options(load_config).get("physical_design") or {}
    if not isinstance(physical, Mapping):
        blocked("mssql.strategy.scd2.physical_design", "physical_design must be an object")
    indexes = physical.get("indexes") or {}
    if not isinstance(indexes, Mapping):
        blocked("mssql.strategy.scd2.physical_design.indexes", "indexes must be an object")
    raw_primary = indexes.get("primary_key")
    if raw_primary in (None, []):
        return
    if isinstance(raw_primary, str):
        values = (raw_primary,)
    elif isinstance(raw_primary, (list, tuple)):
        values = tuple(raw_primary)
    else:
        blocked(
            "mssql.strategy.scd2.physical_primary_key",
            "physical_design.indexes.primary_key must be a string or an ordered array",
        )
    primary_key = tuple(str(value).strip() for value in values if str(value).strip())
    certified = (*business_key, valid_from_column)
    if primary_key != certified:
        blocked(
            "mssql.strategy.scd2.physical_primary_key",
            "SCD2 physical primary_key must be the ordered business unique_key followed by "
            f"{valid_from_column}; received {primary_key!r}",
        )


def _positive_integer(value: Any, *, blocker: str, field: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        blocked_from(blocker, f"{field} must be an integer", exc)
    if parsed < 1:
        blocked(blocker, f"{field} must be >= 1")
    return parsed


def _reject_unbound_source_scope(load_config: Any, *, destructive: bool, blocker: str) -> None:
    """Reject scoped extracts whose missing-row action is target-global.

    ``source_custom_predicate`` is expressed in source SQL and therefore is
    not a proven target-side scope.  Applying a target-global anti-join after
    such an extract would delete or expire rows outside the source scope.
    """

    try:
        source_scope = resolve_source_scope(load_config)
    except SourceScopeContractError as exc:
        blocked(f"{blocker}.ambiguous", str(exc))
    if destructive and source_scope.predicate:
        blocked(
            blocker,
            "a source_custom_predicate cannot be combined with a target-global missing-row action; "
            "use delete_policy=ignore until a typed target-scope contract is available",
        )


def reject_cross_dialect_raw_predicate(load_config: Any, *, blocker: str) -> None:
    """Reject use of one raw SQL predicate as two dialects' scope authority."""

    configured = options(load_config)
    source = canonical_endpoint_type(str(configured.get("source_type") or ""))
    sink = configured.get("sink_type") or "mssql"
    if source == "postgres" and is_mssql_dialect(sink):
        blocked(
            blocker,
            "a raw PostgreSQL predicate is not a portable SQL Server target-scope contract; "
            "use a typed portable scope once source and target renderers are configured",
        )


__all__ = [
    "BackfillPolicy",
    "FullRefreshPolicy",
    "IncrementalAppendPolicy",
    "IncrementalMergePolicy",
    "MSSQLLoadStrategyContract",
    "PartitionReplacePolicy",
    "SCD2Policy",
    "SnapshotDiffPolicy",
    "incremental_merge_policy",
    "load_strategy",
    "options",
    "partition_replace_policy",
    "reject_irrelevant_direct_options",
    "reject_irrelevant_strategy_sections",
    "reject_cross_dialect_raw_predicate",
    "scd2_policy",
    "snapshot_diff_policy",
    "strict_bool",
    "unique_key",
    "blocked",
]
