"""SQL Server backfill strategy capability policy."""

from __future__ import annotations

from typing import Any

from dpone.backfill.execution_policy import execution_policy_from_load_config
from dpone.config.mssql_strategy_contract_models import (
    BackfillPolicy,
    IncrementalAppendPolicy,
    IncrementalMergePolicy,
    PartitionReplacePolicy,
)
from dpone.config.mssql_strategy_contract_policies import (
    incremental_merge_policy,
    partition_replace_policy,
    reject_cross_dialect_raw_predicate,
)
from dpone.config.mssql_strategy_contract_validation import (
    blocked,
    blocked_from,
    options,
    strict_bool,
    unique_key,
)
from dpone.config.source_scope_contract import SourceScopeContractError, resolve_source_scope
from dpone.contracts.portable_relation_scope import resolve_portable_relation_scope
from dpone.contracts.sink_dialect import is_mssql_dialect


def backfill_policy(
    load_config: Any,
    *,
    allow_planner_replace_scope: bool = False,
) -> BackfillPolicy:
    """Normalize the MSSQL backfill inner strategy and publication contract.

    Campaign admission may validate an unscoped ``replace`` config before the
    trusted planner issues its per-chunk scope. Runtime admission never sets
    that flag and therefore still requires the concrete scope.
    """

    try:
        execution_policy = execution_policy_from_load_config(load_config)
    except ValueError as exc:
        blocked_from("mssql.strategy.backfill.backfill", str(exc), exc)
    inner_mode = execution_policy.inner_mode
    if inner_mode not in {"partition_replace", "replace", "incremental_append", "incremental_merge"}:
        blocked(
            "mssql.strategy.backfill.inner_mode",
            f"inner_mode={inner_mode!r} is outside the public MSSQL batch contract",
        )
    publication = execution_policy.publication
    if publication.mode == "shadow_swap" and inner_mode != "incremental_append":
        blocked(
            "mssql.strategy.backfill.shadow_swap_inner_mode",
            "publication.mode=shadow_swap requires inner_mode=incremental_append",
        )
    inner_policy = _inner_policy(
        load_config,
        execution_policy=execution_policy,
        allow_planner_replace_scope=allow_planner_replace_scope,
    )
    return BackfillPolicy(
        inner_mode=inner_mode,
        parallel_workers=execution_policy.parallel_workers,
        publication_mode=publication.mode,
        retain_backup=publication.retain_backup,
        inner_policy=inner_policy,
    )


def _inner_policy(
    load_config: Any,
    *,
    execution_policy: Any,
    allow_planner_replace_scope: bool,
) -> IncrementalAppendPolicy | IncrementalMergePolicy | PartitionReplacePolicy | None:
    inner_mode = execution_policy.inner_mode
    if inner_mode == "partition_replace":
        return partition_replace_policy(load_config)
    if inner_mode == "incremental_merge":
        return incremental_merge_policy(load_config)
    if inner_mode == "incremental_append":
        return _shadow_append_policy(load_config, execution_policy=execution_policy)
    if inner_mode == "replace":
        _require_replace_scope(
            load_config,
            allow_planner_scope=(allow_planner_replace_scope and execution_policy.chunk is not None),
        )
    return None


def _shadow_append_policy(load_config: Any, *, execution_policy: Any) -> IncrementalAppendPolicy:
    publication = execution_policy.publication
    keys = unique_key(load_config)
    only_new = strict_bool(getattr(load_config, "only_new_rows", False), "only_new_rows")
    requirements = (
        (
            publication.mode != "shadow_swap",
            "incremental_append_publication",
            "backfill inner_mode=incremental_append requires publication.mode=shadow_swap",
        ),
        (
            execution_policy.chunk is None,
            "shadow_swap_chunk",
            "publication.mode=shadow_swap requires a deterministic backfill.chunk plan",
        ),
        (
            only_new,
            "shadow_swap_only_new_rows",
            "shadow initial load requires only_new_rows=false because chunks are disjoint and receipted",
        ),
        (
            not keys,
            "shadow_swap_unique_key",
            "publication.mode=shadow_swap requires unique_key for exact duplicate validation",
        ),
        (
            not publication.retain_backup,
            "shadow_swap_backup_retention",
            "shadow publication requires retain_backup=true; cleanup is an explicit operator action",
        ),
        (
            execution_policy.state.backend != "audit_schema" or not execution_policy.state.require_distributed_lock,
            "shadow_swap_state",
            "publication.mode=shadow_swap requires audit_schema state with require_distributed_lock=true",
        ),
    )
    for rejected, code, message in requirements:
        if rejected:
            blocked(f"mssql.strategy.backfill.{code}", message)
    configured = options(load_config)
    source = str(configured.get("source_type") or "").strip().casefold()
    sink = configured.get("sink_type") or "mssql"
    if source not in {"postgres", "postgresql"} or not is_mssql_dialect(sink):
        blocked(
            "mssql.strategy.backfill.shadow_swap_route",
            "publication.mode=shadow_swap is certified only for PostgreSQL to MSSQL",
        )
    return IncrementalAppendPolicy(only_new_rows=False, unique_key=keys)


def _require_replace_scope(
    load_config: Any,
    *,
    allow_planner_scope: bool,
) -> None:
    try:
        source_scope = resolve_source_scope(load_config)
    except SourceScopeContractError as exc:
        blocked("mssql.strategy.backfill.source_scope_ambiguous", str(exc))
    portable_scope = resolve_portable_relation_scope(load_config)
    if source_scope.predicate is not None and portable_scope is not None:
        blocked(
            "mssql.strategy.backfill.scope_ambiguous",
            "raw custom_predicate and portable_scope cannot both own one backfill chunk",
        )
    if source_scope.predicate is None and portable_scope is None:
        if allow_planner_scope:
            return
        blocked(
            "mssql.strategy.backfill.scope_required",
            "backfill inner_mode=replace requires a runtime portable scope or same-dialect predicate",
        )
    if portable_scope is None:
        reject_cross_dialect_raw_predicate(
            load_config,
            blocker="mssql.strategy.backfill.replace_cross_dialect_raw_predicate",
        )


__all__ = ["backfill_policy"]
