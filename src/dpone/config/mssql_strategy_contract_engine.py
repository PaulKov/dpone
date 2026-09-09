"""Single normalization authority for SQL Server load strategies."""

from __future__ import annotations

from typing import Any

from dpone.backfill.portable_scope_runtime import backfill_scope_authoring_violation
from dpone.config import mssql_strategy_contract_policies as contract
from dpone.config.load_strategy import LoadStrategy
from dpone.config.mssql_strategy_backfill_policy import backfill_policy
from dpone.config.mssql_strategy_contract_models import ReplacePolicy
from dpone.config.source_scope_contract import SourceScopeContractError, resolve_source_scope
from dpone.contracts.portable_relation_scope import (
    PortableScopeContractError,
    portable_scope_sha256,
    resolve_portable_relation_scope,
)
from dpone.contracts.sink_dialect import is_mssql_dialect


def normalize_mssql_load_strategy(load_config: Any) -> contract.MSSQLLoadStrategyContract:
    """Normalize and validate every MSSQL strategy option before staging."""

    return _normalize_mssql_load_strategy(
        load_config,
        allow_planner_replace_scope=False,
    )


def normalize_mssql_backfill_campaign_strategy(load_config: Any) -> contract.MSSQLLoadStrategyContract:
    """Validate MSSQL campaign authoring before catalog or durable-state I/O.

    A campaign-level ``replace`` has no relation scope yet because the trusted
    planner owns that scope. Every other invariant is identical to runtime
    normalization; each concrete chunk is normalized again after binding.
    """

    strategy = contract.load_strategy(load_config)
    if strategy != LoadStrategy.BACKFILL:
        contract.blocked(
            "mssql.strategy.backfill.campaign_mode",
            "MSSQL backfill campaign normalization requires load_strategy=backfill",
        )
    scope_violation = backfill_scope_authoring_violation(load_config)
    if scope_violation is not None:
        contract.blocked(*scope_violation)
    return _normalize_mssql_load_strategy(
        load_config,
        allow_planner_replace_scope=True,
    )


def normalize_mssql_authoring_strategy(load_config: Any) -> contract.MSSQLLoadStrategyContract:
    """Validate an authored MSSQL config at its correct lifecycle boundary.

    Backfill campaign authoring precedes planner-issued chunk scope, while all
    other strategies already contain their complete runtime scope. Concrete
    backfill chunks continue to use :func:`normalize_mssql_load_strategy`.
    """

    if contract.load_strategy(load_config) == LoadStrategy.BACKFILL:
        return normalize_mssql_backfill_campaign_strategy(load_config)
    return normalize_mssql_load_strategy(load_config)


def _normalize_mssql_load_strategy(
    load_config: Any,
    *,
    allow_planner_replace_scope: bool,
) -> contract.MSSQLLoadStrategyContract:
    strategy = contract.load_strategy(load_config)
    configured_options = contract.options(load_config)
    contract.reject_irrelevant_strategy_sections(strategy, configured_options)
    contract.reject_irrelevant_direct_options(strategy, load_config)
    try:
        portable_scope = resolve_portable_relation_scope(load_config)
    except PortableScopeContractError as exc:
        contract.blocked("mssql.strategy.portable_scope", str(exc))
    if portable_scope is not None and strategy not in {LoadStrategy.REPLACE, LoadStrategy.BACKFILL}:
        contract.blocked(
            f"mssql.strategy.{strategy.value}.irrelevant_portable_scope",
            "portable_scope is only valid for ordinary replace or a runtime-issued backfill chunk",
        )

    if strategy == LoadStrategy.FULL_REFRESH:
        overwrite = str(
            getattr(load_config, "overwrite_type", None)
            or configured_options.get("overwrite_type")
            or "truncate_insert"
        )
        if overwrite not in {"truncate_insert", "exchange"}:
            contract.blocked(
                "mssql.strategy.full_refresh.overwrite_type",
                f"unknown overwrite_type={overwrite!r}",
            )
        if overwrite == "exchange":
            contract.blocked(
                "mssql.strategy.full_refresh.exchange_physical_preservation",
                "exchange is not certified to preserve existing SQL Server indexes and compression; "
                "use truncate_insert",
            )
        return contract.MSSQLLoadStrategyContract(
            mode=strategy.value,
            full_refresh=contract.FullRefreshPolicy(overwrite_type=overwrite),
        )

    if strategy == LoadStrategy.INCREMENTAL_APPEND:
        only_new = contract.strict_bool(getattr(load_config, "only_new_rows", False), "only_new_rows")
        keys = contract.unique_key(load_config)
        if only_new and not keys:
            contract.blocked(
                "mssql.strategy.incremental_append.unique_key",
                "only_new_rows=true requires unique_key",
            )
        return contract.MSSQLLoadStrategyContract(
            mode=strategy.value,
            incremental_append=contract.IncrementalAppendPolicy(
                only_new_rows=only_new,
                unique_key=keys,
            ),
        )

    if strategy == LoadStrategy.INCREMENTAL_MERGE:
        return contract.MSSQLLoadStrategyContract(
            mode=strategy.value,
            incremental_merge=contract.incremental_merge_policy(load_config),
        )

    if strategy == LoadStrategy.REPLACE:
        try:
            source_scope = resolve_source_scope(load_config)
        except SourceScopeContractError as exc:
            contract.blocked("mssql.strategy.replace.source_scope_ambiguous", str(exc))
        if source_scope.predicate is not None and portable_scope is not None:
            contract.blocked(
                "mssql.strategy.replace.scope_ambiguous",
                "custom_predicate and portable_scope cannot both own one replace scope",
            )
        if source_scope.predicate is None and portable_scope is None:
            contract.blocked(
                "mssql.strategy.replace.custom_predicate",
                "replace requires custom_predicate or portable_scope",
            )
        if portable_scope is not None:
            source = str(configured_options.get("source_type") or "").strip().casefold()
            sink = configured_options.get("sink_type") or "mssql"
            if source not in {"postgres", "postgresql"} or not is_mssql_dialect(sink):
                contract.blocked(
                    "mssql.strategy.replace.portable_scope_route",
                    "portable_scope currently requires a PostgreSQL to MSSQL route",
                )
            return contract.MSSQLLoadStrategyContract(
                mode=strategy.value,
                replace=ReplacePolicy(
                    scope_kind="portable_relation_scope_v1",
                    portable_scope_sha256=portable_scope_sha256(portable_scope).hex(),
                ),
            )
        contract.reject_cross_dialect_raw_predicate(
            load_config,
            blocker="mssql.strategy.replace.cross_dialect_raw_predicate",
        )
        return contract.MSSQLLoadStrategyContract(
            mode=strategy.value,
            replace=ReplacePolicy(
                scope_kind="legacy_raw_same_dialect",
                portable_scope_sha256=None,
            ),
        )

    if strategy == LoadStrategy.PARTITION_REPLACE:
        return contract.MSSQLLoadStrategyContract(
            mode=strategy.value,
            partition_replace=contract.partition_replace_policy(load_config),
        )

    if strategy == LoadStrategy.SNAPSHOT_DIFF:
        return contract.MSSQLLoadStrategyContract(
            mode=strategy.value,
            snapshot_diff=contract.snapshot_diff_policy(load_config),
        )

    if strategy == LoadStrategy.SCD2:
        return contract.MSSQLLoadStrategyContract(
            mode=strategy.value,
            scd2=contract.scd2_policy(load_config),
        )

    if strategy == LoadStrategy.BACKFILL:
        return contract.MSSQLLoadStrategyContract(
            mode=strategy.value,
            backfill=backfill_policy(
                load_config,
                allow_planner_replace_scope=allow_planner_replace_scope,
            ),
        )

    contract.blocked(
        "mssql.strategy.mode",
        f"strategy {strategy.value!r} is not implemented by the MSSQL batch sink",
    )


__all__ = [
    "normalize_mssql_authoring_strategy",
    "normalize_mssql_backfill_campaign_strategy",
    "normalize_mssql_load_strategy",
]
