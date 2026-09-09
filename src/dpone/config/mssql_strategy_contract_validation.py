"""Shared parsing and fail-closed validation for MSSQL strategy policies."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, NoReturn

from dpone.config.load_strategy import LoadStrategy
from dpone.config.mssql_strategy_contract_error import MSSQLStrategyContractError


def reject_irrelevant_strategy_sections(strategy: LoadStrategy, options: Mapping[str, Any]) -> None:
    owners = {
        "diff": LoadStrategy.SNAPSHOT_DIFF,
        "scd2": LoadStrategy.SCD2,
        "backfill": LoadStrategy.BACKFILL,
        "cdc": LoadStrategy.CDC_APPLY,
    }
    for section_name, owner in owners.items():
        value = options.get(section_name)
        if value not in (None, {}, False) and strategy != owner:
            blocked(
                f"mssql.strategy.{strategy.value}.irrelevant_{section_name}",
                f"{section_name} options are only valid for {owner.value}",
            )


def reject_irrelevant_direct_options(strategy: LoadStrategy, load_config: Any) -> None:
    if strategy != LoadStrategy.FULL_REFRESH and getattr(load_config, "overwrite_type", None) is not None:
        blocked(
            f"mssql.strategy.{strategy.value}.irrelevant_overwrite_type",
            "overwrite_type is only valid for full_refresh",
        )
    backfill = options(load_config).get("backfill") if strategy == LoadStrategy.BACKFILL else None
    backfill_append = isinstance(backfill, Mapping) and str(backfill.get("inner_mode") or "").strip().casefold() == (
        "incremental_append"
    )
    if (
        strategy != LoadStrategy.INCREMENTAL_APPEND
        and not backfill_append
        and getattr(load_config, "only_new_rows", False) is True
    ):
        blocked(
            f"mssql.strategy.{strategy.value}.irrelevant_only_new_rows",
            "only_new_rows is only valid for incremental_append or backfill inner_mode=incremental_append",
        )
    if strategy not in {LoadStrategy.INCREMENTAL_MERGE, LoadStrategy.BACKFILL}:
        merge_policy = str(getattr(load_config, "merge_policy", "auto") or "auto")
        if merge_policy != "auto":
            blocked(
                f"mssql.strategy.{strategy.value}.irrelevant_merge_policy",
                "merge_policy is only valid for incremental_merge or backfill inner_mode=incremental_merge",
            )
    if strategy not in {LoadStrategy.PARTITION_REPLACE, LoadStrategy.BACKFILL} and getattr(
        load_config, "partition", None
    ):
        blocked(
            f"mssql.strategy.{strategy.value}.irrelevant_partition",
            "partition options are only valid for partition_replace or backfill inner_mode=partition_replace",
        )


def load_strategy(load_config: Any) -> LoadStrategy:
    raw = getattr(load_config, "load_strategy", None)
    try:
        return raw if isinstance(raw, LoadStrategy) else LoadStrategy(str(raw))
    except ValueError as exc:
        raise MSSQLStrategyContractError("mssql.strategy.mode", f"unknown load strategy={raw!r}") from exc


def options(load_config: Any) -> Mapping[str, Any]:
    raw = getattr(load_config, "options", None) or {}
    if not isinstance(raw, Mapping):
        blocked("mssql.strategy.options", "options must be an object")
    return raw


def section(load_config: Any, name: str) -> Mapping[str, Any]:
    raw = options(load_config).get(name) or {}
    if not isinstance(raw, Mapping):
        blocked(f"mssql.strategy.{name}", f"{name} must be an object")
    return raw


def unique_key(load_config: Any) -> tuple[str, ...]:
    raw = getattr(load_config, "unique_key", None)
    if raw is None:
        return ()
    values: Sequence[Any] = raw.split(",") if isinstance(raw, str) else raw
    keys = tuple(str(value).strip() for value in values if str(value).strip())
    if len(set(keys)) != len(keys):
        blocked("mssql.strategy.unique_key", "unique_key contains duplicate columns")
    return keys


def strict_bool(value: Any, field: str) -> bool:
    if isinstance(value, bool):
        return value
    blocked(f"mssql.strategy.{field}", f"{field} must be a boolean")


def reject_unknown_keys(prefix: str, raw: Mapping[str, Any], allowed: set[str]) -> None:
    unknown = sorted(str(key) for key in raw if str(key) not in allowed)
    if unknown:
        blocked(prefix, f"unsupported options: {', '.join(unknown)}")


def blocked(blocker: str, detail: str) -> NoReturn:
    raise MSSQLStrategyContractError(blocker, detail)


def blocked_from(blocker: str, detail: str, cause: Exception) -> NoReturn:
    """Raise the stable contract error while retaining the parsing cause."""

    raise MSSQLStrategyContractError(blocker, detail) from cause


__all__ = [
    "blocked",
    "blocked_from",
    "load_strategy",
    "options",
    "reject_irrelevant_direct_options",
    "reject_irrelevant_strategy_sections",
    "reject_unknown_keys",
    "section",
    "strict_bool",
    "unique_key",
]
