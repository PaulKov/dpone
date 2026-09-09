"""Partition-contract projection for manifest load configuration."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.config.load_strategy import LoadStrategy


def resolve_partition_contract(
    *,
    strategy_config: Mapping[str, Any],
    source_options: Mapping[str, Any],
    load_strategy: LoadStrategy,
) -> Any:
    """Materialize the partition authority selected by ``strategy.mode: auto``."""

    authored = strategy_config.get("partition")
    if authored is not None:
        return authored
    requested = str(strategy_config.get("mode") or LoadStrategy.FULL_REFRESH.value).strip().lower()
    column = source_options.get("partition_column")
    if requested == "auto" and load_strategy is LoadStrategy.PARTITION_REPLACE and isinstance(column, str):
        normalized = column.strip()
        if normalized:
            return {"column": normalized, "values_from_staging": True}
    return {}


__all__ = ["resolve_partition_contract"]
