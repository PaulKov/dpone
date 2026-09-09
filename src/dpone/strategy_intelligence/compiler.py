from __future__ import annotations

from typing import Any

from dpone.config.load_strategy import LoadStrategy
from dpone.strategy_intelligence.advisor import StrategyAdvisor, StrategyContext


class StrategyAutoCompiler:
    """Compile public `strategy.mode: auto` into a concrete LoadStrategy."""

    def __init__(self, advisor: StrategyAdvisor | None = None) -> None:
        self._advisor = advisor or StrategyAdvisor()

    def compile(
        self,
        *,
        source_type: str,
        sink_type: str,
        strategy_cfg: dict[str, Any],
        source_options: dict[str, Any],
        sink_options: dict[str, Any] | None = None,
        source_table: str = "",
        target_table: str = "",
    ) -> tuple[LoadStrategy, dict[str, Any] | None]:
        sink_options = sink_options or {}
        requested = str(strategy_cfg.get("mode", LoadStrategy.FULL_REFRESH.value) or LoadStrategy.FULL_REFRESH.value)
        context = StrategyContext(
            source_type=source_type,
            sink_type=sink_type,
            requested_mode=requested,
            requested_merge_policy=str(strategy_cfg.get("merge_policy", "auto") or "auto"),
            unique_key=_unique_key_tuple(strategy_cfg.get("unique_key") or source_options.get("unique_key")),
            estimated_rows=_optional_int(source_options.get("estimated_rows")),
            changed_percent=_optional_float(source_options.get("changed_percent")),
            delete_percent=_optional_float(source_options.get("delete_percent")),
            partition_column=source_options.get("partition_column") or _partition_column(strategy_cfg),
            cdc_available=bool(source_options.get("cdc_enabled", False)),
            source_cursor=source_options.get("incremental_column"),
            source_table=source_table,
            target_table=target_table,
            source_options=source_options,
            sink_options=sink_options,
        )
        decision = self._advisor.advise(context)
        strategy = LoadStrategy(decision.strategy_mode)
        return strategy, {"decision": decision.to_dict()}


def _unique_key_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return tuple(part.strip() for part in value.split(",") if part.strip())
    if isinstance(value, list | tuple):
        return tuple(str(part).strip() for part in value if str(part).strip())
    return (str(value),)


def _partition_column(strategy_cfg: dict[str, Any]) -> str | None:
    partition = strategy_cfg.get("partition")
    if isinstance(partition, dict):
        raw = partition.get("column")
        return str(raw) if raw else None
    return None


def _optional_int(value: Any) -> int | None:
    if value in {None, ""}:
        return None
    return int(value)


def _optional_float(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    return float(value)
