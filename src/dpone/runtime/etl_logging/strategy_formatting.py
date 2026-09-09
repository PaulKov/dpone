from __future__ import annotations

from typing import Any


def build_strategy_string(
    strategy: str,
    unique_key: str | list[str] | None = None,
    custom_predicate: str | None = None,
) -> str:
    strategy_upper = strategy.upper()
    if "FULL_REFRESH" in strategy_upper or "FULL" in strategy_upper:
        return "FULL_REFRESH"
    if "REPLACE" in strategy_upper:
        return "REPLACE"
    if "INCREMENTAL_MERGE" in strategy_upper:
        return "INCREMENTAL_MERGE"
    if "INCREMENTAL_APPEND" in strategy_upper:
        return "INCREMENTAL_APPEND"
    if "INCREMENTAL" in strategy_upper or "INCREMENT" in strategy_upper:
        return "INCREMENTAL"
    return strategy_upper


def format_strategy_with_color(
    strategy: str,
    *,
    colors: Any,
    show_colors: bool,
    unique_key: str | list[str] | None = None,
    custom_predicate: str | None = None,
) -> str:
    if not show_colors:
        return build_strategy_string(strategy, unique_key, custom_predicate)

    strategy_upper = strategy.upper()
    strategy_string = build_strategy_string(strategy, unique_key, custom_predicate)
    if "FULL_REFRESH" in strategy_upper or "FULL" in strategy_upper:
        return f"{colors.FULL_REFRESH}{colors.BOLD}{strategy_string}{colors.RESET}"
    if "INCREMENTAL" in strategy_upper or "INCREMENT" in strategy_upper:
        return f"{colors.INCREMENTAL}{colors.BOLD}{strategy_string}{colors.RESET}"
    if "REPLACE" in strategy_upper:
        return f"{colors.REPLACE}{colors.BOLD}{strategy_string}{colors.RESET}"
    return f"{colors.BLUE}{colors.BOLD}{strategy_string}{colors.RESET}"


__all__ = ["build_strategy_string", "format_strategy_with_color"]
