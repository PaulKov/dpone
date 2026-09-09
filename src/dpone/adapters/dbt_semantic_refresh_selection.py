"""Thin dbt CLI argument adapter for semantic refresh V2 exact selection."""

from __future__ import annotations

from dpone.contracts.dbt_semantic_refresh_selection import V2_DBT_INDIRECT_SELECTION


def build_semantic_refresh_selection_arguments(
    selectors: tuple[str, ...],
) -> tuple[str, ...]:
    """Build the only admitted V2 selection fragment without invoking dbt."""

    if (
        not isinstance(selectors, tuple)
        or not selectors
        or len(selectors) != len(set(selectors))
        or tuple(sorted(selectors)) != selectors
        or any(
            not isinstance(selector, str)
            or not selector.startswith("fqn:")
            or selector.startswith("+fqn:")
            or any(character.isspace() or character == "\x00" for character in selector)
            for selector in selectors
        )
    ):
        raise ValueError("semantic refresh selectors must be sorted exact FQN tokens")
    return (
        "--indirect-selection",
        V2_DBT_INDIRECT_SELECTION,
        "--select",
        *selectors,
    )


__all__ = ["build_semantic_refresh_selection_arguments"]
