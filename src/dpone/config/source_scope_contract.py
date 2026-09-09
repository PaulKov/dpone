"""Typed authority for source-side row-scope predicates.

Raw SQL predicates are intentionally source-dialect expressions.  This module
only resolves their authored location and detects ambiguous dual declarations;
it does not claim that the expression is portable to a target dialect.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


class SourceScopeContractError(ValueError):
    """Raised when one load has more than one conflicting source scope."""

    code = "DPONE_SOURCE_SCOPE_CONTRACT_BLOCKED"

    def __init__(self, blocker: str, message: str) -> None:
        self.blocker = blocker
        super().__init__(f"{blocker}: {message}")


@dataclass(frozen=True, slots=True)
class SourceScope:
    """One resolved source-dialect predicate and its authoring origin."""

    predicate: str | None
    origin: str | None


def resolve_source_scope(load_config: Any) -> SourceScope:
    """Resolve the two legacy authoring locations without silent precedence."""

    raw_options = getattr(load_config, "options", {}) or {}
    options = raw_options if isinstance(raw_options, Mapping) else {}
    option_predicate = _normalized(options.get("source_custom_predicate"))
    direct_predicate = _normalized(getattr(load_config, "custom_predicate", None))
    if option_predicate and direct_predicate and option_predicate != direct_predicate:
        raise SourceScopeContractError(
            "source_scope.ambiguous",
            "options.source_custom_predicate and custom_predicate differ",
        )
    if option_predicate:
        return SourceScope(option_predicate, "options.source_custom_predicate")
    if direct_predicate:
        return SourceScope(direct_predicate, "custom_predicate")
    return SourceScope(None, None)


def _normalized(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


__all__ = ["SourceScope", "SourceScopeContractError", "resolve_source_scope"]
