"""Admission for the currently unenforced strategy-level source-byte budget.

This rule concerns only ``sink.strategy.max_source_bytes``. It does not define
byte measurement or alter independently enforced file-staging policy limits.
"""

from __future__ import annotations

from collections.abc import Mapping


def source_byte_budget_rejection(strategy: Mapping[str, object]) -> str | None:
    """Return an actionable rejection for any explicit strategy budget.

    Presence includes null and malformed values: none may silently become an
    omitted safety limit. An absent field preserves legacy admission. Callers
    translate this pure decision into their existing error or issue contract.
    """

    if "max_source_bytes" not in strategy:
        return None
    return (
        "sink.strategy.max_source_bytes cannot be enforced by this runtime; "
        "this configuration is rejected before transfer execution. "
        "Use an already supported strategy or wait for a release with an approved source-byte budget contract."
    )
