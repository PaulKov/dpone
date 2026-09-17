"""Compatibility surface for the source-byte budget rejection gate.

The ClickHouse runtime now enforces bounded full refresh directly. The legacy
predicate remains fail-closed because external callers may still use it before
they have negotiated an enforcing sink capability.
"""

from __future__ import annotations

from collections.abc import Mapping

SOURCE_BYTE_BUDGET_FIELD = "max_source_bytes"


def source_byte_budget_rejection(strategy: Mapping[str, object]) -> str | None:
    """Reject an explicit budget when no enforcing capability was negotiated.

    Canonical builders do not call this compatibility predicate after they have
    selected the ClickHouse enforcing runtime. Existing external callers retain
    the previous safe default instead of silently losing their admission gate.
    """

    if SOURCE_BYTE_BUDGET_FIELD not in strategy:
        return None
    return (
        "sink.strategy.max_source_bytes requires a negotiated enforcing runtime; "
        "this configuration is rejected before transfer execution"
    )


__all__ = ["SOURCE_BYTE_BUDGET_FIELD", "source_byte_budget_rejection"]
