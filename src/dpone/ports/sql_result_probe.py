"""Dialect-neutral SQL helpers for resolving SELECT result metadata.

Belongs in the ports layer so connectors and default port methods share one
header-probe contract without runtime → ports inversion.
"""

from __future__ import annotations

from typing import Any


def header_probe_sql(query: Any) -> str:
    """Return a zero-row probe that preserves result column metadata.

    Prefer ``LIMIT 0`` over ``WHERE 1 = 0``: clickhouse-connect HTTP often
    omits ``column_names`` for false-predicate probes while still returning
    metadata for ``LIMIT 0`` (required for streaming ``as_dict``).
    """

    return f"SELECT * FROM ({query}) AS sub LIMIT 0"


__all__ = ["header_probe_sql"]
