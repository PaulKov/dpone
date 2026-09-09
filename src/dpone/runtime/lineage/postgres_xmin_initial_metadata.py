"""Strategy-metadata policy for the PostgreSQL XMin initial handoff.

The initial backfill and the subsequent incremental key-snapshot route share
one SQL Server target.  Although each initial chunk uses ``incremental_merge``
for bounded idempotent DML, the durable rows must already carry the snapshot
metadata consumed by the incremental phase.  This module is the pure,
artifact-neutral authority for that cross-phase target contract.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.config.load_strategy import LoadStrategy


def is_postgres_xmin_initial_metadata_route(load_config: Any) -> bool:
    """Return whether SQL Server must generate baseline snapshot metadata.

    The public XMin route validator separately proves the complete source,
    sink, backfill, state, and unique-key contract.  This predicate deliberately
    performs no connector I/O and only selects the metadata projection after
    that admission boundary.
    """

    options = getattr(load_config, "options", None)
    if not isinstance(options, Mapping):
        return False
    source = str(options.get("source_type") or "").strip().casefold()
    sink = str(options.get("sink_type") or "").strip().casefold()
    if source not in {"postgres", "postgresql"} or sink not in {
        "mssql",
        "sqlserver",
        "sql_server",
    }:
        return False
    policy = options.get("xmin_execution")
    return isinstance(policy, Mapping) and policy.get("mode") == "initial"


def target_strategy_metadata_strategy(load_config: Any) -> LoadStrategy:
    """Resolve metadata semantics independently of the bounded DML wrapper."""

    if is_postgres_xmin_initial_metadata_route(load_config):
        return LoadStrategy.SNAPSHOT_DIFF
    strategy = getattr(load_config, "load_strategy", None)
    if not isinstance(strategy, LoadStrategy):
        raise ValueError("strategy_metadata.load_strategy_required")
    return strategy


__all__ = [
    "is_postgres_xmin_initial_metadata_route",
    "target_strategy_metadata_strategy",
]
