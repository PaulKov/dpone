"""Shared PostgreSQL XMin freeze-horizon verification."""

from __future__ import annotations

from typing import Any


def require_checkpoint_above_freeze_horizon(
    connector: Any,
    load_config: Any,
    *,
    checkpoint_xmin: int,
) -> int:
    """Return relation frozen XID only when a checkpoint is still replayable."""

    if type(checkpoint_xmin) is not int or checkpoint_xmin < 1:
        raise ValueError("postgres_xmin_handoff.anchor_invalid")
    rows = connector.get_records(
        "SELECT (age(%s::text::xid) > age(c.relfrozenxid) "
        "OR age(%s::text::xid) > age(d.datfrozenxid)) AS checkpoint_frozen, "
        "c.relfrozenxid::text::bigint AS relation_frozen_xid, "
        "d.datfrozenxid::text::bigint AS database_frozen_xid "
        "FROM pg_catalog.pg_class AS c "
        "INNER JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
        "INNER JOIN pg_catalog.pg_database AS d ON d.datname = current_database() "
        "WHERE n.nspname = %s AND c.relname = %s AND c.relkind IN ('r', 'p')",
        (
            checkpoint_xmin % (2**32),
            checkpoint_xmin % (2**32),
            load_config.source_schema,
            load_config.source_table,
        ),
        as_dict=True,
    )
    if not rows:
        raise ValueError("postgres_xmin_freeze_horizon_unverified")
    if bool(rows[0].get("checkpoint_frozen")):
        raise ValueError("postgres_xmin_handoff.anchor_too_old")
    return int(rows[0]["relation_frozen_xid"])


__all__ = ["require_checkpoint_above_freeze_horizon"]
