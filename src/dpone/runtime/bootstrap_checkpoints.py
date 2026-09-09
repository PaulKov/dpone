"""Checkpoint-store builders for runtime bootstrap."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def build_sql_partition_checkpoint_store(*, state_type: str, connector: Any, state_cfg: Mapping[str, Any]) -> Any:
    """Build a SQL-backed native transfer checkpoint store for state backends."""

    checkpoint_cfg = state_cfg.get("partition_checkpoint_table", {})
    schema = checkpoint_cfg.get("schema") or state_cfg.get("table", {}).get("schema", "etl_state")
    table = checkpoint_cfg.get("name", "dpone_partition_checkpoints")
    from dpone.runtime.lineage.partition_checkpoint_sql_store import (
        MSSQLCheckpointDialect,
        PostgresCheckpointDialect,
        SqlPartitionCheckpointStore,
    )

    dialect = MSSQLCheckpointDialect() if state_type == "mssql" else PostgresCheckpointDialect()
    return SqlPartitionCheckpointStore(connector=connector, dialect=dialect, schema=schema, table=table)


__all__ = ["build_sql_partition_checkpoint_store"]
