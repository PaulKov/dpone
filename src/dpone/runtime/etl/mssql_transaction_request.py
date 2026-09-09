"""Shared immutable attempt construction for governed MSSQL loads."""

from __future__ import annotations

from typing import Any

from dpone.backfill.shadow_append_authority import require_shadow_append_authority


def live_target_coordinates(load_config: Any) -> tuple[str, str, str]:
    """Return the physical live relation used by target identity fencing."""

    database = str(getattr(load_config, "target_database", "") or "").strip()
    schema = str(getattr(load_config, "target_schema", "") or "").strip()
    table = str(getattr(load_config, "target_table", "") or "").strip()
    shadow = require_shadow_append_authority(load_config)
    if shadow is not None:
        return shadow.live_database, shadow.live_schema, shadow.live_table
    return database, schema, table


def attempt_target_coordinates(
    load_config: Any,
    *,
    physical_target: Any,
) -> tuple[str, str, str]:
    """Return the mutation coordinates persisted on an immutable attempt."""

    if require_shadow_append_authority(load_config) is not None:
        return (
            str(getattr(load_config, "target_database", "") or "").strip(),
            str(getattr(load_config, "target_schema", "") or "").strip(),
            str(getattr(load_config, "target_table", "") or "").strip(),
        )
    return (
        str(getattr(physical_target, "database_name", "") or "").strip(),
        str(getattr(physical_target, "schema_name", "") or "").strip(),
        str(getattr(physical_target, "table_name", "") or "").strip(),
    )


__all__ = [
    "attempt_target_coordinates",
    "live_target_coordinates",
]
