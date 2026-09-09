"""Derived physical-target authority for campaign-owned MSSQL shadows."""

from __future__ import annotations

from typing import Any

from dpone.backfill.shadow_append_authority import require_shadow_append_authority
from dpone.runtime.sinks.mssql_backfill_publication_catalog import (
    publication_target,
    require_shadow_owner,
)


def assert_shadow_append_target_identity(
    connector: Any,
    load_config: Any,
    operation: Any,
    *,
    identity_assertion: Any,
) -> None:
    """Revalidate the live binding plus the derived shadow owner under DML fencing."""

    request = operation.attempt.request
    authority = require_shadow_append_authority(load_config)
    if authority is None:
        identity_assertion(
            connector,
            database=request.target_database,
            schema=request.target_schema,
            table=request.target_table,
            expected=operation.attempt.target_identity,
        )
        return
    expected_shadow = (
        authority.live_database,
        authority.live_schema,
        authority.shadow_table,
    )
    actual_shadow = (
        request.target_database,
        request.target_schema,
        request.target_table,
    )
    if actual_shadow != expected_shadow:
        raise RuntimeError("mssql_backfill_publication.shadow_operation_coordinates_mismatch")
    identity_assertion(
        connector,
        database=authority.live_database,
        schema=authority.live_schema,
        table=authority.live_table,
        expected=operation.attempt.target_identity,
    )
    require_shadow_owner(
        connector,
        publication_target(
            database=authority.live_database,
            schema=authority.live_schema,
            table=authority.shadow_table,
        ),
        run_key=authority.run_key,
    )


__all__ = [
    "assert_shadow_append_target_identity",
]
