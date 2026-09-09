"""Loss-prevention contract for ClickHouse target-derived cursors.

The legacy ClickHouse incremental strategy reads
``MAX(incremental_column)`` from the target and applies a strict ``>`` source
predicate.  Equal finite-precision values, out-of-order commits, older late
rows, and ``NULL`` are not represented by that checkpoint.  MSSQL receipt
atomicity protects the target commit but cannot recover those absent rows.
"""

from __future__ import annotations

from typing import Any

from dpone.contracts.target_max_incremental_cursor import (
    CLICKHOUSE_MSSQL_POLICY,
    UnsafeClickHouseMssqlCursorError,
    assert_target_max_mssql_cursor_supported,
    target_max_mssql_cursor_is_unsafe,
)

CLICKHOUSE_MSSQL_CURSOR_ERROR_CODE = CLICKHOUSE_MSSQL_POLICY.runtime_error_code
CLICKHOUSE_MSSQL_CURSOR_GUIDANCE = CLICKHOUSE_MSSQL_POLICY.guidance


def clickhouse_mssql_cursor_is_unsafe(
    *,
    source_type: Any,
    sink_type: Any,
    load_strategy: Any,
    incremental_column: Any,
) -> bool:
    """Return whether authoring selects the unsafe incremental route.

    An absent column is rejected too: the current implementation falls back to
    a full scan and then appends it, which duplicates an existing MSSQL target.
    ``incremental_column`` remains in the signature so validation call sites
    make the relevant authoring input explicit.
    """

    del incremental_column
    return target_max_mssql_cursor_is_unsafe(
        source_type=source_type,
        sink_type=sink_type,
        load_strategy=load_strategy,
    )


def assert_clickhouse_mssql_cursor_supported(
    *,
    configured_sink: Any,
    sink_connector: Any,
) -> None:
    """Reject a route using explicit dialect authority, never adapter names."""

    assert_target_max_mssql_cursor_supported(
        source_type="clickhouse",
        configured_sink=configured_sink,
        sink_connector=sink_connector,
    )


__all__ = [
    "CLICKHOUSE_MSSQL_CURSOR_ERROR_CODE",
    "CLICKHOUSE_MSSQL_CURSOR_GUIDANCE",
    "UnsafeClickHouseMssqlCursorError",
    "assert_clickhouse_mssql_cursor_supported",
    "clickhouse_mssql_cursor_is_unsafe",
]
