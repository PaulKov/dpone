"""Safety contract for PostgreSQL column-based incremental extraction.

PostgreSQL column cursors historically derived their checkpoint from
``MAX(incremental_column)`` in the target.  A finite-precision value is not a
durable source boundary: two transactions may commit rows with the same value,
and a retry can therefore advance past a row that was not in the earlier
source snapshot.

The contract in this module is intentionally shared by manifest validation and
runtime strategy selection.  Keeping one policy prevents a manifest-only gate
from being bypassed by programmatic ``LoadConfig`` construction.
"""

from __future__ import annotations

from typing import Any

from dpone._compat import StrEnum
from dpone.contracts.sink_dialect import (
    SinkDialectAuthorityConflictError,
    is_mssql_dialect,
    resolve_sink_dialect,
)
from dpone.contracts.target_max_incremental_cursor import (
    POSTGRES_MSSQL_COLUMN_POLICY,
    UnsafePostgresMssqlColumnCursorError,
    assert_target_max_mssql_cursor_supported,
    target_max_cursor_policy,
    target_max_mssql_cursor_is_unsafe,
)


class PostgresIncrementalStrategy(StrEnum):
    """Canonical PostgreSQL incremental extraction modes."""

    XMIN = "xmin"
    COLUMN = "column"


_ALIASES = {
    "xmin": PostgresIncrementalStrategy.XMIN,
    "postgres_xmin": PostgresIncrementalStrategy.XMIN,
    "pg_xmin": PostgresIncrementalStrategy.XMIN,
    "column": PostgresIncrementalStrategy.COLUMN,
    "column_cursor": PostgresIncrementalStrategy.COLUMN,
    "incremental_column": PostgresIncrementalStrategy.COLUMN,
}
POSTGRES_MSSQL_COLUMN_CURSOR_ERROR_CODE = POSTGRES_MSSQL_COLUMN_POLICY.runtime_error_code
POSTGRES_MSSQL_COLUMN_CURSOR_GUIDANCE = POSTGRES_MSSQL_COLUMN_POLICY.guidance


def normalize_postgres_incremental_strategy(value: Any) -> PostgresIncrementalStrategy | None:
    """Return a canonical explicit strategy, or ``None`` for absent/unknown input."""

    if value is None:
        return None
    normalized = str(value).strip().lower().replace("-", "_")
    return _ALIASES.get(normalized)


def resolve_postgres_incremental_strategy(
    value: Any,
    *,
    incremental_column: Any,
) -> PostgresIncrementalStrategy:
    """Resolve explicit and legacy PostgreSQL strategy selection.

    Unknown explicit values are rejected here so direct runtime callers receive
    the same fail-closed behavior as manifest authors.
    """

    resolved = normalize_postgres_incremental_strategy(value)
    if value is not None and resolved is None:
        raise ValueError(
            f"Unknown PostgreSQL source.options.incremental_strategy {value!r}. Supported values: xmin, column."
        )
    if resolved is not None:
        return resolved
    if incremental_column:
        return PostgresIncrementalStrategy.COLUMN
    return PostgresIncrementalStrategy.XMIN


def assert_postgres_column_cursor_route_supported(*, configured_sink: Any, sink_connector: Any) -> None:
    """Reject SQL Server column cursors using the canonical dialect resolver."""

    assert_target_max_mssql_cursor_supported(
        source_type="postgres",
        configured_sink=configured_sink,
        sink_connector=sink_connector,
    )


def postgres_mssql_column_cursor_is_unsafe(
    *,
    source_type: Any,
    sink_type: Any,
    strategy: Any,
    incremental_column: Any,
    load_strategy: Any = "incremental_merge",
) -> bool:
    """Return whether the selected route would use the lossy legacy cursor."""

    policy = target_max_cursor_policy(source_type)
    if policy is None or policy.source_type != "postgres" or not is_mssql_dialect(sink_type):
        return False
    resolved = normalize_postgres_incremental_strategy(strategy)
    if strategy is not None and resolved is None:
        return False
    effective = resolved or (
        PostgresIncrementalStrategy.COLUMN if incremental_column else PostgresIncrementalStrategy.XMIN
    )
    return effective is PostgresIncrementalStrategy.COLUMN and target_max_mssql_cursor_is_unsafe(
        source_type=source_type,
        sink_type=sink_type,
        load_strategy=load_strategy,
    )


__all__ = [
    "POSTGRES_MSSQL_COLUMN_CURSOR_ERROR_CODE",
    "POSTGRES_MSSQL_COLUMN_CURSOR_GUIDANCE",
    "PostgresIncrementalStrategy",
    "SinkDialectAuthorityConflictError",
    "UnsafePostgresMssqlColumnCursorError",
    "assert_postgres_column_cursor_route_supported",
    "is_mssql_dialect",
    "normalize_postgres_incremental_strategy",
    "postgres_mssql_column_cursor_is_unsafe",
    "resolve_postgres_incremental_strategy",
    "resolve_sink_dialect",
]
