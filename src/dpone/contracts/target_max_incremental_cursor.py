"""Shared fail-closed policy for target-derived single-column cursors.

The built-in ClickHouse, MySQL, MSSQL, and legacy PostgreSQL column
strategies derive their lower bound from ``MAX(incremental_column)`` in the
target and query the source with a strict ``>`` predicate.  That pair is not a
complete source checkpoint: equal finite-precision values, out-of-order
commits, older late rows, and ``NULL`` can be absent forever.

This module is the single policy authority used by manifest validation,
source facades, direct strategy entry points, and MSSQL transaction admission.
Source-specific modules re-export their historical diagnostics so public error
codes remain stable.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from dpone.contracts.sink_dialect import is_mssql_dialect, require_sink_dialect_authority


@dataclass(frozen=True, slots=True)
class TargetMaxCursorPolicy:
    """Stable diagnostics and affected load modes for one source dialect."""

    source_type: str
    manifest_error_code: str
    runtime_error_code: str
    guidance: str
    incremental_strategies: frozenset[str]


_INCREMENTAL_APPEND = frozenset({"incremental_append"})
_INCREMENTAL_APPEND_MERGE = frozenset({"incremental_append", "incremental_merge"})

CLICKHOUSE_MSSQL_POLICY = TargetMaxCursorPolicy(
    source_type="clickhouse",
    manifest_error_code="CLICKHOUSE_MSSQL_TARGET_MAX_CURSOR_UNSAFE",
    runtime_error_code="clickhouse_mssql.target_max_cursor_unsafe",
    guidance=(
        "ClickHouse to MSSQL incremental extraction is disabled because target-derived "
        "MAX(incremental_column) with a strict > predicate can skip equal-precision, "
        "out-of-order, late older, and NULL rows. Use full_refresh for a complete table, "
        "or a complete bounded replace/partition_replace/backfill window. Adding lookback_days "
        "or a unique_key alone is not sufficient until dpone ships an atomic bounded-overlap "
        "or source-snapshot cursor contract."
    ),
    incremental_strategies=_INCREMENTAL_APPEND,
)
MYSQL_MSSQL_POLICY = TargetMaxCursorPolicy(
    source_type="mysql",
    manifest_error_code="MYSQL_MSSQL_TARGET_MAX_CURSOR_UNSAFE",
    runtime_error_code="mysql_mssql.target_max_cursor_unsafe",
    guidance=(
        "MySQL to MSSQL incremental extraction is disabled because target-derived "
        "MAX(incremental_column) with a strict > predicate can skip equal-precision, "
        "out-of-order, late older, and NULL rows. Use a complete full_refresh or a complete "
        "bounded replace/partition_replace/backfill window until dpone provides a source-owned "
        "composite/binlog checkpoint committed atomically with the MSSQL receipt."
    ),
    incremental_strategies=_INCREMENTAL_APPEND_MERGE,
)
MSSQL_MSSQL_POLICY = TargetMaxCursorPolicy(
    source_type="mssql",
    manifest_error_code="MSSQL_MSSQL_TARGET_MAX_CURSOR_UNSAFE",
    runtime_error_code="mssql_mssql.target_max_cursor_unsafe",
    guidance=(
        "MSSQL to MSSQL incremental extraction is disabled because target-derived "
        "MAX(incremental_column) with a strict > predicate can skip equal-precision, "
        "out-of-order, late older, and NULL rows. Use a complete full_refresh or a complete "
        "bounded replace/partition_replace/backfill window until dpone provides a source-owned "
        "composite/CDC checkpoint committed atomically with the target receipt."
    ),
    incremental_strategies=_INCREMENTAL_APPEND_MERGE,
)
POSTGRES_MSSQL_COLUMN_POLICY = TargetMaxCursorPolicy(
    source_type="postgres",
    manifest_error_code="POSTGRES_MSSQL_COLUMN_CURSOR_UNSAFE",
    runtime_error_code="postgres_mssql.column_cursor_atomic_composite_state_required",
    guidance=(
        "PostgreSQL to MSSQL column-cursor extraction is disabled because a target-derived "
        "single-column MAX checkpoint can skip concurrent rows with an equal or older finite-precision value. "
        "Use source.options.incremental_strategy=xmin and remove incremental_column. A column cursor may be "
        "re-enabled only after a typed composite boundary and its upper bound are committed atomically with "
        "the MSSQL target receipt."
    ),
    incremental_strategies=_INCREMENTAL_APPEND_MERGE,
)

_POLICIES = {
    policy.source_type: policy
    for policy in (
        CLICKHOUSE_MSSQL_POLICY,
        MYSQL_MSSQL_POLICY,
        MSSQL_MSSQL_POLICY,
        POSTGRES_MSSQL_COLUMN_POLICY,
    )
}


class UnsafeTargetMaxMssqlCursorError(ValueError):
    """Raised before source or target I/O for one lossy built-in route."""

    def __init__(self, policy: TargetMaxCursorPolicy) -> None:
        self.policy = policy
        super().__init__(f"{policy.runtime_error_code}: {policy.guidance}")


class UnsafeClickHouseMssqlCursorError(UnsafeTargetMaxMssqlCursorError):
    """Stable ClickHouse -> MSSQL cursor diagnostic."""

    def __init__(self) -> None:
        super().__init__(CLICKHOUSE_MSSQL_POLICY)


class UnsafeMySQLMssqlCursorError(UnsafeTargetMaxMssqlCursorError):
    """Stable MySQL -> MSSQL cursor diagnostic."""

    def __init__(self) -> None:
        super().__init__(MYSQL_MSSQL_POLICY)


class UnsafeMssqlMssqlCursorError(UnsafeTargetMaxMssqlCursorError):
    """Stable MSSQL -> MSSQL cursor diagnostic."""

    def __init__(self) -> None:
        super().__init__(MSSQL_MSSQL_POLICY)


class UnsafePostgresMssqlColumnCursorError(UnsafeTargetMaxMssqlCursorError):
    """Stable PostgreSQL -> MSSQL column-cursor diagnostic."""

    def __init__(self) -> None:
        super().__init__(POSTGRES_MSSQL_COLUMN_POLICY)


_ERROR_FACTORIES: dict[str, Callable[[], UnsafeTargetMaxMssqlCursorError]] = {
    "clickhouse": UnsafeClickHouseMssqlCursorError,
    "mysql": UnsafeMySQLMssqlCursorError,
    "mssql": UnsafeMssqlMssqlCursorError,
    "postgres": UnsafePostgresMssqlColumnCursorError,
}


def target_max_cursor_policy(source_type: Any) -> TargetMaxCursorPolicy | None:
    """Return the canonical source policy without guessing from class names."""

    return _POLICIES.get(_normalize_source_type(source_type))


def target_max_mssql_cursor_is_unsafe(
    *,
    source_type: Any,
    sink_type: Any,
    load_strategy: Any,
) -> bool:
    """Return whether one declared route selects a lossy built-in cursor."""

    policy = target_max_cursor_policy(source_type)
    if policy is None or not is_mssql_dialect(sink_type):
        return False
    strategy = str(getattr(load_strategy, "value", load_strategy) or "").strip().lower()
    return strategy in policy.incremental_strategies


def assert_target_max_mssql_cursor_supported(
    *,
    source_type: Any,
    configured_sink: Any,
    sink_connector: Any,
) -> str:
    """Resolve explicit sink authority and reject MSSQL before any connector I/O.

    Direct strategy construction must still provide either the hydrated sink
    dialect or an adapter-declared ``dialect``/``connection_descriptor``.  An
    absent authority is not treated as a non-MSSQL grant.
    """

    policy = target_max_cursor_policy(source_type)
    if policy is None:
        raise ValueError(f"incremental_cursor.target_max_source_unsupported:{source_type!r}")
    dialect = require_sink_dialect_authority(configured=configured_sink, connector=sink_connector)
    if is_mssql_dialect(dialect):
        raise _ERROR_FACTORIES[policy.source_type]()
    return dialect


def raise_unsafe_target_max_mssql_cursor(source_type: Any) -> None:
    """Raise the source-specific stable runtime diagnostic for admission."""

    policy = target_max_cursor_policy(source_type)
    if policy is None:
        raise RuntimeError("mssql_transaction.source_checkpoint_not_atomic:target_derived_single_column_unsafe")
    raise _ERROR_FACTORIES[policy.source_type]()


def _normalize_source_type(value: Any) -> str:
    normalized = str(value or "").strip().lower().replace("-", "_")
    aliases = {
        "postgresql": "postgres",
        "sqlserver": "mssql",
        "sql_server": "mssql",
    }
    return aliases.get(normalized, normalized)


__all__ = [
    "CLICKHOUSE_MSSQL_POLICY",
    "MSSQL_MSSQL_POLICY",
    "MYSQL_MSSQL_POLICY",
    "POSTGRES_MSSQL_COLUMN_POLICY",
    "TargetMaxCursorPolicy",
    "UnsafeClickHouseMssqlCursorError",
    "UnsafeMssqlMssqlCursorError",
    "UnsafeMySQLMssqlCursorError",
    "UnsafePostgresMssqlColumnCursorError",
    "UnsafeTargetMaxMssqlCursorError",
    "assert_target_max_mssql_cursor_supported",
    "raise_unsafe_target_max_mssql_cursor",
    "target_max_cursor_policy",
    "target_max_mssql_cursor_is_unsafe",
]
