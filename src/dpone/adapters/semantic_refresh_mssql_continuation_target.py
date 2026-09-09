"""Bounded SQL Server target observation for continuation reconciliation."""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any, Protocol

from dpone.ports.semantic_refresh_mssql_authority_models import MssqlCanonicalAdmissionBundle
from dpone.ports.semantic_refresh_mssql_authority_records import (
    MssqlProtectedResourcePolicy,
    MssqlProtectedWritableColumn,
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_HARD_JSON_BYTES = 67_108_864


class SemanticRefreshMssqlContinuationTargetError(RuntimeError):
    """Fail closed when bounded target observation cannot be proven."""


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _Cursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...

    def fetchall(self) -> Sequence[tuple[Any, ...]]: ...


class _TimeoutConnection(Protocol):
    timeout: int


def apply_connection_query_timeout(connection: _TimeoutConnection, maximum: int) -> None:
    """Set and acknowledge the protected timeout before a cursor is created."""

    try:
        configured = getattr(connection, "timeout", 0)
        timeout = (
            configured
            if isinstance(configured, int) and not isinstance(configured, bool) and configured > 0
            else maximum
        )
        protected_timeout = min(timeout, maximum)
        connection.timeout = protected_timeout
        acknowledged = getattr(connection, "timeout", None)
    except Exception as exc:
        raise _budget_unverified("continuation query timeout cannot be applied") from exc
    if acknowledged != protected_timeout:
        raise _budget_unverified("continuation query timeout acknowledgement differs")


def require_continuation_query_timeout(cursor: _Cursor, bundle: MssqlCanonicalAdmissionBundle) -> None:
    """Recheck that continuation uses a cursor created under a protected timeout."""

    maximum = min(item.resource_policy.max_statement_seconds for item in bundle.model_resources)
    connection = getattr(cursor, "connection", None)
    if connection is None:
        raise _budget_unverified("continuation cursor does not expose its query-timeout connection")
    try:
        configured = getattr(connection, "timeout")
    except Exception as exc:
        raise _budget_unverified("continuation query timeout cannot be read") from exc
    if not isinstance(configured, int) or isinstance(configured, bool) or configured <= 0 or configured > maximum:
        raise _budget_unverified("continuation query timeout is outside the protected limit")


def observe_continuation_target(
    cursor: _Cursor,
    *,
    target_resource_id: str,
    writable_columns: tuple[MssqlProtectedWritableColumn, ...],
    effective_keys: tuple[MssqlProtectedWritableColumn, ...],
    event_time_column: str,
    scope_start: str,
    scope_end: str,
    resource_policy: MssqlProtectedResourcePolicy,
) -> tuple[str, int]:
    """Preflight the locked scope before materializing its canonical JSON."""

    relation = _relation(target_resource_id)
    columns = ", ".join(_quoted(item.name) for item in writable_columns)
    keys = ", ".join(_order_expression(item) for item in effective_keys)
    event_time = _quoted(event_time_column)
    bounded_row_bytes = " + ".join(
        f"(CONVERT(bigint, 512) + (CONVERT(bigint, COALESCE(DATALENGTH({_quoted(item.name)}), 0)) * 12))"
        for item in writable_columns
    )
    cursor.execute(
        f"""
DECLARE @dpone_continuation_target_rows bigint;
DECLARE @dpone_continuation_target_preflight_bytes bigint;
SELECT @dpone_continuation_target_rows = COUNT_BIG(*),
       @dpone_continuation_target_preflight_bytes = COALESCE(SUM({bounded_row_bytes}), 2)
FROM {relation} WITH (UPDLOCK, HOLDLOCK)
WHERE {event_time} >= ? AND {event_time} < ?;
SELECT @dpone_continuation_target_rows, @dpone_continuation_target_preflight_bytes;
""".strip(),
        scope_start,
        scope_end,
    )
    preflight = cursor.fetchone()
    if preflight is None or len(preflight) != 2:
        raise _budget_unverified("continuation target preflight is invalid")
    observed_rows_value, observed_bytes_value = preflight
    if any(
        not isinstance(item, int) or isinstance(item, bool) or item < 0
        for item in (observed_rows_value, observed_bytes_value)
    ):
        raise _budget_unverified("continuation target preflight is invalid")
    observed_rows, observed_bytes = int(observed_rows_value), int(observed_bytes_value)
    if _exceeds_policy(observed_rows, observed_bytes, resource_policy):
        raise SemanticRefreshMssqlContinuationTargetError(
            "DPONE_SEMANTIC_REFRESH_RESOURCE_BUDGET_EXCEEDED: continuation target exceeds protected limits"
        )
    cursor.execute(
        f"""
DECLARE @dpone_continuation_target_json nvarchar(max);
SELECT @dpone_continuation_target_json = (
    SELECT {columns} FROM {relation} WITH (UPDLOCK, HOLDLOCK)
    WHERE {event_time} >= ? AND {event_time} < ?
    ORDER BY {keys} FOR JSON PATH, INCLUDE_NULL_VALUES
);
SELECT 'sha256:' + LOWER(CONVERT(varchar(64), HASHBYTES(
           'SHA2_256', COALESCE(@dpone_continuation_target_json, N'[]')
       ), 2));
""".strip(),
        scope_start,
        scope_end,
    )
    row = cursor.fetchone()
    if row is None or len(row) != 1 or not isinstance(row[0], str):
        raise SemanticRefreshMssqlContinuationTargetError("current target observation is invalid")
    return str(row[0]), observed_rows


def _exceeds_policy(rows: int, size: int, policy: MssqlProtectedResourcePolicy) -> bool:
    return bool(
        rows > policy.max_target_scope_rows
        or rows > policy.max_after_image_rows
        or size > policy.max_after_image_bytes
        or size > policy.max_scope_image_total_bytes
        or size > _HARD_JSON_BYTES
    )


def _budget_unverified(message: str) -> SemanticRefreshMssqlContinuationTargetError:
    return SemanticRefreshMssqlContinuationTargetError(f"DPONE_SEMANTIC_REFRESH_RESOURCE_BUDGET_UNVERIFIED: {message}")


def _relation(value: str) -> str:
    parts = value.split(".")
    if len(parts) != 3:
        raise SemanticRefreshMssqlContinuationTargetError("target resource identity is invalid")
    return ".".join(_quoted(item) for item in parts)


def _quoted(value: str) -> str:
    if _IDENTIFIER.fullmatch(value) is None:
        raise SemanticRefreshMssqlContinuationTargetError("MSSQL identifier is outside the closed subset")
    return f"[{value}]"


def _order_expression(column: MssqlProtectedWritableColumn) -> str:
    quoted = _quoted(column.name)
    return f"CONVERT(char(36), {quoted})" if column.source_type.lower() == "uniqueidentifier" else quoted


__all__ = [
    "SemanticRefreshMssqlContinuationTargetError",
    "apply_connection_query_timeout",
    "observe_continuation_target",
    "require_continuation_query_timeout",
]
