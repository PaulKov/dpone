"""Strict row and compare-and-set validation for MSSQL workflow failure."""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any, Protocol

from dpone.ports.semantic_refresh_mssql_failure_records import MssqlFailureJournalReference
from dpone.ports.semantic_refresh_mssql_primitives import MssqlImageKeyColumn


class SemanticRefreshWorkflowFailureError(RuntimeError):
    """Raised when evidence-derived workflow failure state is invalid."""


class _OutputCursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _OutputCursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...

    def fetchall(self) -> Sequence[tuple[Any, ...]]: ...


def require_rows(
    cursor: _OutputCursor,
    expected: Sequence[tuple[object, ...]],
    label: str,
) -> None:
    """Require the exact compare-and-set OUTPUT identity closure."""

    observed = tuple(sorted((tuple(row) for row in cursor.fetchall()), key=_row_sort_key))
    wanted = tuple(sorted(expected, key=_row_sort_key))
    if observed != wanted:
        raise SemanticRefreshWorkflowFailureError(f"{label} compare-and-set failed")


def _row_sort_key(row: tuple[object, ...]) -> tuple[str, ...]:
    return tuple(str(value) for value in row)


def require_application_lock(cursor: _OutputCursor, workflow_id: str) -> None:
    """Acquire the transaction-owned workflow-failure application lock."""

    cursor.execute(
        """
DECLARE @dpone_lock_result int;
EXEC @dpone_lock_result = sys.sp_getapplock
    @Resource = ?, @LockMode = N'Exclusive',
    @LockOwner = N'Transaction', @LockTimeout = 0;
SELECT @dpone_lock_result;
""".strip(),
        f"dpone:semantic-refresh:failure:{workflow_id}",
    )
    row = cursor.fetchone()
    if row is None or isinstance(row[0], bool) or not isinstance(row[0], int) or row[0] < 0:
        raise SemanticRefreshWorkflowFailureError("workflow failure lock was not acquired")


def journal_reference(row: tuple[Any, ...]) -> MssqlFailureJournalReference:
    """Decode one locked journal row with exact shape and typed key authority."""

    if len(row) != 23:
        raise SemanticRefreshWorkflowFailureError("workflow journal identity is invalid")
    return MssqlFailureJournalReference(
        model_unique_id=str(row[0]),
        operation_id=str(row[1]),
        operation_plan_sha256=str(row[2]),
        attempt_binding_sha256=str(row[3]),
        strategy_authority_json=str(row[4]),
        strategy_authority_sha256=str(row[5]),
        baseline_receipt_sha256=str(row[6]),
        baseline_kind=str(row[7]),
        baseline_receipt_json=str(row[8]),
        baseline_status=str(row[9]),
        image_key_columns=_image_key_columns(row[10]),
        target_resource_id=str(row[11]),
        publication_database=str(row[12]),
        publication_target_table=str(row[13]),
        publication_scope_id=str(row[14]),
        target_predecessor_generation_id=str(row[15]),
        scope_predecessor_operation_id=None if row[16] is None else str(row[16]),
        fencing_epoch=positive_integer(row[17], "journal fencing epoch"),
        replaces_failed_operation_id=None if row[18] is None else str(row[18]),
        owner_id=str(row[19]),
        status=str(row[20]),
        persisted_outcome=None if row[21] is None else str(row[21]),
        persisted_evidence_sha256=None if row[22] is None else str(row[22]),
    )


def positive_integer(value: object, label: str) -> int:
    """Decode a strictly positive SQL integer without accepting booleans."""

    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise SemanticRefreshWorkflowFailureError(f"{label} is invalid")
    return value


def _image_key_columns(value: object) -> tuple[MssqlImageKeyColumn, ...]:
    if not isinstance(value, str):
        raise SemanticRefreshWorkflowFailureError("journal image key columns are invalid")
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise SemanticRefreshWorkflowFailureError("journal image key columns are invalid") from exc
    if not isinstance(parsed, list) or not parsed:
        raise SemanticRefreshWorkflowFailureError("journal image key columns are invalid")
    try:
        columns = tuple(
            MssqlImageKeyColumn(name=item["name"], order_encoding=item["order_encoding"])
            for item in parsed
            if isinstance(item, dict) and set(item) == {"name", "order_encoding"}
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise SemanticRefreshWorkflowFailureError("journal image key columns are invalid") from exc
    if len(columns) != len(parsed) or len({item.name for item in columns}) != len(columns):
        raise SemanticRefreshWorkflowFailureError("journal image key columns are invalid")
    return columns


__all__ = [
    "SemanticRefreshWorkflowFailureError",
    "journal_reference",
    "positive_integer",
    "require_application_lock",
    "require_rows",
]
