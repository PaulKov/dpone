"""Locked target-head and terminal-receipt checks for MSSQL admission."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol
from uuid import UUID

from dpone.contracts.dbt_semantic_refresh_recovery_head import (
    semantic_refresh_recovery_target_head_sha256,
)

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_mssql import MssqlJournalPreparation
    from dpone.ports.semantic_refresh_mssql_recovery_heads import (
        MssqlAdmissionTargetHeadAuthority,
    )


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _Cursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...

    def fetchall(self) -> list[tuple[Any, ...]]: ...


class MssqlTargetHeadAuthorityConflict(RuntimeError):
    """Internal locked-head conflict translated by the admission boundary."""


class MssqlTargetHeadAuthorityQueries:
    """Compare a protected head projection with current and terminal state."""

    def __init__(self, control_schema: str) -> None:
        self._control_schema = control_schema

    def require_current(
        self,
        cursor: _Cursor,
        journal: MssqlJournalPreparation,
        head: MssqlAdmissionTargetHeadAuthority,
    ) -> tuple[int, str, str, str]:
        """Return the exact current head after locked lineage reconciliation."""

        cursor.execute(
            f"""
SELECT target_generation, target_generation_id, target_uuid, operation_id
FROM {self._table("semantic_refresh_target_heads")} WITH (UPDLOCK, HOLDLOCK)
WHERE database_name = ? AND target_table = ?;
""".strip(),
            journal.publication_database,
            journal.publication_target_table,
        )
        row = cursor.fetchone()
        if row is None:
            raise MssqlTargetHeadAuthorityConflict("target predecessor head authority conflict")
        target = (
            _positive(row[0], "target predecessor generation"),
            str(row[1]),
            _uuid_text(row[2]),
            str(row[3]),
        )
        if target != (
            head.target_generation,
            head.target_generation_id,
            head.target_uuid,
            head.owner_operation_id,
        ):
            raise MssqlTargetHeadAuthorityConflict("target predecessor head authority conflict")
        self._require_receipt(cursor, journal, head, target)
        return target

    def _require_receipt(
        self,
        cursor: _Cursor,
        journal: MssqlJournalPreparation,
        head: MssqlAdmissionTargetHeadAuthority,
        target: tuple[int, str, str, str],
    ) -> None:
        terminal_receipt = head.terminal_receipt_sha256
        if terminal_receipt is None:
            if head.head_authority_receipt_sha256 != journal.baseline_receipt_sha256:
                raise MssqlTargetHeadAuthorityConflict("baseline target head authority conflict")
            return
        cursor.execute(
            f"""
SELECT TOP (2) operation_id, status, terminal_receipt_sha256,
       terminal_target_generation, terminal_target_generation_id, target_uuid,
       terminal_target_mutation_outcome, predecessor_target_operation_id
FROM {self._table("semantic_refresh_journals")} WITH (UPDLOCK, HOLDLOCK)
WHERE terminal_receipt_sha256 = ?
  AND publication_database = ? AND publication_target_table = ?
ORDER BY operation_id;
""".strip(),
            terminal_receipt,
            journal.publication_database,
            journal.publication_target_table,
        )
        rows = tuple(tuple(row) for row in cursor.fetchall())
        if len(rows) != 1:
            raise MssqlTargetHeadAuthorityConflict("target head terminal receipt authority conflict")
        terminal = rows[0]
        outcome = str(terminal[6])
        if outcome == "TARGET_COMMITTED":
            terminal_owner = str(terminal[0])
        elif outcome == "NOT_REQUIRED_EMPTY_SCOPE":
            terminal_owner = str(terminal[7])
        else:
            raise MssqlTargetHeadAuthorityConflict("target head terminal mutation authority conflict")
        terminal_target = (
            _positive(terminal[3], "terminal target generation"),
            str(terminal[4]),
            _uuid_text(terminal[5]),
            terminal_owner,
        )
        if terminal[1:3] != ("COMPLETE", terminal_receipt) or terminal_target != target:
            raise MssqlTargetHeadAuthorityConflict("target head terminal lineage conflict")
        receipt = semantic_refresh_recovery_target_head_sha256(
            model_unique_id=head.model_unique_id,
            clickhouse_target_authority_id=head.clickhouse_target_authority_id,
            target_generation=target[0],
            target_generation_id=target[1],
            target_uuid=target[2],
            owner_operation_id=target[3],
            terminal_receipt_sha256=terminal_receipt,
        )
        if receipt != head.head_authority_receipt_sha256:
            raise MssqlTargetHeadAuthorityConflict("target head authority receipt conflict")

    def _table(self, table_name: str) -> str:
        return f"[{self._control_schema}].[{table_name}]"


def _positive(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise MssqlTargetHeadAuthorityConflict(f"{field_name} is invalid")
    return value


def _uuid_text(value: object) -> str:
    try:
        return str(UUID(str(value)))
    except (TypeError, ValueError, AttributeError) as exc:
        raise MssqlTargetHeadAuthorityConflict("target head UUID authority conflict") from exc


__all__ = ["MssqlTargetHeadAuthorityConflict", "MssqlTargetHeadAuthorityQueries"]
