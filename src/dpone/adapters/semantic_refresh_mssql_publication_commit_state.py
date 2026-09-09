"""SQL Server repository for durable semantic-refresh post-exchange states."""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from typing import Any, Protocol


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _Cursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...


class MssqlPublicationCommitStateConflict(RuntimeError):
    """Raised when exact exchange evidence or its journal edge differs."""


class MssqlPublicationCommitStateStore:
    """Persist TARGET_COMMITTED/COMMITTED_INCOMPLETE without touching heads."""

    def __init__(self, table: Callable[[str], str]) -> None:
        self._table = table

    def record_target_committed(
        self,
        cursor: _Cursor,
        request: Mapping[str, object],
        journal: tuple[Any, ...],
    ) -> str:
        """CAS COMMITTING to TARGET_COMMITTED or reconcile later exact states."""

        state = str(journal[3])
        if state in {"COMMITTING", "COMMIT_UNKNOWN"}:
            if (journal[4], _uuid_text(journal[5])) != (
                request["prepare_receipt_sha256"],
                request["expected_target_uuid"],
            ):
                raise MssqlPublicationCommitStateConflict("exchange predecessor evidence differs")
            cursor.execute(
                f"""
UPDATE {self._table("semantic_refresh_journals")} WITH (UPDLOCK, HOLDLOCK)
SET status = N'TARGET_COMMITTED', clickhouse_commit_receipt_sha256 = ?, target_uuid = ?
OUTPUT inserted.operation_id
WHERE operation_id = ? AND status = ?;
""".strip(),
                request["clickhouse_commit_receipt_sha256"],
                request["target_uuid"],
                request["operation_id"],
                state,
            )
            _require_operation(cursor, request, "TARGET_COMMITTED")
            return "TARGET_COMMITTED"
        if state not in {"TARGET_COMMITTED", "COMMITTED_INCOMPLETE", "COMPLETE"}:
            raise MssqlPublicationCommitStateConflict("post-exchange journal predecessor differs")
        self.assert_exchange_evidence(request, journal)
        return state

    def record_committed_incomplete(
        self,
        cursor: _Cursor,
        request: Mapping[str, object],
        journal: tuple[Any, ...],
    ) -> None:
        """CAS TARGET_COMMITTED to COMMITTED_INCOMPLETE or reconcile replay."""

        self.assert_exchange_evidence(request, journal)
        if journal[3] == "TARGET_COMMITTED":
            cursor.execute(
                f"""
UPDATE {self._table("semantic_refresh_journals")} WITH (UPDLOCK, HOLDLOCK)
SET status = N'COMMITTED_INCOMPLETE'
OUTPUT inserted.operation_id
WHERE operation_id = ? AND status = N'TARGET_COMMITTED';
""".strip(),
                request["operation_id"],
            )
            _require_operation(cursor, request, "COMMITTED_INCOMPLETE")
        elif journal[3] != "COMMITTED_INCOMPLETE":
            raise MssqlPublicationCommitStateConflict("incomplete publication predecessor differs")

    @staticmethod
    def assert_exchange_evidence(
        request: Mapping[str, object],
        journal: tuple[Any, ...],
    ) -> None:
        """Require immutable receipt digest and post-exchange target UUID."""

        if (journal[4], journal[8], _uuid_text(journal[5])) != (
            request["prepare_receipt_sha256"],
            request["clickhouse_commit_receipt_sha256"],
            request["target_uuid"],
        ):
            raise MssqlPublicationCommitStateConflict("durable exchange evidence differs")


def _uuid_text(value: object) -> str | None:
    return None if value is None else str(uuid.UUID(str(value)))


def _require_operation(cursor: _Cursor, request: Mapping[str, object], label: str) -> None:
    row = cursor.fetchone()
    if row is None or tuple(row) != (request["operation_id"],):
        raise MssqlPublicationCommitStateConflict(f"{label} compare-and-set failed")


__all__ = [
    "MssqlPublicationCommitStateConflict",
    "MssqlPublicationCommitStateStore",
]
