"""Create-once MSSQL repository for cross-task ClickHouse PREPARED documents."""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from typing import Any, Protocol

from dpone.ports.semantic_refresh_clickhouse_prepared import (
    DurableClickHousePreparedPublication,
)


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _Cursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...


class MssqlPreparedPublicationConflict(RuntimeError):
    """Raised when PREPARED documents are absent, changed, or unauthenticated."""


class MssqlPublicationTransitionConflict(RuntimeError):
    """Raised when a journal transition cannot be proven exactly."""


class MssqlPreparedPublicationStore:
    """Persist and reload the exact plan, receipt, and manifest version boundary."""

    def __init__(self, table: Callable[[str], str]) -> None:
        self._table = table

    def persist_or_reconcile(
        self,
        cursor: _Cursor,
        request: Mapping[str, object],
        journal: tuple[Any, ...],
    ) -> None:
        """Create documents once or prove the byte-exact PREPARED replay."""

        if journal[3] == "PREPARED":
            if (journal[4], _uuid_text(journal[5])) != (
                request["prepare_receipt_sha256"],
                request["target_uuid"],
            ):
                raise MssqlPreparedPublicationConflict("journal transition replay differs from durable receipt")
            self._assert_replay(cursor, request)
            return
        if journal[3] != "PREPARING":
            raise MssqlPreparedPublicationConflict("journal transition predecessor differs")
        cursor.execute(
            f"""
UPDATE {self._table("semantic_refresh_journals")} WITH (UPDLOCK, HOLDLOCK)
SET status = N'PREPARED', prepare_receipt_sha256 = ?, target_uuid = ?,
    prepare_plan_sha256 = ?, prepare_plan_json = ?, prepared_receipt_json = ?,
    artifact_manifest_key = ?, artifact_manifest_version = ?, artifact_manifest_sha256 = ?
OUTPUT inserted.operation_id
WHERE operation_id = ? AND status = N'PREPARING';
""".strip(),
            request["prepare_receipt_sha256"],
            request["target_uuid"],
            request["prepare_plan_sha256"],
            request["prepare_plan_json"],
            request["prepared_receipt_json"],
            request["artifact_manifest_key"],
            request["artifact_manifest_version"],
            request["artifact_manifest_sha256"],
            request["operation_id"],
        )
        created = cursor.fetchone()
        if created is None or tuple(created) != (request["operation_id"],):
            raise MssqlPreparedPublicationConflict("journal transition compare-and-set failed")

    def load(
        self,
        cursor: _Cursor,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> DurableClickHousePreparedPublication:
        """Lock and return one exact PREPARED boundary by protected run identity."""

        value = self.find(
            cursor,
            workflow_execution_binding_sha256,
            operation_id,
        )
        if value is None:
            raise MssqlPreparedPublicationConflict("durable PREPARED documents are absent")
        return value

    def find(
        self,
        cursor: _Cursor,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> DurableClickHousePreparedPublication | None:
        """Distinguish one locked PREPARING row from durable or invalid state."""

        cursor.execute(
            f"""
SELECT j.prepare_plan_sha256, j.prepare_plan_json,
       j.prepare_receipt_sha256, j.prepared_receipt_json,
       j.artifact_manifest_key, j.artifact_manifest_version,
       j.artifact_manifest_sha256, j.status
FROM {self._table("semantic_refresh_journals")} AS j WITH (UPDLOCK, HOLDLOCK)
JOIN {self._table("semantic_refresh_workflow_executions")} AS w WITH (UPDLOCK, HOLDLOCK)
  ON w.workflow_id = j.workflow_id
WHERE w.workflow_execution_binding_sha256 = ? AND j.operation_id = ?;
""".strip(),
            workflow_execution_binding_sha256,
            operation_id,
        )
        row = cursor.fetchone()
        if row is None:
            raise MssqlPreparedPublicationConflict("publication journal is absent")
        if row[7] == "PREPARING" and all(value is None for value in row[:7]):
            return None
        if row[7] not in {
            "PREPARED",
            "COMMITTING",
            "TARGET_COMMITTED",
            "COMMIT_UNKNOWN",
            "COMMITTED_INCOMPLETE",
            "COMPLETE",
        } or any(value is None for value in row[:7]):
            raise MssqlPreparedPublicationConflict("durable PREPARED document state is invalid")
        return DurableClickHousePreparedPublication(
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            operation_id=operation_id,
            prepare_plan_sha256=str(row[0]),
            prepare_plan_json=str(row[1]),
            prepared_receipt_sha256=str(row[2]),
            prepared_receipt_json=str(row[3]),
            artifact_manifest_key=str(row[4]),
            artifact_manifest_version=str(row[5]),
            artifact_manifest_sha256=str(row[6]),
        )

    def _assert_replay(
        self,
        cursor: _Cursor,
        request: Mapping[str, object],
    ) -> None:
        cursor.execute(
            f"""
SELECT prepare_plan_sha256, prepare_plan_json, prepare_receipt_sha256,
       prepared_receipt_json, artifact_manifest_key, artifact_manifest_version,
       artifact_manifest_sha256
FROM {self._table("semantic_refresh_journals")} WITH (UPDLOCK, HOLDLOCK)
WHERE operation_id = ?;
""".strip(),
            request["operation_id"],
        )
        row = cursor.fetchone()
        expected = (
            request["prepare_plan_sha256"],
            request["prepare_plan_json"],
            request["prepared_receipt_sha256"],
            request["prepared_receipt_json"],
            request["artifact_manifest_key"],
            request["artifact_manifest_version"],
            request["artifact_manifest_sha256"],
        )
        if row is None or tuple(row) != expected:
            raise MssqlPreparedPublicationConflict("durable PREPARED replay differs")


class MssqlPublicationTransitionStore:
    """Apply create-once and recovery transitions under the caller's lock."""

    def __init__(
        self,
        table: Callable[[str], str],
        prepared: MssqlPreparedPublicationStore,
    ) -> None:
        self._table = table
        self._prepared = prepared

    def apply(
        self,
        cursor: _Cursor,
        request: Mapping[str, object],
        journal: tuple[Any, ...],
    ) -> None:
        """Persist or reconcile one exact transition against the locked journal."""

        next_state = str(request["next_journal_state"])
        if next_state == "PREPARED" and request["expected_journal_state"] == "PREPARING":
            self._prepared.persist_or_reconcile(cursor, request, journal)
        elif next_state == "PREPARED" and journal[3] == "COMMITTING":
            if (journal[4], _uuid_text(journal[5])) != (
                request["prepare_receipt_sha256"],
                request["target_uuid"],
            ):
                raise MssqlPublicationTransitionConflict("COMMITTING recovery differs from durable PREPARED receipt")
            self._committing_to_prepared(cursor, request)
        elif journal[3] == next_state:
            if (journal[4], _uuid_text(journal[5])) != (
                request["prepare_receipt_sha256"],
                request["target_uuid"],
            ):
                raise MssqlPublicationTransitionConflict("journal transition replay differs from durable receipt")
        elif journal[3] == request["expected_journal_state"]:
            cursor.execute(
                f"""
UPDATE {self._table("semantic_refresh_journals")} WITH (UPDLOCK, HOLDLOCK)
SET status = ?, prepare_receipt_sha256 = ?, target_uuid = ?
OUTPUT inserted.operation_id
WHERE operation_id = ? AND status = ?;
""".strip(),
                next_state,
                request["prepare_receipt_sha256"],
                request["target_uuid"],
                request["operation_id"],
                request["expected_journal_state"],
            )
            created = cursor.fetchone()
            if created is None or tuple(created) != (request["operation_id"],):
                raise MssqlPublicationTransitionConflict("journal transition compare-and-set failed")
        else:
            raise MssqlPublicationTransitionConflict("journal transition predecessor differs")

    def _committing_to_prepared(
        self,
        cursor: _Cursor,
        request: Mapping[str, object],
    ) -> None:
        for current, successor in (
            ("COMMITTING", "COMMIT_UNKNOWN"),
            ("COMMIT_UNKNOWN", "PREPARED"),
        ):
            cursor.execute(
                f"""
UPDATE {self._table("semantic_refresh_journals")} WITH (UPDLOCK, HOLDLOCK)
SET status = ?
OUTPUT inserted.operation_id
WHERE operation_id = ? AND status = ?;
""".strip(),
                successor,
                request["operation_id"],
                current,
            )
            created = cursor.fetchone()
            if created is None or tuple(created) != (request["operation_id"],):
                raise MssqlPublicationTransitionConflict("COMMITTING recovery compare-and-set failed")


def _uuid_text(value: object) -> str | None:
    return None if value is None else str(uuid.UUID(str(value)))


__all__ = [
    "MssqlPreparedPublicationConflict",
    "MssqlPreparedPublicationStore",
    "MssqlPublicationTransitionConflict",
    "MssqlPublicationTransitionStore",
]
