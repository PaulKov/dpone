"""SQL Server repository for atomic semantic-refresh head publication."""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from typing import Any, Protocol


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _Cursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...


class MssqlPublicationHeadConflict(RuntimeError):
    """Raised when one predecessor/successor head differs."""


class MssqlPublicationHeadStore:
    """Lock, compare and atomically update target/scope/checkpoint heads."""

    def __init__(self, table: Callable[[str], str]) -> None:
        self._table = table

    def read(
        self,
        cursor: _Cursor,
        request: Mapping[str, object],
    ) -> tuple[tuple[Any, ...] | None, tuple[Any, ...] | None, tuple[Any, ...] | None]:
        """Read all three heads under SERIALIZABLE update locks."""

        return (
            self._target(cursor, request),
            self._scope(cursor, request),
            self._checkpoint(cursor, request),
        )

    def assert_predecessors(
        self,
        request: Mapping[str, object],
        journal: tuple[Any, ...],
        heads: tuple[tuple[Any, ...] | None, tuple[Any, ...] | None, tuple[Any, ...] | None],
    ) -> None:
        """Require the exact pre-publication head set."""

        if journal[3] != request["expected_journal_state"]:
            raise MssqlPublicationHeadConflict("journal predecessor differs")
        self.assert_head_predecessors(request, heads)

    def assert_head_predecessors(
        self,
        request: Mapping[str, object],
        heads: tuple[tuple[Any, ...] | None, tuple[Any, ...] | None, tuple[Any, ...] | None],
    ) -> None:
        """Require exact target, scope, and checkpoint predecessors."""

        target, scope, checkpoint = heads
        if target is None or (target[0], target[1], _uuid_text(target[2]), target[3]) != (
            request["expected_target_generation"],
            request["target_predecessor_generation_id"],
            request["expected_target_uuid"],
            request["predecessor_target_operation_id"],
        ):
            raise MssqlPublicationHeadConflict("target head predecessor differs")
        expected_scope = (
            request["predecessor_scope_revision"],
            request["scope_predecessor_operation_id"],
        )
        if not (
            (scope is None and expected_scope == (None, None)) or (scope is not None and tuple(scope) == expected_scope)
        ):
            raise MssqlPublicationHeadConflict("scope head predecessor differs")
        expected_checkpoint = (
            request["predecessor_checkpoint_sha256"],
            request["predecessor_checkpoint_version"],
            request["predecessor_checkpoint_operation_id"],
        )
        if not (
            (checkpoint is None and expected_checkpoint == (None, None, None))
            or (checkpoint is not None and tuple(checkpoint) == expected_checkpoint)
        ):
            raise MssqlPublicationHeadConflict("checkpoint predecessor differs")

    def assert_successors(
        self,
        request: Mapping[str, object],
        journal: tuple[Any, ...],
        heads: tuple[tuple[Any, ...] | None, tuple[Any, ...] | None, tuple[Any, ...] | None],
    ) -> None:
        """Reconcile an exact already-COMPLETE head set."""

        target, scope, checkpoint = heads
        expected_target = (
            (
                request["expected_target_generation"],
                request["target_predecessor_generation_id"],
                request["expected_target_uuid"],
                request["predecessor_target_operation_id"],
            )
            if request["target_mutation_outcome"] == "NOT_REQUIRED_EMPTY_SCOPE"
            else (
                request["target_generation"],
                request["target_generation_id"],
                request["target_uuid"],
                request["operation_id"],
            )
        )
        if (
            target is None
            or scope is None
            or checkpoint is None
            or (target[0], target[1], _uuid_text(target[2]), target[3]) != expected_target
            or tuple(scope) != (request["scope_revision"], request["operation_id"])
            or tuple(checkpoint)
            != (
                request["checkpoint_sha256"],
                _next_checkpoint_version(request),
                request["operation_id"],
            )
            or (journal[8], journal[9], _uuid_text(journal[5]))
            != (
                request["clickhouse_commit_receipt_sha256"],
                request["terminal_receipt_sha256"],
                request["target_uuid"],
            )
            or journal[19:26]
            != (
                request["target_generation"],
                request["target_generation_id"],
                request["scope_revision"],
                request["checkpoint_sha256"],
                _next_checkpoint_version(request),
                request["target_mutation_outcome"],
                request["value_conversion_outcome"],
            )
        ):
            raise MssqlPublicationHeadConflict("terminal publication replay differs")

    def publish(
        self,
        cursor: _Cursor,
        request: Mapping[str, object],
        *,
        scope_exists: bool,
    ) -> None:
        """Apply all four CAS writes in the caller's one transaction."""

        if request["target_mutation_outcome"] == "TARGET_COMMITTED":
            self._publish_target(cursor, request)
        self._publish_scope(cursor, request, scope_exists=scope_exists)
        self._publish_checkpoint(
            cursor, request, checkpoint_exists=request["predecessor_checkpoint_sha256"] is not None
        )
        self._publish_journal(cursor, request)

    def _target(self, cursor: _Cursor, request: Mapping[str, object]) -> tuple[Any, ...] | None:
        cursor.execute(
            f"""
SELECT target_generation, target_generation_id, target_uuid, operation_id
FROM {self._table("semantic_refresh_target_heads")} WITH (UPDLOCK, HOLDLOCK)
WHERE database_name = ? AND target_table = ?;
""".strip(),
            request["database"],
            request["target_table"],
        )
        return _row(cursor)

    def _scope(self, cursor: _Cursor, request: Mapping[str, object]) -> tuple[Any, ...] | None:
        cursor.execute(
            f"""
SELECT scope_revision, operation_id
FROM {self._table("semantic_refresh_scope_heads")} WITH (UPDLOCK, HOLDLOCK)
WHERE database_name = ? AND target_table = ? AND scope_id = ?;
""".strip(),
            request["database"],
            request["target_table"],
            request["scope_id"],
        )
        return _row(cursor)

    def _checkpoint(self, cursor: _Cursor, request: Mapping[str, object]) -> tuple[Any, ...] | None:
        cursor.execute(
            f"""
SELECT checkpoint_sha256, checkpoint_version, operation_id
FROM {self._table("semantic_refresh_checkpoints")} WITH (UPDLOCK, HOLDLOCK)
WHERE database_name = ? AND target_table = ? AND scope_id = ?;
""".strip(),
            request["database"],
            request["target_table"],
            request["scope_id"],
        )
        return _row(cursor)

    def _publish_target(self, cursor: _Cursor, request: Mapping[str, object]) -> None:
        cursor.execute(
            f"""
UPDATE {self._table("semantic_refresh_target_heads")} WITH (UPDLOCK, HOLDLOCK)
SET target_generation = ?, target_generation_id = ?, target_uuid = ?, operation_id = ?
OUTPUT inserted.operation_id
WHERE database_name = ? AND target_table = ?
  AND target_generation = ? AND target_generation_id = ?
  AND target_uuid = ? AND operation_id = ?;
""".strip(),
            request["target_generation"],
            request["target_generation_id"],
            request["target_uuid"],
            request["operation_id"],
            request["database"],
            request["target_table"],
            request["expected_target_generation"],
            request["target_predecessor_generation_id"],
            request["expected_target_uuid"],
            request["predecessor_target_operation_id"],
        )
        _require_operation(cursor, request, "target head")

    def _publish_scope(
        self,
        cursor: _Cursor,
        request: Mapping[str, object],
        *,
        scope_exists: bool,
    ) -> None:
        if not scope_exists:
            cursor.execute(
                f"""
INSERT INTO {self._table("semantic_refresh_scope_heads")} (
    database_name, target_table, scope_id, scope_revision, operation_id
)
OUTPUT inserted.operation_id
VALUES (?, ?, ?, ?, ?);
""".strip(),
                request["database"],
                request["target_table"],
                request["scope_id"],
                request["scope_revision"],
                request["operation_id"],
            )
            _require_operation(cursor, request, "scope head")
            return
        cursor.execute(
            f"""
UPDATE {self._table("semantic_refresh_scope_heads")} WITH (UPDLOCK, HOLDLOCK)
SET scope_revision = ?, operation_id = ?
OUTPUT inserted.operation_id
WHERE database_name = ? AND target_table = ? AND scope_id = ?
  AND scope_revision = ? AND operation_id = ?;
""".strip(),
            request["scope_revision"],
            request["operation_id"],
            request["database"],
            request["target_table"],
            request["scope_id"],
            request["expected_scope_revision"],
            request["scope_predecessor_operation_id"],
        )
        _require_operation(cursor, request, "scope head")

    def _publish_checkpoint(
        self,
        cursor: _Cursor,
        request: Mapping[str, object],
        *,
        checkpoint_exists: bool,
    ) -> None:
        next_version = _next_checkpoint_version(request)
        if not checkpoint_exists:
            cursor.execute(
                f"""
INSERT INTO {self._table("semantic_refresh_checkpoints")} (
    database_name, target_table, scope_id, checkpoint_sha256, checkpoint_version, operation_id
)
OUTPUT inserted.operation_id
VALUES (?, ?, ?, ?, ?, ?);
""".strip(),
                request["database"],
                request["target_table"],
                request["scope_id"],
                request["checkpoint_sha256"],
                next_version,
                request["operation_id"],
            )
            _require_operation(cursor, request, "checkpoint")
            return
        cursor.execute(
            f"""
UPDATE {self._table("semantic_refresh_checkpoints")} WITH (UPDLOCK, HOLDLOCK)
SET checkpoint_sha256 = ?, checkpoint_version = ?, operation_id = ?
OUTPUT inserted.operation_id
WHERE database_name = ? AND target_table = ? AND scope_id = ?
  AND checkpoint_sha256 = ? AND checkpoint_version = ? AND operation_id = ?;
""".strip(),
            request["checkpoint_sha256"],
            next_version,
            request["operation_id"],
            request["database"],
            request["target_table"],
            request["scope_id"],
            request["predecessor_checkpoint_sha256"],
            request["predecessor_checkpoint_version"],
            request["predecessor_checkpoint_operation_id"],
        )
        _require_operation(cursor, request, "checkpoint")

    def _publish_journal(self, cursor: _Cursor, request: Mapping[str, object]) -> None:
        cursor.execute(
            f"""
UPDATE {self._table("semantic_refresh_journals")} WITH (UPDLOCK, HOLDLOCK)
SET status = N'COMPLETE', clickhouse_commit_receipt_sha256 = ?,
    terminal_receipt_sha256 = ?, target_uuid = ?,
    terminal_target_generation = ?, terminal_target_generation_id = ?,
    terminal_scope_revision = ?, terminal_checkpoint_sha256 = ?,
    terminal_checkpoint_version = ?, terminal_target_mutation_outcome = ?,
    terminal_value_conversion_outcome = ?
OUTPUT inserted.operation_id
WHERE operation_id = ? AND status = ?;
""".strip(),
            request["clickhouse_commit_receipt_sha256"],
            request["terminal_receipt_sha256"],
            request["target_uuid"],
            request["target_generation"],
            request["target_generation_id"],
            request["scope_revision"],
            request["checkpoint_sha256"],
            _next_checkpoint_version(request),
            request["target_mutation_outcome"],
            request["value_conversion_outcome"],
            request["operation_id"],
            request["expected_journal_state"],
        )
        _require_operation(cursor, request, "terminal journal")


def _row(cursor: _Cursor) -> tuple[Any, ...] | None:
    row = cursor.fetchone()
    return None if row is None else tuple(row)


def _uuid_text(value: object) -> str | None:
    return None if value is None else str(uuid.UUID(str(value)))


def _next_checkpoint_version(request: Mapping[str, object]) -> int:
    value = request["predecessor_checkpoint_version"]
    if value is None:
        return 1
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise MssqlPublicationHeadConflict("checkpoint predecessor version is invalid")
    return value + 1


def _require_operation(cursor: _Cursor, request: Mapping[str, object], label: str) -> None:
    row = cursor.fetchone()
    if row is None or tuple(row) != (request["operation_id"],):
        raise MssqlPublicationHeadConflict(f"{label} compare-and-set failed")


__all__ = ["MssqlPublicationHeadConflict", "MssqlPublicationHeadStore"]
