"""SQL Server task-attempt fencing for one active dbt workspace."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from dpone.adapters.dbt_workspace_mssql_activation_connection import close, rollback, row
from dpone.contracts.dbt_workspace_control import (
    DbtWorkspaceActivationError,
    DbtWorkspaceAttemptReceipt,
    DbtWorkspaceAttemptRequest,
    DbtWorkspaceAttemptTerminalState,
    DbtWorkspaceGuardEpoch,
    require_attempt_receipt,
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

if TYPE_CHECKING:
    from dpone.adapters.dbt_workspace_mssql_activation_connection import (
        WorkspaceActivationConnection,
        WorkspaceActivationCursor,
    )


class MssqlDbtWorkspaceAttemptAdmission:
    """Fence overlapping task attempts against the activation's held epochs."""

    def __init__(
        self,
        connection_factory: Callable[[], WorkspaceActivationConnection],
        *,
        control_schema: str = "dpone_control",
    ) -> None:
        if _IDENTIFIER.fullmatch(control_schema) is None:
            raise ValueError("control_schema must be a simple SQL identifier")
        self._connection_factory = connection_factory
        self._control_schema = control_schema

    def admit(self, request: DbtWorkspaceAttemptRequest) -> DbtWorkspaceAttemptReceipt:
        """Persist RUNNING only while the exact activation and guard subset are current."""

        request.__post_init__()
        return self._mutate(request, expected_state="RUNNING", mutation=self._admit)

    def terminalize(
        self,
        request: DbtWorkspaceAttemptRequest,
        *,
        state: DbtWorkspaceAttemptTerminalState,
    ) -> DbtWorkspaceAttemptReceipt:
        """Persist one idempotent terminal decision without releasing activation guards."""

        request.__post_init__()
        if state not in {"SUCCEEDED", "FAILED", "COMMIT_UNKNOWN"}:
            raise DbtWorkspaceActivationError("attempt_terminal_state")
        return self._mutate(
            request,
            expected_state=state,
            mutation=lambda cursor, value: self._terminalize(cursor, value, state=state),
        )

    def require(
        self,
        request: DbtWorkspaceAttemptRequest,
        *,
        state: str,
    ) -> DbtWorkspaceAttemptReceipt:
        """Read exact durable attempt state through a fresh serializable transaction."""

        request.__post_init__()
        connection: WorkspaceActivationConnection | None = None
        cursor: WorkspaceActivationCursor | None = None
        try:
            connection = self._connection_factory()
            connection.autocommit = False
            cursor = connection.cursor()
            self._begin(cursor)
            receipt = self._read_exact(cursor, request, state=state)
            connection.commit()
            return receipt
        except DbtWorkspaceActivationError:
            rollback(connection)
            raise
        except Exception:
            rollback(connection)
            raise DbtWorkspaceActivationError("attempt_durable_readback") from None
        finally:
            close(cursor)
            close(connection)

    def _admit(
        self,
        cursor: WorkspaceActivationCursor,
        request: DbtWorkspaceAttemptRequest,
    ) -> DbtWorkspaceAttemptReceipt:
        existing = self._attempt_row(cursor, request.attempt_id)
        if existing is not None:
            return self._read_exact(cursor, request, state="RUNNING", attempt_row=existing)
        self._require_active_activation(cursor, request.activation_id)
        guards = self._resolve_guards(cursor, request)
        self._require_no_overlapping_attempt(cursor, request, guards)
        cursor.execute(
            f"""
INSERT INTO {self._table("dbt_workspace_attempts")} (
    attempt_id, activation_id, request_sha256, workflow_id, state, terminal_receipt_sha256
) VALUES (?, ?, ?, ?, N'RUNNING', NULL);
""".strip(),
            request.attempt_id,
            request.activation_id,
            request.request_sha256,
            request.workflow_id,
        )
        for guard in guards:
            cursor.execute(
                f"""
INSERT INTO {self._table("dbt_workspace_attempt_guards")} (
    attempt_id, guard_id, fencing_epoch
) VALUES (?, ?, ?);
""".strip(),
                request.attempt_id,
                guard.guard_id,
                guard.fencing_epoch,
            )
        return self._read_exact(cursor, request, state="RUNNING")

    def _terminalize(
        self,
        cursor: WorkspaceActivationCursor,
        request: DbtWorkspaceAttemptRequest,
        *,
        state: DbtWorkspaceAttemptTerminalState,
    ) -> DbtWorkspaceAttemptReceipt:
        existing = self._attempt_row(cursor, request.attempt_id)
        if existing is not None and existing[3] == state:
            return self._read_exact(cursor, request, state=state, attempt_row=existing)
        running = self._read_exact(cursor, request, state="RUNNING", attempt_row=existing)
        terminal = DbtWorkspaceAttemptReceipt.build(request=request, state=state, guard_epochs=running.guard_epochs)
        cursor.execute(
            f"""
UPDATE {self._table("dbt_workspace_attempts")} WITH (UPDLOCK, HOLDLOCK)
SET state = ?, terminal_receipt_sha256 = ?, updated_at_utc = SYSUTCDATETIME()
OUTPUT inserted.state, inserted.terminal_receipt_sha256
WHERE attempt_id = ? AND request_sha256 = ? AND state = N'RUNNING'
  AND terminal_receipt_sha256 IS NULL;
""".strip(),
            state,
            terminal.receipt_sha256,
            request.attempt_id,
            request.request_sha256,
        )
        if row(cursor) != (state, terminal.receipt_sha256):
            raise DbtWorkspaceActivationError("attempt_transition")
        return self._read_exact(cursor, request, state=state)

    def _require_active_activation(self, cursor: WorkspaceActivationCursor, activation_id: str) -> None:
        cursor.execute(
            f"""
SELECT state FROM {self._table("dbt_workspace_activations")} WITH (UPDLOCK, HOLDLOCK)
WHERE activation_id = ?;
""".strip(),
            activation_id,
        )
        if row(cursor) != ("ACTIVE",):
            raise DbtWorkspaceActivationError("attempt_activation_not_active")

    def _resolve_guards(
        self,
        cursor: WorkspaceActivationCursor,
        request: DbtWorkspaceAttemptRequest,
    ) -> tuple[DbtWorkspaceGuardEpoch, ...]:
        placeholders = ", ".join("?" for _ in request.write_subjects)
        cursor.execute(
            f"""
SELECT subject.write_subject_sha256, subject.guard_id, ownership.fencing_epoch,
       guard.owner_id, guard.workflow_id, guard.operation_id, guard.status, guard.fencing_epoch
FROM {self._table("dbt_workspace_activation_write_subjects")} AS subject WITH (UPDLOCK, HOLDLOCK)
JOIN {self._table("dbt_workspace_activation_guards")} AS ownership WITH (UPDLOCK, HOLDLOCK)
  ON ownership.activation_id = subject.activation_id AND ownership.guard_id = subject.guard_id
JOIN {self._table("semantic_refresh_guards")} AS guard WITH (UPDLOCK, HOLDLOCK)
  ON guard.resource_id = subject.guard_id
WHERE subject.activation_id = ? AND subject.write_subject_sha256 IN ({placeholders})
ORDER BY subject.write_subject_sha256;
""".strip(),
            request.activation_id,
            *request.write_subjects,
        )
        rows = tuple(tuple(item) for item in cursor.fetchall())
        if tuple(item[0] for item in rows) != request.write_subjects:
            raise DbtWorkspaceActivationError("attempt_write_closure")
        owner = f"dbt-workspace:{request.activation_id}"
        if any(item[3:] != (owner, request.activation_id, None, "HELD", item[2]) for item in rows):
            raise DbtWorkspaceActivationError("attempt_guard_stale")
        return tuple(sorted({DbtWorkspaceGuardEpoch(str(item[1]), int(item[2])) for item in rows}))

    def _require_no_overlapping_attempt(
        self,
        cursor: WorkspaceActivationCursor,
        request: DbtWorkspaceAttemptRequest,
        guards: tuple[DbtWorkspaceGuardEpoch, ...],
    ) -> None:
        placeholders = ", ".join("?" for _ in guards)
        cursor.execute(
            f"""
SELECT TOP (1) attempt.attempt_id
FROM {self._table("dbt_workspace_attempt_guards")} AS owned WITH (UPDLOCK, HOLDLOCK)
JOIN {self._table("dbt_workspace_attempts")} AS attempt WITH (UPDLOCK, HOLDLOCK)
  ON attempt.attempt_id = owned.attempt_id
WHERE owned.guard_id IN ({placeholders}) AND attempt.state = N'RUNNING'
  AND attempt.attempt_id <> ?;
""".strip(),
            *(item.guard_id for item in guards),
            request.attempt_id,
        )
        if row(cursor) is not None:
            raise DbtWorkspaceActivationError("attempt_guard_conflict")

    def _read_exact(
        self,
        cursor: WorkspaceActivationCursor,
        request: DbtWorkspaceAttemptRequest,
        *,
        state: str,
        attempt_row: tuple[Any, ...] | None = None,
    ) -> DbtWorkspaceAttemptReceipt:
        current = attempt_row if attempt_row is not None else self._attempt_row(cursor, request.attempt_id)
        if current is None or current[:4] != (
            request.activation_id,
            request.request_sha256,
            request.workflow_id,
            state,
        ):
            raise DbtWorkspaceActivationError("attempt_occurrence_mismatch")
        cursor.execute(
            f"""
SELECT owned.guard_id, owned.fencing_epoch
FROM {self._table("dbt_workspace_attempt_guards")} AS owned WITH (UPDLOCK, HOLDLOCK)
WHERE owned.attempt_id = ? ORDER BY owned.guard_id;
""".strip(),
            request.attempt_id,
        )
        guards = tuple(DbtWorkspaceGuardEpoch(str(item[0]), int(item[1])) for item in cursor.fetchall())
        receipt = DbtWorkspaceAttemptReceipt.build(request=request, state=state, guard_epochs=guards)
        terminal_sha256 = current[4]
        if (state == "RUNNING" and terminal_sha256 is not None) or (
            state != "RUNNING" and terminal_sha256 != receipt.receipt_sha256
        ):
            raise DbtWorkspaceActivationError("attempt_terminal_readback")
        return require_attempt_receipt(receipt, request, state=state)

    def _attempt_row(self, cursor: WorkspaceActivationCursor, attempt_id: str) -> tuple[Any, ...] | None:
        cursor.execute(
            f"""
SELECT LOWER(CONVERT(char(36), activation_id)), request_sha256, workflow_id,
       state, terminal_receipt_sha256
FROM {self._table("dbt_workspace_attempts")} WITH (UPDLOCK, HOLDLOCK)
WHERE attempt_id = ?;
""".strip(),
            attempt_id,
        )
        return row(cursor)

    def _mutate(
        self,
        request: DbtWorkspaceAttemptRequest,
        *,
        expected_state: str,
        mutation: Callable[[WorkspaceActivationCursor, DbtWorkspaceAttemptRequest], DbtWorkspaceAttemptReceipt],
    ) -> DbtWorkspaceAttemptReceipt:
        connection: WorkspaceActivationConnection | None = None
        cursor: WorkspaceActivationCursor | None = None
        commit_started = False
        try:
            connection = self._connection_factory()
            connection.autocommit = False
            cursor = connection.cursor()
            self._begin(cursor)
            receipt = mutation(cursor, request)
            commit_started = True
            connection.commit()
            return receipt
        except DbtWorkspaceActivationError:
            if not commit_started:
                rollback(connection)
                raise
        except Exception:
            if not commit_started:
                rollback(connection)
                raise DbtWorkspaceActivationError("attempt_durable_mutation") from None
        finally:
            close(cursor)
            close(connection)
        try:
            return self.require(request, state=expected_state)
        except Exception:
            raise DbtWorkspaceActivationError("attempt_commit_unknown") from None

    @staticmethod
    def _begin(cursor: WorkspaceActivationCursor) -> None:
        cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")

    def _table(self, name: str) -> str:
        return f"[{self._control_schema}].[{name}]"


__all__ = ["MssqlDbtWorkspaceAttemptAdmission"]
