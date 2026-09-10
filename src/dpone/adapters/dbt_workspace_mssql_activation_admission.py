"""SQL Server durable admission for exact dbt workspace activation occurrences."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import TYPE_CHECKING

from dpone.adapters.dbapi_lifecycle import close, rollback, row
from dpone.adapters.dbt_workspace_mssql_retirement import (
    activation_record as activation_row,
)
from dpone.adapters.dbt_workspace_mssql_retirement import (
    begin_retirement,
    finalize_retirement,
    read_retired,
)
from dpone.adapters.dbt_workspace_mssql_retirement import (
    insert_activation_record as insert_activation,
)
from dpone.adapters.dbt_workspace_mssql_retirement import (
    read_exact_activation as read_exact,
)
from dpone.contracts.dbt_workspace_control import (
    DbtWorkspaceActivationError,
    DbtWorkspaceActivationReceipt,
    DbtWorkspaceActivationRequest,
    DbtWorkspaceGuardEpoch,
    canonical_fingerprint,
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

if TYPE_CHECKING:
    from dpone.ports.sql_connection import SqlControlConnection as WorkspaceActivationConnection
    from dpone.ports.sql_connection import SqlControlCursor as WorkspaceActivationCursor


class MssqlDbtWorkspaceActivationAdmission:
    """Reserve shared physical guards and persist one exact occurrence atomically."""

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

    def prepare(self, request: DbtWorkspaceActivationRequest) -> DbtWorkspaceActivationReceipt:
        """Reserve the complete guard closure or replay the exact PREPARED row."""

        request.__post_init__()
        return self._mutate(request, expected_state="PREPARED", mutation=self._prepare)

    def activate(self, request: DbtWorkspaceActivationRequest) -> DbtWorkspaceActivationReceipt:
        """Move the exact PREPARED occurrence to ACTIVE and verify owned epochs."""

        request.__post_init__()
        return self._mutate(request, expected_state="ACTIVE", mutation=self._activate)

    def require_active(self, request: DbtWorkspaceActivationRequest) -> DbtWorkspaceActivationReceipt:
        """Read one exact ACTIVE occurrence without creating or advancing a guard."""

        request.__post_init__()
        connection: WorkspaceActivationConnection | None = None
        cursor: WorkspaceActivationCursor | None = None
        try:
            connection = self._connection_factory()
            connection.autocommit = False
            cursor = connection.cursor()
            self._begin(cursor)
            receipt = read_exact(cursor, request, state="ACTIVE", table=self._table)
            connection.commit()
            return receipt
        except DbtWorkspaceActivationError:
            rollback(connection)
            raise
        except Exception:
            rollback(connection)
            raise DbtWorkspaceActivationError("durable_readback") from None
        finally:
            close(cursor)
            close(connection)

    def begin_retirement(self, request: DbtWorkspaceActivationRequest) -> DbtWorkspaceActivationReceipt:
        """Close admission for new attempts while preserving all held epochs."""

        request.__post_init__()
        return self._mutate(request, expected_state="RETIRING", mutation=self._begin_retirement)

    def finalize_retirement(self, request: DbtWorkspaceActivationRequest) -> DbtWorkspaceActivationReceipt:
        """Release exact epochs only after every admitted attempt is safely terminal."""

        request.__post_init__()
        return self._mutate(request, expected_state="RETIRED", mutation=self._finalize_retirement)

    def _mutate(
        self,
        request: DbtWorkspaceActivationRequest,
        *,
        expected_state: str,
        mutation: Callable[[WorkspaceActivationCursor, DbtWorkspaceActivationRequest], DbtWorkspaceActivationReceipt],
    ) -> DbtWorkspaceActivationReceipt:
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
                raise DbtWorkspaceActivationError("durable_mutation") from None
        finally:
            close(cursor)
            close(connection)
        return self._reconcile_commit_unknown(request, state=expected_state)

    def _prepare(
        self,
        cursor: WorkspaceActivationCursor,
        request: DbtWorkspaceActivationRequest,
    ) -> DbtWorkspaceActivationReceipt:
        existing = activation_row(cursor, request.activation_id, table=self._table)
        if existing is not None:
            return read_exact(cursor, request, state="PREPARED", table=self._table, existing=existing)
        insert_activation(cursor, request, table=self._table)
        epochs = tuple(self._reserve_guard(cursor, request, resource.guard_id) for resource in request.resources)
        for resource, epoch in zip(request.resources, epochs, strict=True):
            cursor.execute(
                f"""
INSERT INTO {self._table("dbt_workspace_activation_guards")} (
    activation_id, guard_id, resource_sha256, fencing_epoch
) VALUES (?, ?, ?, ?);
""".strip(),
                request.activation_id,
                resource.guard_id,
                canonical_fingerprint(resource.to_dict()),
                epoch.fencing_epoch,
            )
            for write_subject in resource.write_subjects:
                cursor.execute(
                    f"""
INSERT INTO {self._table("dbt_workspace_activation_write_subjects")} (
    activation_id, write_subject_sha256, guard_id
) VALUES (?, ?, ?);
""".strip(),
                    request.activation_id,
                    write_subject,
                    resource.guard_id,
                )
        return read_exact(cursor, request, state="PREPARED", table=self._table)

    def _activate(
        self,
        cursor: WorkspaceActivationCursor,
        request: DbtWorkspaceActivationRequest,
    ) -> DbtWorkspaceActivationReceipt:
        existing = activation_row(cursor, request.activation_id, table=self._table)
        if existing is not None and existing[-1] == "ACTIVE":
            return read_exact(cursor, request, state="ACTIVE", table=self._table, existing=existing)
        read_exact(cursor, request, state="PREPARED", table=self._table, existing=existing)
        cursor.execute(
            f"""
UPDATE {self._table("dbt_workspace_activations")} WITH (UPDLOCK, HOLDLOCK)
SET state = N'ACTIVE', updated_at_utc = SYSUTCDATETIME()
OUTPUT inserted.state
WHERE activation_id = ? AND request_sha256 = ? AND state = N'PREPARED';
""".strip(),
            request.activation_id,
            request.request_sha256,
        )
        if row(cursor) != ("ACTIVE",):
            raise DbtWorkspaceActivationError("occurrence_transition")
        return read_exact(cursor, request, state="ACTIVE", table=self._table)

    def _begin_retirement(
        self,
        cursor: WorkspaceActivationCursor,
        request: DbtWorkspaceActivationRequest,
    ) -> DbtWorkspaceActivationReceipt:
        return begin_retirement(
            cursor,
            request,
            table=self._table,
            activation_row=lambda item, activation_id: activation_row(item, activation_id, table=self._table),
            read_exact=lambda item, value, **kwargs: read_exact(
                item,
                value,
                table=self._table,
                state=kwargs["state"],
                existing=kwargs.get("activation_row"),
            ),
        )

    def _finalize_retirement(
        self,
        cursor: WorkspaceActivationCursor,
        request: DbtWorkspaceActivationRequest,
    ) -> DbtWorkspaceActivationReceipt:
        return finalize_retirement(
            cursor,
            request,
            table=self._table,
            activation_row=lambda item, activation_id: activation_row(item, activation_id, table=self._table),
            read_exact=lambda item, value, **kwargs: read_exact(
                item,
                value,
                table=self._table,
                state=kwargs["state"],
                existing=kwargs.get("activation_row"),
            ),
        )

    def _reserve_guard(
        self,
        cursor: WorkspaceActivationCursor,
        request: DbtWorkspaceActivationRequest,
        guard_id: str,
    ) -> DbtWorkspaceGuardEpoch:
        self._lock_guard(cursor, guard_id)
        cursor.execute(
            f"""
SELECT fencing_epoch, owner_id, workflow_id, operation_id, status
FROM {self._table("semantic_refresh_guards")} WITH (UPDLOCK, HOLDLOCK)
WHERE resource_id = ?;
""".strip(),
            guard_id,
        )
        existing = row(cursor)
        owner = _owner(request.activation_id)
        if existing is None:
            epoch = 1
            cursor.execute(
                f"""
INSERT INTO {self._table("semantic_refresh_guards")} (
    resource_id, fencing_epoch, owner_id, workflow_id, operation_id, status
) VALUES (?, ?, ?, ?, NULL, N'HELD');
""".strip(),
                guard_id,
                epoch,
                owner,
                request.activation_id,
            )
            return DbtWorkspaceGuardEpoch(guard_id, epoch)
        epoch, current_owner, workflow_id, operation_id, status = existing
        if (
            isinstance(epoch, bool)
            or not isinstance(epoch, int)
            or epoch < 0
            or status != "AVAILABLE"
            or current_owner is not None
            or workflow_id is not None
            or operation_id is not None
        ):
            raise DbtWorkspaceActivationError("guard_conflict")
        next_epoch = epoch + 1
        cursor.execute(
            f"""
UPDATE {self._table("semantic_refresh_guards")} WITH (UPDLOCK, HOLDLOCK)
SET fencing_epoch = ?, owner_id = ?, workflow_id = ?, operation_id = NULL, status = N'HELD'
OUTPUT inserted.resource_id
WHERE resource_id = ? AND fencing_epoch = ? AND status = N'AVAILABLE'
  AND owner_id IS NULL AND workflow_id IS NULL AND operation_id IS NULL;
""".strip(),
            next_epoch,
            owner,
            request.activation_id,
            guard_id,
            epoch,
        )
        if row(cursor) != (guard_id,):
            raise DbtWorkspaceActivationError("guard_conflict")
        return DbtWorkspaceGuardEpoch(guard_id, next_epoch)

    def _reconcile_commit_unknown(
        self,
        request: DbtWorkspaceActivationRequest,
        *,
        state: str,
    ) -> DbtWorkspaceActivationReceipt:
        try:
            if state == "ACTIVE":
                return self.require_active(request)
            return self._require_state(request, state=state)
        except Exception:
            raise DbtWorkspaceActivationError("commit_unknown") from None

    def _require_state(
        self,
        request: DbtWorkspaceActivationRequest,
        *,
        state: str,
    ) -> DbtWorkspaceActivationReceipt:
        connection: WorkspaceActivationConnection | None = None
        cursor: WorkspaceActivationCursor | None = None
        try:
            connection = self._connection_factory()
            connection.autocommit = False
            cursor = connection.cursor()
            self._begin(cursor)
            result = (
                read_retired(
                    cursor,
                    request,
                    table=self._table,
                    activation_row=lambda item, activation_id: activation_row(item, activation_id, table=self._table),
                )
                if state == "RETIRED"
                else read_exact(cursor, request, state=state, table=self._table)
            )
            connection.commit()
            return result
        except Exception:
            rollback(connection)
            raise
        finally:
            close(cursor)
            close(connection)

    @staticmethod
    def _begin(cursor: WorkspaceActivationCursor) -> None:
        cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")

    @staticmethod
    def _lock_guard(cursor: WorkspaceActivationCursor, guard_id: str) -> None:
        cursor.execute(
            """
DECLARE @dpone_lock_result int;
EXEC @dpone_lock_result = sys.sp_getapplock
    @Resource = ?, @LockMode = N'Exclusive',
    @LockOwner = N'Transaction', @LockTimeout = 0;
SELECT @dpone_lock_result;
""".strip(),
            f"dpone:semantic-refresh:{guard_id}",
        )
        result = row(cursor)
        if result is None or isinstance(result[0], bool) or not isinstance(result[0], int) or result[0] < 0:
            raise DbtWorkspaceActivationError("guard_lock")

    def _table(self, name: str) -> str:
        return f"[{self._control_schema}].[{name}]"


def _owner(activation_id: str) -> str:
    return f"dbt-workspace:{activation_id}"


__all__ = ["MssqlDbtWorkspaceActivationAdmission"]
