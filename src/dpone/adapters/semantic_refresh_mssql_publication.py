"""Transactional SQL Server state authority for ClickHouse publication."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from dpone.adapters.semantic_refresh_mssql_activation_transaction import (
    ActivationConnection as _Connection,
)
from dpone.adapters.semantic_refresh_mssql_activation_transaction import (
    ActivationCursor as _Cursor,
)
from dpone.adapters.semantic_refresh_mssql_publication_authority import (
    PUBLICATION_AUTHORITY_FIELDS,
    MssqlPublicationCanonicalAuthority,
)
from dpone.adapters.semantic_refresh_mssql_publication_commit_state import (
    MssqlPublicationCommitStateConflict,
    MssqlPublicationCommitStateStore,
)
from dpone.adapters.semantic_refresh_mssql_publication_heads import (
    MssqlPublicationHeadConflict,
    MssqlPublicationHeadStore,
)
from dpone.adapters.semantic_refresh_mssql_publication_prepared import (
    DurableClickHousePreparedPublication,
    MssqlPreparedPublicationStore,
    MssqlPublicationTransitionConflict,
    MssqlPublicationTransitionStore,
)
from dpone.adapters.semantic_refresh_mssql_publication_support import (
    publication_acknowledgement as _acknowledgement,
)
from dpone.adapters.semantic_refresh_mssql_publication_support import (
    publication_uuid_text as _uuid_text,
)
from dpone.adapters.semantic_refresh_mssql_publication_support import (
    rollback_quietly as _rollback,
)
from dpone.adapters.semantic_refresh_mssql_publication_validation import (
    SemanticRefreshMssqlPublicationError,
    assert_publication_authority,
    assert_transition_authority,
    require_identifier,
    validate_commit_reconciliation_request,
    validate_committing_request,
    validate_post_exchange_request,
    validate_prepared_request,
    validate_publication_request,
)


class MssqlSemanticRefreshPublicationState:
    """Persist PREPARED/COMMITTING and atomically publish all terminal heads."""

    def __init__(
        self,
        connection_factory: Callable[[], _Connection],
        *,
        control_schema: str = "dpone_control",
        require_protected_authority: bool = False,
    ) -> None:
        self._connection_factory = connection_factory
        self._control_schema = require_identifier(control_schema, "control_schema")
        self._heads = MssqlPublicationHeadStore(self._table)
        self._committed = MssqlPublicationCommitStateStore(self._table)
        self._prepared = MssqlPreparedPublicationStore(self._table)
        self._transitions = MssqlPublicationTransitionStore(self._table, self._prepared)
        self._canonical_authority = MssqlPublicationCanonicalAuthority(
            self._table,
            required=require_protected_authority,
        )

    def persist_prepared(self, request: Mapping[str, object]) -> Mapping[str, object]:
        """Persist one exact PREPARED receipt under its active fence."""

        validate_prepared_request(
            request,
            protected=self._canonical_authority.required,
            authority_fields=PUBLICATION_AUTHORITY_FIELDS,
        )
        return self._transition(request)

    def mark_committing(self, request: Mapping[str, object]) -> Mapping[str, object]:
        """Move one exact PREPARED receipt to COMMITTING under its fence."""

        validate_committing_request(
            request,
            protected=self._canonical_authority.required,
            authority_fields=PUBLICATION_AUTHORITY_FIELDS,
        )
        return self._transition(request)

    def record_target_committed(self, request: Mapping[str, object]) -> Mapping[str, object]:
        """Persist exact UUID/exchange evidence after proven target mutation."""

        validate_post_exchange_request(
            request,
            protected=self._canonical_authority.required,
            incomplete=False,
            authority_fields=PUBLICATION_AUTHORITY_FIELDS,
        )
        return self._record_target_committed(request)

    def record_committed_incomplete(self, request: Mapping[str, object]) -> Mapping[str, object]:
        """Persist incomplete terminal publication without losing commit evidence."""

        validate_post_exchange_request(
            request,
            protected=self._canonical_authority.required,
            incomplete=True,
            authority_fields=PUBLICATION_AUTHORITY_FIELDS,
        )
        return self._record_committed_incomplete(request)

    def record_commit_unknown(self, request: Mapping[str, object]) -> Mapping[str, object]:
        """Persist an unresolved exchange outcome under the active fence."""

        validate_commit_reconciliation_request(
            request,
            protected=self._canonical_authority.required,
            commit_unknown=True,
            authority_fields=PUBLICATION_AUTHORITY_FIELDS,
        )
        return self._transition(request)

    def reconcile_prepared(self, request: Mapping[str, object]) -> Mapping[str, object]:
        """Restore PREPARED only when ClickHouse proves the predecessor UUID map."""

        validate_commit_reconciliation_request(
            request,
            protected=self._canonical_authority.required,
            commit_unknown=False,
            authority_fields=PUBLICATION_AUTHORITY_FIELDS,
        )
        return self._transition(request)

    def load_prepared(
        self,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> DurableClickHousePreparedPublication:
        """Load the create-once PREPARED documents for a distinct COMMIT task."""

        connection, cursor = self._open(operation_id)
        try:
            value = self._prepared.load(
                cursor,
                workflow_execution_binding_sha256,
                operation_id,
            )
            connection.commit()
            return value
        except Exception as exc:
            _rollback(connection)
            if isinstance(exc, SemanticRefreshMssqlPublicationError):
                raise
            raise SemanticRefreshMssqlPublicationError("durable PREPARED load failed") from exc
        finally:
            cursor.close()
            connection.close()

    def find_prepared(
        self,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> DurableClickHousePreparedPublication | None:
        """Return durable PREPARED, or None only for locked pristine PREPARING."""

        connection, cursor = self._open(operation_id)
        try:
            value = self._prepared.find(
                cursor,
                workflow_execution_binding_sha256,
                operation_id,
            )
            connection.commit()
            return value
        except Exception as exc:
            _rollback(connection)
            if isinstance(exc, SemanticRefreshMssqlPublicationError):
                raise
            raise SemanticRefreshMssqlPublicationError("durable PREPARED lookup failed") from exc
        finally:
            cursor.close()
            connection.close()

    def publish_or_reconcile(self, request: Mapping[str, object]) -> Mapping[str, object]:
        """Publish target/scope/checkpoint/journal in one SERIALIZABLE transaction."""

        return self._publish_terminal(request)

    def publish_empty_scope(self, request: Mapping[str, object]) -> Mapping[str, object]:
        """Publish an evidenced no-swap scope/checkpoint/journal transaction."""

        return self._publish_terminal(request)

    def _publish_terminal(self, request: Mapping[str, object]) -> Mapping[str, object]:
        """Validate and reconcile either exact terminal publication mode."""

        validate_publication_request(
            request,
            protected=self._canonical_authority.required,
            authority_fields=PUBLICATION_AUTHORITY_FIELDS,
        )
        connection, cursor = self._open(request["operation_id"])
        try:
            journal = self._journal(cursor, str(request["operation_id"]))
            assert_publication_authority(self._canonical_authority, cursor, request, journal, uuid_text=_uuid_text)
            heads = self._heads.read(cursor, request)
            if journal[3] == "COMPLETE":
                self._heads.assert_successors(request, journal, heads)
            else:
                self._heads.assert_predecessors(request, journal, heads)
                self._heads.publish(cursor, request, scope_exists=heads[1] is not None)
            connection.commit()
        except Exception as exc:
            _rollback(connection)
            if isinstance(exc, SemanticRefreshMssqlPublicationError):
                raise
            if isinstance(exc, MssqlPublicationHeadConflict):
                raise SemanticRefreshMssqlPublicationError(str(exc)) from exc
            raise SemanticRefreshMssqlPublicationError("semantic refresh terminal state publication failed") from exc
        finally:
            cursor.close()
            connection.close()
        return _acknowledgement(request, "COMPLETE")

    def _transition(self, request: Mapping[str, object]) -> Mapping[str, object]:
        connection, cursor = self._open(request["operation_id"])
        try:
            journal = self._journal(cursor, str(request["operation_id"]))
            assert_transition_authority(self._canonical_authority, cursor, request, journal, uuid_text=_uuid_text)
            if request["next_journal_state"] == "COMMITTING" and self._canonical_authority.required:
                heads = self._heads.read(cursor, request)
                self._heads.assert_head_predecessors(request, heads)
            self._transitions.apply(cursor, request, journal)
            connection.commit()
        except Exception as exc:
            _rollback(connection)
            if isinstance(exc, SemanticRefreshMssqlPublicationError):
                raise
            if isinstance(exc, MssqlPublicationHeadConflict):
                raise SemanticRefreshMssqlPublicationError(str(exc)) from exc
            if isinstance(exc, MssqlPublicationTransitionConflict):
                raise SemanticRefreshMssqlPublicationError(str(exc)) from exc
            raise SemanticRefreshMssqlPublicationError("semantic refresh journal transition failed") from exc
        finally:
            cursor.close()
            connection.close()
        return _acknowledgement(request, str(request["next_journal_state"]))

    def _record_target_committed(self, request: Mapping[str, object]) -> Mapping[str, object]:
        connection, cursor = self._open(request["operation_id"])
        try:
            journal = self._journal(cursor, str(request["operation_id"]))
            assert_transition_authority(self._canonical_authority, cursor, request, journal, uuid_text=_uuid_text)
            state = self._committed.record_target_committed(cursor, request, journal)
            connection.commit()
        except Exception as exc:
            _rollback(connection)
            if isinstance(exc, (SemanticRefreshMssqlPublicationError, MssqlPublicationCommitStateConflict)):
                if isinstance(exc, MssqlPublicationCommitStateConflict):
                    raise SemanticRefreshMssqlPublicationError(str(exc)) from exc
                raise
            raise SemanticRefreshMssqlPublicationError("TARGET_COMMITTED persistence failed") from exc
        finally:
            cursor.close()
            connection.close()
        return _acknowledgement(request, state)

    def _record_committed_incomplete(self, request: Mapping[str, object]) -> Mapping[str, object]:
        connection, cursor = self._open(request["operation_id"])
        try:
            journal = self._journal(cursor, str(request["operation_id"]))
            assert_transition_authority(self._canonical_authority, cursor, request, journal, uuid_text=_uuid_text)
            self._committed.record_committed_incomplete(cursor, request, journal)
            connection.commit()
        except Exception as exc:
            _rollback(connection)
            if isinstance(exc, (SemanticRefreshMssqlPublicationError, MssqlPublicationCommitStateConflict)):
                if isinstance(exc, MssqlPublicationCommitStateConflict):
                    raise SemanticRefreshMssqlPublicationError(str(exc)) from exc
                raise
            raise SemanticRefreshMssqlPublicationError("COMMITTED_INCOMPLETE persistence failed") from exc
        finally:
            cursor.close()
            connection.close()
        return _acknowledgement(request, "COMMITTED_INCOMPLETE")

    def _open(self, operation_id: object) -> tuple[_Connection, _Cursor]:
        connection = self._connection_factory()
        connection.autocommit = False
        cursor = connection.cursor()
        cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
        cursor.execute(
            """
DECLARE @dpone_lock_result int;
EXEC @dpone_lock_result = sys.sp_getapplock
    @Resource = ?, @LockMode = N'Exclusive',
    @LockOwner = N'Transaction', @LockTimeout = 0;
SELECT @dpone_lock_result;
""".strip(),
            f"dpone:semantic-refresh:publication:{operation_id}",
        )
        row = cursor.fetchone()
        if row is None or not isinstance(row[0], int) or isinstance(row[0], bool) or row[0] < 0:
            _rollback(connection)
            cursor.close()
            connection.close()
            raise SemanticRefreshMssqlPublicationError("publication application lock was not acquired")
        return connection, cursor

    def _journal(self, cursor: _Cursor, operation_id: str) -> tuple[Any, ...]:
        cursor.execute(
            f"""
SELECT operation_plan_sha256, attempt_binding_sha256, fencing_epoch, status,
       prepare_receipt_sha256, target_uuid, workflow_id, target_resource_id,
       clickhouse_commit_receipt_sha256, terminal_receipt_sha256,
       target_predecessor_generation_id, scope_predecessor_operation_id,
       predecessor_target_generation, predecessor_target_uuid,
       predecessor_target_operation_id, predecessor_scope_revision,
       predecessor_checkpoint_sha256, predecessor_checkpoint_operation_id,
       predecessor_checkpoint_version, terminal_target_generation,
       terminal_target_generation_id, terminal_scope_revision,
       terminal_checkpoint_sha256, terminal_checkpoint_version,
       terminal_target_mutation_outcome, terminal_value_conversion_outcome
FROM {self._table("semantic_refresh_journals")} WITH (UPDLOCK, HOLDLOCK)
WHERE operation_id = ?;
""".strip(),
            operation_id,
        )
        row = cursor.fetchone()
        if row is None:
            raise SemanticRefreshMssqlPublicationError("publication journal is absent")
        return tuple(row)

    def _table(self, table_name: str) -> str:
        return f"[{self._control_schema}].[{table_name}]"


__all__ = [
    "MssqlSemanticRefreshPublicationState",
    "SemanticRefreshMssqlPublicationError",
]
