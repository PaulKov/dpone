"""Thin MSSQL state adapter for evidence-derived failure terminalization."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from typing import Any, Protocol

from dpone.adapters.semantic_refresh_mssql_failure_records import (
    SemanticRefreshWorkflowFailureError,
    journal_reference,
    positive_integer,
    require_application_lock,
    require_rows,
)
from dpone.adapters.semantic_refresh_mssql_failure_validation import (
    assert_decision_identity,
    assert_replay,
)
from dpone.ports.semantic_refresh_mssql import (
    MssqlGuardClaim,
    MssqlJournalPreparation,
    mssql_guard_set_sha256,
    mssql_journal_set_sha256,
)
from dpone.ports.semantic_refresh_mssql_failure import (
    MssqlFailureContext,
    MssqlFailureDecision,
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _Cursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...

    def fetchall(self) -> Sequence[tuple[Any, ...]]: ...

    def close(self) -> None: ...


class _Connection(Protocol):
    autocommit: bool

    def cursor(self) -> _Cursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


class MssqlSemanticRefreshWorkflowFailureState:
    """Read failure context and atomically persist a derived terminal decision."""

    def __init__(
        self,
        connection_factory: Callable[[], _Connection],
        *,
        control_schema: str = "dpone_control",
    ) -> None:
        if not isinstance(control_schema, str) or _IDENTIFIER.fullmatch(control_schema) is None:
            raise ValueError("control_schema must be a simple SQL identifier")
        self._connection_factory = connection_factory
        self._control_schema = control_schema

    def load_failure_context(self, workflow_id: str) -> MssqlFailureContext:
        """Read the exact admitted journal closure without accepting outcomes."""

        connection = self._connection_factory()
        connection.autocommit = False
        cursor = connection.cursor()
        try:
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            context = self._context(cursor, workflow_id)
            connection.commit()
            return context
        except Exception as exc:
            connection.rollback()
            if isinstance(exc, SemanticRefreshWorkflowFailureError):
                raise
            raise SemanticRefreshWorkflowFailureError("workflow failure context read failed") from exc
        finally:
            cursor.close()
            connection.close()

    def persist_failure(self, decision: MssqlFailureDecision) -> None:
        """Persist only the service-derived decision under the workflow lock."""

        connection = self._connection_factory()
        connection.autocommit = False
        cursor = connection.cursor()
        try:
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            require_application_lock(cursor, decision.workflow_id)
            context = self._context(cursor, decision.workflow_id)
            assert_decision_identity(context, decision, error_type=SemanticRefreshWorkflowFailureError)
            if context.status == "FAILED_PRE_COMMIT":
                assert_replay(context, decision, error_type=SemanticRefreshWorkflowFailureError)
            else:
                self._terminalize(cursor, context, decision)
            connection.commit()
        except Exception as exc:
            connection.rollback()
            if isinstance(exc, SemanticRefreshWorkflowFailureError):
                raise
            raise SemanticRefreshWorkflowFailureError("failed-precommit publication failed") from exc
        finally:
            cursor.close()
            connection.close()

    def _context(self, cursor: _Cursor, workflow_id: str) -> MssqlFailureContext:
        cursor.execute(
            f"""
SELECT workflow_plan_sha256, workflow_execution_binding_sha256,
       guard_set_sha256, journal_set_sha256, workflow_guard_resource_id,
       guard_count, owner_id, terminal_summary_sha256, terminal_summary_json, status
FROM {self._table("semantic_refresh_workflow_executions")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_id = ?;
""".strip(),
            workflow_id,
        )
        execution = cursor.fetchone()
        if execution is None or execution[9] not in {"PREPARING", "FAILED_PRE_COMMIT"}:
            raise SemanticRefreshWorkflowFailureError("workflow execution is not failure-finalizable")
        cursor.execute(
            f"""
SELECT model_unique_id, operation_id, operation_plan_sha256,
       attempt_binding_sha256, strategy_authority_json, strategy_authority_sha256,
       baseline_receipt_sha256, baseline_kind, baseline_receipt_json,
       baseline_status, image_key_columns_json,
       target_resource_id, publication_database, publication_target_table,
       publication_scope_id, target_predecessor_generation_id,
       scope_predecessor_operation_id, fencing_epoch,
       replaces_failed_operation_id, owner_id, status,
       mssql_outcome, mssql_evidence_sha256
FROM {self._table("semantic_refresh_journals")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_id = ? ORDER BY operation_id;
""".strip(),
            workflow_id,
        )
        journals = tuple(journal_reference(tuple(row)) for row in cursor.fetchall())
        if not journals:
            raise SemanticRefreshWorkflowFailureError("workflow journal closure is empty")
        context = MssqlFailureContext(
            workflow_id=workflow_id,
            workflow_plan_sha256=str(execution[0]),
            workflow_execution_binding_sha256=str(execution[1]),
            guard_set_sha256=str(execution[2]),
            journal_set_sha256=str(execution[3]),
            workflow_guard_resource_id=str(execution[4]),
            guard_count=positive_integer(execution[5], "workflow guard count"),
            owner_id=str(execution[6]),
            terminal_summary_sha256=None if execution[7] is None else str(execution[7]),
            terminal_summary_json=None if execution[8] is None else str(execution[8]),
            status=str(execution[9]),
            journals=journals,
        )
        self._require_journal_closure(context)
        self._require_guard_closure(cursor, context)
        self._require_reservation_closure(cursor, context)
        return context

    @staticmethod
    def _require_journal_closure(context: MssqlFailureContext) -> None:
        preparations = tuple(
            MssqlJournalPreparation(
                model_unique_id=item.model_unique_id,
                operation_id=item.operation_id,
                operation_plan_sha256=item.operation_plan_sha256,
                attempt_binding_sha256=item.attempt_binding_sha256,
                strategy_authority_json=item.strategy_authority_json,
                strategy_authority_sha256=item.strategy_authority_sha256,
                baseline_receipt_sha256=item.baseline_receipt_sha256,
                baseline_kind=item.baseline_kind,
                baseline_receipt_json=item.baseline_receipt_json,
                baseline_status=item.baseline_status,
                image_key_columns=item.image_key_columns,
                target_resource_id=item.target_resource_id,
                publication_database=item.publication_database,
                publication_target_table=item.publication_target_table,
                publication_scope_id=item.publication_scope_id,
                target_predecessor_generation_id=item.target_predecessor_generation_id,
                scope_predecessor_operation_id=item.scope_predecessor_operation_id,
                fencing_epoch=item.fencing_epoch,
                replaces_failed_operation_id=item.replaces_failed_operation_id,
            )
            for item in context.journals
        )
        if mssql_journal_set_sha256(preparations) != context.journal_set_sha256:
            raise SemanticRefreshWorkflowFailureError("workflow journal closure digest differs")
        if any(item.owner_id != context.owner_id for item in context.journals):
            raise SemanticRefreshWorkflowFailureError("workflow journal owner closure differs")
        expected_status = "PREPARING" if context.status == "PREPARING" else "FAILED_PRE_COMMIT"
        if any(item.status != expected_status for item in context.journals):
            raise SemanticRefreshWorkflowFailureError("workflow journal status closure differs")
        if context.status == "PREPARING" and any(
            item.persisted_outcome is not None or item.persisted_evidence_sha256 is not None
            for item in context.journals
        ):
            raise SemanticRefreshWorkflowFailureError("preparing journal already contains terminal evidence")
        if context.status == "PREPARING" and (
            context.terminal_summary_sha256 is not None or context.terminal_summary_json is not None
        ):
            raise SemanticRefreshWorkflowFailureError("preparing workflow already contains terminal summary")
        if context.status == "FAILED_PRE_COMMIT" and any(
            item.persisted_outcome is None or item.persisted_evidence_sha256 is None for item in context.journals
        ):
            raise SemanticRefreshWorkflowFailureError("failed journal terminal evidence is incomplete")
        if context.status == "FAILED_PRE_COMMIT" and (
            context.terminal_summary_sha256 is None or context.terminal_summary_json is None
        ):
            raise SemanticRefreshWorkflowFailureError("failed workflow terminal summary is incomplete")

    def _require_guard_closure(self, cursor: _Cursor, context: MssqlFailureContext) -> None:
        cursor.execute(
            f"""
SELECT resource_id, fencing_epoch, owner_id, workflow_id, operation_id,
       operation_plan_sha256, attempt_binding_sha256, strategy_authority_sha256, status
FROM {self._table("semantic_refresh_guards")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_id = ? ORDER BY resource_id;
""".strip(),
            context.workflow_id,
        )
        rows = tuple(tuple(row) for row in cursor.fetchall())
        if len(rows) != context.guard_count:
            raise SemanticRefreshWorkflowFailureError("workflow guard closure count differs")
        by_resource = {item.target_resource_id: item for item in context.journals}
        workflow_claim: MssqlGuardClaim | None = None
        resource_claims: list[MssqlGuardClaim] = []
        expected_status = "HELD" if context.status == "PREPARING" else "RELEASED"
        for row in rows:
            resource_id = str(row[0])
            fencing_epoch = positive_integer(row[1], "guard fencing epoch")
            if row[2:4] != (context.owner_id, context.workflow_id) or row[8] != expected_status:
                raise SemanticRefreshWorkflowFailureError("workflow guard identity differs")
            claim = MssqlGuardClaim(resource_id, fencing_epoch - 1, fencing_epoch)
            if resource_id == context.workflow_guard_resource_id:
                if workflow_claim is not None or any(value is not None for value in row[4:8]):
                    raise SemanticRefreshWorkflowFailureError("workflow guard identity differs")
                workflow_claim = claim
                continue
            journal = by_resource.get(resource_id)
            if journal is None:
                if any(value is not None for value in row[4:8]):
                    raise SemanticRefreshWorkflowFailureError("dependency guard identity differs")
            elif (
                row[4:8]
                != (
                    journal.operation_id,
                    journal.operation_plan_sha256,
                    journal.attempt_binding_sha256,
                    journal.strategy_authority_sha256,
                )
                or fencing_epoch != journal.fencing_epoch
            ):
                raise SemanticRefreshWorkflowFailureError("journal guard identity differs")
            resource_claims.append(claim)
        if workflow_claim is None or set(by_resource).difference(item.resource_id for item in resource_claims):
            raise SemanticRefreshWorkflowFailureError("workflow guard closure is incomplete")
        if mssql_guard_set_sha256(workflow_claim, tuple(resource_claims)) != context.guard_set_sha256:
            raise SemanticRefreshWorkflowFailureError("workflow guard closure digest differs")

    def _require_reservation_closure(self, cursor: _Cursor, context: MssqlFailureContext) -> None:
        cursor.execute(
            f"""
SELECT workflow_execution_binding_sha256, status
FROM {self._table("semantic_refresh_reservations")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_id = ?;
""".strip(),
            context.workflow_id,
        )
        rows = tuple(tuple(row) for row in cursor.fetchall())
        expected_status = "PREPARING" if context.status == "PREPARING" else "FAILED_PRE_COMMIT"
        if rows != ((context.workflow_execution_binding_sha256, expected_status),):
            raise SemanticRefreshWorkflowFailureError("workflow reservation closure differs")

    def _terminalize(
        self,
        cursor: _Cursor,
        context: MssqlFailureContext,
        decision: MssqlFailureDecision,
    ) -> None:
        for journal, outcome in zip(context.journals, decision.models, strict=True):
            if journal.status != "PREPARING" or journal.persisted_outcome is not None:
                raise SemanticRefreshWorkflowFailureError("workflow preparing journal closure differs")
            cursor.execute(
                f"""
UPDATE {self._table("semantic_refresh_journals")} WITH (UPDLOCK, HOLDLOCK)
SET status = N'FAILED_PRE_COMMIT', mssql_outcome = ?, mssql_evidence_sha256 = ?
OUTPUT inserted.operation_id
WHERE operation_id = ? AND attempt_binding_sha256 = ?
  AND status = N'PREPARING' AND mssql_outcome IS NULL AND mssql_evidence_sha256 IS NULL;
""".strip(),
                outcome.mssql_outcome,
                outcome.mssql_evidence_sha256,
                journal.operation_id,
                journal.attempt_binding_sha256,
            )
            require_rows(cursor, ((journal.operation_id,),), "model failure journal")
        cursor.execute(
            f"""
UPDATE {self._table("semantic_refresh_workflow_executions")} WITH (UPDLOCK, HOLDLOCK)
SET status = N'FAILED_PRE_COMMIT', terminal_summary_sha256 = ?, terminal_summary_json = ?,
    completed_at_utc = SYSUTCDATETIME()
OUTPUT inserted.workflow_id
WHERE workflow_id = ? AND status = N'PREPARING'
  AND terminal_summary_sha256 IS NULL AND terminal_summary_json IS NULL;
""".strip(),
            decision.terminal_summary_sha256,
            decision.terminal_summary_json,
            decision.workflow_id,
        )
        require_rows(cursor, ((decision.workflow_id,),), "workflow failure")
        cursor.execute(
            f"""
UPDATE {self._table("semantic_refresh_reservations")} WITH (UPDLOCK, HOLDLOCK)
SET status = N'FAILED_PRE_COMMIT'
OUTPUT inserted.workflow_id
WHERE workflow_id = ? AND status = N'PREPARING';
""".strip(),
            decision.workflow_id,
        )
        require_rows(cursor, ((decision.workflow_id,),), "workflow reservation")
        cursor.execute(
            f"SELECT resource_id FROM {self._table('semantic_refresh_guards')} WITH (UPDLOCK, HOLDLOCK) "
            "WHERE workflow_id = ? AND status = N'HELD' ORDER BY resource_id;",
            decision.workflow_id,
        )
        held_resources = tuple((str(row[0]),) for row in cursor.fetchall())
        if len(held_resources) != context.guard_count:
            raise SemanticRefreshWorkflowFailureError("workflow guard closure is incomplete")
        cursor.execute(
            f"""
UPDATE {self._table("semantic_refresh_guards")} WITH (UPDLOCK, HOLDLOCK)
SET status = N'RELEASED'
OUTPUT inserted.resource_id
WHERE workflow_id = ? AND status = N'HELD';
""".strip(),
            decision.workflow_id,
        )
        require_rows(cursor, held_resources, "workflow guard set")

    def _table(self, name: str) -> str:
        return f"[{self._control_schema}].[{name}]"


__all__ = [
    "MssqlSemanticRefreshWorkflowFailureState",
    "SemanticRefreshWorkflowFailureError",
]
