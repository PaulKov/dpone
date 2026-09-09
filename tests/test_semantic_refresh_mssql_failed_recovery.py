from __future__ import annotations

from dataclasses import dataclass

import pytest

from dpone.adapters.semantic_refresh_mssql_replacement import (
    MssqlSemanticRefreshPredecessorStateReader,
    SemanticRefreshMssqlPredecessorReadError,
)
from dpone.ports.semantic_refresh_mssql_replacement import MssqlFailedRecoveryReadRequest
from tests.test_semantic_refresh_mssql_run_authority import (
    _Cursor as _WorkerBindingCursor,
)
from tests.test_semantic_refresh_mssql_run_authority import _worker_bundle

_WORKER_BUNDLE = _worker_bundle()
_EXECUTION = _WORKER_BUNDLE.workflow_execution_id
_BINDING = _WORKER_BUNDLE.execution_binding.workflow_execution_binding_sha256
_PLAN = _WORKER_BUNDLE.workflow_plan.workflow_plan_sha256
_AUTHORITY = _WORKER_BUNDLE.authority_sha256
_SUMMARY = "sha256:" + "4" * 64
_PACK = "sha256:" + "3" * 64
_OPERATION = "sha256:" + "6" * 64
_OPERATION_PLAN = "sha256:" + "7" * 64
_ATTEMPT = "sha256:" + "8" * 64
_EVIDENCE = "sha256:" + "9" * 64


def _request() -> MssqlFailedRecoveryReadRequest:
    return MssqlFailedRecoveryReadRequest(
        workflow_execution_id=_EXECUTION,
        workflow_plan_sha256=_PLAN,
        workflow_execution_binding_sha256=_BINDING,
        canonical_authority_sha256=_AUTHORITY,
    )


class _Cursor(_WorkerBindingCursor):
    def __init__(
        self,
        *,
        journal_status: str = "FAILED_PRE_COMMIT",
        activation_receipt_status: str = "ACTIVE",
        tampered_pack_fingerprint: bool = False,
        projection_plan_sha256: str | None = None,
        stored_terminal_workflow_execution_id: str | None = None,
        stored_terminal_workflow_execution_binding_sha256: str | None = None,
    ) -> None:
        super().__init__(
            admitted=True,
            execution_status="FAILED_PRE_COMMIT",
            journal_status=journal_status,
            activation_receipt_status=activation_receipt_status,
            tampered_pack_fingerprint=tampered_pack_fingerprint,
            projection_plan_sha256=projection_plan_sha256,
        )
        self.journal_status = journal_status
        self.stored_terminal_workflow_execution_id = stored_terminal_workflow_execution_id
        self.stored_terminal_workflow_execution_binding_sha256 = stored_terminal_workflow_execution_binding_sha256

    def execute(self, sql: str, *parameters: object) -> _Cursor:
        if "semantic_refresh_workflow_executions" in sql:
            self.executions.append((sql, tuple(parameters)))
            self._rows = ()
            self._row = (
                self.stored_terminal_workflow_execution_id or _EXECUTION,
                _PLAN,
                _AUTHORITY,
                _SUMMARY,
                "{}",
                "FAILED_PRE_COMMIT",
                self.stored_terminal_workflow_execution_binding_sha256 or _BINDING,
            )
        elif "semantic_refresh_journals" in sql:
            self.executions.append((sql, tuple(parameters)))
            self._row = None
            self._rows = (
                (
                    "model.analytics.events",
                    _OPERATION,
                    _ATTEMPT,
                    "ROLLED_BACK",
                    _EVIDENCE,
                    _OPERATION_PLAN,
                    7,
                    "{}",
                    "sha256:" + "a" * 64,
                    None,
                    None,
                    None,
                    None,
                    self.journal_status,
                ),
            )
        else:
            super().execute(sql, *parameters)
        return self


@dataclass
class _Connection:
    cursor_instance: _Cursor
    autocommit: bool = True
    commits: int = 0
    rollbacks: int = 0

    def cursor(self) -> _Cursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        return None


def test_failed_recovery_reader_locks_authority_pack_summary_and_journals() -> None:
    connection = _Connection(_Cursor())

    result = MssqlSemanticRefreshPredecessorStateReader(lambda: connection).load_failed_recovery_authority(_request())

    assert result.plan_bundle_sha256 == _PACK
    assert result.terminal_summary_sha256 == _SUMMARY
    assert result.terminal_summary_json == "{}"
    assert result.models[0].mssql_outcome == "ROLLED_BACK"
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert all("WITH (UPDLOCK, HOLDLOCK)" in sql for sql, _ in connection.cursor_instance.executions if "SELECT" in sql)


def test_failed_recovery_reader_rejects_nonterminal_journal_closure() -> None:
    connection = _Connection(_Cursor(journal_status="PREPARING"))

    with pytest.raises(SemanticRefreshMssqlPredecessorReadError, match="journal authority"):
        MssqlSemanticRefreshPredecessorStateReader(lambda: connection).load_failed_recovery_authority(_request())

    assert connection.commits == 0
    assert connection.rollbacks == 1


@pytest.mark.parametrize(
    "cursor",
    [
        _Cursor(tampered_pack_fingerprint=True),
        _Cursor(projection_plan_sha256="sha256:" + "f" * 64),
        _Cursor(activation_receipt_status="INACTIVE"),
    ],
)
def test_failed_recovery_reader_rejects_incomplete_activated_pack_authority(
    cursor: _Cursor,
) -> None:
    connection = _Connection(cursor)

    with pytest.raises(SemanticRefreshMssqlPredecessorReadError, match="protected"):
        MssqlSemanticRefreshPredecessorStateReader(lambda: connection).load_failed_recovery_authority(_request())

    assert connection.commits == 0
    assert connection.rollbacks == 1


@pytest.mark.parametrize(
    "cursor",
    [
        _Cursor(stored_terminal_workflow_execution_id=_EXECUTION.upper()),
        _Cursor(stored_terminal_workflow_execution_binding_sha256=_BINDING.upper()),
    ],
)
def test_failed_recovery_reader_rejects_collation_equivalent_terminal_execution(
    cursor: _Cursor,
) -> None:
    connection = _Connection(cursor)

    with pytest.raises(SemanticRefreshMssqlPredecessorReadError, match="execution"):
        MssqlSemanticRefreshPredecessorStateReader(lambda: connection).load_failed_recovery_authority(_request())

    assert connection.commits == 0
    assert connection.rollbacks == 1
