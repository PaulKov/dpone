from __future__ import annotations

import pytest

from dpone.adapters.semantic_refresh_mssql_workflow_publication import (
    MssqlSemanticRefreshWorkflowPublicationReader,
    SemanticRefreshWorkflowPublicationReadError,
)


def _digest(character: str) -> str:
    return "sha256:" + character * 64


class _Cursor:
    def __init__(self, operation_rows: list[tuple[object, ...]]) -> None:
        self.operation_rows = operation_rows
        self.current = ""
        self.execution_reads = 0

    def execute(self, sql: str, *_parameters: object) -> _Cursor:
        self.current = sql
        return self

    def fetchone(self) -> tuple[object, ...] | None:
        if "semantic_refresh_workflow_executions" not in self.current:
            return None
        self.execution_reads += 1
        return ("workflow-row",) if self.execution_reads == 1 else None

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.operation_rows

    def close(self) -> None:
        return None


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self.autocommit = True
        self.cursor_instance = cursor
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> _Cursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        return None


def test_workflow_publication_reader_returns_exact_durable_closure() -> None:
    operations = (_digest("1"), _digest("2"))
    connection = _Connection(
        _Cursor(
            [
                (
                    operations[0],
                    _digest("2"),
                    _digest("3"),
                    "COMPLETE",
                    _digest("4"),
                    _digest("5"),
                    _digest("6"),
                    2,
                    1,
                    "TARGET_COMMITTED",
                    2,
                    1,
                    operations[0],
                    operations[0],
                    _digest("f"),
                ),
                (
                    operations[1],
                    _digest("7"),
                    _digest("8"),
                    "COMMITTED_INCOMPLETE",
                    _digest("9"),
                    _digest("a"),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ),
            ]
        )
    )
    records = MssqlSemanticRefreshWorkflowPublicationReader(lambda: connection).read(
        workflow_execution_id="scheduled__2026-08-08",
        workflow_execution_binding_sha256=_digest("6"),
        expected_operation_ids=operations,
    )

    assert tuple(item.operation_id for item in records) == operations
    assert tuple(item.status for item in records) == ("COMPLETE", "COMMITTED_INCOMPLETE")
    assert connection.commits == 1
    assert connection.rollbacks == 0


def test_workflow_publication_reader_rejects_missing_operation() -> None:
    operation = _digest("1")
    connection = _Connection(
        _Cursor(
            [
                (
                    operation,
                    _digest("2"),
                    _digest("3"),
                    "COMPLETE",
                    _digest("4"),
                    _digest("5"),
                    _digest("6"),
                    2,
                    1,
                    "TARGET_COMMITTED",
                    2,
                    1,
                    operation,
                    operation,
                    _digest("f"),
                )
            ]
        )
    )
    reader = MssqlSemanticRefreshWorkflowPublicationReader(lambda: connection)

    with pytest.raises(SemanticRefreshWorkflowPublicationReadError, match="closure differs"):
        reader.read(
            workflow_execution_id="scheduled__2026-08-08",
            workflow_execution_binding_sha256=_digest("6"),
            expected_operation_ids=(_digest("1"), _digest("2")),
        )

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_workflow_publication_reader_rejects_wrong_terminal_head_lineage() -> None:
    operation = _digest("1")
    connection = _Connection(
        _Cursor(
            [
                (
                    operation,
                    _digest("2"),
                    _digest("3"),
                    "COMPLETE",
                    _digest("4"),
                    _digest("5"),
                    _digest("6"),
                    2,
                    1,
                    "TARGET_COMMITTED",
                    2,
                    1,
                    _digest("7"),
                    operation,
                    _digest("f"),
                )
            ]
        )
    )

    with pytest.raises(SemanticRefreshWorkflowPublicationReadError, match="head lineage differs"):
        MssqlSemanticRefreshWorkflowPublicationReader(lambda: connection).read(
            workflow_execution_id="scheduled__2026-08-08",
            workflow_execution_binding_sha256=_digest("8"),
            expected_operation_ids=(operation,),
        )


def test_workflow_publication_reader_accepts_empty_scope_predecessor_target_owner() -> None:
    operation = _digest("1")
    predecessor = _digest("2")
    connection = _Connection(
        _Cursor(
            [
                (
                    operation,
                    _digest("3"),
                    _digest("4"),
                    "COMPLETE",
                    _digest("5"),
                    _digest("6"),
                    _digest("7"),
                    1,
                    2,
                    "NOT_REQUIRED_EMPTY_SCOPE",
                    1,
                    2,
                    predecessor,
                    operation,
                    predecessor,
                )
            ]
        )
    )

    records = MssqlSemanticRefreshWorkflowPublicationReader(lambda: connection).read(
        workflow_execution_id="scheduled__2026-08-08",
        workflow_execution_binding_sha256=_digest("8"),
        expected_operation_ids=(operation,),
    )

    assert records[0].target_generation == 1
    assert records[0].scope_revision == 2
