from __future__ import annotations

from dataclasses import dataclass

import pytest

from dpone.adapters.semantic_refresh_mssql_protected_authority import (
    MssqlSemanticRefreshProtectedOperationState,
    SemanticRefreshMssqlProtectedAuthorityError,
)

_DIGEST = "sha256:" + "1" * 64


class _Cursor:
    def __init__(self, predecessor_uuid: object) -> None:
        self.predecessor_uuid = predecessor_uuid
        self.rows: list[tuple[object, ...]] = []

    def execute(self, sql: str, *_parameters: object) -> _Cursor:
        if "semantic_refresh_canonical_authorities" in sql:
            self.rows = [("daily/run-1", _DIGEST, "{}", "ACTIVE")]
        elif "semantic_refresh_workflow_executions" in sql:
            self.rows = [
                (
                    "daily/run-1",
                    _DIGEST,
                    _DIGEST,
                    _DIGEST,
                    _DIGEST,
                    1,
                    "owner",
                    "mssql://target",
                    "HELD",
                    "PREPARING",
                    1,
                    "{}",
                    _DIGEST,
                    _DIGEST,
                    None,
                    1,
                    self.predecessor_uuid,
                    _DIGEST,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
            ]
        else:
            self.rows = []
        return self

    def fetchone(self) -> tuple[object, ...] | None:
        return None if not self.rows else self.rows.pop(0)

    def fetchall(self) -> list[tuple[object, ...]]:
        rows = list(self.rows)
        self.rows = []
        return rows

    def close(self) -> None:
        return None


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


def test_protected_operation_reader_normalizes_database_uuid_to_canonical_lowercase() -> None:
    connection = _Connection(_Cursor("0198F11C-6956-74F2-984B-4CFCB1653B88"))

    state = MssqlSemanticRefreshProtectedOperationState(lambda: connection).load_operation_state(
        workflow_execution_binding_sha256=_DIGEST,
        operation_id=_DIGEST,
    )

    assert state.predecessor_target_uuid == "0198f11c-6956-74f2-984b-4cfcb1653b88"
    assert connection.commits == 1
    assert connection.rollbacks == 0


def test_protected_operation_reader_rejects_invalid_database_uuid() -> None:
    connection = _Connection(_Cursor("not-a-uuid"))

    with pytest.raises(SemanticRefreshMssqlProtectedAuthorityError, match="UUID is invalid"):
        MssqlSemanticRefreshProtectedOperationState(lambda: connection).load_operation_state(
            workflow_execution_binding_sha256=_DIGEST,
            operation_id=_DIGEST,
        )

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_protected_plan_reader_returns_one_atomic_operation_closure() -> None:
    connection = _Connection(_Cursor("0198F11C-6956-74F2-984B-4CFCB1653B88"))

    states = MssqlSemanticRefreshProtectedOperationState(lambda: connection).load_plan_states(
        workflow_execution_binding_sha256=_DIGEST,
    )

    assert tuple(item.operation_id for item in states) == (_DIGEST,)
    assert states[0].predecessor_target_uuid == "0198f11c-6956-74f2-984b-4cfcb1653b88"
    assert connection.commits == 1
    assert connection.rollbacks == 0
