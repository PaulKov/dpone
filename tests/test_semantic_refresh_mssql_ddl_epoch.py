"""Protected MSSQL DDL epoch observation tests."""

from __future__ import annotations

import pytest

from dpone.adapters.semantic_refresh_mssql_ddl_epoch import (
    MssqlSemanticRefreshDdlEpochObserver,
    SemanticRefreshMssqlDdlEpochObservationError,
)


class _Cursor:
    def __init__(self, row: tuple[object, ...] | None) -> None:
        self.row = row
        self.executions: list[str] = []
        self.closed = False

    def execute(self, sql: str, *parameters: object):
        assert parameters == ()
        self.executions.append(sql)
        return self

    def fetchone(self):
        return self.row

    def close(self) -> None:
        self.closed = True


class _Connection:
    def __init__(self, row: tuple[object, ...] | None) -> None:
        self.autocommit = True
        self.cursor_value = _Cursor(row)
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self) -> _Cursor:
        return self.cursor_value

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


def test_ddl_epoch_observer_returns_locked_positive_singleton() -> None:
    connection = _Connection((7,))

    observed = MssqlSemanticRefreshDdlEpochObserver(
        lambda: connection,
        control_schema="dpone_control",
    ).observe()

    assert observed == 7
    assert connection.autocommit is False
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert connection.closed is True
    assert connection.cursor_value.closed is True
    assert any("SERIALIZABLE" in sql for sql in connection.cursor_value.executions)
    assert any("WITH (UPDLOCK, HOLDLOCK)" in sql for sql in connection.cursor_value.executions)


@pytest.mark.parametrize("row", [None, (0,), (-1,), (True,), ("7",)])
def test_ddl_epoch_observer_rejects_absent_or_noncanonical_epoch(
    row: tuple[object, ...] | None,
) -> None:
    connection = _Connection(row)

    with pytest.raises(SemanticRefreshMssqlDdlEpochObservationError, match="absent or invalid"):
        MssqlSemanticRefreshDdlEpochObserver(lambda: connection).observe()

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert connection.closed is True
