from __future__ import annotations

import pytest

from dpone.runtime.sinks.strategies.mssql.mssql_counted_dml import (
    MssqlDmlCountError,
    execute_counted_dml,
)


class _Connector:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def get_records(self, query, params=(), as_dict=False):
        self.calls.append((query, params, as_dict))
        return self.rows


def test_counted_dml_uses_same_batch_vendor_rowcount_including_zero() -> None:
    connector = _Connector([{"__dpone__affected_rows": 0}])

    assert execute_counted_dml(connector, "DELETE FROM [dbo].[t] WHERE [id] = ?;", (7,)) == 0
    query, params, as_dict = connector.calls[0]
    assert query == (
        "DELETE FROM [dbo].[t] WHERE [id] = ?; SELECT CONVERT(bigint, @@ROWCOUNT) AS [__dpone__affected_rows]"
    )
    assert params == (7,)
    assert as_dict is True


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [{"other": 1}],
        [{"__dpone__affected_rows": -1}],
        [{"__dpone__affected_rows": True}],
        [{"__dpone__affected_rows": "not-an-int"}],
        [{"__dpone__affected_rows": 1}, {"__dpone__affected_rows": 2}],
    ],
)
def test_counted_dml_fails_closed_on_unproven_vendor_count(rows) -> None:
    with pytest.raises(MssqlDmlCountError, match="mssql_dml_rowcount_result_invalid"):
        execute_counted_dml(_Connector(rows), "INSERT INTO [dbo].[t] VALUES (1)")


@pytest.mark.parametrize("statement", ["", "  ", "SELECT '\x00'"])
def test_counted_dml_rejects_invalid_statement(statement: str) -> None:
    with pytest.raises(MssqlDmlCountError, match="mssql_dml_statement_invalid"):
        execute_counted_dml(_Connector([]), statement)
