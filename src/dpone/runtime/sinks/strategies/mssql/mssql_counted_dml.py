"""Exact SQL Server DML cardinality authority.

``pyodbc.Cursor.rowcount`` is allowed to be ``-1`` for ``INSERT .. SELECT``
and other set-based statements.  Clamping that sentinel to zero corrupts load
metrics and commit receipts even though the target mutation succeeded.  This
module reads ``@@ROWCOUNT`` in the same batch and transaction, immediately
after the authored DML statement.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any


class MssqlDmlCountError(RuntimeError):
    """Raised when SQL Server does not return one exact affected-row count."""

    code = "DPONE_MSSQL_DML_COUNT_UNAVAILABLE"

    def __init__(self, blocker: str) -> None:
        self.blocker = blocker
        super().__init__(f"{self.code}:{blocker}")


def execute_counted_dml(
    connector: Any,
    statement: str,
    params: Iterable[Any] | None = None,
) -> int:
    """Execute one DML statement and return vendor ``@@ROWCOUNT`` exactly."""

    sql = str(statement).strip()
    if not sql or "\x00" in sql:
        raise MssqlDmlCountError("mssql_dml_statement_invalid")
    rows = connector.get_records(
        f"{sql.rstrip(';')}; SELECT CONVERT(bigint, @@ROWCOUNT) AS [__dpone__affected_rows]",
        tuple(params or ()),
        as_dict=True,
    )
    if len(rows) != 1 or "__dpone__affected_rows" not in rows[0]:
        raise MssqlDmlCountError("mssql_dml_rowcount_result_invalid")
    value = rows[0]["__dpone__affected_rows"]
    if isinstance(value, bool):
        raise MssqlDmlCountError("mssql_dml_rowcount_result_invalid")
    try:
        count = int(value)
    except (TypeError, ValueError) as exc:
        raise MssqlDmlCountError("mssql_dml_rowcount_result_invalid") from exc
    if count < 0:
        raise MssqlDmlCountError("mssql_dml_rowcount_result_invalid")
    return count


__all__ = ["MssqlDmlCountError", "execute_counted_dml"]
