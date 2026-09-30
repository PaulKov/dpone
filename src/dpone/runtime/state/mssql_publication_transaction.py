"""One owned DBAPI transaction; result rows alone never authorize ClickHouse DDL.

Composition supplies a factory for a *fresh dedicated* SQL Server connection,
never a caller's business transaction. There are no retries or readback here.
The validator must bind the result to the exact write before COMMIT; the result
is released only after all result sets/errors and the commit ACK are consumed.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from contextlib import suppress
from typing import Any, TypeVar

from dpone.ports.mssql_publication import PublicationSessionFactory, PublicationSqlCursor

_Result = TypeVar("_Result")


class PublicationTransactionUnknown(RuntimeError):
    """The invocation cannot issue a permit, even if its write is later found."""


def execute_publication_transaction(
    session_factory: PublicationSessionFactory,
    statement: str,
    params: Sequence[Any],
    *,
    validate: Callable[[list[tuple[Any, ...]]], _Result],
) -> _Result:
    """Execute once, validate one result set, commit once, and close ownership.

    Failure at any stage is intentionally conservative and redacted. A caller
    must reconcile an unknown outcome rather than repeat a mutation. Rollback
    after a lost commit ACK is cleanup, not evidence that the commit failed.
    """
    connection = None
    cursor = None
    try:
        connection = session_factory()
        connection.autocommit = False
        cursor = connection.cursor()
        cursor.execute("SET XACT_ABORT ON; SET NOCOUNT ON;\n" + statement, tuple(params))
        rows = _one_result(cursor)
        result = validate(rows)
        cursor.close()
        cursor = None
        connection.commit()
        connection.close()
        connection = None
        return result
    except Exception:
        raise PublicationTransactionUnknown("publication transaction was not fully acknowledged") from None
    finally:
        if cursor is not None:
            with suppress(Exception):
                cursor.close()
        if connection is not None:
            with suppress(Exception):
                connection.rollback()
            with suppress(Exception):
                connection.close()


def _one_result(cursor: PublicationSqlCursor) -> list[tuple[Any, ...]]:
    result = None
    while True:
        if cursor.description is not None:
            if result is not None:
                raise ValueError("unexpected publication result set")
            result = [tuple(row) for row in cursor.fetchall()]
        if not cursor.nextset():
            break
    if result is None:
        raise ValueError("missing publication result set")
    return result
