from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from dpone.adapters.semantic_refresh_mssql_after_image import (
    MssqlSemanticRefreshAfterImageReader,
    SemanticRefreshMssqlAfterImageReadError,
)
from tests.test_semantic_refresh_clickhouse_http import (
    _digest,
    _ProtectedOperations,
)


class _Cursor:
    def __init__(self, *, digest: str = _digest("f")) -> None:
        self.digest = digest
        self.sql: list[str] = []
        self.executions: list[tuple[str, tuple[object, ...]]] = []
        self.current = ""

    def execute(self, sql: str, *parameters: object) -> _Cursor:
        self.current = sql
        self.sql.append(sql)
        self.executions.append((sql, tuple(parameters)))
        return self

    def fetchone(self) -> tuple[object, ...] | None:
        if "HASHBYTES" in self.current:
            return (self.digest, 2)
        return None

    def fetchall(self) -> list[tuple[object, ...]]:
        if "sys.columns" in self.current:
            return [("event_id",), ("occurred_at",), ("amount",)]
        return [
            (1, datetime(2026, 8, 8, tzinfo=UTC), Decimal("10.00")),
            (2, datetime(2026, 8, 8, 1, tzinfo=UTC), Decimal("20.00")),
        ]

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


def _operation():
    return _ProtectedOperations().load_operation(
        workflow_execution_binding_sha256=_digest("c"),
        operation_id=_digest("0"),
    )


def test_after_image_reader_proves_schema_digest_count_and_order_in_one_transaction() -> None:
    cursor = _Cursor()
    connection = _Connection(cursor)
    snapshot = MssqlSemanticRefreshAfterImageReader(
        lambda: connection,
        mssql_connection_authority_id="mssql-connection-primary",
    ).read(_operation())

    assert snapshot.relation_id == "dpone_control.after_image_orders"
    assert snapshot.image_sha256 == _digest("f")
    assert snapshot.row_count == 2
    assert tuple(column.name for column in snapshot.columns) == ("event_id", "occurred_at", "amount")
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert any("SERIALIZABLE" in sql for sql in cursor.sql)
    assert any("HASHBYTES" in sql and "WITH (HOLDLOCK)" in sql for sql in cursor.sql)


def test_after_image_reader_rejects_digest_drift_before_returning_rows() -> None:
    cursor = _Cursor(digest=_digest("e"))
    connection = _Connection(cursor)
    reader = MssqlSemanticRefreshAfterImageReader(
        lambda: connection,
        mssql_connection_authority_id="mssql-connection-primary",
    )

    with pytest.raises(SemanticRefreshMssqlAfterImageReadError, match="digest differs"):
        reader.read(_operation())

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_after_image_reader_uses_the_protected_database_catalog_for_cross_database_relation() -> None:
    cursor = _Cursor()
    connection = _Connection(cursor)
    operation = replace(
        _operation(),
        after_image_relation="[DWH].[dpone_scope_images].[after_image_orders]",
    )

    MssqlSemanticRefreshAfterImageReader(
        lambda: connection,
        mssql_connection_authority_id="mssql-connection-primary",
    ).read(operation)

    schema_query, parameters = next((sql, values) for sql, values in cursor.executions if "sys.columns" in sql)
    assert "FROM [DWH].sys.columns" in schema_query
    assert parameters == ("[DWH].[dpone_scope_images].[after_image_orders]",)
