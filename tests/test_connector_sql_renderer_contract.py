from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pytest

from dpone.ports.db_connector import AbstractConnector
from dpone.runtime.connectors import mssql_sql


class MinimalConnector(AbstractConnector):
    @property
    def connection(self):
        return object()

    def execute_query(self, query: Any, params: Iterable[Any] | None = None) -> int:
        del query, params
        return 0

    def get_records(self, query: Any, params: Iterable[Any] | None = None, as_dict: bool = False) -> list[Any]:
        del query, params, as_dict
        return []

    def commit_transaction(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def begin(self) -> None:
        return None

    def build_select_query(
        self,
        schema: str,
        table: str,
        columns: list[str],
        limit: int | None = None,
        offset: int | None = None,
    ) -> Any:
        del schema, table, columns, limit, offset
        return "SELECT 1"


def test_base_connector_does_not_emit_generic_max_sql() -> None:
    connector = MinimalConnector()

    with pytest.raises(NotImplementedError, match="SqlQueryRenderer"):
        connector.build_max_query("dbo", "orders", "updated_at")


def test_mssql_sql_renderer_is_only_public_mssql_sql_contract() -> None:
    renderer = mssql_sql.MSSQLSqlRenderer()

    assert renderer.build_max_query("dbo", "orders", "updated_at") == (
        "SELECT MAX([updated_at]) AS max_val FROM [dbo].[orders]"
    )
    assert "MSSQLSqlRenderer" in mssql_sql.__all__
    assert "MSSQLSqlHelper" not in mssql_sql.__all__
    assert not hasattr(mssql_sql, "MSSQLSqlHelper")
