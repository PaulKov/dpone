"""Shared target-atomic SQL Server state location primitives."""

from __future__ import annotations

from contextlib import suppress
from typing import Any

from dpone.contracts.mssql_object_name import MSSQLObjectName
from dpone.runtime.state.mssql_fresh_session import MssqlFreshSessionFactory


class MssqlGenericStateBase:
    """Bind one external three-part state location and fresh-session factory."""

    def __init__(
        self,
        connector: Any,
        *,
        database: str,
        schema: str,
        fresh_session_factory: MssqlFreshSessionFactory | None = None,
    ) -> None:
        if not database or not schema:
            raise ValueError("mssql_transaction.state_location_incomplete")
        self.connector = connector
        self.database = database
        self.schema = schema
        self.fresh_sessions = fresh_session_factory or MssqlFreshSessionFactory()

    def qualified(self, table: str) -> str:
        return MSSQLObjectName.from_parts(
            database=self.database,
            schema=self.schema,
            table=table,
            strict=True,
        ).quoted()

    @staticmethod
    def close(connector: Any) -> None:
        closer = getattr(connector, "close", None)
        if callable(closer):
            with suppress(Exception):
                closer()


__all__ = ["MssqlGenericStateBase"]
