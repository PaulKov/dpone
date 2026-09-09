"""SQL Server connector contracts consumed by runtime services."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from contextlib import AbstractContextManager
from typing import Any, Protocol


class MSSQLConnectorPort(Protocol):
    """Thin SQL Server port used by sources and sinks without concrete imports."""

    bcp_path: str
    query_timeout: int
    trust_server_certificate: str

    def bounded_query_timeout(self, seconds: int) -> AbstractContextManager[None]:
        """Temporarily cap statement timeouts and restore prior connector state."""

    def open_session(self, *, application_name: str) -> MSSQLConnectorPort:
        """Return an independently closable ODBC session with the same credentials."""

    def quote_identifier(self, name: str) -> str:
        """Return a SQL Server quoted identifier."""

    def qualified_name(self, schema: str, table: str, *, database: str | None = None) -> str:
        """Return a fully qualified SQL Server table name."""

    def execute_query(self, query: Any, params: Iterable[Any] | None = None) -> int:
        """Execute a SQL Server statement and return the affected-row count."""

    def get_records(
        self,
        query: Any,
        params: Iterable[Any] | None = None,
        as_dict: bool = False,
    ) -> list[Any]:
        """Return SQL Server query results."""

    def get_records_iterator(self, query: Any, params: Iterable[Any] | None = None):
        """Yield SQL Server rows as dictionaries."""

    def build_select_query(
        self,
        schema: str,
        table: str,
        columns: Sequence[str],
        limit: int | None = None,
        offset: int | None = None,
        database: str | None = None,
    ) -> str:
        """Build a dialect-correct SELECT query."""

    def fetch_schema(self, schema: str, table: str, *, database: str | None = None) -> list[tuple[str, str]]:
        """Return ordered column names and database types for a table."""

    def table_exists(self, schema: str, table: str, *, database: str | None = None) -> bool:
        """Return whether a table exists."""

    def bcp_queryout(self, query: str, output_path: str, *, options: Any | None = None) -> int:
        """Export query results through bcp."""

    def bcp_import(
        self,
        schema: str,
        table: str,
        file_path: str,
        *,
        options: Any | None = None,
        database: str | None = None,
    ) -> int:
        """Import a file into a table through bcp."""

    def bcp_import_format(
        self,
        schema: str,
        table: str,
        file_path: str,
        format_path: str,
        *,
        options: Any | None = None,
        database: str | None = None,
    ) -> int:
        """Import a length-prefixed UTF-8 file through an explicit format."""
