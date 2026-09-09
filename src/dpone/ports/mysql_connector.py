"""MySQL connector contracts consumed by runtime sources."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any, Protocol


class MySQLConnectorPort(Protocol):
    """Thin MySQL port required by source extraction strategies."""

    def execute_query(self, query: Any, params: Iterable[Any] | None = None) -> int:
        """Execute a statement and return affected-row count."""

    def get_records(
        self,
        query: Any,
        params: Iterable[Any] | None = None,
        as_dict: bool = False,
    ) -> list[Any]:
        """Return query results."""

    def get_records_streaming(
        self,
        query: Any,
        params: Iterable[Any] | None = None,
        *,
        as_dict: bool = True,
        batch_size: int = 10000,
    ) -> Iterable[list[Any]]:
        """Yield batches of query results."""

    def quote_identifier(self, name: str) -> str:
        """Quote a MySQL identifier."""

    def build_select_query(
        self,
        schema: str,
        table: str,
        columns: Sequence[str],
        limit: int | None = None,
        offset: int | None = None,
        database: str | None = None,
    ) -> str:
        """Build a SELECT for the given table projection."""

    def get_table_column_types(
        self,
        schema: str,
        table: str,
        *,
        database: str | None = None,
    ) -> dict[str, str]:
        """Return column name → MySQL data type for the table."""

    def get_max_column_value(
        self,
        schema: str,
        table: str,
        column: str,
        *,
        database: str | None = None,
    ) -> Any:
        """Return MAX(column) for watermark incremental extract."""

    def export_mssql_delimited_to_file(
        self,
        query: str,
        output_path: str,
        schema: Sequence[tuple[str, str]],
        *,
        params: Iterable[Any] | None = None,
        compress: bool = False,
        batch_size: int = 10000,
        bulk_text_codec: Any | None = None,
        field_terminator: str | None = None,
    ) -> dict[str, Any]:
        """Stream a SELECT into an mssql-delimited text file for BCP ingest."""
