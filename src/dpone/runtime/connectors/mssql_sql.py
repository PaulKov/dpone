"""SQL Server identifier and query rendering helpers."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from dpone.contracts.mssql_object_name import MSSQLObjectName, quote_mssql_identifier


class MSSQLSqlRenderer:
    """SQL Server dialect renderer for :class:`MSSQLConnector`."""

    def quote_identifier(self, name: str) -> str:
        return quote_mssql_identifier(name)

    def qualified_name(self, schema: str, table: str, *, database: str | None = None) -> str:
        return MSSQLObjectName.from_parts(schema=schema, table=table, database=database).quoted()

    def build_select_query(
        self,
        schema: str,
        table: str,
        columns: Sequence[str],
        limit: int | None = None,
        offset: int | None = None,
        database: str | None = None,
    ) -> str:
        column_sql = ", ".join(self.quote_identifier(column) for column in columns)
        table_sql = self.qualified_name(schema, table, database=database)
        query = f"SELECT {column_sql} FROM {table_sql}"
        if offset is not None:
            query += f" ORDER BY 1 OFFSET {int(offset)} ROWS"
            if limit is not None:
                query += f" FETCH NEXT {int(limit)} ROWS ONLY"
        elif limit is not None:
            query = f"SELECT TOP ({int(limit)}) {column_sql} FROM {table_sql}"
        return query

    def build_max_query(self, schema: str, table: str, column: str, *, database: str | None = None) -> str:
        return (
            f"SELECT MAX({self.quote_identifier(column)}) AS max_val "
            f"FROM {self.qualified_name(schema, table, database=database)}"
        )

    @staticmethod
    def bool_option(value: str | bool) -> str:
        if isinstance(value, bool):
            return "yes" if value else "no"
        normalized = str(value).strip().lower()
        return "yes" if normalized in {"1", "true", "yes", "y"} else "no"

    @staticmethod
    def format_type(row: dict[str, Any]) -> str:
        data_type = str(row["DATA_TYPE"])
        if data_type in {"varchar", "nvarchar", "char", "nchar", "binary", "varbinary"}:
            length = row.get("CHARACTER_MAXIMUM_LENGTH")
            rendered = f"{data_type}(max)" if length in (-1, None) else f"{data_type}({int(length)})"
        elif data_type in {"decimal", "numeric"}:
            precision = row.get("NUMERIC_PRECISION") or 18
            scale = row.get("NUMERIC_SCALE") or 0
            rendered = f"{data_type}({int(precision)},{int(scale)})"
        elif data_type in {"datetime2", "datetimeoffset", "time"}:
            precision = row.get("DATETIME_PRECISION")
            if isinstance(precision, bool) or not isinstance(precision, int) or not 0 <= precision <= 7:
                raise ValueError(f"mssql source catalog precision unavailable for {data_type}")
            rendered = f"{data_type}({precision})"
        elif data_type == "float":
            precision = row.get("NUMERIC_PRECISION")
            if isinstance(precision, bool) or not isinstance(precision, int) or precision not in {24, 53}:
                raise ValueError("mssql source catalog precision unavailable for float")
            rendered = f"float({precision})"
        else:
            rendered = data_type
        if str(row.get("IS_NULLABLE", "")).upper() == "YES":
            return f"{rendered} nullable"
        return rendered


__all__ = ["MSSQLSqlRenderer"]
