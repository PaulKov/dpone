"""Read-only MSSQL source rows for one sealed ClickHouse generation schema."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from dpone.adapters.dbapi_lifecycle import close
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_clickhouse_dispatch import ClickHouseDispatchColumn

_MSSQL_TYPES = {
    "int": "Int32",
    "bigint": "Int64",
    "smallint": "Int16",
    "tinyint": "UInt8",
    "bit": "UInt8",
    "float": "Float64",
    "real": "Float32",
    "uniqueidentifier": "UUID",
    "date": "Date32",
    "nvarchar": "String",
    "varchar": "String",
    "nchar": "String",
    "char": "String",
    "sysname": "String",
}


def clickhouse_type_for_mssql(type_name: str, *, nullable: bool, precision: int | None, scale: int | None) -> str:
    """Map a closed MSSQL scalar onto the ClickHouse Native grammar."""

    key = type_name.strip().lower()
    if key in {"decimal", "numeric"}:
        if type(precision) is not int or type(scale) is not int or not 1 <= precision <= 76 or scale > precision:
            raise CompositionAdmissionError("clickhouse_source_payload")
        mapped = f"Decimal({precision}, {scale})"
    elif key in {"datetime", "datetime2", "smalldatetime"}:
        mapped = "DateTime64(7, 'UTC')" if key == "datetime2" else "DateTime64(3, 'UTC')"
    else:
        found = _MSSQL_TYPES.get(key)
        if found is None:
            raise CompositionAdmissionError("clickhouse_source_payload")
        mapped = found
    return f"Nullable({mapped})" if nullable else mapped


class MssqlClickHouseSourceReader:
    """SELECT only the sealed column set from the admitted MSSQL source table."""

    def __init__(
        self,
        *,
        open_connection: Callable[[], Any],
        table: Mapping[str, Any],
        columns: Sequence[ClickHouseDispatchColumn],
    ) -> None:
        if not columns or not callable(open_connection):
            raise CompositionAdmissionError("clickhouse_source_payload")
        self._open = open_connection
        self._table = table
        self._columns = tuple(columns)

    def __call__(self, attempt: Any) -> tuple[tuple[object, ...], ...]:
        del attempt
        connection = cursor = None
        try:
            connection = self._open()
            cursor = connection.cursor()
            self._require_schema(cursor)
            names = ", ".join(f"[{column.name}]" for column in self._columns)
            cursor.execute(
                f"SELECT {names} FROM [{self._ident('database')}].[{self._ident('schema')}].[{self._ident('name')}];"
            )
            rows = tuple(tuple(row) for row in cursor.fetchall())
        except CompositionAdmissionError:
            raise
        except Exception:
            raise CompositionAdmissionError("clickhouse_source_payload") from None
        finally:
            close(cursor)
            close(connection)
        return rows

    def _ident(self, field: str) -> str:
        value = self._table.get(field)
        if type(value) is not str or not value or "[" in value or "]" in value or "." in value:
            raise CompositionAdmissionError("clickhouse_source_payload")
        return value

    def _require_schema(self, cursor: Any) -> None:
        cursor.execute(
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, NUMERIC_PRECISION, NUMERIC_SCALE "
            "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG=? AND TABLE_SCHEMA=? AND TABLE_NAME=? "
            "ORDER BY ORDINAL_POSITION;",
            self._ident("database"),
            self._ident("schema"),
            self._ident("name"),
        )
        observed = tuple(
            ClickHouseDispatchColumn(
                str(name),
                clickhouse_type_for_mssql(
                    str(kind),
                    nullable=str(nullable).upper() == "YES",
                    precision=precision if type(precision) is int else None,
                    scale=scale if type(scale) is int else None,
                ),
            )
            for name, kind, nullable, precision, scale in cursor.fetchall()
        )
        if observed != self._columns:
            raise CompositionAdmissionError("clickhouse_source_payload")


__all__ = ["MssqlClickHouseSourceReader", "clickhouse_type_for_mssql"]
