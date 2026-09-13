"""Read-only MSSQL source rows for one sealed ClickHouse generation schema."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from typing import Any

from dpone.adapters.dbapi_lifecycle import close, rollback
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_clickhouse_dispatch import ClickHouseDispatchColumn
from dpone.contracts.composition_snapshot import SnapshotLimits
from dpone.contracts.composition_snapshot_materialization import require_snapshot_columns, snapshot_scalar
from dpone.contracts.strict_json import canonical_json_bytes
from dpone.runtime.clickhouse_binary_encoding import (
    default_non_null_value,
    encode_clickhouse_non_null,
    encode_clickhouse_string,
    unwrap_nullable,
    var_uint,
)
from dpone.runtime.clickhouse_rowbinary import preflight_value
from dpone.runtime.support.type_mapping.mssql_clickhouse import MssqlClickHouseTypePolicy

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
        columns: Sequence[ClickHouseDispatchColumn] | None = None,
        limits: SnapshotLimits,
        require_source: Callable[[Any], bytes] | None = None,
    ) -> None:
        if not callable(open_connection):
            raise CompositionAdmissionError("clickhouse_source_payload")
        self._open = open_connection
        self._table = table
        self._columns = tuple(columns or ())
        limits.__post_init__()
        self._limits = limits
        self._require_source = require_source
        self.source_identity_original: bytes | None = None
        # UTF-16 source text can use twice canonical UTF-8 ASCII bytes; fixed
        # supported SQL scalars need at most 32 bytes per column. This guard
        # bounds driver allocations; canonical source accounting remains exact.
        self._driver_row_limit = 2 * limits.max_source_bytes + 32 * len(self._columns)
        if self._columns:
            require_snapshot_columns(self._columns)

    @property
    def limits(self) -> SnapshotLimits:
        """Expose the exact source ceiling for protected capture binding checks."""
        return self._limits

    @property
    def table_identity(self) -> tuple[str, str, str]:
        """Expose validated table identity without credentials or a connection."""
        return self._ident("database"), self._ident("schema"), self._ident("name")

    def __call__(self, attempt: Any) -> tuple[tuple[object, ...], ...]:
        del attempt
        return self.read_snapshot()[1]

    def read_snapshot(self) -> tuple[tuple[ClickHouseDispatchColumn, ...], tuple[tuple[object, ...], ...]]:
        """Discover schema and read rows under one shared serializable table lock."""
        connection = cursor = None
        try:
            connection = self._open()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            self.source_identity_original = self._source_identity(connection)
            self._require_schema(cursor)
            transaction_id = self._require_transaction(cursor)
            self._driver_row_limit = 2 * self._limits.max_source_bytes + 32 * len(self._columns)
            # The server suppresses an oversized row's values before the driver
            # can allocate them. The size sentinel makes this a rejection, never
            # NULL substitution or filtering. TOP also bounds server cardinality.
            size = " + ".join(f"COALESCE(CONVERT(bigint,DATALENGTH([{c.name}])),0)" for c in self._columns)
            names = ", ".join(
                f"CASE WHEN ({size}) <= {self._driver_row_limit} THEN [{c.name}] ELSE NULL END AS [{c.name}]"
                for c in self._columns
            )
            cursor.execute(
                f"SELECT TOP ({self._limits.max_rows + 1}) ({size}) AS __dpone_source_bytes, {names} "
                f"FROM [{self._ident('database')}].[{self._ident('schema')}].[{self._ident('name')}] WITH (TABLOCK,HOLDLOCK);"
            )
            rows = tuple(bounded_clickhouse_rows(self._fetch(cursor), self._columns, self._limits))
            self._require_schema(cursor)
            if self._source_identity(connection) != self.source_identity_original:
                raise CompositionAdmissionError("clickhouse_source_identity_changed")
            if self._require_transaction(cursor) != transaction_id:
                raise CompositionAdmissionError("clickhouse_source_transaction_changed")
            connection.commit()
        except CompositionAdmissionError:
            rollback(connection)
            raise
        except Exception:
            rollback(connection)
            raise CompositionAdmissionError("clickhouse_source_payload") from None
        finally:
            close(cursor)
            close(connection)
        return self._columns, rows

    @staticmethod
    def _require_transaction(cursor: Any) -> int:
        """Pin the one committable SQL Server transaction started by the driver."""
        cursor.execute("SELECT @@TRANCOUNT, XACT_STATE(), CURRENT_TRANSACTION_ID();")
        rows = cursor.fetchmany(1)
        if len(rows) != 1 or len(rows[0]) != 3 or cursor.fetchmany(1):
            raise CompositionAdmissionError("clickhouse_source_transaction")
        count, state, identity = rows[0]
        if (
            any(type(value) is not int for value in (count, state, identity))
            or (count, state) != (1, 1)
            or identity < 1
        ):
            raise CompositionAdmissionError("clickhouse_source_transaction")
        return int(identity)

    def _source_identity(self, connection: Any) -> bytes | None:
        if self._require_source is None:
            return None
        original = self._require_source(connection)
        if type(original) is not bytes or not 1 <= len(original) <= 16384:
            raise CompositionAdmissionError("clickhouse_source_identity")
        return original

    def _fetch(self, cursor: Any) -> Iterator[tuple[object, ...]]:
        while True:
            batch = cursor.fetchmany(1)
            if not batch:
                return
            if len(batch) != 1 or len(batch[0]) != len(self._columns) + 1:
                raise CompositionAdmissionError("clickhouse_source_payload")
            size, *values = batch[0]
            if type(size) is not int or not 0 <= size <= self._driver_row_limit:
                raise CompositionAdmissionError("clickhouse_source_budget")
            yield tuple(values)

    def _ident(self, field: str) -> str:
        value = self._table.get(field)
        if type(value) is not str or not value or "[" in value or "]" in value or "." in value:
            raise CompositionAdmissionError("clickhouse_source_payload")
        return value

    def _require_schema(self, cursor: Any) -> None:
        cursor.execute(
            f"SELECT TOP ({len(self._columns) + 1 if self._columns else 65}) COLUMN_NAME, DATA_TYPE, IS_NULLABLE, NUMERIC_PRECISION, NUMERIC_SCALE "
            f"FROM [{self._ident('database')}].INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG=? AND TABLE_SCHEMA=? AND TABLE_NAME=? "
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
            for name, kind, nullable, precision, scale in _schema_rows(
                cursor, len(self._columns) if self._columns else 64
            )
        )
        require_snapshot_columns(observed)
        if self._columns and observed != self._columns:
            raise CompositionAdmissionError("clickhouse_source_payload")
        self._columns = observed


def _schema_rows(cursor: Any, count: int) -> Iterator[tuple[Any, ...]]:
    for _ in range(count + 1):
        rows = cursor.fetchmany(1)
        if not rows:
            return
        if len(rows) != 1 or len(rows[0]) != 5:
            raise CompositionAdmissionError("clickhouse_source_payload")
        yield tuple(rows[0])
    raise CompositionAdmissionError("clickhouse_source_payload")


def bounded_clickhouse_rows(
    rows: Iterable[Sequence[object]], columns: tuple[ClickHouseDispatchColumn, ...], limits: SnapshotLimits
) -> Iterator[tuple[object, ...]]:
    """Validate source and exact Native ceilings before retaining each next row.

    Source bytes are the sum of canonical typed JSON row bytes (the values used
    by snapshot_content_sha256), with no dataset envelope. Producers must use
    this same explicit measure for SnapshotGeneration.source_bytes. Wire bytes
    are exact existing Native blocks at the encoder's unchanged 65,536 row size.
    Bounds cover retained values and encoded buffers, not Python object overhead
    or a driver's internal buffers; the concrete reader adds SQL size guards.
    """
    limits.__post_init__()
    require_snapshot_columns(columns)
    source_bytes = wire_bytes = count = 0
    header = len(var_uint(len(columns))) + sum(
        len(encode_clickhouse_string(c.name)) + len(encode_clickhouse_string(c.type_name)) for c in columns
    )
    policy = MssqlClickHouseTypePolicy()
    iterator = iter(rows)
    try:
        for row in iterator:
            if count >= limits.max_rows or isinstance(row, str | bytes) or len(row) != len(columns):
                raise CompositionAdmissionError("clickhouse_source_budget")
            values = tuple(row)
            source_bytes += clickhouse_source_row_bytes(
                values, columns, remaining_bytes=limits.max_source_bytes - source_bytes
            )
            block_rows = count % 65536
            wire_bytes += (
                header + len(var_uint(1))
                if block_rows == 0
                else len(var_uint(block_rows + 1)) - len(var_uint(block_rows))
            )
            for value, column in zip(values, columns, strict=True):
                nullable, inner = unwrap_nullable(column.type_name)
                actual = default_non_null_value(inner) if value is None else value
                preflight_value(
                    actual, inner, column.type_name, policy, limits.max_wire_bytes - wire_bytes - int(nullable)
                )
                wire_bytes += int(nullable) + len(encode_clickhouse_non_null(actual, inner, column.type_name, policy))
                if wire_bytes > limits.max_wire_bytes:
                    raise CompositionAdmissionError("clickhouse_wire_budget")
            count += 1
            yield values
    except CompositionAdmissionError:
        raise
    except Exception:
        raise CompositionAdmissionError("clickhouse_source_payload") from None
    finally:
        close(iterator)


def clickhouse_source_row_bytes(
    row: Sequence[object], columns: tuple[ClickHouseDispatchColumn, ...], *, remaining_bytes: int
) -> int:
    """Producer/reader shared canonical source-byte measure, with a hard ceiling."""
    if (
        type(remaining_bytes) is not int
        or remaining_bytes < 0
        or isinstance(row, str | bytes)
        or len(row) != len(columns)
    ):
        raise CompositionAdmissionError("clickhouse_source_budget")
    if any(isinstance(value, str | bytes) and len(value) > remaining_bytes for value in row):
        raise CompositionAdmissionError("clickhouse_source_budget")
    normalized = [snapshot_scalar(v, c.type_name) for v, c in zip(row, columns, strict=True)]
    size = len(canonical_json_bytes(normalized))
    if size > remaining_bytes:
        raise CompositionAdmissionError("clickhouse_source_budget")
    return size


__all__ = [
    "MssqlClickHouseSourceReader",
    "clickhouse_type_for_mssql",
    "bounded_clickhouse_rows",
    "clickhouse_source_row_bytes",
]
