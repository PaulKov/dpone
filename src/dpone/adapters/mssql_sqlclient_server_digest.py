"""Exact SQL Server-side aggregation for the native typed multiset digest.

The query reproduces the admitted native row bytes inside SQL Server, hashes
each row with SHA-256, and returns only a row count plus eight 32-bit word sums.
Python folds carries modulo 2**256 and applies the existing digest envelope.
No business value crosses the management connection.
"""

from collections.abc import Callable
from decimal import Decimal
from typing import Any

ERROR = "mssql_native.sqlclient_server_digest_invalid"
_WORD_BITS = 32
_WORD_MASK = (1 << _WORD_BITS) - 1
_WORD_COUNT = 8
_MAX_SQL_BYTES = 4 * 1024 * 1024


def _quoted(value: str) -> str:
    """Quote an identifier already admitted by both enclosing contracts."""
    return "[" + value.replace("]", "]]") + "]"


def _little_endian_integer(expression: str, width: int) -> str:
    """Project the low ``width`` bytes of one signed integer as little endian."""
    binary = f"CONVERT(binary(8), CONVERT(bigint, {expression}))"
    return "(" + " + ".join(f"SUBSTRING({binary}, {position}, 1)" for position in range(8, 8 - width, -1)) + ")"


def _nullable(column: str, payload: str, *, prefix_width: int, fixed_length: int | None) -> str:
    if prefix_width == 0:
        return payload
    missing = "0x" + "ff" * prefix_width
    if fixed_length is None:
        present = _little_endian_integer(f"DATALENGTH({column})", prefix_width) + " + " + payload
    else:
        present = "0x" + fixed_length.to_bytes(prefix_width, "little", signed=True).hex() + " + " + payload
    return f"(CASE WHEN {column} IS NULL THEN {missing} ELSE {present} END)"


def _column_bytes(column: Any) -> str:
    name = _quoted(column.name)
    if column.storage_type == "nvarchar":
        payload = f"CONVERT(varbinary(max), {name})"
    elif column.storage_type in {"bigint", "float"}:
        raw = f"CONVERT(binary(8), {name})"
        payload = "(" + " + ".join(f"SUBSTRING({raw}, {position}, 1)" for position in range(8, 0, -1)) + ")"
    elif column.storage_type == "datetime2":
        midnight = f"CONVERT(datetime2(6), CONVERT(date, {name}))"
        ticks = f"DATEDIFF_BIG(MICROSECOND, {midnight}, {name}) * CONVERT(bigint, 10)"
        days = f"DATEDIFF(DAY, CONVERT(date, '00010101', 112), CONVERT(date, {name}))"
        payload = _little_endian_integer(ticks, 5) + " + " + _little_endian_integer(days, 3)
    else:  # The descriptor validator is closed, but keep this branch explicit.
        raise ValueError(ERROR)
    return _nullable(name, payload, prefix_width=column.prefix_width, fixed_length=column.fixed_length)


def build_server_digest_sql(stage: Any, descriptor: Any) -> str:
    """Build one identifier-safe target-local digest batch.

    The temporary heap is an intentional optimizer barrier.  Without it SQL
    Server may expand the row-hash expression independently for each of the
    eight aggregate words and calculate SHA-256 eight times per business row.
    The heap stores only fixed-width hashes, never business values, and is
    released before the settlement transaction ends.
    """
    try:
        # The settlement boundary checks the exact contract classes before this
        # internal helper is called.  Re-run both closed contract validators so
        # no malformed or subsequently replaced value can reach SQL rendering.
        stage.__post_init__()
        descriptor.__post_init__()
        expected_types = {
            "bigint": "bigint",
            "float": "float53",
            "nvarchar": "nvarcharmax",
            "datetime2": "datetime2_6",
        }
        if len(stage.columns) != len(descriptor.columns) or any(
            observed.name != declared.name
            or observed.nullable is not declared.nullable
            or observed.type.value != expected_types.get(declared.storage_type)
            for observed, declared in zip(stage.columns, descriptor.columns, strict=True)
        ):
            raise ValueError
        target = _quoted(stage.schema_name) + "." + _quoted(stage.table_name)
        row_limit = min(descriptor.expected.rows + 1, 2**63 - 1)
        native_row = "CAST(0x AS varbinary(max)) + " + " + ".join(
            _column_bytes(column) for column in descriptor.columns
        )
        sums = ", ".join(
            "COALESCE(SUM(CONVERT(decimal(38, 0), CONVERT(bigint, "
            f"SUBSTRING([row_hash], {offset}, 4)))), CONVERT(decimal(38, 0), 0))"
            f" AS [word_{ordinal}]"
            for ordinal, offset in enumerate(range(1, 33, 4))
        )
        sql = (
            "SET NOCOUNT ON; "
            "CREATE TABLE #dpone_digest ([row_hash] binary(32) NOT NULL); "
            "INSERT INTO #dpone_digest WITH (TABLOCK) ([row_hash]) "
            f"SELECT TOP ({row_limit}) HASHBYTES('SHA2_256', "
            + native_row
            + ") FROM "
            + target
            + " WITH (HOLDLOCK, TABLOCK); "
            "SELECT COUNT_BIG(*) AS [row_count], COUNT_BIG([row_hash]) AS [hash_count], "
            + sums
            + " FROM #dpone_digest; "
            "DROP TABLE #dpone_digest;"
        )
        if len(sql.encode("utf-8")) > _MAX_SQL_BYTES:
            raise ValueError
        return sql
    except (ValueError, TypeError, OverflowError, AttributeError):
        raise ValueError(ERROR) from None


def _integer(value: Any) -> int:
    if type(value) is int:
        return value
    if type(value) is Decimal and value.is_finite() and value == value.to_integral_value():
        return int(value)
    raise ValueError(ERROR)


def decode_server_digest_row(
    row: Any,
    *,
    expected_rows: int,
    finalize_digest: Callable[[int, int], str],
) -> tuple[int, str, int]:
    """Fold eight exact word sums into the existing 256-bit digest contract."""
    try:
        if type(expected_rows) is not int or not 0 <= expected_rows <= 2**63 - 1 or not callable(finalize_digest):
            raise ValueError
        if row is None or isinstance(row, (str, bytes, bytearray)) or len(row) != 2 + _WORD_COUNT:
            raise ValueError
        values = tuple(row)
        if len(values) != 2 + _WORD_COUNT:
            raise ValueError
        count, hash_count, *raw_words = (_integer(value) for value in values)
        if count != expected_rows or hash_count != count:
            raise ValueError
        words = [_integer(value) for value in raw_words]
        if any(value < 0 or value > _WORD_MASK * count for value in words):
            raise ValueError
        carry = 0
        folded = [0] * _WORD_COUNT
        for index in range(_WORD_COUNT - 1, -1, -1):
            value = words[index] + carry
            folded[index] = value & _WORD_MASK
            carry = value >> _WORD_BITS
        total = 0
        for value in folded:
            total = (total << _WORD_BITS) | value
        digest = finalize_digest(count, total)
        if type(digest) is not str or len(digest) != 64:
            raise ValueError
        return count, digest, total
    except (ValueError, TypeError, OverflowError):
        raise ValueError(ERROR) from None


__all__ = ("build_server_digest_sql", "decode_server_digest_row")
