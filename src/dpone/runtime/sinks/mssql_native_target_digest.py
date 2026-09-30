"""Compile bounded MSSQL native-row hashes and decode their aggregate evidence.

The compiler admits only the first target-local raw layout. It reproduces the
existing native encoder's field framing and returns aggregate values only;
the caller retains responsibility for stage identity and transaction fencing.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from dpone.runtime.mssql_native_chunks_files import native_multiset_digest
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract, validate_mssql_native_contract
from dpone.runtime.sinks.mssql_native_target_local_layout import TARGET_LOCAL_LAYOUT_MATRIX_V1

if TYPE_CHECKING:
    from dpone.runtime.native_wire_models import NativeWireColumnLayout, SourceNativeWireContract

_WORD_BASE = 1 << 32
_MAX_EXPECTED_ROWS = (1 << 63) - 2
_PAYLOAD_CHUNK_WIDTH = 4
_PREPARED_VARCHAR_TYPES = frozenset({"varchar(26)", "varchar(32)", "varchar(64)"})
_FRAMEWORK_LAYOUTS: dict[str, tuple[str, bool]] = {
    "__dpone__run_id": ("varchar(26)", False),
    "__dpone__load_id": ("varchar(26)", False),
    "__dpone__loaded_at": ("datetime2(7)", False),
    "__dpone__row_id": ("varchar(64)", True),
    "__dpone__extracted_at": ("datetime2(7)", False),
    "__dpone__parent_row_id": ("varchar(64)", True),
    "__dpone__root_row_id": ("varchar(64)", True),
    "__dpone__list_index": ("int", True),
    "__dpone__op": ("varchar(32)", True),
    "__dpone__meta": ("nvarchar(max)", True),
    "__dpone__row_hash": ("varchar(64)", False),
}
_BRACKETED_PART = r"\[(?:[^\]\x00-\x1f]|\]\])+\]"
_QUALIFIED_STAGE = re.compile(rf"{_BRACKETED_PART}(?:\.{_BRACKETED_PART}){{1,2}}\Z")


@dataclass(frozen=True, slots=True)
class TargetDigest:
    """Bounded row count and the existing versioned native multiset evidence."""

    rows: int
    typed_digest: str
    typed_sum: int


@dataclass(frozen=True, slots=True)
class PreparedTargetDigests:
    """Business and full prepared-stage evidence from one bounded observation."""

    business: TargetDigest
    full: TargetDigest


def full_prepared_contract(stage: Any) -> SourceNativeWireContract:
    """Build the exact prepared layout consumed by both digest implementations."""
    return build_mssql_bcp_native_contract(
        schema=tuple(
            (
                name,
                stage.column_types[name] + (" nullable" if stage.target_column_nullability.get(name, True) else ""),
            )
            for name in stage.columns
        ),
        query="prepared-native-stage",
        target_format="mssql_native",
    )


def build_target_digest_sql(qualified_stage: str, contract: SourceNativeWireContract, expected_rows: int) -> str:
    """Build one bounded hash-heap query for the closed four-type raw profile.

    ``qualified_stage`` must already be a two- or three-part bracket-quoted
    identifier. The compiler validates it before interpolation and quotes each
    contract column itself. The returned SELECT has count, overflow, and eight
    decimal limb sums; no business value is projected back to Python.
    """
    _validate_expected_rows(expected_rows)
    _validate_stage(qualified_stage)
    require_target_local_raw_layout(contract)
    fields = _payload_fields(contract.columns, prepared=False)
    return _build_sql(qualified_stage, expected_rows, (("row_hash", fields),))


def require_target_local_raw_layout(contract: SourceNativeWireContract) -> None:
    """Reject unsupported target-local layouts before source or writer I/O."""
    validate_mssql_native_contract(contract)
    if len(contract.columns) > TARGET_LOCAL_LAYOUT_MATRIX_V1.max_business_columns:
        raise ValueError("mssql_native.target_digest_column_count")
    if any(not TARGET_LOCAL_LAYOUT_MATRIX_V1.admits(column) for column in contract.columns):
        raise ValueError("mssql_native.target_digest_unsupported_type")
    _payload_fields(contract.columns, prepared=False)


def build_prepared_target_digest_sql(
    qualified_stage: str,
    business_contract: SourceNativeWireContract,
    full_contract: SourceNativeWireContract,
    expected_rows: int,
    *,
    include_mutation_watermark: bool = False,
) -> str:
    """Compile business and full prepared digests with a finite metadata allowance.

    The business prefix is exactly the raw route's at-most-100-column layout.
    Additional physical columns must match known framework names, types, and
    nullability. A non-NULL unbounded metadata placeholder aborts the SQL batch.
    """
    _validate_expected_rows(expected_rows)
    _validate_stage(qualified_stage)
    validate_mssql_native_contract(business_contract)
    validate_mssql_native_contract(full_contract)
    business_columns = business_contract.columns
    full_columns = full_contract.columns
    if len(business_columns) > 100:
        raise ValueError("mssql_native.target_digest_column_count")
    if len(full_columns) < len(business_columns):
        raise ValueError("mssql_native.target_digest_business_layout")
    for original, prepared_column in zip(business_columns, full_columns, strict=False):
        if (
            prepared_column.name != original.name
            or _base_type(prepared_column) != _base_type(original)
            or any(
                getattr(prepared_column, field) != getattr(original, field)
                for field in ("storage_type", "fixed_length", "encoding", "precision", "scale")
            )
        ):
            raise ValueError("mssql_native.target_digest_business_layout")
    for column in full_columns[len(business_columns) :]:
        expected = _FRAMEWORK_LAYOUTS.get(column.name)
        if expected != (_base_type(column), column.nullable):
            raise ValueError("mssql_native.target_digest_metadata_layout")
    null_metadata = tuple(
        column.name for column in full_columns[len(business_columns) :] if column.name == "__dpone__meta"
    )
    business_fields = _payload_fields(business_columns, prepared=False)
    full_fields = _payload_fields(full_columns, prepared=True, null_sentinels=frozenset(null_metadata))
    return _build_sql(
        qualified_stage,
        expected_rows,
        (("business_hash", business_fields), ("full_hash", full_fields)),
        null_metadata=null_metadata,
        mutation_watermark=include_mutation_watermark,
    )


def _build_sql(
    qualified_stage: str,
    expected_rows: int,
    hashes: tuple[tuple[str, tuple[str, ...]], ...],
    *,
    null_metadata: tuple[str, ...] = (),
    mutation_watermark: bool = False,
) -> str:
    # Each APPLY owns a shallow expression while the final hash concatenates
    # only aliases. SQL Server materializes fixed-size hashes, never row payloads.
    chunks = tuple((name, _payload_chunks(fields)) for name, fields in hashes)
    selections = [
        "HASHBYTES('SHA2_256', "
        + " + ".join(
            ["CONVERT(varbinary(max), 0x)", *(f"{name}_chunk_{index}.payload" for index in range(len(values)))]
        )
        + f") AS {name}"
        for name, values in chunks
    ]
    if null_metadata:
        invalid = " OR ".join(f"s.{_quote_column(name)} IS NOT NULL" for name in null_metadata)
        selections.append(f"CONVERT(bit, CASE WHEN {invalid} THEN 1 ELSE 0 END) AS invalid_metadata")
    if mutation_watermark:
        selections.append("CONVERT(binary(8), s.[__dpone__mutation_version]) AS mutation_watermark")
    projected = ",\n    ".join(selections)
    applies = "\n".join(
        f"CROSS APPLY (VALUES ({chunk})) AS {name}_chunk_{index}(payload)"
        for name, values in chunks
        for index, chunk in enumerate(values)
    )
    guard = (
        "IF EXISTS (SELECT 1 FROM #dpone_target_hashes WHERE invalid_metadata = 1)\n"
        "BEGIN THROW 51000, 'mssql_native.unbounded_metadata_changed', 1; END;\n"
        if null_metadata
        else ""
    )
    limbs = ",\n    ".join(
        f"COALESCE(SUM(CONVERT(decimal(38,0), ({_word_sql(index, name)}))), CONVERT(decimal(38,0), 0)) "
        f"AS {name}_limb_{index}"
        for name, _fields in hashes
        for index in range(8)
    )
    return (
        "SET NOCOUNT ON;\n"
        "DROP TABLE IF EXISTS #dpone_target_hashes;\n"
        f"SELECT TOP ({expected_rows + 1}) {projected}\n"
        f"INTO #dpone_target_hashes FROM {qualified_stage} AS s WITH (TABLOCKX, HOLDLOCK)\n"
        f"{applies};\n"
        f"{guard}"
        "SELECT COUNT_BIG(*) AS rows, "
        f"CONVERT(bit, CASE WHEN COUNT_BIG(*) > {expected_rows} THEN 1 ELSE 0 END) AS count_overflow,\n    "
        + ("MAX(mutation_watermark) AS mutation_watermark,\n    " if mutation_watermark else "")
        + f"{limbs}\nFROM #dpone_target_hashes;\n"
        "DROP TABLE #dpone_target_hashes;"
    )


def _payload_fields(
    columns: tuple[NativeWireColumnLayout, ...], *, prepared: bool, null_sentinels: frozenset[str] = frozenset()
) -> tuple[str, ...]:
    # NULL-only framework values are checked separately in the same bounded
    # scan. Hashing their actual value first would allow unbounded target work.
    return tuple(
        "CONVERT(varbinary(max), 0xFFFFFFFFFFFFFFFF)"
        if column.name in null_sentinels
        else _field_sql(column, prepared=prepared)
        for column in columns
    )


def _payload_chunks(fields: tuple[str, ...]) -> tuple[str, ...]:
    """Split canonical bytes without changing their left-to-right order."""
    return tuple(
        " + ".join(["CONVERT(varbinary(max), 0x)", *fields[offset : offset + _PAYLOAD_CHUNK_WIDTH]])
        for offset in range(0, len(fields), _PAYLOAD_CHUNK_WIDTH)
    )


def _base_type(column: NativeWireColumnLayout) -> str:
    return column.source_type.strip().lower().removesuffix(" nullable")


def decode_prepared_target_digest_row(row: Any, *, expected_rows: int) -> PreparedTargetDigests:
    """Decode the count, overflow, business limbs, and full limbs (18 fields)."""
    values = _row_values(row, 18)
    head = values[:2]
    return PreparedTargetDigests(
        decode_target_digest_row((*head, *values[2:10]), expected_rows=expected_rows),
        decode_target_digest_row((*head, *values[10:18]), expected_rows=expected_rows),
    )


def decode_prepared_target_digest_with_watermark(row: Any, *, expected_rows: int) -> tuple[PreparedTargetDigests, int]:
    """Decode prepared digests plus their same-scan rowversion watermark."""
    values = _row_values(row, 19)
    watermark = values[2]
    if expected_rows == 0 and watermark is None:
        mutation = 0
    elif type(watermark) is bytes and len(watermark) == 8:
        mutation = int.from_bytes(watermark, "big")
    else:
        raise ValueError("mssql_native.persisted_hash_watermark")
    digest_values = (*values[:2], *values[3:])
    return decode_prepared_target_digest_row(digest_values, expected_rows=expected_rows), mutation


def decode_target_digest_row(row: Any, *, expected_rows: int) -> TargetDigest:
    """Validate the ten-column SQL result and fold its big-endian 32-bit limbs.

    A count beyond the expected bound is an overflow observation, never a
    successful larger count. Driver coercion, NULL aggregate fields, and
    fractional or negative sums fail closed.
    """
    _validate_expected_rows(expected_rows)
    values = _row_values(row, 10)
    rows, overflow, *limbs = values
    if type(rows) is not int or rows < 0 or rows > expected_rows + 1 or overflow not in (0, 1, False, True):
        raise ValueError("mssql_native.target_digest_row_values")
    if type(overflow) not in (bool, int) or (rows > expected_rows) != bool(overflow):
        raise ValueError("mssql_native.target_digest_count_overflow")
    if overflow:
        raise ValueError("mssql_native.target_digest_count_overflow")
    total = 0
    maximum_limb_sum = rows * (_WORD_BASE - 1)
    for limb in limbs:
        if type(limb) is int:
            word = limb
        elif type(limb) is Decimal and limb.is_finite() and limb == limb.to_integral_value():
            word = int(limb)
        else:
            raise ValueError("mssql_native.target_digest_limb")
        if not 0 <= word <= maximum_limb_sum:
            raise ValueError("mssql_native.target_digest_limb")
        total = ((total << 32) + word) % (1 << 256)
    return TargetDigest(rows, native_multiset_digest(rows, total), total)


def _row_values(row: Any, length: int) -> tuple[Any, ...]:
    try:
        if isinstance(row, (str, bytes, bytearray)) or len(row) != length:
            raise ValueError
        return tuple(row[index] for index in range(length))
    except (TypeError, IndexError, KeyError, ValueError):
        raise ValueError("mssql_native.target_digest_row_shape") from None


def _validate_expected_rows(expected_rows: int) -> None:
    if type(expected_rows) is not int or not 0 <= expected_rows <= _MAX_EXPECTED_ROWS:
        raise ValueError("mssql_native.target_digest_expected_rows")


def _validate_stage(qualified_stage: str) -> None:
    if not isinstance(qualified_stage, str) or _QUALIFIED_STAGE.fullmatch(qualified_stage) is None:
        raise ValueError("mssql_native.target_digest_stage_identifier")
    for part in re.findall(_BRACKETED_PART, qualified_stage):
        if len(part[1:-1].replace("]]", "]")) > 128:
            raise ValueError("mssql_native.target_digest_stage_identifier")


def _quote_column(name: str) -> str:
    if not isinstance(name, str) or not name or len(name) > 128 or any(ord(char) < 32 for char in name):
        raise ValueError("mssql_native.target_digest_column_identifier")
    return "[" + name.replace("]", "]]") + "]"


def _little_endian_sql(binary_expression: str, width: int) -> str:
    """Reverse an integer's big-endian binary cast without character coercion."""
    return " + ".join(
        f"CONVERT(varbinary(max), SUBSTRING({binary_expression}, {offset}, 1))" for offset in range(width, 0, -1)
    )


def _field_sql(column: NativeWireColumnLayout, *, prepared: bool) -> str:
    value = f"s.{_quote_column(column.name)}"
    dtype = _base_type(column)
    if dtype == "bigint" and column.storage_type == "bigint" and column.fixed_length == 8:
        encoded = _little_endian_sql(f"CONVERT(binary(8), {value})", 8)
    elif prepared and dtype == "int" and column.storage_type == "int" and column.fixed_length == 4:
        encoded = _little_endian_sql(f"CONVERT(binary(4), {value})", 4)
    elif dtype == "float(53)" and column.storage_type == "float" and column.fixed_length == 8:
        encoded = _little_endian_sql(f"CONVERT(binary(8), {value})", 8)
    elif dtype == "nvarchar(max)" and column.storage_type == "nvarchar" and column.prefix_width == 8:
        encoded = f"CONVERT(varbinary(max), {value})"
    elif (
        prepared and dtype in _PREPARED_VARCHAR_TYPES and column.storage_type == "varchar" and column.prefix_width == 2
    ):
        # UTF-8 bytes remain comparable after a non-ASCII stage mutation.
        encoded = f"CONVERT(varbinary(max), {value} COLLATE Latin1_General_100_BIN2_UTF8)"
    elif (
        (dtype == "datetime2(6)" or (prepared and dtype == "datetime2(7)"))
        and column.storage_type == "datetime2"
        and column.fixed_length == 8
    ):
        # SQL Server's binary cast prepends a precision byte to the native 8-byte payload.
        encoded = f"CONVERT(varbinary(max), SUBSTRING(CONVERT(varbinary(9), CONVERT(datetime2(7), {value})), 2, 8))"
    else:
        raise ValueError("mssql_native.target_digest_unsupported_type")
    if column.prefix_width == 0:
        return encoded
    if column.storage_type == "nvarchar":
        present = _little_endian_sql(f"CONVERT(binary(8), CONVERT(bigint, DATALENGTH({value})))", 8) + f" + {encoded}"
        missing = "0xFFFFFFFFFFFFFFFF"
    elif column.storage_type == "varchar":
        present = _little_endian_sql(
            f"CONVERT(binary(2), CONVERT(smallint, DATALENGTH({value} COLLATE Latin1_General_100_BIN2_UTF8)))",
            2,
        )
        present += f" + {encoded}"
        missing = "0xFFFF"
    else:
        present = f"CONVERT(varbinary(max), 0x{column.fixed_length:02X}) + {encoded}"
        missing = "0xFF"
    return f"(CASE WHEN {value} IS NULL THEN CONVERT(varbinary(max), {missing}) ELSE {present} END)"


def _word_sql(index: int, hash_column: str) -> str:
    first = index * 4 + 1
    terms = (
        f"CONVERT(bigint, CONVERT(tinyint, SUBSTRING({hash_column}, {first + offset}, 1))) * {256 ** (3 - offset)}"
        for offset in range(4)
    )
    return " + ".join(terms)
