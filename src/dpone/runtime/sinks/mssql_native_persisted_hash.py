"""Fixed-width evidence for SqlClient persisted native hashes.

The first observation sums writer-produced SHA-256 row hashes under the normal
stage barrier. Later observations use SQL Server's rowversion as a mutation
watermark and never rescan business values or rebuild their canonical bytes.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from dpone.runtime.mssql_native_chunks_files import native_multiset_digest

_BRACKETED_PART = r"\[(?:[^\]\x00-\x1f]|\]\])+\]"
_QUALIFIED_STAGE = re.compile(rf"{_BRACKETED_PART}(?:\.{_BRACKETED_PART}){{1,2}}\Z")
_MAX_EXPECTED_ROWS = (1 << 63) - 2
_WORD_BASE = 1 << 32


@dataclass(frozen=True, slots=True)
class PersistedHashObservation:
    """Independent initial content aggregate and its mutation watermark."""

    rows: int
    typed_digest: str
    typed_sum: int
    mutation_watermark: int


@dataclass(frozen=True, slots=True)
class MutationWatermark:
    """Constant-width repeat observation of one exact stage."""

    rows: int
    mutation_watermark: int


def build_initial_persisted_hash_sql(qualified_stage: str, expected_rows: int) -> str:
    """Compile a bounded aggregate over only hash and rowversion columns."""
    _validate(qualified_stage, expected_rows)
    limbs = ",\n    ".join(
        "COALESCE(SUM(CONVERT(decimal(38,0), "
        f"CONVERT(bigint, 0x00000000 + SUBSTRING([__dpone__native_row_hash], {1 + index * 4}, 4)))), 0) "
        f"AS limb_{index}"
        for index in range(8)
    )
    return (
        "SET NOCOUNT ON;\n"
        f"WITH bounded AS (SELECT TOP ({expected_rows + 1}) [__dpone__native_row_hash], "
        f"[__dpone__mutation_version] FROM {qualified_stage} WITH (TABLOCKX, HOLDLOCK))\n"
        "SELECT COUNT_BIG(*) AS rows, "
        f"CONVERT(bit, CASE WHEN COUNT_BIG(*) > {expected_rows} THEN 1 ELSE 0 END) AS count_overflow, "
        "MAX(CONVERT(binary(8), [__dpone__mutation_version])) AS mutation_watermark,\n    "
        f"{limbs}\nFROM bounded;"
    )


def build_repeat_watermark_sql(qualified_stage: str, expected_rows: int) -> str:
    """Compile the constant-width mutation check used after first verification."""
    _validate(qualified_stage, expected_rows)
    return (
        "SET NOCOUNT ON;\n"
        f"WITH bounded AS (SELECT TOP ({expected_rows + 1}) [__dpone__mutation_version] "
        f"FROM {qualified_stage} WITH (TABLOCKX, HOLDLOCK))\n"
        "SELECT COUNT_BIG(*), MAX(CONVERT(binary(8), [__dpone__mutation_version])) FROM bounded;"
    )


def decode_initial_persisted_hash(row: Any, *, expected_rows: int) -> PersistedHashObservation:
    """Validate the initial 11-field aggregate without driver coercion."""
    return _decode_persisted_hash(row, expected_rows=expected_rows, require_exact_count=True)


def decode_persisted_hash_observation(row: Any, *, expected_rows: int) -> PersistedHashObservation:
    """Decode a bounded recovery observation containing at most the planned rows.

    Recovery must be able to describe an interrupted writer that persisted zero
    or some of its planned rows.  This decoder still rejects overflow and
    malformed aggregate evidence; only the exact-count requirement is deferred
    to the caller that compares the observation with the source receipt.
    """
    return _decode_persisted_hash(row, expected_rows=expected_rows, require_exact_count=False)


def _decode_persisted_hash(row: Any, *, expected_rows: int, require_exact_count: bool) -> PersistedHashObservation:
    values = _values(row, 11)
    rows, overflow, watermark, *limbs = values
    _validate_count(rows, overflow, expected_rows, require_exact=require_exact_count)
    total = 0
    maximum = rows * (_WORD_BASE - 1)
    for limb in limbs:
        if type(limb) is int:
            value = limb
        elif type(limb) is Decimal and limb.is_finite() and limb == limb.to_integral_value():
            value = int(limb)
        else:
            raise ValueError("mssql_native.persisted_hash_limb")
        if not 0 <= value <= maximum:
            raise ValueError("mssql_native.persisted_hash_limb")
        total = ((total << 32) + value) % (1 << 256)
    mutation = _watermark(watermark, rows)
    return PersistedHashObservation(rows, native_multiset_digest(rows, total), total, mutation)


def decode_repeat_watermark(row: Any, *, expected_rows: int) -> MutationWatermark:
    """Decode an exact count and unsigned database-local rowversion."""
    rows, raw = _values(row, 2)
    if type(rows) is not int or rows != expected_rows:
        raise ValueError("mssql_native.persisted_hash_count")
    return MutationWatermark(rows, _watermark(raw, rows))


def _watermark(raw: Any, rows: int) -> int:
    if rows == 0 and raw is None:
        return 0
    if type(raw) is not bytes or len(raw) != 8:
        raise ValueError("mssql_native.persisted_hash_watermark")
    return int.from_bytes(raw, "big")


def _validate_count(rows: Any, overflow: Any, expected_rows: int, *, require_exact: bool) -> None:
    if type(rows) is not int or rows < 0 or rows > expected_rows:
        raise ValueError("mssql_native.persisted_hash_count")
    if type(overflow) not in (bool, int) or overflow not in (0, 1, False, True):
        raise ValueError("mssql_native.persisted_hash_count")
    if bool(overflow) or (require_exact and rows != expected_rows):
        raise ValueError("mssql_native.persisted_hash_count")


def _values(row: Any, length: int) -> tuple[Any, ...]:
    try:
        if isinstance(row, (str, bytes, bytearray)) or len(row) != length:
            raise ValueError
        return tuple(row[index] for index in range(length))
    except (TypeError, IndexError, KeyError, ValueError):
        raise ValueError("mssql_native.persisted_hash_row_shape") from None


def _validate(qualified_stage: str, expected_rows: int) -> None:
    if type(expected_rows) is not int or not 0 <= expected_rows <= _MAX_EXPECTED_ROWS:
        raise ValueError("mssql_native.persisted_hash_expected_rows")
    if type(qualified_stage) is not str or _QUALIFIED_STAGE.fullmatch(qualified_stage) is None:
        raise ValueError("mssql_native.persisted_hash_stage_identifier")
