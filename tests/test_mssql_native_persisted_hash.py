"""Persisted native hashes retain content proof and bound repeat verification."""

from decimal import Decimal

import pytest

from dpone.runtime.sinks.mssql_native_persisted_hash import (
    build_initial_persisted_hash_sql,
    build_repeat_watermark_sql,
    decode_initial_persisted_hash,
    decode_persisted_hash_observation,
    decode_repeat_watermark,
)


def test_initial_hash_query_reads_only_technical_fixed_width_columns():
    sql = build_initial_persisted_hash_sql("[db].[dbo].[stage]", 10)

    assert "__dpone__native_row_hash" in sql
    assert "__dpone__mutation_version" in sql
    assert "SELECT *" not in sql
    assert "TOP (11)" in sql
    assert "TABLOCKX, HOLDLOCK" in sql


def test_repeat_query_is_constant_width_and_barrier_held():
    sql = build_repeat_watermark_sql("[dbo].[stage]", 10)

    assert "COUNT_BIG(*)" in sql
    assert "MAX(CONVERT(binary(8), [__dpone__mutation_version]))" in sql
    assert "__dpone__native_row_hash" not in sql
    assert "TABLOCKX, HOLDLOCK" in sql


def test_initial_hash_decoder_returns_sum_and_unsigned_watermark():
    row = (2, False, bytes.fromhex("0000000000000007"), *(Decimal(i) for i in range(1, 9)))

    observed = decode_initial_persisted_hash(row, expected_rows=2)

    assert observed.rows == 2
    assert observed.mutation_watermark == 7
    assert observed.typed_sum == sum(i << (32 * (7 - index)) for index, i in enumerate(range(1, 9)))


@pytest.mark.parametrize(
    ("rows", "watermark"),
    [(0, None), (1, bytes.fromhex("0000000000000007"))],
)
def test_recovery_hash_decoder_accepts_bounded_partial_observation(rows, watermark):
    observed = decode_persisted_hash_observation(
        (rows, False, watermark, *([Decimal(0)] * 8)),
        expected_rows=2,
    )

    assert observed.rows == rows
    assert observed.mutation_watermark == (0 if watermark is None else 7)


def test_initial_hash_decoder_keeps_exact_count_contract():
    with pytest.raises(ValueError, match="mssql_native.persisted_hash_count"):
        decode_initial_persisted_hash((1, False, b"\0" * 8, *([0] * 8)), expected_rows=2)


@pytest.mark.parametrize(
    "row",
    [
        (3, False, b"\0" * 8, *([0] * 8)),
        (2, True, b"\0" * 8, *([0] * 8)),
    ],
)
def test_recovery_hash_decoder_rejects_overflow(row):
    with pytest.raises(ValueError, match="mssql_native.persisted_hash_count"):
        decode_persisted_hash_observation(row, expected_rows=2)


@pytest.mark.parametrize(
    "row",
    [
        (2, bytes.fromhex("0000000000000007")),
        (1, bytes.fromhex("0000000000000008")),
    ],
)
def test_repeat_watermark_decoder_preserves_exact_observation(row):
    observed = decode_repeat_watermark(row, expected_rows=row[0])
    assert (observed.rows, observed.mutation_watermark) == (row[0], int.from_bytes(row[1], "big"))


def test_watermark_decoder_rejects_empty_watermark_for_nonempty_stage():
    with pytest.raises(ValueError, match="mssql_native.persisted_hash_watermark"):
        decode_repeat_watermark((1, None), expected_rows=1)
