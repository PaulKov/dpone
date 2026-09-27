"""Hermetic contract tests for the bounded SQL Server native digest kernel."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from dpone.runtime.mssql_native_chunks_files import native_multiset_digest
from dpone.runtime.mssql_native_encoder import MssqlNativeEncoder
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.sinks.mssql_native_target_digest import (
    build_prepared_target_digest_sql,
    build_target_digest_sql,
    decode_prepared_target_digest_row,
    decode_target_digest_row,
)


def _contract(*columns: tuple[str, str]):
    return build_mssql_bcp_native_contract(schema=columns, query="SELECT 1")


@pytest.mark.parametrize(
    ("dtype", "payload_fragment"),
    [
        ("bigint", "SUBSTRING(CONVERT(binary(8), s.[v]), 8, 1)"),
        ("float(53)", "CONVERT(binary(8), s.[v])"),
        ("nvarchar(max)", "CONVERT(varbinary(max), s.[v])"),
        ("datetime2(6)", "SUBSTRING(CONVERT(varbinary(9), CONVERT(datetime2(7), s.[v])), 2, 8)"),
    ],
)
def test_compiler_encodes_only_admitted_native_types(dtype: str, payload_fragment: str) -> None:
    sql = build_target_digest_sql("[db].[dbo].[stage]", _contract(("v", dtype)), 7)

    assert payload_fragment in sql
    assert "HASHBYTES('SHA2_256'" in sql
    assert sql.count("HASHBYTES('SHA2_256'") == 1
    assert "TOP (8)" in sql
    assert "FROM [db].[dbo].[stage] AS s" in sql
    assert "INTO #dpone_target_hashes" in sql
    assert "COUNT_BIG(*)" in sql
    assert sql.count("SUM(CONVERT(decimal(38,0)") == 8
    assert "SELECT *" not in sql.upper()
    assert "REVERSE(" not in sql


def test_nullable_framing_keeps_missing_and_present_values_distinct() -> None:
    sql = build_target_digest_sql(
        "[dbo].[stage]", _contract(("n", "nvarchar(max) nullable"), ("i", "bigint nullable")), 1
    )

    assert "0xFFFFFFFFFFFFFFFF" in sql
    assert "0xFF" in sql
    assert "DATALENGTH" in sql
    assert "s.[n] IS NULL" in sql
    assert "s.[i] IS NULL" in sql
    assert "SUBSTRING(CONVERT(binary(8)" in sql
    assert "THEN CONVERT(varbinary(max), 0xFF)" in sql
    assert "THEN CONVERT(varbinary(max), 0xFFFFFFFFFFFFFFFF)" in sql


def test_float_payload_reverses_sql_binary_cast_to_native_little_endian() -> None:
    sql = build_target_digest_sql("[dbo].[stage]", _contract(("v", "float(53)")), 1)

    offsets = [sql.index(f"SUBSTRING(CONVERT(binary(8), s.[v]), {offset}, 1)") for offset in range(8, 0, -1)]
    assert offsets == sorted(offsets)


def test_native_vectors_pin_high_bit_negative_nul_and_multibyte_prefix() -> None:
    contract = _contract(
        ("negative", "bigint nullable"),
        ("nul", "nvarchar(max) nullable"),
        ("long", "nvarchar(max)"),
    )
    encoded = MssqlNativeEncoder(contract, max_row_bytes=2048).encode_row((-129, "A\x00B", "é" * 128))
    assert encoded[:9] == bytes.fromhex("08 7f ff ff ff ff ff ff ff")
    assert encoded[9:23] == bytes.fromhex("06 00 00 00 00 00 00 00 41 00 00 00 42 00")
    assert encoded[23:31] == bytes.fromhex("00 01 00 00 00 00 00 00")

    sql = build_target_digest_sql("[dbo].[stage]", contract, 1)
    assert "REVERSE(" not in sql
    for offset in range(8, 0, -1):
        assert f"SUBSTRING(CONVERT(binary(8), s.[negative]), {offset}, 1)" in sql
    for offset in range(8, 0, -1):
        assert f"SUBSTRING(CONVERT(binary(8), CONVERT(bigint, DATALENGTH(s.[long]))), {offset}, 1)" in sql
    assert "CONVERT(varbinary(max), s.[nul])" in sql


def test_compiler_accepts_100_columns_but_rejects_101_and_other_types() -> None:
    hundred = _contract(*((f"c{i}", "bigint") for i in range(100)))
    sql = build_target_digest_sql("[dbo].[stage]", hundred, 0)
    assert "s.[c99]" in sql
    assert "TOP (1)" in sql

    too_wide = _contract(*((f"c{i}", "bigint") for i in range(101)))
    with pytest.raises(ValueError, match="column_count"):
        build_target_digest_sql("[dbo].[stage]", too_wide, 0)
    with pytest.raises(ValueError, match="unsupported_type"):
        build_target_digest_sql("[dbo].[stage]", _contract(("x", "int")), 0)


def test_wide100_composes_short_fragments_without_materializing_business_payload() -> None:
    contract = _contract(*((f"c{i}", "bigint") for i in range(100)))

    sql = build_target_digest_sql("[dbo].[stage]", contract, 4)

    assert sql.count("FROM [dbo].[stage] AS s WITH (TABLOCKX, HOLDLOCK)") == 1
    assert "SELECT TOP (5)" in sql
    assert "INTO #dpone_target_hashes" in sql
    assert "INTO #dpone_target_fields" not in sql
    assert "CROSS APPLY (VALUES" in sql
    assert "AS row_hash_chunk_0(payload)" in sql
    assert "HASHBYTES('SHA2_256', CONVERT(varbinary(max), 0x) + row_hash_chunk_0.payload" in sql
    fragments = [line for line in sql.splitlines() if line.startswith("CROSS APPLY")]
    assert len(fragments) > 1
    assert max(map(len, fragments)) < 8_000


_FRAMEWORK_COLUMNS = (
    ("__dpone__run_id", "varchar(26)"),
    ("__dpone__load_id", "varchar(26)"),
    ("__dpone__loaded_at", "datetime2(7)"),
    ("__dpone__row_id", "varchar(64) nullable"),
    ("__dpone__extracted_at", "datetime2(7)"),
    ("__dpone__parent_row_id", "varchar(64) nullable"),
    ("__dpone__root_row_id", "varchar(64) nullable"),
    ("__dpone__list_index", "int nullable"),
    ("__dpone__op", "varchar(32) nullable"),
    ("__dpone__meta", "nvarchar(max) nullable"),
)


def test_prepared_compiler_accepts_wide100_plus_real_framework_and_returns_18_aggregates() -> None:
    business_columns = tuple((f"c{i}", "bigint") for i in range(100))
    business = _contract(*business_columns)
    full = _contract(*business_columns, *_FRAMEWORK_COLUMNS)

    sql = build_prepared_target_digest_sql("[dbo].[stage]", business, full, 5)

    assert "TOP (6)" in sql
    assert "s.[c99]" in sql
    assert "s.[__dpone__meta]" in sql
    assert sql.count("HASHBYTES('SHA2_256'") == 2
    assert sql.count("SUM(CONVERT(decimal(38,0)") == 16
    assert "AS business_hash" in sql
    assert "AS full_hash" in sql
    assert "IF EXISTS" in sql and "invalid_metadata" in sql and "THROW" in sql
    assert "REVERSE(" not in sql
    assert sql.count("INTO #dpone_target_hashes") == 1
    assert "SELECT *" not in sql.upper()


def test_prepared_compiler_uses_binary_utf8_bytes_and_prefix_for_framework_varchar() -> None:
    business = _contract(("v", "bigint"))
    full = _contract(("v", "bigint nullable"), ("__dpone__op", "varchar(32) nullable"))

    sql = build_prepared_target_digest_sql("[dbo].[stage]", business, full, 1)
    assert "CONVERT(varbinary(max), s.[__dpone__op] COLLATE Latin1_General_100_BIN2_UTF8)" in sql
    assert "DATALENGTH(s.[__dpone__op] COLLATE Latin1_General_100_BIN2_UTF8)" in sql
    assert "SUBSTRING(CONVERT(binary(2)" in sql
    assert "THEN CONVERT(varbinary(max), 0xFFFF)" in sql


def test_prepared_unbounded_metadata_uses_null_sentinel_without_hashing_mutated_value() -> None:
    business = _contract(("v", "bigint"))
    full = _contract(("v", "bigint"), ("__dpone__meta", "nvarchar(max) nullable"))

    sql = build_prepared_target_digest_sql("[dbo].[stage]", business, full, 2)
    assert "CONVERT(varbinary(max), 0xFFFFFFFFFFFFFFFF))) AS full_hash_chunk_0(payload)" in sql
    assert sql.count("s.[__dpone__meta]") == 1
    assert "s.[__dpone__meta] IS NOT NULL" in sql
    assert "IF EXISTS (SELECT 1 FROM #dpone_target_hashes WHERE invalid_metadata = 1)" in sql
    assert "THROW 51000" in sql
    assert sql.count("FROM [dbo].[stage] AS s") == 1
    assert sql.count("SUM(CONVERT(decimal(38,0)") == 16


@pytest.mark.parametrize(
    "metadata",
    [
        (("__dpone__unknown", "int"),),
        (("__dpone__run_id", "varchar(32)"),),
        (("__dpone__run_id", "varchar(26) nullable"),),
        (("unowned", "varchar(26)"),),
    ],
)
def test_prepared_compiler_rejects_unknown_or_mutated_framework_layout(metadata: tuple[tuple[str, str], ...]) -> None:
    business = _contract(("v", "bigint"))
    full = _contract(("v", "bigint"), *metadata)
    with pytest.raises(ValueError, match="metadata_layout"):
        build_prepared_target_digest_sql("[dbo].[stage]", business, full, 1)


def test_prepared_compiler_rejects_101_business_columns_and_prefix_drift() -> None:
    business_columns = tuple((f"c{i}", "bigint") for i in range(101))
    with pytest.raises(ValueError, match="column_count"):
        build_prepared_target_digest_sql(
            "[dbo].[stage]", _contract(*business_columns), _contract(*business_columns, *_FRAMEWORK_COLUMNS), 1
        )
    with pytest.raises(ValueError, match="business_layout"):
        build_prepared_target_digest_sql(
            "[dbo].[stage]", _contract(("v", "bigint")), _contract(("different", "bigint")), 1
        )
    with pytest.raises(ValueError, match="business_layout"):
        build_prepared_target_digest_sql(
            "[dbo].[stage]", _contract(("v", "datetime2(6)")), _contract(("v", "datetime2(7)")), 1
        )


def test_prepared_decoder_returns_business_and_full_digest_from_18_fields() -> None:
    row = (2, 0, *([Decimal(0)] * 7), Decimal(2), *([Decimal(0)] * 7), Decimal(3))
    result = decode_prepared_target_digest_row(row, expected_rows=2)
    assert result.business.rows == result.full.rows == 2
    assert result.business.typed_sum == 2
    assert result.full.typed_sum == 3
    assert result.business.typed_digest == native_multiset_digest(2, 2)
    assert result.full.typed_digest == native_multiset_digest(2, 3)
    with pytest.raises(ValueError, match="row_shape"):
        decode_prepared_target_digest_row(row[:-1], expected_rows=2)
    with pytest.raises(ValueError, match="count_overflow"):
        decode_prepared_target_digest_row((3, 1, *row[2:]), expected_rows=2)


@pytest.mark.parametrize("stage", ["dbo.stage", "[dbo].[stage];DROP TABLE x", "[dbo].[stage].[extra].[part]"])
def test_compiler_rejects_untrusted_stage_names(stage: str) -> None:
    with pytest.raises(ValueError, match="stage_identifier"):
        build_target_digest_sql(stage, _contract(("v", "bigint")), 1)


@pytest.mark.parametrize("expected", [-1, True, 2**63 - 1])
def test_compiler_rejects_invalid_count_bound(expected: int) -> None:
    with pytest.raises(ValueError, match="expected_rows"):
        build_target_digest_sql("[dbo].[stage]", _contract(("v", "bigint")), expected)


def test_decoder_propagates_limb_carry_into_existing_digest_envelope() -> None:
    row = (2, 0, *([Decimal(0)] * 6), Decimal(1), Decimal(2**32 + 1))
    result = decode_target_digest_row(row, expected_rows=2)
    expected_sum = (2 << 32) + 1

    assert result.rows == 2
    assert result.typed_sum == expected_sum
    assert result.typed_digest == native_multiset_digest(2, expected_sum)
    with pytest.raises(FrozenInstanceError):
        result.rows = 1  # type: ignore[misc]


def test_decoder_rejects_count_overflow_sentinel() -> None:
    with pytest.raises(ValueError, match="count_overflow"):
        decode_target_digest_row((3, 1, *([Decimal(0)] * 8)), expected_rows=2)
    with pytest.raises(ValueError, match="count_overflow"):
        decode_target_digest_row((3, 0, *([Decimal(0)] * 8)), expected_rows=2)


@pytest.mark.parametrize(
    "row",
    [
        None,
        (0, 0),
        (True, 0, *([Decimal(0)] * 8)),
        (-1, 0, *([Decimal(0)] * 8)),
        (0, None, *([Decimal(0)] * 8)),
        (0, 0, *([Decimal(0)] * 7), Decimal("0.5")),
        (0, 0, *([Decimal(0)] * 7), -1),
        (0, 0, *([Decimal(0)] * 7), float("nan")),
    ],
)
def test_decoder_rejects_malformed_driver_rows(row: object) -> None:
    with pytest.raises(ValueError, match="target_digest"):
        decode_target_digest_row(row, expected_rows=1)
