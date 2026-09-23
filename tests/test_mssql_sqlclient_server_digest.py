"""Exact target-local SQLClient typed-digest aggregation."""

from dataclasses import replace
from hashlib import sha256

import pytest

from dpone.adapters.mssql_sqlclient_server_digest import (
    build_server_digest_sql,
    decode_server_digest_row,
)
from dpone.contracts.mssql_native_chunks import TdsInputReceipt
from dpone.contracts.mssql_sqlclient_input import SqlClientFileIdentity, SqlClientInputDescriptor
from dpone.contracts.mssql_tds_create import TdsCreateObservedColumn, TdsCreateType
from dpone.contracts.native_wire_layout import NativeWireColumnLayout
from dpone.runtime.mssql_native_chunks_files import native_multiset_digest
from tests.test_mssql_sqlclient_writer_settlement_adapter import _inputs


def _word_sums(payloads: tuple[bytes, ...]) -> tuple[int, ...]:
    hashes = tuple(sha256(payload).digest() for payload in payloads)
    return tuple(
        sum(int.from_bytes(value[offset : offset + 4], "big") for value in hashes) for offset in range(0, 32, 4)
    )


class _DriverRow:
    """Minimal pyodbc.Row shape, which is not a Sequence ABC."""

    def __init__(self, values: tuple[object, ...]) -> None:
        self._values = values

    def __len__(self) -> int:
        return len(self._values)

    def __iter__(self):
        return iter(self._values)


def _all_type_inputs():
    _, _, _, _, stage, _, _ = _inputs()
    profiles = (
        ("i", "bigint", "Int64", "bigint", 8, None, None, None, TdsCreateType.BIGINT, (8, 19, 0, None)),
        ("f", "float(53)", "Float64", "float", 8, 53, None, None, TdsCreateType.FLOAT53, (8, 53, 0, None)),
        (
            "s",
            "nvarchar(max)",
            "String",
            "nvarchar",
            None,
            None,
            None,
            "utf-16le",
            TdsCreateType.NVARCHARMAX,
            (-1, 0, 0, "Latin1_General_100_BIN2"),
        ),
        (
            "d",
            "datetime2(6)",
            "DateTime64(6)",
            "datetime2",
            8,
            None,
            7,
            None,
            TdsCreateType.DATETIME2_6,
            (8, 26, 6, None),
        ),
    )
    columns, observed = [], []
    for ordinal, profile in enumerate(profiles, 1):
        name, declared, target, storage, fixed, precision, scale, encoding, kind, physical = profile
        columns.append(
            NativeWireColumnLayout(
                name,
                declared,
                target,
                False,
                storage,
                8 if storage == "nvarchar" else 0,
                fixed,
                precision,
                scale,
                encoding,
            )
        )
        max_length, sql_precision, sql_scale, collation = physical
        observed.append(
            TdsCreateObservedColumn(ordinal, name, kind, False, max_length, sql_precision, sql_scale, collation)
        )
    descriptor = SqlClientInputDescriptor(
        1,
        3,
        tuple(columns),
        TdsInputReceipt(1, 1, "0" * 64),
        1 << 20,
        SqlClientFileIdentity(1, 1, 1, 1, 1),
    )
    return replace(stage, columns=tuple(observed)), descriptor


def test_server_digest_reconstructs_existing_multiset_contract() -> None:
    payloads = (
        (7).to_bytes(8, "little", signed=True),
        (-3).to_bytes(8, "little", signed=True),
        (7).to_bytes(8, "little", signed=True),
    )
    expected_sum = sum(int.from_bytes(sha256(payload).digest(), "big") for payload in payloads) % (1 << 256)

    count, digest, typed_sum = decode_server_digest_row(
        (len(payloads), len(payloads), *_word_sums(payloads)),
        expected_rows=len(payloads),
        finalize_digest=native_multiset_digest,
    )

    assert count == len(payloads)
    assert typed_sum == expected_sum
    assert digest == native_multiset_digest(count, expected_sum)


def test_server_digest_accepts_bounded_driver_row_shape() -> None:
    payload = (7).to_bytes(8, "little", signed=True)
    words = _word_sums((payload,))

    count, digest, typed_sum = decode_server_digest_row(
        _DriverRow((1, 1, *words)),
        expected_rows=1,
        finalize_digest=native_multiset_digest,
    )

    expected_sum = int.from_bytes(sha256(payload).digest(), "big")
    assert (count, digest, typed_sum) == (1, native_multiset_digest(1, expected_sum), expected_sum)


def test_server_digest_discards_only_the_256_bit_carry() -> None:
    maximum_word = (1 << 32) - 1
    row = (2, 2, *(maximum_word * 2 for _ in range(8)))

    count, digest, typed_sum = decode_server_digest_row(
        row,
        expected_rows=2,
        finalize_digest=native_multiset_digest,
    )

    expected = (int.from_bytes(b"\xff" * 32, "big") * 2) % (1 << 256)
    assert (count, typed_sum, digest) == (2, expected, native_multiset_digest(2, expected))


def test_server_digest_rejects_malformed_or_wrong_count_rows() -> None:
    good = (1, 1, *_word_sums(((1).to_bytes(8, "little", signed=True),)))
    for row in (
        None,
        good[:-1],
        (True, *good[1:]),
        (1, 0, *good[2:]),
        (1, 1, *good[2:-1], -1),
        (2, 2, *good[2:]),
    ):
        with pytest.raises(ValueError, match="sqlclient_server_digest_invalid"):
            decode_server_digest_row(row, expected_rows=1, finalize_digest=native_multiset_digest)


def test_server_digest_sql_is_bounded_target_local_and_covers_all_admitted_types() -> None:
    stage, descriptor = _all_type_inputs()

    sql = build_server_digest_sql(stage, descriptor)

    assert sql.count("HASHBYTES('SHA2_256'") == 1
    assert "CREATE TABLE #dpone_digest" in sql
    assert "INSERT INTO #dpone_digest WITH (TABLOCK)" in sql
    assert "SELECT TOP (2) HASHBYTES" in sql
    assert "DROP TABLE #dpone_digest" in sql
    assert "COUNT_BIG(*)" in sql
    assert sql.count("SUM(CONVERT(decimal(38, 0), CONVERT(bigint, SUBSTRING([row_hash]") == 8
    assert "WITH (HOLDLOCK, TABLOCK)" in sql
    assert "SELECT [value] FROM" not in sql
    assert "OPTION (MAXDOP" not in sql
    assert "DATALENGTH([s])" in sql
    assert "DATEDIFF_BIG(MICROSECOND" in sql
    assert "CONVERT(binary(8), [f])" in sql
    assert "CONVERT(binary(8), [i])" in sql
    assert len(sql.encode("utf-8")) < 131_072


def test_server_digest_sql_preserves_nullable_native_framing() -> None:
    stage, descriptor = _all_type_inputs()
    columns = tuple(
        replace(
            column,
            source_type=column.source_type + " nullable",
            target_type=f"Nullable({column.target_type})",
            nullable=True,
            prefix_width=8 if column.storage_type == "nvarchar" else 1,
        )
        for column in descriptor.columns
    )
    descriptor = replace(descriptor, columns=columns)
    stage = replace(stage, columns=tuple(replace(column, nullable=True) for column in stage.columns))

    sql = build_server_digest_sql(stage, descriptor)

    assert "CASE WHEN [i] IS NULL THEN 0xff" in sql
    assert "CASE WHEN [f] IS NULL THEN 0xff" in sql
    assert "CASE WHEN [s] IS NULL THEN 0xffffffffffffffff" in sql
    assert "CASE WHEN [d] IS NULL THEN 0xff" in sql


def test_server_digest_sql_admits_maximum_wide_datetime_layout() -> None:
    stage, descriptor = _all_type_inputs()
    physical = descriptor.columns[-1]
    observed = stage.columns[-1]
    names = tuple(f"d{ordinal:03d}_" + "x" * 123 for ordinal in range(100))
    descriptor = replace(
        descriptor,
        columns=tuple(replace(physical, name=name) for name in names),
    )
    stage = replace(
        stage,
        columns=tuple(replace(observed, ordinal=ordinal + 1, name=name) for ordinal, name in enumerate(names)),
    )

    sql = build_server_digest_sql(stage, descriptor)

    assert sql.count("DATEDIFF_BIG(MICROSECOND") >= 100
    assert len(sql.encode("utf-8")) <= 4 * 1024 * 1024
