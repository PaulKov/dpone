"""One prepared readback retains both legacy native digest authorities."""

from collections.abc import Iterator
from dataclasses import FrozenInstanceError
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID

import pytest

from dpone.runtime.mssql_native_encoder import MssqlNativeEncoder
from dpone.runtime.native_wire_mssql import _MssqlDateTime2, _MssqlTime, build_mssql_bcp_native_contract
from dpone.runtime.sinks.mssql_native_prepare import MssqlNativeStagePreparer
from dpone.runtime.sinks.mssql_native_prepared_digests import PreparedDigests, digest_prepared_rows


def wire(schema):
    return build_mssql_bcp_native_contract(schema=schema, query="prepared-native-stage", target_format="mssql_native")


class OneShot(Iterator):
    """A second iterator acquisition fails, even after exhaustion."""

    def __init__(self, rows):
        self.rows = iter(rows)
        self.iterations = 0
        self.reads = 0

    def __iter__(self):
        self.iterations += 1
        assert self.iterations == 1, "prepared rows were scanned twice"
        return self

    def __next__(self):
        row = next(self.rows)
        self.reads += 1
        return row


def legacy(rows, business, full, limit):
    """Exercise the existing preparer's actual two-scan implementation."""

    def read(sql):
        names = sql.split(" FROM ")[0].removeprefix("SELECT ").split(", ")
        return iter({name: row[name] for name in names} for row in rows)

    strategy = SimpleNamespace(
        connector=SimpleNamespace(quote_identifier=lambda name: name, get_records_iterator=read),
        _staging_name=lambda stage: "prepared",
    )
    stage = SimpleNamespace(
        columns=[column.name for column in full.columns],
        column_types={column.name: column.source_type for column in full.columns},
        target_column_nullability={column.name: column.nullable for column in full.columns},
        row_count=len(rows),
    )
    context = SimpleNamespace(wire_contract=business, max_row_bytes=limit)
    return PreparedDigests(
        MssqlNativeStagePreparer._stage_digest(strategy, stage, context),
        MssqlNativeStagePreparer._stage_digest(strategy, stage, context, all_columns=True),
        len(rows),
    )


@pytest.mark.parametrize("count", [0, 1, 3])
def test_one_iterator_two_encodings_per_row_and_golden_multiset(count, monkeypatch):
    contract = wire((("n", "int"),))
    source = OneShot([{"n": 7}] * count)
    calls = []
    original = MssqlNativeEncoder.encode_row

    def encode(self, row):
        calls.append(self)
        return original(self, row)

    monkeypatch.setattr(MssqlNativeEncoder, "encode_row", encode)
    result = digest_prepared_rows(
        source, business_contract=contract, full_contract=contract, max_row_bytes=4, expected_rows=count
    )
    # Frozen legacy JSON-envelope SHA-256 values for the native int32 bytes
    # 07 00 00 00. Include multiplicity and the empty envelope.
    golden = {
        0: "c6e78d3d47035008350c6c45819524ec91fdb3c0822cd9d0ebc0df247d36c0a7",
        1: "f671ff73e1e57bfc9890e299b6b7f4adc9164163265d170aaa6e9bcef0644a14",
        3: "9d48267a0b459391737bd86bf539d4d19ef8ef4eaa3b3d6598cc9d6d862e4b5e",
    }[count]
    assert result == PreparedDigests(golden, golden, count)
    assert source.iterations == 1 and source.reads == count
    assert len(calls) == 2 * count
    if count:
        assert len({id(encoder) for encoder in calls}) == 2


@pytest.mark.parametrize(
    ("dtype", "value"),
    [
        ("int", -(2**31)),
        ("bigint", 2**63 - 1),
        ("nvarchar(40)", "O'Brien 雪 😀"),
        ("nvarchar(40)", ""),
        ("nvarchar(40)", "NULL\x00"),
        ("varbinary(40)", b"\x00\xff'\x00"),
        ("varbinary(40)", b""),
        ("decimal(38,9)", Decimal("-12345678901234567890123456789.123456789")),
        ("date", date(1, 1, 1)),
        ("time(7)", _MssqlTime(time(23, 59, 59, 999999), submicrosecond_100ns=9)),
        ("datetime2(7)", _MssqlDateTime2(datetime(9999, 12, 31, 23, 59, 59, 999999), submicrosecond_100ns=9)),
        ("datetimeoffset(7)", datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=14)))),
        ("uniqueidentifier", UUID("00112233-4455-6677-8899-aabbccddeeff")),
        ("float", 0.125),
    ],
)
def test_parity_with_legacy_preserves_typed_values_nulls_duplicates_and_order(dtype, value):
    business = wire((("v", dtype + " nullable"),))
    full = wire(
        (("v", dtype + " nullable"), ("__dpone__run_id", "nvarchar(26)"), ("__dpone__meta", "nvarchar(max) nullable"))
    )
    rows = [dict(v=value, __dpone__run_id="run'雪", __dpone__meta=None)] * 2
    rows += [dict(v=None, __dpone__run_id="run'雪", __dpone__meta=None)]
    expected = legacy(rows, business, full, 128)
    for ordered in (rows, list(reversed(rows))):
        assert (
            digest_prepared_rows(
                OneShot(ordered), business_contract=business, full_contract=full, max_row_bytes=128, expected_rows=3
            )
            == expected
        )


def test_finite_allowance_keeps_full_business_budget_and_nullable_framing():
    business = wire((("n", "int"),))
    full = wire((("n", "int nullable"), ("__dpone__load_id", "varchar(26)")))
    rows = [{"n": 7, "__dpone__load_id": "x" * 26}]
    result = digest_prepared_rows(
        OneShot(rows), business_contract=business, full_contract=full, max_row_bytes=4, expected_rows=1
    )
    assert result == legacy(rows, business, full, 4)
    assert result.business_digest != result.full_digest
    with pytest.raises(ValueError, match="mssql_native_row_bytes_exceeded"):
        digest_prepared_rows(
            iter(rows), business_contract=business, full_contract=full, max_row_bytes=3, expected_rows=1
        )


@pytest.mark.parametrize("expected", [0, 2])
def test_count_mismatch_retains_classification(expected):
    contract = wire((("n", "int"),))
    with pytest.raises(ValueError, match="mssql_native.prepared_count_mismatch"):
        digest_prepared_rows(
            iter([{"n": 1}]),
            business_contract=contract,
            full_contract=contract,
            max_row_bytes=4,
            expected_rows=expected,
        )


@pytest.mark.parametrize(
    ("row", "code"),
    [
        ({"n": 1, "__dpone__meta": "tampered"}, "mssql_native.unbounded_metadata_changed"),
        ({"n": None, "__dpone__meta": None}, "mssql_native_unexpected_null"),
        ({"n": 2**31, "__dpone__meta": None}, "mssql_native_invalid_value"),
        ({"n": 1, "__dpone__meta": None, "extra": 1}, "mssql_native_columns_mismatch"),
        ({"__dpone__meta": None}, "mssql_native_columns_mismatch"),
        ({"n": 1}, "mssql_native_columns_mismatch"),
        ((1, None), "mssql_native_columns_mismatch"),
    ],
)
def test_invalid_rows_fail_closed(row, code):
    business = wire((("n", "int"),))
    full = wire((("n", "int nullable"), ("__dpone__meta", "nvarchar(max) nullable")))
    with pytest.raises(ValueError, match=code):
        digest_prepared_rows(
            iter([row]), business_contract=business, full_contract=full, max_row_bytes=4, expected_rows=1
        )


@pytest.mark.parametrize(
    ("schema", "code"),
    [
        ((("other", "int"),), "verification_column_mismatch"),
        ((("n", "bigint"),), "verification_type_mismatch"),
        ((("n", "int"), ("unowned", "int")), "unowned_verification_column"),
    ],
)
def test_incompatible_contracts_fail_before_readback(schema, code):
    source = OneShot([])
    with pytest.raises(ValueError, match=code):
        digest_prepared_rows(
            source,
            business_contract=wire((("n", "int"),)),
            full_contract=wire(schema),
            max_row_bytes=4,
            expected_rows=0,
        )
    assert source.iterations == 0


def test_iterator_failure_is_not_reclassified_or_retried():
    def broken():
        yield {"n": 1}
        raise RuntimeError("readback failed")

    contract = wire((("n", "int"),))
    with pytest.raises(RuntimeError, match="readback failed"):
        digest_prepared_rows(
            broken(), business_contract=contract, full_contract=contract, max_row_bytes=4, expected_rows=1
        )


def test_reused_mapping_is_encoded_before_next_pull_and_result_is_frozen():
    contract = wire((("n", "int"),))

    def reused():
        row = {"n": 0}
        for value in (1, 2, 1):
            row["n"] = value
            yield row

    result = digest_prepared_rows(
        reused(), business_contract=contract, full_contract=contract, max_row_bytes=4, expected_rows=3
    )
    assert result == legacy([{"n": 1}, {"n": 2}, {"n": 1}], contract, contract, 4)
    with pytest.raises(FrozenInstanceError):
        result.rows = 10


def test_full_mapping_order_is_irrelevant_and_finite_metadata_overflow_fails():
    business = wire((("n", "int"),))
    full = wire((("n", "int"), ("__dpone__run_id", "varchar(26)")))
    rows = [{"__dpone__run_id": "x" * 26, "n": 1}]
    assert digest_prepared_rows(
        iter(rows), business_contract=business, full_contract=full, max_row_bytes=4, expected_rows=1
    ) == legacy(rows, business, full, 4)
    with pytest.raises(ValueError, match="mssql_native_field_length_exceeded"):
        digest_prepared_rows(
            iter([{"n": 1, "__dpone__run_id": "x" * 27}]),
            business_contract=business,
            full_contract=full,
            max_row_bytes=100,
            expected_rows=1,
        )


@pytest.mark.parametrize("limit", [0, -1, True])
def test_invalid_business_byte_limit_fails_before_iteration(limit):
    source = OneShot([])
    contract = wire((("n", "int"),))
    with pytest.raises(ValueError, match="mssql_native_invalid_max_row_bytes"):
        digest_prepared_rows(
            source, business_contract=contract, full_contract=contract, max_row_bytes=limit, expected_rows=0
        )
    assert source.iterations == 0
