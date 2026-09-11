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
    """Exercise the preparer's two independent SQL projection readbacks."""

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
    ("dtype", "value", "business_digest", "full_digest"),
    [
        (
            "int",
            -(2**31),
            "cb3b7fb3b569074a4ff3ed2eaa9b0ef23c9c26b96681e3aad22025088c57fb24",
            "c86cf58397744231edfde3b439c743c00f1be364ae232c1fffc05277279164e3",
        ),
        (
            "bigint",
            2**63 - 1,
            "85cc1040f9278a79dd61edd758521e8d4fd41e6ab46afb32c97b28934a40a0aa",
            "d87f298f9a2fbac11dde47f53a923564bb1be58bdcb8de4c94d6f6b1f3e835ed",
        ),
        (
            "nvarchar(40)",
            "O'Brien 雪 😀",
            "0d847956b3ef993ff9878813d6e0ad4a4f14ac3bdd73b0090d38d6b6daaa53aa",
            "11efe97ca76e8b17759a55db467234f1eaefe44c3b1118476cf01ed434c03bfd",
        ),
        (
            "nvarchar(40)",
            "",
            "9e310a1cdd3bc92d0a71877b8cb952c24ea5c8ffbae404f8824da191c37c3ea3",
            "35356fb0f2446dd2f0203e99303c656fd391ca9b6783db9d9487520078748f1b",
        ),
        (
            "nvarchar(40)",
            "NULL\x00",
            "151dac96c67016b14c1684d56e69adb48cfef2d7ca6047917751c17bb47dbdcc",
            "1f8a3834a01e20a5b07ea179734cb59a3163e19219845b54a307b20f85690914",
        ),
        (
            "varbinary(40)",
            b"\x00\xff'\x00",
            "ef1a7cc32e40fc9f2c38e76f648afd42d4894b3b91a3ed14a0c5940f19e16f6e",
            "0b065c928fd6a7731e81be2df8d85b870e3d1cae402fbe22ea028babbd3b9fe9",
        ),
        (
            "varbinary(40)",
            b"",
            "9e310a1cdd3bc92d0a71877b8cb952c24ea5c8ffbae404f8824da191c37c3ea3",
            "35356fb0f2446dd2f0203e99303c656fd391ca9b6783db9d9487520078748f1b",
        ),
        (
            "decimal(38,9)",
            Decimal("-12345678901234567890123456789.123456789"),
            "46befe21e07a6df422177bcfb8d99349e0649368a0ae13b423be04acdd2eeb06",
            "8989898ee97c837b829957ec0bcb73e8bca133008d863dc2aa37298190bd242d",
        ),
        (
            "date",
            date(1, 1, 1),
            "2f6dd79b858a8a0704b661283a5c35ee55feb915c94829be8a0efb1f7cbda13f",
            "74517772a37f80adea6b7539c4ebf9c2b99461c60271c59805b3e834e9b34ae6",
        ),
        (
            "time(7)",
            _MssqlTime(time(23, 59, 59, 999999), submicrosecond_100ns=9),
            "0c21f9b0386e561c2c7c02e7e3d0088c0f40f87740c24da2b422e287ee673838",
            "ab16a2a90e79066ddaeb675ce941f58d5c30aea392c140290a0aad6b9f46de73",
        ),
        (
            "datetime2(7)",
            _MssqlDateTime2(datetime(9999, 12, 31, 23, 59, 59, 999999), submicrosecond_100ns=9),
            "c35da03b7eadf27954db5b717d59d94ac332af7ca30c57093242a5b6169d9f21",
            "2c71c6de0b78fd2cf234069497c90dd224086538b7b532f46ba48e26f19f7a40",
        ),
        (
            "datetimeoffset(7)",
            datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=14))),
            "734831586f4be7e5e7efa22686f37fcb25e1aff8ef3b1db497e99a59a119759b",
            "2c7dcc3ed9619d437a0fe6b1845056975facc26fa58c22f609d4b5f7c9cfd46b",
        ),
        (
            "uniqueidentifier",
            UUID("00112233-4455-6677-8899-aabbccddeeff"),
            "9342ade4be9735212bcb7313c878664ce41dab2ece1bbbad05475b2e3b44b86c",
            "145617c61366a59f0e116c7b42fb669e471da49c02c586673b7ed83b944908e1",
        ),
        (
            "float",
            0.125,
            "20723197bd1dd8169f54571d8ec718dcd7380431d56ffd49dbcc77c2aa671039",
            "0d2697f995db9463b112ec55219c0dea7efe5619e07f0a973bfb1d9c8afdb3dc",
        ),
    ],
)
def test_parity_with_legacy_preserves_typed_values_nulls_duplicates_and_order(
    dtype, value, business_digest, full_digest
):
    business = wire((("v", dtype + " nullable"),))
    full = wire(
        (("v", dtype + " nullable"), ("__dpone__run_id", "nvarchar(26)"), ("__dpone__meta", "nvarchar(max) nullable"))
    )
    rows = [dict(v=value, __dpone__run_id="run'雪", __dpone__meta=None)] * 2
    rows += [dict(v=None, __dpone__run_id="run'雪", __dpone__meta=None)]
    # Literal pre-refactor expectations do not depend on either digest helper.
    expected = PreparedDigests(business_digest, full_digest, 3)
    assert legacy(rows, business, full, 128) == expected
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
    assert result.full_digest == "9be26eddcf06b22493cf3de052483a296c741596c416d797a640bd0c577075c9"
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
    assert result.business_digest == "7a8e7791fded601382e83f1a03fb13b540515cedf1974b82cd7a8e14fc173ab7"
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


@pytest.mark.parametrize("count", [0, 1, 3])
def test_single_projection_encodes_once_per_row_and_opens_readback_once(count, monkeypatch):
    from dpone.runtime.sinks.mssql_native_prepared_digests import digest_prepared_projection

    contract = wire((("n", "int"),))
    source = OneShot([{"n": 7}] * count)
    encodings, reads = [], []
    original = MssqlNativeEncoder.encode_row

    def encode(encoder, row):
        encodings.append(row)
        return original(encoder, row)

    def read():
        reads.append(True)
        return source

    monkeypatch.setattr(MssqlNativeEncoder, "encode_row", encode)
    result = digest_prepared_projection(
        read, contract=contract, business_contract=contract, max_row_bytes=4, expected_rows=count
    )
    expected = {
        0: "c6e78d3d47035008350c6c45819524ec91fdb3c0822cd9d0ebc0df247d36c0a7",
        1: "f671ff73e1e57bfc9890e299b6b7f4adc9164163265d170aaa6e9bcef0644a14",
        3: "9d48267a0b459391737bd86bf539d4d19ef8ef4eaa3b3d6598cc9d6d862e4b5e",
    }[count]
    assert result == expected
    assert len(encodings) == count and reads == [True]
    assert source.iterations == 1 and source.reads == count


@pytest.mark.parametrize("all_columns", [False, True])
def test_stage_digest_rejects_invalid_limit_before_quoting_or_eager_sql(all_columns):
    def forbidden(*args):
        raise AssertionError("SQL construction or iterator request before validation")

    strategy = SimpleNamespace(
        connector=SimpleNamespace(quote_identifier=forbidden, get_records_iterator=forbidden),
        _staging_name=forbidden,
    )
    stage = SimpleNamespace(
        columns=["n"], column_types={"n": "int"}, target_column_nullability={"n": False}, row_count=0
    )
    context = SimpleNamespace(wire_contract=wire((("n", "int"),)), max_row_bytes=0)
    with pytest.raises(ValueError, match="mssql_native_invalid_max_row_bytes"):
        MssqlNativeStagePreparer._stage_digest(strategy, stage, context, all_columns=all_columns)


def test_single_projection_validates_allowance_before_encoder_and_eager_readback():
    from dpone.runtime.sinks.mssql_native_prepared_digests import digest_prepared_projection

    def forbidden():
        raise AssertionError("readback requested before validation")

    with pytest.raises(ValueError, match="mssql_native.verification_column_mismatch"):
        digest_prepared_projection(
            forbidden,
            contract=wire((("other", "int"),)),
            business_contract=wire((("n", "int"),)),
            max_row_bytes=0,
            expected_rows=0,
        )


@pytest.mark.parametrize("expected", [0, 2])
def test_single_projection_count_mismatch_keeps_error(expected):
    from dpone.runtime.sinks.mssql_native_prepared_digests import digest_prepared_projection

    contract = wire((("n", "int"),))
    with pytest.raises(ValueError, match="mssql_native.prepared_count_mismatch"):
        digest_prepared_projection(
            lambda: iter([{"n": 7}]),
            contract=contract,
            business_contract=contract,
            max_row_bytes=4,
            expected_rows=expected,
        )


def test_single_projection_propagates_eager_iterator_failure_without_retry():
    from dpone.runtime.sinks.mssql_native_prepared_digests import digest_prepared_projection

    contract = wire((("n", "int"),))
    failure = RuntimeError("readback failed")
    requests = []

    def read():
        requests.append(True)
        raise failure

    with pytest.raises(RuntimeError) as caught:
        digest_prepared_projection(
            read, contract=contract, business_contract=contract, max_row_bytes=4, expected_rows=0
        )
    assert caught.value is failure and requests == [True]
