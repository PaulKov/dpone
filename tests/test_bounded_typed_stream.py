"""Synthetic boundaries for bounded RowBinary encoding."""

import pytest

from dpone.runtime.clickhouse_rowbinary import ClickHouseRowBinaryEncoder


def encoder(dtype="String", **kwargs):
    return ClickHouseRowBinaryEncoder([("v", dtype)], schema_kind="clickhouse", **kwargs)


def test_byte_bound_preserves_rows():
    assert list(encoder(max_batch_bytes=6).iter_batches([("aa",)] * 5)) == [b"\x02aa" * 2, b"\x02aa" * 2, b"\x02aa"]


def test_oversized_row():
    with pytest.raises(ValueError, match="row_bytes"):
        list(encoder(max_batch_bytes=3).iter_batches([("abc",)]))


@pytest.mark.parametrize("row", [{}, {"v": None, "extra": 1}, (), (1, 2), "ab"])
def test_malformed_rows(row):
    with pytest.raises(ValueError, match="row_shape"):
        list(encoder("Nullable(String)").iter_batches([row]))


@pytest.mark.parametrize("value", ["1.234", "1000.00", "NaN", "Infinity"])
def test_decimal_rejects_loss(value):
    with pytest.raises(ValueError):
        list(encoder("Decimal(5,2)").iter_batches([(value,)]))


def test_negative_fractional_timestamp():
    from datetime import UTC, datetime

    assert list(encoder("DateTime64(6)").iter_batches([(datetime(1969, 12, 31, 23, 59, 59, 500000, UTC),)])) == [
        (-500000).to_bytes(8, "little", signed=True)
    ]


@pytest.mark.parametrize("bits", [8, 16, 32, 64])
@pytest.mark.parametrize("signed", [True, False])
def test_integer_boundaries(bits, signed):
    import struct

    dtype = f"{'Int' if signed else 'UInt'}{bits}"
    low = -(1 << (bits - 1)) if signed else 0
    high = (1 << (bits - int(signed))) - 1
    for value in (low, high):
        assert b"".join(encoder(dtype, max_batch_bytes=8).iter_batches([(value,)])) == value.to_bytes(
            bits // 8, "little", signed=signed
        )
    for value in (low - 1, high + 1):
        with pytest.raises(struct.error):
            list(encoder(dtype).iter_batches([(value,)]))


@pytest.mark.parametrize("value", ["", "NULL", "\\N", "é", "😀"])
def test_utf8_and_literal_null(value):
    raw = value.encode()
    assert list(encoder("Nullable(String)", max_batch_bytes=len(raw) + 2).iter_batches([(value,)])) == [
        b"\x00" + bytes([len(raw)]) + raw
    ]
    assert list(encoder("Nullable(String)", max_batch_bytes=1).iter_batches([(None,)])) == [b"\x01"]


@pytest.mark.parametrize("limit", [0, -1, True, "1", 1.2])
def test_bad_byte_limits(limit):
    with pytest.raises(ValueError, match="positive_integer"):
        encoder(max_batch_bytes=limit)


def test_no_oversized_string_copy(monkeypatch):
    import dpone.runtime.clickhouse_rowbinary as module

    def forbidden(*args):
        raise AssertionError("oversized value reached allocator")

    monkeypatch.setattr(module, "encode_clickhouse_value", forbidden)
    with pytest.raises(ValueError, match="row_bytes"):
        list(encoder(max_batch_bytes=8).iter_batches([("😀" * 100000,)]))


def test_fixedstring_padding_is_bounded():
    with pytest.raises(ValueError, match="row_bytes"):
        list(encoder("FixedString(1000000000)", max_batch_bytes=8).iter_batches([("",)]))


def test_slow_consumer_does_not_pull_unbounded_rows():
    seen = []

    def rows():
        for index in range(1000):
            seen.append(index)
            yield ("a",)

    chunks = encoder(max_batch_bytes=4).iter_batches(rows())
    assert next(chunks) == b"\x01a\x01a"
    assert len(seen) == 3  # At most one pending row beyond a full batch.
    chunks.close()
    assert len(seen) == 3


@pytest.mark.parametrize("factory", ["row", "array"])
def test_artifact_limits_and_cancellation(factory):
    from types import SimpleNamespace

    from dpone.runtime.sources.strategies.mssql.mssql_odbc_array_artifacts import build_odbc_array_rowbinary_artifact
    from dpone.runtime.sources.strategies.mssql.mssql_row_stream_artifacts import build_typed_binary_row_stream_artifact

    closed = []

    class Connector:
        def get_records_streaming(self, query, **kwargs):
            try:
                while True:
                    yield [{"v": "a"}] * 3
            finally:
                closed.append(True)

    config = SimpleNamespace(
        batch_size=3,
        options={
            "sink_type": "clickhouse",
            "native_transfer": {"wire": {"mode": "typed_binary", "block_bytes": "4B", "block_rows": 1}},
        },
    )
    arguments = dict(connector=Connector(), load_config=config, query="select synthetic", schema=[("v", "varchar(10)")])
    if factory == "row":
        artifact = build_typed_binary_row_stream_artifact(
            **arguments, sink_connector=type("ClickHouseConnector", (), {})()
        )
    else:
        artifact = build_odbc_array_rowbinary_artifact(**arguments, provider_id="synthetic", bulk_wire_contract=None)
    chunks = artifact.iter_bytes()
    assert len(next(chunks)) <= 4
    chunks.close()
    assert closed == [True]


@pytest.mark.parametrize("value", ["999.99", "-999.99", "0", "1.2300"])
def test_decimal_exact_edges(value):
    from decimal import Decimal

    result = b"".join(encoder("Decimal(5,2)", max_batch_bytes=4).iter_batches([(Decimal(value),)]))
    assert int.from_bytes(result, "little", signed=True) == int(Decimal(value) * 100)


@pytest.mark.parametrize("days", [0, 65535])
def test_date_edges(days):
    from datetime import date, timedelta

    value = date(1970, 1, 1) + timedelta(days=days)
    assert list(encoder("Date", max_batch_bytes=2).iter_batches([(value,)])) == [days.to_bytes(2, "little")]


@pytest.mark.parametrize("days", [-1, 65536])
def test_date_overflow(days):
    import struct
    from datetime import date, timedelta

    with pytest.raises(struct.error):
        list(encoder("Date").iter_batches([(date(1970, 1, 1) + timedelta(days=days),)]))


@pytest.mark.parametrize("scale", [0, 3, 6, 7, 9])
def test_timestamp_precision(scale):
    from datetime import UTC, datetime

    value = datetime(2020, 1, 1, 0, 0, 0, 123456, UTC)
    actual = int.from_bytes(b"".join(encoder(f"DateTime64({scale})").iter_batches([(value,)])), "little", signed=True)
    assert actual == 1577836800 * 10**scale + 123456 * 10**scale // 1000000


def test_empty_stream_and_row_limit():
    assert list(encoder(max_batch_bytes=4).iter_batches([])) == []
    assert list(encoder(chunk_rows=1).iter_batches([("a",), ("b",)])) == [b"\x01a", b"\x01b"]


@pytest.mark.parametrize(
    "options",
    [
        {"block_rows": 0},
        {"block_rows": True},
        {"block_bytes": "0"},
        {"block_bytes": "1-2MB"},
        {"block_bytes": True},
        {"block_bytes": "1e100MB"},
    ],
)
def test_option_validation(options):
    from dpone.runtime.typed_stream_limits import TypedStreamLimits

    with pytest.raises(ValueError):
        TypedStreamLimits.from_options({"native_transfer": {"wire": options}}, 10)


@pytest.mark.parametrize("bits,precision", [(32, 9), (64, 18), (128, 38), (256, 76)])
def test_decimal_alias_has_declared_width_and_scale(bits, precision):
    from decimal import Decimal

    value = Decimal("1.25")
    assert list(encoder(f"Decimal{bits}(2)").iter_batches([(value,)])) == [
        (125).to_bytes(bits // 8, "little", signed=True)
    ]
    with pytest.raises(ValueError, match="precision"):
        list(encoder(f"Decimal{bits}(2)").iter_batches([(Decimal(f"1e{precision - 2}"),)]))


@pytest.mark.parametrize(
    "dtype", ["Decimal", "Decimal32", "Decimal32(2,3)", "Decimal512(2)", "Decimal(5)", "Decimal32(10)"]
)
def test_invalid_decimal_declaration_fails(dtype):
    with pytest.raises(ValueError):
        list(encoder(dtype).iter_batches([(1,)]))


def test_bounded_integer_rejects_fractional_decimal():
    from decimal import Decimal

    with pytest.raises(ValueError, match="precision"):
        list(encoder("Int32", max_batch_bytes=4).iter_batches([(Decimal("1.9"),)]))
    assert list(encoder("Int32").iter_batches([(Decimal("1.9"),)])) == [b"\x01\x00\x00\x00"]


def test_bounded_timestamp_rejects_fractional_loss():
    from datetime import UTC, datetime

    value = datetime(2026, 1, 1, microsecond=3333, tzinfo=UTC)
    with pytest.raises(ValueError, match="precision"):
        list(encoder("DateTime64(3)", max_batch_bytes=8).iter_batches([(value,)]))
    assert len(b"".join(encoder("DateTime64(3)").iter_batches([(value,)]))) == 8


@pytest.mark.parametrize("seconds", [0, 4294967295])
def test_bounded_datetime_uint32_edges(seconds):
    from datetime import UTC, datetime, timedelta

    value = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(seconds=seconds)
    assert list(encoder("DateTime", max_batch_bytes=4).iter_batches([(value,)])) == [seconds.to_bytes(4, "little")]


@pytest.mark.parametrize("seconds", [-1, 4294967296])
def test_bounded_datetime_uint32_overflow(seconds):
    import struct
    from datetime import UTC, datetime, timedelta

    value = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(seconds=seconds)
    with pytest.raises(struct.error):
        list(encoder("DateTime", max_batch_bytes=4).iter_batches([(value,)]))


@pytest.mark.parametrize("dtype", ["Date", "Date32", "DateTime", "DateTime64(0)", "DateTime64(3)"])
def test_bounded_temporal_precision_loss(dtype):
    from datetime import UTC, datetime

    with pytest.raises(ValueError, match="precision"):
        list(encoder(dtype, max_batch_bytes=8).iter_batches([(datetime(2026, 1, 1, microsecond=3333, tzinfo=UTC),)]))


@pytest.mark.parametrize("dtype", ["DateTime64(10)", "DateTime64(-1)", "DateTime64"])
def test_bounded_temporal_invalid_scale(dtype):
    from datetime import UTC, datetime

    with pytest.raises(ValueError, match="precision"):
        list(encoder(dtype, max_batch_bytes=8).iter_batches([(datetime(2026, 1, 1, tzinfo=UTC),)]))


def test_bounded_datetime64_nanosecond_overflow():
    import struct
    from datetime import UTC, datetime

    with pytest.raises(struct.error):
        list(encoder("DateTime64(9)", max_batch_bytes=8).iter_batches([(datetime(2300, 1, 1, tzinfo=UTC),)]))


def test_bounded_timestamp_exact_milliseconds():
    from datetime import UTC, datetime

    value = datetime(1970, 1, 1, microsecond=3000, tzinfo=UTC)
    assert list(encoder("DateTime64( 3)", max_batch_bytes=8).iter_batches([(value,)])) == [
        (3).to_bytes(8, "little", signed=True)
    ]


def test_bounded_timestamp_text_cannot_hide_submicroseconds():
    with pytest.raises(ValueError, match="precision"):
        list(encoder("DateTime64(7)", max_batch_bytes=8).iter_batches([("2026-01-01T00:00:00.1234567Z",)]))


@pytest.mark.parametrize("bits", [32, 64, 128, 256])
def test_native_decimal_alias_has_exact_wire_width(bits):
    from decimal import Decimal

    from dpone.runtime.clickhouse_native import ClickHouseNativeEncoder

    dtype = f"Decimal{bits}(2)"
    native = ClickHouseNativeEncoder([("v", dtype)], schema_kind="clickhouse")
    expected = (
        b"\x01\x01\x01v" + bytes([len(dtype)]) + dtype.encode() + (125).to_bytes(bits // 8, "little", signed=True)
    )
    assert list(native.iter_batches([(Decimal("1.25"),)])) == [expected]
