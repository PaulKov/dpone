"""Binary-safe source adaptation is explicit and closes the acquired iterator."""

import pytest

from dpone.runtime.sinks.mssql_native_source_values import native_source_rows


def contract():
    from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract

    return build_mssql_bcp_native_contract(
        schema=(("text", "nvarchar(max) nullable"), ("raw", "varbinary(max) nullable")), query="test"
    )


def test_unicode_decode_does_not_change_binary_or_literal_null_markers():
    source = iter([(b"NULL", b"\xff\x00"), ("\u0410".encode(), b""), (None, None)])
    assert list(native_source_rows(source, contract(), 1024)) == [("NULL", b"\xff\x00"), ("\u0410", b""), (None, None)]


def test_invalid_utf8_fails_and_closes_source():
    events = []

    def source():
        try:
            yield (b"\xff", b"")
        finally:
            events.append("closed")

    with pytest.raises(ValueError, match="invalid_utf8"):
        list(native_source_rows(source(), contract(), 1024))
    assert events == ["closed"]


def test_oversized_text_rejected_before_decoding():
    with pytest.raises(ValueError, match="row_bytes_exceeded"):
        list(native_source_rows(iter([(b"abcd", b"")]), contract(), 3))


def test_utf8_cjk_that_fits_native_unicode_limit_is_accepted():
    from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract

    wire = build_mssql_bcp_native_contract(schema=(("text", "nvarchar(max)"),), query="cjk")
    value = "中" * 10
    assert len(value.encode("utf-8")) == 30
    assert list(native_source_rows(iter([(value.encode("utf-8"),)]), wire, 28)) == [(value,)]


def test_utf8_small_source_that_exceeds_native_unicode_limit_is_rejected():
    from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract

    wire = build_mssql_bcp_native_contract(schema=(("text", "nvarchar(max)"),), query="ascii")
    with pytest.raises(ValueError, match="row_bytes_exceeded"):
        list(native_source_rows(iter([(b"a" * 11,)]), wire, 28))
