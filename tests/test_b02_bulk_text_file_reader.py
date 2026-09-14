"""Independent wire fixtures for the producer-shared bounded logical reader."""

from io import BytesIO

import pytest

from dpone.runtime.connectors.bulk_text_codec import BulkTextCodec
from dpone.runtime.support.bulk_text_file_reader import BulkTextFileReadError, iter_rows


@pytest.mark.parametrize(
    ("wire", "expected"),
    [
        (b"", None),
        (b"\x1dE", ""),
        (b"tab\x1dTtext", "tab\ttext"),
        (b"line\x1dNtext", "line\ntext"),
        (b"carriage\x1dRtext", "carriage\rtext"),
        (b"\x1dP", "\x1d"),
        (b"\x1dU|\x1dS", "\x1f|\x1e"),
        (b"\x1dPE|\x1dPT|\x1dPN", "\x1dE|\x1dT|\x1dN"),
        (b'"quoted"|a""b', '"quoted"|a""b'),
        (b"\\N|\\t|\\n|\\r|\\x1dE", r"\N|\t|\n|\r|\x1dE"),
        (bytes.fromhex("c3 a9 7c 65 cc 81 7c d0 96 7c f0 9f 98 80"), "é|e\u0301|Ж|😀"),
        (bytes.fromhex("1d 50 1d 50 54 1d 54 1d 50 45"), "\x1d\x1dT\t\x1dE"),
        (b"NULL", "NULL"),
        (b" ", " "),
    ],
)
def test_text_values_preserve_exact_source_semantics(wire, expected):
    rows = iter_rows(
        BytesIO(b"17\t" + wire + b"\t-31\n"),
        (("before", "int"), ("value", "nvarchar(max)"), ("after", "int")),
        BulkTextCodec(),
        max_record_bytes=1024,
    )
    assert list(rows) == [("17", expected, "-31")]


@pytest.mark.parametrize("wire,value", [(b"", None), (b"\x1dE", b""), (b"00 FF", b"\x00\xff")])
def test_binary_preserves_producer_hex_whitespace_grammar(wire, value):
    assert list(iter_rows(BytesIO(wire + b"\n"), (("v", "varbinary(max)"),), BulkTextCodec())) == [(value,)]


def test_record_limit_includes_terminating_lf():
    assert list(iter_rows(BytesIO(b"abc\n"), (("v", "text"),), BulkTextCodec(), max_record_bytes=4)) == [("abc",)]
    with pytest.raises(BulkTextFileReadError, match="record_limit"):
        list(iter_rows(BytesIO(b"abcd\n"), (("v", "text"),), BulkTextCodec(), max_record_bytes=4))


@pytest.mark.parametrize(
    "wire,blocker",
    [(b"value", "row_terminator_mismatch"), (b"a\tb\n", "row_width_mismatch"), (b"\xff\n", "utf8_invalid")],
)
def test_malformed_wire_is_rejected(wire, blocker):
    with pytest.raises(BulkTextFileReadError, match=blocker):
        list(iter_rows(BytesIO(wire), (("v", "text"),), BulkTextCodec(), max_record_bytes=1024))
