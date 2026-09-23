"""Closed native descriptor retains ordered physical layout and sealed identity."""

import json
from dataclasses import replace
from hashlib import sha256
from pathlib import Path

import pytest

from dpone.contracts.mssql_native_chunks import TdsInputReceipt
from dpone.contracts.mssql_sqlclient_input import (
    SqlClientFileIdentity,
    SqlClientInputDescriptor,
    decode_input_descriptor,
    encode_input_descriptor,
    input_descriptor_digest,
)
from dpone.contracts.native_wire_layout import NativeWireColumnLayout

LAYOUTS = tuple(
    NativeWireColumnLayout(**v)
    for v in json.loads((Path(__file__).parent / "fixtures/mssql_sqlclient/native-layouts-v1.json").read_text())
)


def descriptor():
    return SqlClientInputDescriptor(
        1, 8, LAYOUTS, TdsInputReceipt(3, 128, "a" * 64), 1024, SqlClientFileIdentity(1, 2, 128, 3, 4)
    )


def test_roundtrip_exact_producer_layouts_and_order():
    value = descriptor()
    assert decode_input_descriptor(encode_input_descriptor(value)) == value
    reverse = replace(value, columns=tuple(reversed(value.columns)))
    assert input_descriptor_digest(value) != input_descriptor_digest(reverse)
    assert (
        input_descriptor_digest(value)
        == sha256(b"dpone.sqlclient.input.v1\0" + encode_input_descriptor(value)).hexdigest()
    )


@pytest.mark.parametrize(
    "field,bad",
    [
        ("target_type", "bigint"),
        ("source_type", "BIGINT"),
        ("nullable", 1),
        ("prefix_width", True),
        ("fixed_length", 9),
        ("scale", 6),
        ("encoding", "utf-8"),
        ("precision", 53),
    ],
)
def test_mismatched_column_metadata_rejects(field, bad):
    value = descriptor()
    with pytest.raises(ValueError):
        replace(value, columns=(replace(value.columns[0], **{field: bad}),))


def test_empty_requires_canonical_hash_and_stat_size():
    value = replace(
        descriptor(),
        expected=TdsInputReceipt(0, 0, sha256(b"").hexdigest()),
        file_identity=SqlClientFileIdentity(1, 2, 0, 3, 4),
    )
    assert decode_input_descriptor(encode_input_descriptor(value)) == value
    with pytest.raises(ValueError):
        replace(value, expected=TdsInputReceipt(0, 0, "a" * 64))
    with pytest.raises(ValueError):
        replace(value, file_identity=SqlClientFileIdentity(1, 2, 1, 3, 4))


def test_unicode_identifiers_and_exact_uint64_stat_fields():
    value = replace(
        descriptor(),
        columns=(replace(LAYOUTS[0], name="🧪" * 64),),
        file_identity=SqlClientFileIdentity(2**64 - 1, 2**64 - 1, 128, 3, 4),
    )
    assert decode_input_descriptor(encode_input_descriptor(value)) == value
    with pytest.raises(ValueError):
        replace(value, columns=(replace(LAYOUTS[0], name="🧪" * 65),))
    with pytest.raises(ValueError):
        SqlClientFileIdentity(True, 2, 128, 3, 4)


@pytest.mark.parametrize("mutation", ["extra", "missing", "column_extra", "duplicate", "too_many", "stat_bool"])
def test_closed_nested_fields(mutation):
    raw = encode_input_descriptor(descriptor())
    data = json.loads(raw)
    if mutation == "duplicate":
        raw = b'{"schema_version":1,' + raw[1:]
    else:
        if mutation == "extra":
            data["unexpected"] = 1
        if mutation == "missing":
            del data["fd"]
        if mutation == "column_extra":
            data["columns"][0]["unexpected"] = 1
        if mutation == "too_many":
            data["columns"] *= 13
        if mutation == "stat_bool":
            data["file_identity"]["size"] = True
        raw = json.dumps(data).encode()
    with pytest.raises(ValueError):
        decode_input_descriptor(raw)
