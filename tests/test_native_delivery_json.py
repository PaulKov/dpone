"""Adversarial and boundary acceptance for native immutable JSON documents."""

import pytest

from dpone.contracts.native_delivery_json import (
    NativeJsonError,
    decode_native_delivery_json,
    encode_native_delivery_json,
)


def test_exact_canonical_roundtrip_preserves_integer_and_unicode_identity():
    value = {"z": [None, True, False, -(2**127), 2**256 - 1], "a": "é😀"}
    payload = encode_native_delivery_json(value)
    assert payload.startswith('{"a":"é😀","z":'.encode())
    assert decode_native_delivery_json(payload) == value
    assert encode_native_delivery_json(decode_native_delivery_json(payload)) == payload


@pytest.mark.parametrize(
    "payload",
    [
        b'{"x":1.0}',
        b'{"x":1e2}',
        b'{"x":NaN}',
        b'{"x":Infinity}',
        b'{"x":-Infinity}',
        b'{"x":01}',
        b'{"x":+1}',
        b'{"x":true,}',
        b'{"a":1,"a":2}',
        b'{"a":1,"\\u0061":2}',
        b'{"x":"\\ud800"}',
        b'{"x":"\\udfff"}',
        b'{"x":"\\ud800a"}',
        b'{"x":"\\q"}',
        b'{"x":"\x01"}',
        b"\xef\xbb\xbf{}",
        b'{"x":"\xff"}',
        b"[]",
        b"null",
        b"{}{}",
        b"{",
        b'{"x":"unterminated}',
        "{}",
        bytearray(b"{}"),
    ],
)
def test_invalid_noncanonical_or_nonbyte_input_is_rejected(payload):
    with pytest.raises(NativeJsonError):
        decode_native_delivery_json(payload)


@pytest.mark.parametrize(
    "value",
    [{"x": 1.0}, {"x": float("nan")}, {"x": (1, 2)}, {1: "value"}, {"x": b"bytes"}, {"x": "\ud800"}, {"x": object()}],
)
def test_encoder_rejects_coercions_and_unsupported_values(value):
    with pytest.raises(NativeJsonError):
        encode_native_delivery_json(value)


def test_ancestor_cycles_fail_but_shared_values_are_valid():
    shared = [1, 2]
    assert decode_native_delivery_json(encode_native_delivery_json({"a": shared, "b": shared})) == {
        "a": shared,
        "b": shared,
    }
    shared.append(shared)
    with pytest.raises(NativeJsonError):
        encode_native_delivery_json({"a": shared})


def test_integer_digit_limit_is_separate_from_sql_bigint():
    value = {"x": -(10**128 - 1)}
    assert decode_native_delivery_json(encode_native_delivery_json(value)) == value
    for payload in (b'{"x":' + b"1" * 129 + b"}", b'{"x":-' + b"1" * 129 + b"}"):
        with pytest.raises(NativeJsonError):
            decode_native_delivery_json(payload)
    with pytest.raises(NativeJsonError):
        encode_native_delivery_json({"x": 10**128})


def test_string_budget_counts_utf8_and_applies_to_keys():
    for value in ({"x": "😀" * 1024}, {"é" * 2048: 0}):
        assert decode_native_delivery_json(encode_native_delivery_json(value)) == value
    for value in ({"x": "😀" * 1025}, {"é" * 2049: 0}):
        with pytest.raises(NativeJsonError):
            encode_native_delivery_json(value)
        import json

        with pytest.raises(NativeJsonError):
            decode_native_delivery_json(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode())


def test_depth_is_container_depth_including_root_not_string_braces():
    valid = b'{"x":' + b"[" * 31 + b'"{[\\""' + b"]" * 31 + b"}"
    assert decode_native_delivery_json(valid)
    too_deep = b'{"x":' + b"[" * 32 + b"0" + b"]" * 32 + b"}"
    with pytest.raises(NativeJsonError):
        decode_native_delivery_json(too_deep)
    value = {"x": 0}
    for _ in range(32):
        value = {"x": value}
    with pytest.raises(NativeJsonError):
        encode_native_delivery_json(value)


def test_lexical_token_boundary_includes_structural_punctuation():
    # 32764 integers plus an empty array => exactly 65536 lexical tokens.
    payload = b'{"x":[' + b"0," * 32764 + b"[]]}"
    assert len(decode_native_delivery_json(payload)["x"]) == 32765
    with pytest.raises(NativeJsonError):
        decode_native_delivery_json(b'{"x":[' + b"0," * 32765 + b"[]]}")
    with pytest.raises(NativeJsonError):
        encode_native_delivery_json({"x": [0] * 32766})


def test_document_byte_boundary_is_checked_for_decode_and_encode():
    value = {f"k{i:03}": "x" * 4096 for i in range(255)}
    base = encode_native_delivery_json(value)
    padding = 1048576 - len(base) - len(b',"tail":""')
    value["tail"] = "x" * padding
    payload = encode_native_delivery_json(value)
    assert len(payload) == 1048576
    assert decode_native_delivery_json(payload) == value
    with pytest.raises(NativeJsonError):
        decode_native_delivery_json(payload + b" ")
    value["tail"] += "x"
    with pytest.raises(NativeJsonError):
        encode_native_delivery_json(value)


def test_decoder_accepts_valid_noncanonical_input_but_encoder_fixes_identity():
    assert decode_native_delivery_json(b' {"z":-0,"a":"\\ud83d\\ude00"}\n') == {"z": 0, "a": "😀"}
    assert encode_native_delivery_json(decode_native_delivery_json(b'{"x":"\\u0061"}')) == b'{"x":"a"}'


def test_unicode_is_not_normalized_or_merged():
    value = {"é": "é", "e\u0301": "e\u0301"}
    assert decode_native_delivery_json(encode_native_delivery_json(value)) == value
    assert encode_native_delivery_json({"x": "é"}) != encode_native_delivery_json({"x": "e\u0301"})


def test_exact_builtin_types_do_not_execute_custom_iteration():
    class UnsafeDict(dict):
        def items(self):
            pytest.fail("a custom mapping must not be traversed")

    class CustomInt(int):
        pass

    class CustomString(str):
        pass

    for value in (UnsafeDict(x=1), {"x": CustomInt(1)}, {"x": CustomString("a")}, {CustomString("a"): 1}):
        with pytest.raises(NativeJsonError):
            encode_native_delivery_json(value)


def test_shared_strict_parser_keeps_existing_float_behavior():
    from dpone.contracts.strict_json import strict_json_object

    assert strict_json_object(b'{"x":1.5}') == {"x": 1.5}
    with pytest.raises(NativeJsonError):
        decode_native_delivery_json(b'{"x":1.5}')


def test_encoder_permits_primitive_roots_without_weakening_object_decoder():
    assert encode_native_delivery_json([1, True, None]) == b"[1,true,null]"
    with pytest.raises(NativeJsonError):
        decode_native_delivery_json(b"[1,true,null]")
