"""Bounded native JSON primitives, separate from schema and authority validation.

Decoding accepts ordinary duplicate-free JSON object syntax within native bounds.
A stored-original codec must additionally compare its input with canonical
re-encoding, validate its closed schema, and authenticate the original binding.
"""

from __future__ import annotations

import json
import re
from typing import TypeAlias, cast

from dpone.contracts.strict_json import StrictJsonError, canonical_json_bytes, strict_json_object

NativeJsonValue: TypeAlias = None | bool | int | str | list["NativeJsonValue"] | dict[str, "NativeJsonValue"]
MAX_NATIVE_JSON_BYTES = 1024 * 1024
MAX_NATIVE_JSON_STRING_BYTES = 4096
MAX_NATIVE_JSON_DEPTH = 32
MAX_NATIVE_JSON_TOKENS = 65536
MAX_NATIVE_JSON_INTEGER_DIGITS = 128
_LITERAL = re.compile(r"-?(?:0|[1-9][0-9]*)|true|false|null")


class NativeJsonError(ValueError):
    """A native document is malformed or exceeds its primitive resource limits."""


def _string_bytes(value: str) -> int:
    try:
        size = len(value.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise NativeJsonError("native JSON requires Unicode scalar strings") from exc
    if size > MAX_NATIVE_JSON_STRING_BYTES:
        raise NativeJsonError("native JSON string exceeds its UTF-8 byte limit")
    return size


def _string_end(text: str, start: int) -> int:
    """Locate and validate one bounded string before full-document parsing."""
    position = start + 1
    # An ASCII scalar can occupy six source characters as a Unicode escape.
    raw_limit = 6 * MAX_NATIVE_JSON_STRING_BYTES + 2
    while position < len(text):
        if position - start >= raw_limit:
            raise NativeJsonError("native JSON string exceeds its escaped byte limit")
        character = text[position]
        if character == '"':
            try:
                value = json.loads(text[start : position + 1])
            except json.JSONDecodeError as exc:
                raise NativeJsonError("native JSON contains an invalid string") from exc
            _string_bytes(value)
            return position + 1
        position += 2 if character == "\\" else 1
    raise NativeJsonError("native JSON contains an unterminated string")


def _preflight(text: str) -> None:
    """Bound lexical work before recursive grammar parsing or integer conversion.

    Every punctuation mark, string (including a key), number and keyword counts
    as one token. Container depth includes the root. Whitespace is not a token.
    Full grammar and decoded duplicate-key checks remain the strict parser's job.
    """
    position = 0
    tokens = 0
    containers: list[str] = []
    while position < len(text):
        character = text[position]
        if character in " \t\r\n":
            position += 1
            continue
        tokens += 1
        if tokens > MAX_NATIVE_JSON_TOKENS:
            raise NativeJsonError("native JSON exceeds its lexical token limit")
        if character == '"':
            position = _string_end(text, position)
        elif character in "[{":
            containers.append(character)
            if len(containers) > MAX_NATIVE_JSON_DEPTH:
                raise NativeJsonError("native JSON exceeds its container depth limit")
            position += 1
        elif character in "]}":
            expected = "[" if character == "]" else "{"
            if not containers or containers.pop() != expected:
                raise NativeJsonError("native JSON contains unbalanced containers")
            position += 1
        elif character in ",:":
            position += 1
        else:
            token = _LITERAL.match(text, position)
            if token is None:
                raise NativeJsonError("native JSON contains an unsupported token")
            raw = token.group()
            if raw[0] == "-" or raw[0].isdigit():
                if len(raw.removeprefix("-")) > MAX_NATIVE_JSON_INTEGER_DIGITS:
                    raise NativeJsonError("native JSON integer exceeds its digit limit")
            position = token.end()
    if containers:
        raise NativeJsonError("native JSON contains an unterminated container")


def _validate(value: object) -> None:
    """Validate exact built-ins iteratively, distinguishing cycles from sharing."""
    pending: list[tuple[object, int, bool]] = [(value, 0, False)]
    ancestors: set[int] = set()
    visits = 0
    encoded_bytes = 0
    while pending:
        current, depth, leaving = pending.pop()
        if leaving:
            ancestors.remove(id(current))
            continue
        visits += 1
        if visits > MAX_NATIVE_JSON_TOKENS:
            raise NativeJsonError("native JSON exceeds its value limit")
        kind = type(current)
        if current is None or kind is bool:
            encoded_bytes += 4 if current is None or current is True else 5
        elif kind is str:
            _string_bytes(cast(str, current))
            encoded_bytes += len(canonical_json_bytes(current))
        elif kind is int:
            # Compare magnitude before str(): Python's global conversion limit
            # must not affect this protocol's 128-digit integer bound.
            if abs(cast(int, current)) >= 10**MAX_NATIVE_JSON_INTEGER_DIGITS:
                raise NativeJsonError("native JSON integer exceeds its digit limit")
            encoded_bytes += len(str(current))
        elif kind is list or kind is dict:
            if depth >= MAX_NATIVE_JSON_DEPTH:
                raise NativeJsonError("native JSON exceeds its container depth limit")
            if id(current) in ancestors:
                raise NativeJsonError("native JSON contains an ancestor cycle")
            ancestors.add(id(current))
            pending.append((current, depth, True))
            if kind is dict:
                mapping = cast(dict[object, object], current)
                encoded_bytes += 2 + max(0, len(mapping) - 1) + len(mapping)
                if len(pending) + visits + len(mapping) > MAX_NATIVE_JSON_TOKENS:
                    raise NativeJsonError("native JSON object exceeds its entry limit")
                for key, item in mapping.items():
                    if type(key) is not str:
                        raise NativeJsonError("native JSON object keys must be strings")
                    _string_bytes(key)
                    encoded_bytes += len(canonical_json_bytes(key))
                    if encoded_bytes > MAX_NATIVE_JSON_BYTES:
                        raise NativeJsonError("native JSON exceeds its document byte limit")
                    pending.append((item, depth + 1, False))
            else:
                sequence = cast(list[object], current)
                encoded_bytes += 2 + max(0, len(sequence) - 1)
                if len(pending) + visits + len(sequence) > MAX_NATIVE_JSON_TOKENS:
                    raise NativeJsonError("native JSON array exceeds its entry limit")
                pending.extend((item, depth + 1, False) for item in sequence)
        else:
            raise NativeJsonError("native JSON supports exact primitive built-in values only")
        if encoded_bytes > MAX_NATIVE_JSON_BYTES:
            raise NativeJsonError("native JSON exceeds its document byte limit")


def decode_native_delivery_json(payload: bytes) -> dict[str, NativeJsonValue]:
    """Decode a bounded JSON object without accepting floats or duplicate keys.

    Limits are 1 MiB/document, 4096 UTF-8 bytes/string, 32 container levels,
    65536 lexical tokens and 128 decimal integer digits (minus excluded).
    No BOM or invalid Unicode scalar is accepted. This does not authenticate
    evidence, enforce a schema, or establish canonical stored-original bytes.
    """
    if type(payload) is not bytes or len(payload) > MAX_NATIVE_JSON_BYTES:
        raise NativeJsonError("native JSON requires bytes within the document limit")
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise NativeJsonError("native JSON requires strict UTF-8") from exc
    _preflight(text)
    try:
        result = strict_json_object(text)
    except StrictJsonError as exc:
        raise NativeJsonError("native JSON must be a valid duplicate-free object") from exc
    _validate(result)
    return cast(dict[str, NativeJsonValue], result)


def encode_native_delivery_json(value: object) -> bytes:
    """Encode exact bounded primitives canonically without mutating the input.

    Keys are sorted; separators are compact; Unicode is emitted without
    normalization, BOM or newline. Schema codecs explicitly convert DTO tuples
    to arrays. Any primitive root can be encoded; decoding requires an object.
    """
    _validate(value)
    payload = canonical_json_bytes(value)
    if len(payload) > MAX_NATIVE_JSON_BYTES:
        raise NativeJsonError("native JSON exceeds its document byte limit")
    _preflight(payload.decode("utf-8"))
    return payload
