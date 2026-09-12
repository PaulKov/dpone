"""Leaf error type and canonical codec primitives for MSSQL R1 V3."""

from __future__ import annotations

import hashlib
import unicodedata
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import TypeVar
from uuid import UUID

_ARTIFACT_DOMAIN = b"dpone-r1-artifact-v1\0"


EnumT = TypeVar("EnumT", bound=Enum)


class MssqlR1V3ContractError(ValueError):
    """A value or byte stream cannot participate in the exact R1 V3 contract."""


def canonical_bytes(domain: bytes, values: tuple[object, ...]) -> bytes:
    """Encode a tuple using tagged values and unsigned 32-bit length framing."""

    _validate_domain(domain)
    payload = bytearray(domain)
    for value in values:
        encoded = _encode_value(value)
        payload.extend(_u32(len(encoded)))
        payload.extend(encoded)
    return bytes(payload)


def decode_canonical_bytes(payload: bytes, domain: bytes, *, field_count: int) -> tuple[object, ...]:
    """Strictly decode and re-encode one exact-domain canonical tuple."""

    _validate_domain(domain)
    if not isinstance(payload, bytes) or not payload.startswith(domain):
        raise MssqlR1V3ContractError("canonical payload has an unknown or legacy domain")
    cursor = len(domain)
    values: list[object] = []
    while cursor < len(payload):
        size, cursor = _read_u32(payload, cursor)
        end = cursor + size
        if end > len(payload):
            raise MssqlR1V3ContractError("canonical payload is truncated")
        values.append(_decode_value(payload[cursor:end]))
        cursor = end
    if len(values) != field_count:
        raise MssqlR1V3ContractError("canonical payload has a wrong field count or trailing bytes")
    decoded = tuple(values)
    if canonical_bytes(domain, decoded) != payload:
        raise MssqlR1V3ContractError("canonical payload has a noncanonical representation")
    return decoded


def canonical_utf8_fields(domain: bytes, values: tuple[str, ...]) -> bytes:
    """Encode schema-typed UTF-8 fields with unsigned 32-bit length framing."""

    _validate_domain(domain)
    payload = bytearray(domain)
    for value in values:
        if not isinstance(value, str) or unicodedata.normalize("NFC", value) != value:
            raise MssqlR1V3ContractError("canonical field must be NFC-normalized UTF-8 text")
        encoded = value.encode("utf-8")
        payload.extend(_u32(len(encoded)))
        payload.extend(encoded)
    return bytes(payload)


def decode_canonical_utf8_fields(payload: bytes, domain: bytes, *, field_count: int) -> tuple[str, ...]:
    """Strictly decode one exact-domain, schema-typed UTF-8 field sequence."""

    _validate_domain(domain)
    if not isinstance(payload, bytes) or not payload.startswith(domain):
        raise MssqlR1V3ContractError("canonical payload has an unknown or legacy domain")
    cursor = len(domain)
    values: list[str] = []
    for _ in range(field_count):
        size, cursor = _read_u32(payload, cursor)
        end = cursor + size
        if end > len(payload):
            raise MssqlR1V3ContractError("canonical UTF-8 field is truncated")
        try:
            value = payload[cursor:end].decode("utf-8")
        except UnicodeDecodeError as exc:
            raise MssqlR1V3ContractError("canonical field is not valid UTF-8") from exc
        if unicodedata.normalize("NFC", value) != value:
            raise MssqlR1V3ContractError("canonical field is not NFC-normalized")
        values.append(value)
        cursor = end
    if cursor != len(payload):
        raise MssqlR1V3ContractError("canonical payload has trailing bytes")
    return tuple(values)


def encode_artifact_rows(
    artifact_kind: str,
    schema_digest: bytes,
    rows: tuple[tuple[bytes, bytes], ...],
) -> tuple[bytes, int]:
    """Return the exact artifact digest and framed payload-byte count."""

    if artifact_kind not in {"batch_payload", "xmin_delta", "xmin_complete_keys"}:
        raise MssqlR1V3ContractError("artifact kind is unsupported")
    if not isinstance(schema_digest, bytes) or len(schema_digest) != 32 or not isinstance(rows, tuple):
        raise MssqlR1V3ContractError("artifact digest input is invalid")
    kind_bytes = artifact_kind.encode("utf-8")
    payload = bytearray(_ARTIFACT_DOMAIN + _u32(len(kind_bytes)) + kind_bytes + schema_digest)
    payload.extend(_u64(len(rows)))
    previous: bytes | None = None
    observed_size = 0
    for row in rows:
        if not isinstance(row, tuple) or len(row) != 2 or not all(isinstance(item, bytes) for item in row):
            raise MssqlR1V3ContractError("artifact row contract is invalid")
        key, value = row
        if not key:
            raise MssqlR1V3ContractError("artifact key must be non-empty")
        if previous is not None and key <= previous:
            raise MssqlR1V3ContractError("artifact rows require strict unsigned key order")
        if artifact_kind == "xmin_complete_keys" and value:
            raise MssqlR1V3ContractError("complete-keys row payload must be empty")
        payload.extend(_u32(len(key)))
        payload.extend(key)
        payload.extend(_u64(len(value)))
        payload.extend(value)
        observed_size += 12 + len(key) + len(value)
        previous = key
    return hashlib.sha256(payload).digest(), observed_size


def expect_tuple(value: object, field: str, *, size: int | None = None) -> tuple[object, ...]:
    if not isinstance(value, tuple) or (size is not None and len(value) != size):
        raise MssqlR1V3ContractError(f"{field} has invalid tuple shape")
    return value


def expect_text(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise MssqlR1V3ContractError(f"{field} must be text")
    return value


def expect_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise MssqlR1V3ContractError(f"{field} must be an integer")
    return value


def expect_bool(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise MssqlR1V3ContractError(f"{field} must be boolean")
    return value


def expect_enum(enum_type: type[EnumT], value: object, field: str) -> EnumT:
    text = expect_text(value, field)
    try:
        return enum_type(text)
    except ValueError as exc:
        raise MssqlR1V3ContractError(f"{field} is unsupported") from exc


def expect_bytes(value: object, field: str) -> bytes:
    if not isinstance(value, bytes):
        raise MssqlR1V3ContractError(f"{field} must be exact bytes")
    return value


def expect_uuid(value: object, field: str) -> UUID:
    if not isinstance(value, UUID):
        raise MssqlR1V3ContractError(f"{field} must be a UUID")
    return value


def validate_detached_command_binding(
    command_payload: bytes,
    command_bundle: bytes,
    verified_payload: bytes,
    verified_payload_digest: bytes,
    verified_bundle_digest: bytes,
) -> None:
    """Bind verifier evidence to the exact detached-signature command bytes."""

    if (verified_payload, verified_payload_digest, verified_bundle_digest) != (
        command_payload,
        hashlib.sha256(command_payload).digest(),
        hashlib.sha256(command_bundle).digest(),
    ):
        raise MssqlR1V3ContractError("verification differs from the exact signed command")


def validate_signed_command_bytes(payload: bytes, bundle: bytes, domain: bytes) -> None:
    """Validate the untrusted outer shape before a domain decoder handles the payload."""

    if not isinstance(payload, bytes) or not payload.startswith(domain):
        raise MssqlR1V3ContractError("signed command payload has the wrong domain")
    if not isinstance(bundle, bytes) or not bundle:
        raise MssqlR1V3ContractError("signed command requires exact detached bundle bytes")


def _encode_value(value: object) -> bytes:
    if value is None:
        return b"n"
    if isinstance(value, Enum):
        return _encode_value(value.value)
    if isinstance(value, bool):
        return b"t" if value else b"f"
    if isinstance(value, UUID):
        return b"u" + value.bytes
    if isinstance(value, bytes):
        return b"b" + _u64(len(value)) + value
    if isinstance(value, str):
        encoded = value.encode("utf-8")
        return b"s" + _u64(len(encoded)) + encoded
    if isinstance(value, int):
        try:
            return b"i" + value.to_bytes(8, "big", signed=True)
        except OverflowError as exc:
            raise MssqlR1V3ContractError("canonical integer does not fit signed 64-bit") from exc
    if isinstance(value, datetime):
        utc = timezone(timedelta(0))
        if value.tzinfo is None or value.utcoffset() != utc.utcoffset(value):
            raise MssqlR1V3ContractError("canonical datetime must be an aware UTC instant")
        return _encode_value(value.astimezone(utc).isoformat(timespec="microseconds").replace("+00:00", "Z"))
    if isinstance(value, tuple):
        encoded_items = tuple(_encode_value(item) for item in value)
        return b"q" + b"".join(_u32(len(item)) + item for item in encoded_items)
    raise MssqlR1V3ContractError(f"unsupported canonical V3 value: {type(value).__name__}")


def _decode_value(payload: bytes) -> object:
    if not payload:
        raise MssqlR1V3ContractError("canonical value is empty")
    tag, body = payload[:1], payload[1:]
    if tag == b"n" and not body:
        return None
    if tag == b"t" and not body:
        return True
    if tag == b"f" and not body:
        return False
    if tag == b"u" and len(body) == 16:
        return UUID(bytes=body)
    if tag in {b"b", b"s"}:
        size, cursor = _read_u64(body, 0)
        raw = body[cursor:]
        if len(raw) != size:
            raise MssqlR1V3ContractError("canonical bytes/text length is invalid")
        if tag == b"b":
            return raw
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise MssqlR1V3ContractError("canonical text is not valid UTF-8") from exc
        if unicodedata.normalize("NFC", text) != text:
            raise MssqlR1V3ContractError("canonical text is not NFC-normalized")
        return text
    if tag == b"i" and len(body) == 8:
        return int.from_bytes(body, "big", signed=True)
    if tag == b"q":
        return _decode_sequence(body)
    raise MssqlR1V3ContractError("canonical value has an unknown tag or invalid size")


def _decode_sequence(payload: bytes) -> tuple[object, ...]:
    cursor = 0
    values: list[object] = []
    while cursor < len(payload):
        size, cursor = _read_u32(payload, cursor)
        end = cursor + size
        if end > len(payload):
            raise MssqlR1V3ContractError("canonical sequence is truncated")
        values.append(_decode_value(payload[cursor:end]))
        cursor = end
    return tuple(values)


def _validate_domain(domain: bytes) -> None:
    if not isinstance(domain, bytes) or not domain.endswith(b"\0") or len(domain) < 2:
        raise MssqlR1V3ContractError("canonical domain must be non-empty bytes ending in NUL")


def _u32(value: int) -> bytes:
    if not 0 <= value <= 2**32 - 1:
        raise MssqlR1V3ContractError("canonical length exceeds uint32")
    return value.to_bytes(4, "big")


def _u64(value: int) -> bytes:
    return value.to_bytes(8, "big")


def _read_u32(payload: bytes, cursor: int) -> tuple[int, int]:
    end = cursor + 4
    if end > len(payload):
        raise MssqlR1V3ContractError("canonical length prefix is truncated")
    return int.from_bytes(payload[cursor:end], "big"), end


def _read_u64(payload: bytes, cursor: int) -> tuple[int, int]:
    end = cursor + 8
    if end > len(payload):
        raise MssqlR1V3ContractError("canonical length prefix is truncated")
    return int.from_bytes(payload[cursor:end], "big"), end


__all__ = [
    "MssqlR1V3ContractError",
    "canonical_bytes",
    "canonical_utf8_fields",
    "decode_canonical_bytes",
    "decode_canonical_utf8_fields",
    "encode_artifact_rows",
    "expect_bool",
    "expect_bytes",
    "expect_enum",
    "expect_int",
    "expect_text",
    "expect_tuple",
    "expect_uuid",
    "validate_detached_command_binding",
    "validate_signed_command_bytes",
]
