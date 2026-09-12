"""Canonical scalar, row, key, and staging-artifact bytes for R1."""

from __future__ import annotations

import hashlib
import math
import re
import struct
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import NoReturn
from uuid import UUID

from dpone._compat import StrEnum
from dpone.type_system.source_sink.postgres_mssql import PostgresMssqlTypeMapper

_ROW_DOMAIN = b"dpone-r1-row-hash-v1\0"
_KEY_DOMAIN = b"dpone-r1-key-v1\0"
_ARTIFACT_DOMAIN = b"dpone-r1-artifact-v1\0"
_SET_DOMAIN = b"dpone-r1-artifact-set-v1\0"
_EPOCH = datetime(2000, 1, 1)
_DAY_EPOCH = date(2000, 1, 1)
_ARTIFACT_ORDER = ("batch_payload", "xmin_delta", "xmin_complete_keys")


class R1HashContractError(ValueError):
    """A value or declaration is outside the closed lossless R1 grammar."""


class R1ScalarKind(StrEnum):
    BOOL = "bool"
    INT2 = "int2"
    INT4 = "int4"
    INT8 = "int8"
    NUMERIC = "numeric"
    FLOAT4 = "float4"
    FLOAT8 = "float8"
    UUID = "uuid"
    DATE = "date"
    TIME = "time"
    TIMESTAMP = "timestamp"
    TIMESTAMPTZ = "timestamptz"
    TEXT = "text"
    BIT1 = "bit1"
    BYTEA = "bytea"


_TYPE_TAGS = {kind: index for index, kind in enumerate(R1ScalarKind, start=1)}


@dataclass(frozen=True, slots=True)
class R1ScalarType:
    """One normalized declaration accepted by the shared PG-to-MSSQL policy."""

    kind: R1ScalarKind
    source_type: str
    target_type: str
    precision: int | None = None
    scale: int | None = None
    max_utf16_units: int | None = None

    @property
    def type_tag(self) -> int:
        return _TYPE_TAGS[self.kind]

    @classmethod
    def from_postgres(cls, source_type: str) -> R1ScalarType:
        """Resolve through the existing mapper, then narrow to exact R1 types."""

        normalized = " ".join(source_type.strip().lower().split())
        decision = PostgresMssqlTypeMapper().resolve(normalized)
        if decision.requires_explicit_contract and normalized != "bit(1)":
            _unsupported(source_type)
        aliases = {
            "boolean": R1ScalarKind.BOOL,
            "bool": R1ScalarKind.BOOL,
            "smallint": R1ScalarKind.INT2,
            "int2": R1ScalarKind.INT2,
            "integer": R1ScalarKind.INT4,
            "int": R1ScalarKind.INT4,
            "int4": R1ScalarKind.INT4,
            "bigint": R1ScalarKind.INT8,
            "int8": R1ScalarKind.INT8,
            "real": R1ScalarKind.FLOAT4,
            "float4": R1ScalarKind.FLOAT4,
            "double precision": R1ScalarKind.FLOAT8,
            "float8": R1ScalarKind.FLOAT8,
            "uuid": R1ScalarKind.UUID,
            "date": R1ScalarKind.DATE,
            "text": R1ScalarKind.TEXT,
            "varchar": R1ScalarKind.TEXT,
            "character varying": R1ScalarKind.TEXT,
            "bytea": R1ScalarKind.BYTEA,
            "bit(1)": R1ScalarKind.BIT1,
        }
        if kind := aliases.get(normalized):
            return cls(kind, normalized, decision.target_type, max_utf16_units=_nvarchar_limit(decision.target_type))
        if match := re.fullmatch(r"(?:numeric|decimal)\((\d+),(-?\d+)\)", normalized):
            precision, scale = int(match.group(1)), int(match.group(2))
            if precision < 1 or _decimal_precision(precision, scale) > 38:
                _unsupported(source_type)
            return cls(R1ScalarKind.NUMERIC, normalized, decision.target_type, precision, scale)
        if match := re.fullmatch(r"(?:varchar|character varying)\((\d+)\)", normalized):
            if int(match.group(1)) < 1:
                _unsupported(source_type)
            return cls(
                R1ScalarKind.TEXT,
                normalized,
                decision.target_type,
                max_utf16_units=_nvarchar_limit(decision.target_type),
            )
        temporal = _temporal_declaration(normalized)
        if temporal is not None:
            kind, scale = temporal
            return cls(kind, normalized, decision.target_type, scale=scale)
        _unsupported(source_type)


@dataclass(frozen=True, slots=True)
class R1Column:
    ordinal: int
    name: str
    scalar_type: R1ScalarType
    nullable: bool

    def __post_init__(self) -> None:
        if isinstance(self.ordinal, bool) or not 1 <= self.ordinal < 2**32:
            raise R1HashContractError("column ordinal must fit uint32 and be positive")
        if not self.name or self.name != self.name.strip():
            raise R1HashContractError("column name must be canonical non-empty text")


def canonical_row_bytes(columns: tuple[R1Column, ...], values: tuple[object, ...]) -> bytes:
    """Frame one logical row in sealed target ordinal order."""

    if len(columns) != len(values) or not columns:
        raise R1HashContractError("row columns and values must be non-empty and aligned")
    ordinals = tuple(column.ordinal for column in columns)
    if ordinals != tuple(sorted(set(ordinals))):
        raise R1HashContractError("columns must use unique ascending target ordinals")
    result = bytearray(_ROW_DOMAIN)
    for column, value in zip(columns, values, strict=True):
        result.extend(column.ordinal.to_bytes(4, "big"))
        result.append(column.scalar_type.type_tag)
        result.extend(_nullable_value(column.scalar_type, value, column.nullable))
    return bytes(result)


def canonical_row_hash(columns: tuple[R1Column, ...], values: tuple[object, ...]) -> bytes:
    return hashlib.sha256(canonical_row_bytes(columns, values)).digest()


def canonical_key_payload(scalar_type: R1ScalarType, value: object) -> bytes:
    """Frame the only certified non-null scalar integer/UUID business key."""

    if scalar_type.kind not in {R1ScalarKind.INT2, R1ScalarKind.INT4, R1ScalarKind.INT8, R1ScalarKind.UUID}:
        raise R1HashContractError("business key must be one integer or UUID scalar")
    if value is None:
        raise R1HashContractError("business key cannot be NULL")
    payload = _scalar_bytes(scalar_type, value)
    return _KEY_DOMAIN + bytes((scalar_type.type_tag,)) + len(payload).to_bytes(8, "big") + payload


def artifact_digest(
    artifact_kind: str,
    schema_digest: bytes,
    rows: tuple[tuple[bytes, bytes], ...],
) -> bytes:
    """Hash a complete unique-key-ordered immutable staging artifact."""

    _digest(schema_digest, "schema_digest")
    if artifact_kind not in _ARTIFACT_ORDER:
        raise R1HashContractError("artifact kind is unsupported")
    keys = tuple(key for key, _row in rows)
    if keys != tuple(sorted(set(keys))):
        raise R1HashContractError("artifact rows must be uniquely ordered by canonical key")
    result = bytearray(_ARTIFACT_DOMAIN)
    result.extend(_frame(artifact_kind.encode("utf-8"), 4))
    result.extend(schema_digest)
    result.extend(len(rows).to_bytes(8, "big"))
    for key, row in rows:
        result.extend(_frame(key, 4))
        result.extend(_frame(row, 8))
    return hashlib.sha256(result).digest()


def artifact_set_digest(artifacts: tuple[tuple[str, bytes], ...]) -> bytes:
    """Hash the fixed-kind ordered manifest for one Batch or XMin intent."""

    order = tuple(_ARTIFACT_ORDER.index(kind) if kind in _ARTIFACT_ORDER else -1 for kind, _ in artifacts)
    if not artifacts or -1 in order or order != tuple(sorted(set(order))):
        raise R1HashContractError("artifact set must use unique fixed kind order")
    result = bytearray(_SET_DOMAIN)
    result.extend(len(artifacts).to_bytes(4, "big"))
    for kind, digest in artifacts:
        result.extend(_frame(kind.encode("utf-8"), 4))
        result.extend(_frame(_digest(digest, "artifact_digest"), 4))
    return hashlib.sha256(result).digest()


def _nullable_value(scalar_type: R1ScalarType, value: object, nullable: bool) -> bytes:
    if value is None:
        if not nullable:
            raise R1HashContractError("non-nullable column received NULL")
        return b"\x00" + (0).to_bytes(8, "big")
    payload = _scalar_bytes(scalar_type, value)
    return b"\x01" + len(payload).to_bytes(8, "big") + payload


def _scalar_bytes(spec: R1ScalarType, value: object) -> bytes:
    kind = spec.kind
    if kind in {R1ScalarKind.INT2, R1ScalarKind.INT4, R1ScalarKind.INT8}:
        if isinstance(value, bool) or not isinstance(value, int):
            raise R1HashContractError("integer value is required")
        bounds = {
            R1ScalarKind.INT2: (-(2**15), 2**15 - 1),
            R1ScalarKind.INT4: (-(2**31), 2**31 - 1),
            R1ScalarKind.INT8: (-(2**63), 2**63 - 1),
        }
        if not bounds[kind][0] <= value <= bounds[kind][1]:
            raise R1HashContractError("integer value exceeds declared type")
        return _minimal_signed(value)
    if kind in {R1ScalarKind.BOOL, R1ScalarKind.BIT1}:
        if not isinstance(value, bool) and value not in (0, 1):
            raise R1HashContractError("boolean/bit value must be 0 or 1")
        return b"\x01" if bool(value) else b"\x00"
    if kind is R1ScalarKind.NUMERIC:
        return _numeric_bytes(spec, value)
    if kind in {R1ScalarKind.FLOAT4, R1ScalarKind.FLOAT8}:
        if isinstance(value, bool) or not isinstance(value, int | float) or not math.isfinite(float(value)):
            raise R1HashContractError("non-finite float value is unsupported")
        normalized = 0.0 if float(value) == 0.0 else float(value)
        try:
            return struct.pack(">f" if kind is R1ScalarKind.FLOAT4 else ">d", normalized)
        except OverflowError as exc:
            raise R1HashContractError("float value exceeds declared type") from exc
    if kind is R1ScalarKind.UUID:
        if not isinstance(value, UUID):
            raise R1HashContractError("UUID value is required")
        return value.bytes
    if kind is R1ScalarKind.DATE:
        if not isinstance(value, date) or isinstance(value, datetime):
            raise R1HashContractError("date value is required")
        return (value - _DAY_EPOCH).days.to_bytes(4, "big", signed=True)
    if kind is R1ScalarKind.TIME:
        if not isinstance(value, time) or value.tzinfo is not None:
            raise R1HashContractError("naive time value is required")
        _require_temporal_precision(spec, value.microsecond)
        micros = ((value.hour * 60 + value.minute) * 60 + value.second) * 1_000_000 + value.microsecond
        return micros.to_bytes(8, "big")
    if kind in {R1ScalarKind.TIMESTAMP, R1ScalarKind.TIMESTAMPTZ}:
        return _timestamp_bytes(spec, value)
    if kind is R1ScalarKind.TEXT:
        if not isinstance(value, str):
            raise R1HashContractError("text value is required")
        if spec.max_utf16_units is not None and len(value.encode("utf-16-le")) // 2 > spec.max_utf16_units:
            raise R1HashContractError("text exceeds target UTF-16 unit limit")
        try:
            return value.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise R1HashContractError("text must contain valid Unicode scalar values") from exc
    if kind is R1ScalarKind.BYTEA:
        if not isinstance(value, bytes):
            raise R1HashContractError("bytea value must be bytes")
        return value
    raise R1HashContractError("DPONE_POSTGRES_MSSQL_UNSUPPORTED_TYPE")


def _numeric_bytes(spec: R1ScalarType, value: object) -> bytes:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise R1HashContractError("numeric value must be a finite Decimal")
    assert spec.scale is not None and spec.precision is not None
    coefficient = value.scaleb(spec.scale)
    if coefficient != coefficient.to_integral_value():
        raise R1HashContractError("numeric value requires forbidden rounding")
    integer = int(abs(coefficient))
    if len(str(integer)) > spec.precision:
        raise R1HashContractError("numeric value exceeds declared precision")
    return (
        bytes((1 if value.is_signed() and integer else 0,))
        + spec.scale.to_bytes(4, "big", signed=True)
        + _minimal_unsigned(integer)
    )


def _timestamp_bytes(spec: R1ScalarType, value: object) -> bytes:
    if not isinstance(value, datetime):
        raise R1HashContractError("timestamp value is required")
    if spec.kind is R1ScalarKind.TIMESTAMP:
        if value.tzinfo is not None:
            raise R1HashContractError("timestamp without time zone must be naive")
        normalized = value
    else:
        if value.tzinfo is None or value.utcoffset() is None:
            raise R1HashContractError("timestamptz must be timezone-aware")
        normalized = value.astimezone(timezone.utc).replace(tzinfo=None)  # noqa: UP017
    _require_temporal_precision(spec, normalized.microsecond)
    delta = normalized - _EPOCH
    micros = (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
    return micros.to_bytes(8, "big", signed=True)


def _require_temporal_precision(spec: R1ScalarType, microsecond: int) -> None:
    assert spec.scale is not None
    if microsecond % (10 ** (6 - spec.scale)):
        raise R1HashContractError("temporal value exceeds declared precision")


def _minimal_signed(value: int) -> bytes:
    length = max(1, (value.bit_length() + 8) // 8)
    raw = value.to_bytes(length, "big", signed=True)
    while len(raw) > 1 and ((raw[0] == 0 and raw[1] < 128) or (raw[0] == 255 and raw[1] >= 128)):
        raw = raw[1:]
    return raw


def _minimal_unsigned(value: int) -> bytes:
    return value.to_bytes(max(1, (value.bit_length() + 7) // 8), "big")


def _temporal_declaration(source: str) -> tuple[R1ScalarKind, int] | None:
    patterns = (
        (r"time(?:\((\d+)\))?(?: without time zone)?", R1ScalarKind.TIME),
        (r"timestamp(?:\((\d+)\))?(?: without time zone)?", R1ScalarKind.TIMESTAMP),
        (r"(?:timestamptz(?:\((\d+)\))?|timestamp(?:\((\d+)\))? with time zone)", R1ScalarKind.TIMESTAMPTZ),
    )
    for pattern, kind in patterns:
        if match := re.fullmatch(pattern, source):
            scale = int(next((group for group in match.groups() if group is not None), "6"))
            if scale <= 6:
                return kind, scale
    return None


def _decimal_precision(precision: int, scale: int) -> int:
    return max(precision, scale) if scale >= 0 else precision - scale


def _nvarchar_limit(target_type: str) -> int | None:
    match = re.fullmatch(r"nvarchar\((\d+)\)", target_type)
    return int(match.group(1)) if match else None


def _frame(value: bytes, width: int) -> bytes:
    return len(value).to_bytes(width, "big") + value


def _digest(value: bytes, field: str) -> bytes:
    if not isinstance(value, bytes) or len(value) != 32:
        raise R1HashContractError(f"{field} must be exactly 32 bytes")
    return value


def _unsupported(source_type: str) -> NoReturn:
    raise R1HashContractError(f"DPONE_POSTGRES_MSSQL_UNSUPPORTED_TYPE:{source_type}")


__all__ = [
    "R1Column",
    "R1HashContractError",
    "R1ScalarKind",
    "R1ScalarType",
    "artifact_digest",
    "artifact_set_digest",
    "canonical_key_payload",
    "canonical_row_bytes",
    "canonical_row_hash",
]
