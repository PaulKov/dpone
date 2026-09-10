"""Lossless scalar conversion for MSSQL native BCP; framing belongs to its caller."""

from __future__ import annotations

import math
import re
import struct
from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Any
from uuid import UUID

from dpone.runtime.native_wire_models import NativeWireColumnLayout


def encode_native_value(value: Any, column: NativeWireColumnLayout, remaining: int) -> bytes:
    """Encode a checked scalar without truncation, implicit padding or text coercion."""
    storage = column.storage_type
    if column.fixed_length is not None and column.fixed_length > remaining:
        raise ValueError("mssql_native_row_bytes_exceeded")
    try:
        if storage in {"varchar", "nvarchar", "char", "nchar", "binary", "varbinary"}:
            payload = _variable(value, column, remaining)
        elif storage in {"bit", "tinyint", "smallint", "int", "bigint"}:
            if storage == "bit":
                if type(value) not in (int, bool) or value not in (0, 1):
                    raise ValueError
            elif type(value) is not int:
                raise ValueError
            payload = int(value).to_bytes(column.fixed_length or 1, "little", signed=storage not in {"bit", "tinyint"})
        elif storage in {"real", "float"}:
            if type(value) is not float or not math.isfinite(value):
                raise ValueError
            fmt = "<f" if column.fixed_length == 4 else "<d"
            payload = struct.pack(fmt, value)
            if struct.unpack(fmt, payload)[0] != value:
                raise ValueError
        elif storage in {"decimal", "numeric", "money", "smallmoney"}:
            payload = _decimal(value, column)
        elif storage == "uniqueidentifier":
            identifier = value if isinstance(value, UUID) else UUID(value) if type(value) is str else None
            if identifier is None:
                raise ValueError
            return identifier.bytes_le
        else:
            payload = _temporal(value, column)
    except (OverflowError, TypeError, UnicodeError, struct.error):
        raise ValueError("mssql_native_invalid_value") from None
    except ValueError as exc:
        if str(exc).startswith("mssql_native_"):
            raise
        raise ValueError("mssql_native_invalid_value") from None
    if len(payload) > remaining:
        raise ValueError("mssql_native_row_bytes_exceeded")
    return payload


def native_value_size(value: Any, column: NativeWireColumnLayout, remaining: int) -> int:
    """Return exact physical size, checking variable width before allocating bytes.

    Fixed-width scalar domain validation occurs during encoding. This sizing pass
    is for bounded IPC batching, not independent value admission.
    """
    if column.fixed_length is not None:
        if column.fixed_length > remaining:
            raise ValueError("mssql_native_row_bytes_exceeded")
        return column.fixed_length
    binary = column.storage_type in {"binary", "varbinary"}
    if binary:
        if type(value) is not bytes:
            raise ValueError("mssql_native_invalid_value")
        size = len(value)
    else:
        if type(value) is not str:
            raise ValueError("mssql_native_invalid_value")
        wide = column.storage_type in {"nvarchar", "nchar"}
        size = 0
        for character in value:
            code = ord(character)
            if 0xD800 <= code <= 0xDFFF:
                raise ValueError("mssql_native_invalid_value")
            size += _character_width(code, wide=wide)
            if size > remaining:
                raise ValueError("mssql_native_row_bytes_exceeded")
    if size > remaining:
        raise ValueError("mssql_native_row_bytes_exceeded")
    match = re.search(r"\((\d+)\)", column.source_type)
    if match:
        maximum = int(match[1]) * (2 if column.storage_type in {"nvarchar", "nchar"} else 1)
        if size > maximum or (column.storage_type in {"binary", "nchar", "char"} and size != maximum):
            raise ValueError("mssql_native_field_length_exceeded")
    if column.prefix_width and size >= 2 ** (8 * column.prefix_width - 1):
        raise ValueError("mssql_native_field_length_exceeded")
    return size


def _character_width(code: int, *, wide: bool) -> int:
    if wide:
        return 2 if code <= 0xFFFF else 4
    if code < 128:
        return 1
    if code < 2048:
        return 2
    return 3 if code <= 65535 else 4


def _variable(value: Any, column: NativeWireColumnLayout, remaining: int) -> bytes:
    native_value_size(value, column, remaining)
    return value if column.storage_type in {"binary", "varbinary"} else value.encode(column.encoding or "utf-8")


def _coefficient(value: Any, precision: int, scale: int) -> int:
    if type(value) is int:
        value = Decimal(value)
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError
    sign, digits, exponent = value.as_tuple()
    if not isinstance(exponent, int):
        raise ValueError
    if not any(digits):
        return 0
    shift = exponent + scale
    if shift < 0:
        if -shift >= len(digits) or any(digits[shift:]):
            raise ValueError
        digits = digits[:shift]
        shift = 0
    if len(digits) + shift > precision:
        raise ValueError
    magnitude = 0
    for digit in digits:
        magnitude = magnitude * 10 + digit
    return (-1 if sign else 1) * magnitude * 10**shift


def _decimal(value: Any, column: NativeWireColumnLayout) -> bytes:
    if column.storage_type in {"decimal", "numeric"}:
        precision, scale = column.precision or 18, column.scale or 0
        if not 1 <= precision <= 38 or not 0 <= scale <= precision:
            raise ValueError
        number = _coefficient(value, precision, scale)
        return bytes((precision, scale, int(number >= 0))) + abs(number).to_bytes(16, "little")
    number = _coefficient(value, 19, 4)
    if column.storage_type == "smallmoney":
        return number.to_bytes(4, "little", signed=True)
    return (number >> 32).to_bytes(4, "little", signed=True) + (number & 0xFFFFFFFF).to_bytes(4, "little")


def _ticks(value: datetime | time, column: NativeWireColumnLayout) -> int:
    remainder = getattr(value, "submicrosecond_100ns", 0)
    if type(remainder) is not int or not 0 <= remainder <= 9:
        raise ValueError
    ticks = ((value.hour * 60 + value.minute) * 60 + value.second) * 10_000_000 + value.microsecond * 10 + remainder
    match = re.search(r"\((\d+)\)", column.source_type)
    declared_scale = int(match[1]) if match else 7
    if ticks % 10 ** (7 - declared_scale):
        raise ValueError
    return ticks


def _temporal(value: Any, column: NativeWireColumnLayout) -> bytes:
    storage = column.storage_type
    if storage == "date":
        if type(value) is not date:
            raise ValueError
        return (value.toordinal() - 1).to_bytes(3, "little")
    if storage == "time":
        if not isinstance(value, time) or value.tzinfo is not None:
            raise ValueError
        return _ticks(value, column).to_bytes(5, "little")
    if not isinstance(value, datetime):
        raise ValueError
    if storage == "datetimeoffset":
        offset = value.utcoffset()
        if offset is None or offset.total_seconds() % 60 or abs(offset.total_seconds()) > 840 * 60:
            raise ValueError
        local_ticks = _ticks(value, column)
        utc = datetime(
            value.year,
            value.month,
            value.day,
            value.hour,
            value.minute,
            value.second,
            value.microsecond,
            tzinfo=value.tzinfo,
            fold=value.fold,
        ).astimezone(UTC)
        offset_minutes = int(offset.total_seconds() // 60)
        ticks = (local_ticks - offset_minutes * 600_000_000) % 864_000_000_000
        return (
            ticks.to_bytes(5, "little")
            + (utc.toordinal() - 1).to_bytes(3, "little")
            + offset_minutes.to_bytes(2, "little", signed=True)
        )
    if value.tzinfo is not None:
        raise ValueError
    if storage == "datetime2":
        return _ticks(value, column).to_bytes(5, "little") + (value.toordinal() - 1).to_bytes(3, "little")
    ticks = _ticks(value, column)
    days = value.toordinal() - date(1900, 1, 1).toordinal()
    if storage == "smalldatetime":
        if ticks % 600_000_000:
            raise ValueError
        return days.to_bytes(2, "little") + (ticks // 600_000_000).to_bytes(2, "little")
    if storage == "datetime":
        # SQL datetime admits only multiples of 1/300s; accept exact Python ticks.
        if value.year < 1753 or ticks * 300 % 10_000_000:
            raise ValueError
        return days.to_bytes(4, "little", signed=True) + (ticks * 300 // 10_000_000).to_bytes(4, "little")
    raise ValueError("mssql_native_unsupported_type")
