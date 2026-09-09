"""Finite native BCP framing rules and bounded field reads.

This is the layout producer's authority for the existing ``-n`` profile, not a
character/explicit-format parser. Field lengths never come from nullability alone.
"""

from __future__ import annotations

import math
import re
import struct
from collections.abc import Sequence
from typing import BinaryIO

from dpone.runtime.native_wire_models import NativeWireColumnLayout

_FIXED_WIDTHS = {
    "bit": 1,
    "tinyint": 1,
    "smallint": 2,
    "int": 4,
    "bigint": 8,
    "real": 4,
    "float": 8,
    "money": 8,
    "smallmoney": 4,
    "date": 3,
    "datetime": 8,
    "smalldatetime": 4,
    "uniqueidentifier": 16,
    "decimal": 19,
    "numeric": 19,
}
_ALWAYS_PREFIXED = frozenset({"bit", "uniqueidentifier", "decimal", "numeric"})


def native_prefix_width(storage: str, *, nullable: bool) -> int:
    """Return native fixed-field framing, including mandatory NOT NULL prefixes."""
    return int(nullable or storage in _ALWAYS_PREFIXED)


def time_length(scale: int) -> int:
    """Return the admitted temporal payload width without changing its profile."""
    if not 0 <= scale <= 7:
        raise ValueError("native_wire_invalid_layout:temporal_scale")
    return 3 if scale <= 2 else 4 if scale <= 4 else 5


def fixed_length(storage: str, scale: int | None, precision: int | None = None) -> int | None:
    """Return exact native payload width; decimal is BCP storage, not page storage."""
    if storage == "float" and precision is not None:
        if not 1 <= precision <= 53:
            raise ValueError("native_wire_invalid_layout:float_precision")
        return 4 if precision <= 24 else 8
    if storage in _FIXED_WIDTHS:
        return _FIXED_WIDTHS[storage]
    if storage in {"time", "datetime2", "datetimeoffset"}:
        return time_length(7 if scale is None else scale) + {"time": 0, "datetime2": 3, "datetimeoffset": 5}[storage]
    return None


def read_native_payload(handle: BinaryIO, layout: NativeWireColumnLayout, ordinal: int = 0) -> bytes | None:
    """Read one validated field, rejecting malformed lengths before payload reads."""
    context = f"type={layout.storage_type}:ordinal={ordinal}:offset={handle.tell()}:format=native_wire.v1"
    length = layout.fixed_length
    if layout.prefix_width:
        indicator = int.from_bytes(read_exact(handle, layout.prefix_width, context), "little", signed=True)
        if indicator == -1:
            if not layout.nullable:
                raise ValueError(f"native_wire_unexpected_null:{context}")
            return None
        length = indicator
    if length is None or length < 0:
        raise ValueError(f"native_wire_invalid_length:{context}")
    if layout.fixed_length is not None and length != layout.fixed_length:
        raise ValueError(f"native_wire_invalid_length:{context}:expected={layout.fixed_length}:actual={length}")
    if layout.fixed_length is None:
        match = re.search(r"\((\d+)\)", layout.source_type)
        if match:
            maximum = int(match[1]) * {"nvarchar": 2, "nchar": 2, "varchar": 4, "char": 4}.get(layout.storage_type, 1)
            if length > maximum:
                raise ValueError(f"native_wire_invalid_length:{context}:maximum={maximum}:actual={length}")
    payload = read_exact(handle, length, context)
    if layout.storage_type in {"decimal", "numeric"}:
        precision = 18 if layout.precision is None else layout.precision
        scale = 0 if layout.scale is None else layout.scale
        if (
            payload[0] != precision
            or payload[1] != scale
            or payload[2] not in (0, 1)
            or not 1 <= precision <= 38
            or not 0 <= scale <= precision
            or int.from_bytes(payload[3:], "little") >= 10**precision
        ):
            raise ValueError(f"native_wire_invalid_decimal:{context}")
    if layout.storage_type == "bit" and payload[0] not in (0, 1):
        raise ValueError(f"native_wire_invalid_bit:{context}")
    _validate_scalar_domain(payload, layout, context)
    return payload


def _validate_scalar_domain(payload: bytes, field: NativeWireColumnLayout, context: str) -> None:
    """Reject malformed values rather than wrapping days or interpreting invalid text."""
    storage = field.storage_type
    if storage in {"real", "float"} and not math.isfinite(
        struct.unpack("<f" if len(payload) == 4 else "<d", payload)[0]
    ):
        raise ValueError(f"native_wire_invalid_float:{context}")
    if storage in {"nchar", "nvarchar", "char", "varchar"}:
        try:
            payload.decode("utf-16le" if storage in {"nchar", "nvarchar"} else "utf-8")
        except UnicodeError:
            raise ValueError(f"native_wire_invalid_text:{context}") from None
    scale = 7 if field.scale is None else field.scale
    if storage in {"time", "datetime2", "datetimeoffset"}:
        width = 3 if scale <= 2 else 4 if scale <= 4 else 5
        ticks = int.from_bytes(payload[:width], "little")
        day_ticks = 86400 * 10**scale
        declared = re.search(r"\((\d+)\)", field.source_type)
        declared_scale = int(declared[1]) if declared else 7
        if ticks >= day_ticks or ticks % (10 ** (7 - declared_scale)):
            raise ValueError(f"native_wire_invalid_temporal:{context}")
        if storage != "time":
            days = int.from_bytes(payload[width : width + 3], "little")
            if days > 3652058:
                raise ValueError(f"native_wire_invalid_temporal:{context}")
            if storage == "datetimeoffset":
                offset = int.from_bytes(payload[-2:], "little", signed=True)
                local = days * day_ticks + ticks + offset * 60 * 10**scale
                if abs(offset) > 840 or not 0 <= local < 3652059 * day_ticks:
                    raise ValueError(f"native_wire_invalid_temporal:{context}")
    elif storage == "date" and int.from_bytes(payload, "little") > 3652058:
        raise ValueError(f"native_wire_invalid_temporal:{context}")
    elif storage == "datetime":
        days = int.from_bytes(payload[:4], "little", signed=True)
        ticks = int.from_bytes(payload[4:], "little", signed=True)
        if not -53690 <= days <= 2958463 or not 0 <= ticks < 25920000:
            raise ValueError(f"native_wire_invalid_temporal:{context}")
    elif storage == "smalldatetime" and int.from_bytes(payload[2:], "little") >= 1440:
        raise ValueError(f"native_wire_invalid_temporal:{context}")


def read_exact(handle: BinaryIO, length: int, context: str = "format=native_wire.v1") -> bytes:
    """Handle short reads without ever passing a negative/huge count to read()."""
    if length < 0:
        raise ValueError(f"native_wire_invalid_length:{context}")
    if length > 65536:
        position = handle.tell()
        end = handle.seek(0, 2)
        handle.seek(position)
        if length > end - position:
            raise EOFError(f"mssql_bcp_native_unexpected_eof:{context}")
    chunks = bytearray()
    while len(chunks) < length:
        part = handle.read(min(length - len(chunks), 65536))
        if not part:
            raise EOFError(f"mssql_bcp_native_unexpected_eof:{context}")
        chunks.extend(part)
    return bytes(chunks)


def validate_target_schema(
    columns: tuple[NativeWireColumnLayout, ...], target_schema: Sequence[tuple[str, str]] | None
) -> None:
    """Require positional identity and only implemented source/target type families."""
    if target_schema is None:
        pairs = [(column.name, column.target_type) for column in columns]
    else:
        pairs = list(target_schema)
    if [pair[0] for pair in pairs] != [column.name for column in columns]:
        raise ValueError("native_wire_invalid_layout:target_columns")
    integers = {"int8", "uint8", "int16", "uint16", "int32", "uint32", "int64", "uint64"}
    for column, (_, target) in zip(columns, pairs, strict=True):
        inner = str(target).strip().lower()
        if inner.startswith("nullable(") and inner.endswith(")"):
            inner = inner[9:-1]
        root = inner.split("(", 1)[0]
        storage = column.storage_type
        if storage in {"bit", "tinyint", "smallint", "int", "bigint"}:
            allowed = integers | ({"bool"} if storage == "bit" else set())
        elif storage in {"real", "float"}:
            allowed = {"float64"} | ({"float32"} if column.fixed_length == 4 else set())
        elif storage in {"money", "smallmoney", "decimal", "numeric"}:
            allowed = {"decimal"}
        elif storage == "uniqueidentifier":
            allowed = {"uuid"}
        elif storage == "date":
            allowed = {"date", "date32"}
        elif storage in {"datetime", "smalldatetime", "datetime2"}:
            allowed = {"datetime", "datetime64"}
        elif storage == "datetimeoffset":
            allowed = {"datetime64", "string"}
        elif storage == "time":
            allowed = {"string", "uint32"}
        else:
            allowed = {"string", "fixedstring"}
        if root not in allowed:
            raise ValueError("native_wire_unsupported_target_mapping")


def validate_source_schema(columns: Sequence[NativeWireColumnLayout], schema: Sequence[tuple[str, str]]) -> None:
    """Bind encoder order and source types to the validated artifact before I/O."""
    if tuple(schema) != tuple((column.name, column.source_type) for column in columns):
        raise ValueError("native_wire_source_schema_mismatch:re-export_with_matching_schema")
