"""Pure canonical ClickHouse scalar wire primitives, without source coercion.

Legacy source-policy wrappers retain their historical coercion behavior. Strict
protected profiles validate exact Python scalar types before entering here.
"""

from __future__ import annotations

import re
import struct
from decimal import Decimal, localcontext
from uuid import UUID


def pack_integer(value: int, root: str) -> bytes:
    """Encode an already resolved integer, preserving legacy overflow errors."""
    formats = {
        "int8": "<b",
        "uint8": "<B",
        "int16": "<h",
        "uint16": "<H",
        "int32": "<i",
        "uint32": "<I",
        "int64": "<q",
        "uint64": "<Q",
    }
    try:
        return struct.pack(formats[root], value)
    except struct.error:
        raise ValueError("clickhouse_binary_numeric_out_of_range") from None


def encode_temporal_integer(value: int, root: str, scale: int = 0) -> bytes:
    """Validate calendar bounds as well as integer storage capacity."""
    factor = 10**scale
    bounds = {
        "date": (0, 65536, "H"),
        "date32": (-25567, 120530, "i"),
        "datetime": (0, 2**32, "I"),
        "datetime64": (-2208988800 * factor, min(10413792000 * factor, 2**63), "q"),
    }
    low, high, fmt = bounds[root]
    if not 0 <= scale <= 9 or not low <= value < high:
        raise ValueError("clickhouse_binary_temporal_out_of_range")
    return struct.pack("<" + fmt, value)


def encode_decimal(value: Decimal, type_name: str) -> bytes:
    """Encode exact scaled integers, without rounding or context precision loss."""
    precision, decimal_scale = _decimal_precision_scale(type_name)
    width = 4 if precision <= 9 else 8 if precision <= 18 else 16 if precision <= 38 else 32
    if not 1 <= precision <= 76 or not 0 <= decimal_scale <= precision:
        raise ValueError("ClickHouse Decimal precision/scale is invalid")
    if not value.is_finite():
        raise ValueError("ClickHouse Decimal requires a finite value")
    if value and value.adjusted() >= precision - decimal_scale:
        raise ValueError("clickhouse_binary_decimal_out_of_range: ClickHouse Decimal precision overflow")
    with localcontext() as context:
        context.prec = precision + 2
        quantum = Decimal(1).scaleb(-decimal_scale)
        exact = value.quantize(quantum)
        if exact != value:
            raise ValueError("clickhouse_binary_decimal_precision_loss: ClickHouse Decimal scale would lose precision")
        scaled = int(exact.scaleb(decimal_scale))
    return scaled.to_bytes(width, byteorder="little", signed=True)


def _decimal_precision_scale(type_name: str) -> tuple[int, int]:
    generic = re.fullmatch(r"Decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)", type_name.strip(), re.IGNORECASE)
    if generic:
        return int(generic[1]), int(generic[2])
    alias = re.fullmatch(r"Decimal(32|64|128|256)\(\s*(\d+)\s*\)", type_name.strip(), re.IGNORECASE)
    if alias:
        return {32: 9, 64: 18, 128: 38, 256: 76}[int(alias[1])], int(alias[2])
    raise ValueError("ClickHouse Decimal declaration requires precision/scale or a supported width alias")


def encode_string(value: bytes) -> bytes:
    """Byte-preserving length-prefixed String."""
    return var_uint(len(value)) + value


def encode_fixed_string(value: bytes, width: int) -> bytes:
    """Restore the full width, including a driver-stripped zero suffix."""
    if len(value) > width:
        raise ValueError(f"FixedString({width}) value is too long: {len(value)} bytes.")
    return value + b"\x00" * (width - len(value))


def encode_uuid(value: UUID) -> bytes:
    """ClickHouse UUID has reversed bytes within each 64-bit half."""
    raw = value.bytes
    return raw[:8][::-1] + raw[8:][::-1]


def var_uint(value: int) -> bytes:
    """Unsigned base-128 framing; callers own policy/coercion."""
    output = bytearray()
    current = value
    while current >= 0x80:
        output.append((current & 0x7F) | 0x80)
        current >>= 7
    output.append(current)
    return bytes(output)
