"""Exact numeric conversion for native source payloads and target cell widths."""

from __future__ import annotations

import math
import re
import struct
from typing import Protocol


class NumericColumn(Protocol):
    @property
    def storage_type(self) -> str: ...
    @property
    def scale(self) -> int | None: ...


_FORMATS = {
    "bool": "B",
    "int8": "b",
    "uint8": "B",
    "int16": "h",
    "uint16": "H",
    "int32": "i",
    "uint32": "I",
    "int64": "q",
    "uint64": "Q",
    "float32": "f",
    "float64": "d",
}
_SOURCE_FORMATS = {"bit": "B", "tinyint": "B", "smallint": "h", "int": "i", "bigint": "q", "real": "f"}


def encode_numeric(payload: bytes, column: NumericColumn, target_root: str) -> bytes:
    """Encode the target width, rejecting range and precision loss instead of passthrough."""
    source = _SOURCE_FORMATS.get(column.storage_type)
    if column.storage_type == "float":
        source = "f" if len(payload) == 4 else "d"
    if source is None or target_root not in _FORMATS:
        raise ValueError("native_acceleration_unsupported_numeric_mapping")
    value = struct.unpack("<" + source, payload)[0]
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("native_wire_invalid_float")
    candidate = float(value) if target_root.startswith("float") else int(value)
    if candidate != value or (target_root == "bool" and candidate not in (0, 1)):
        raise ValueError("native_acceleration_numeric_precision_loss")
    try:
        result = struct.pack("<" + _FORMATS[target_root], candidate)
    except (struct.error, OverflowError) as error:
        raise ValueError("native_acceleration_numeric_out_of_range") from error
    if struct.unpack("<" + _FORMATS[target_root], result)[0] != value:
        raise ValueError("native_acceleration_numeric_precision_loss")
    return result


def encode_decimal(payload: bytes, column: NumericColumn, target: str) -> bytes:
    """Rescale exact signed magnitudes; precision and scale loss are hard failures."""
    match = re.fullmatch(r"Decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)", target, re.IGNORECASE)
    if not match:
        raise ValueError("native_acceleration_unsupported_decimal_target")
    precision, scale = map(int, match.groups())
    if not 1 <= precision <= 76 or not 0 <= scale <= precision:
        raise ValueError("native_acceleration_invalid_decimal_target")
    storage = column.storage_type
    if storage == "money":
        value = (int.from_bytes(payload[:4], "little", signed=True) << 32) + int.from_bytes(payload[4:], "little")
        source_scale = 4
    elif storage == "smallmoney":
        value, source_scale = struct.unpack("<i", payload)[0], 4
    elif storage in {"decimal", "numeric"}:
        value = int.from_bytes(payload[3:], "little") * (1 if payload[2] else -1)
        source_scale = payload[1]
    else:
        raise ValueError("native_acceleration_unsupported_decimal_source")
    if scale >= source_scale:
        value *= 10 ** (scale - source_scale)
    else:
        divisor = 10 ** (source_scale - scale)
        if value % divisor:
            raise ValueError("native_acceleration_decimal_precision_loss")
        value //= divisor
    if abs(value) >= 10**precision:
        raise ValueError("native_acceleration_decimal_out_of_range")
    width = 4 if precision <= 9 else 8 if precision <= 18 else 16 if precision <= 38 else 32
    return value.to_bytes(width, "little", signed=True)
