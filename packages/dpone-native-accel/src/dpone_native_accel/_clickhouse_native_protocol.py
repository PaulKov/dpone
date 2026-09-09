"""Small ClickHouse Native protocol byte helpers."""

from __future__ import annotations

import getpass
import struct
from typing import BinaryIO


def block_info() -> bytes:
    return var_uint(1) + uint8(0) + var_uint(2) + int32(-1) + var_uint(0)


def format_table(table: str) -> str:
    return ".".join(quote_identifier(part) for part in table.split("."))


def quote_identifier(value: str) -> str:
    text = value.strip()
    if text.startswith("`") and text.endswith("`"):
        return text
    return "`" + text.replace("`", "``") + "`"


def write_string(stream: BinaryIO, value: str) -> None:
    payload = value.encode("utf-8")
    stream.write(var_uint(len(payload)))
    stream.write(payload)


def read_var_uint(payload: bytes, offset: int) -> tuple[int, int]:
    shift = 0
    value = 0
    current = offset
    while current < len(payload):
        byte = payload[current]
        current += 1
        value |= (byte & 0x7F) << shift
        if byte < 0x80:
            return value, current
        shift += 7
    raise ValueError("clickhouse_native_invalid_varuint")


def var_uint(value: int) -> bytes:
    output = bytearray()
    current = int(value)
    while current >= 0x80:
        output.append((current & 0x7F) | 0x80)
        current >>= 7
    output.append(current)
    return bytes(output)


def uint8(value: int) -> bytes:
    return struct.pack("<B", int(value))


def uint64(value: int) -> bytes:
    return struct.pack("<Q", int(value))


def uint128(value: int) -> bytes:
    upper = (int(value) >> 64) & ((1 << 64) - 1)
    lower = int(value) & ((1 << 64) - 1)
    return struct.pack("<QQ", upper, lower)


def int32(value: int) -> bytes:
    return struct.pack("<i", int(value))


def flush(stream: BinaryIO) -> None:
    stream_flush = getattr(stream, "flush", None)
    if callable(stream_flush):
        stream_flush()


def safe_user() -> str:
    try:
        return getpass.getuser()
    except (KeyError, OSError):
        return ""
