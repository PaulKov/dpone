"""Pure length-prefixed UTF-8 host codec for direct native SQL Server BCP."""

from __future__ import annotations

import re
import struct
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import BinaryIO

_PREFIX = struct.Struct("<i")
_TYPE = re.compile(r"^\s*([a-z0-9 ]+?)(?:\s*\(\s*(max|\d+)(?:\s*,\s*\d+)?\s*\))?\s*$", re.I)
_TEXT_TYPES = frozenset({"char", "nchar", "ntext", "nvarchar", "text", "varchar"})


class MssqlTypedBcpError(ValueError):
    """Stable local host-codec failure before vendor BCP starts."""

    def __init__(self, code: str) -> None:
        self.code = f"mssql_typed_bcp.{code}"
        super().__init__(self.code)


def write_length_prefixed_row(handle: BinaryIO, values: tuple[object | None, ...]) -> int:
    """Write one delimiter-free BCP record and return its exact byte size."""

    written = 0
    for value in values:
        payload = _bcp_text(value)
        if payload is None:
            handle.write(_PREFIX.pack(-1))
            written += _PREFIX.size
            continue
        if len(payload) > 2**31 - 1:
            raise MssqlTypedBcpError("field_too_large")
        handle.write(_PREFIX.pack(len(payload)))
        handle.write(payload)
        written += _PREFIX.size + len(payload)
    return written


def write_format_file(path: Path, target_types: tuple[str, ...]) -> None:
    """Write one deterministic non-XML BCP format with four-byte prefixes."""

    lines = ["14.0", str(len(target_types))]
    for position, target_type in enumerate(target_types, start=1):
        base, declared = _type_parts(target_type)
        maximum = _host_max_length(base, declared)
        collation = "SQL_Latin1_General_CP1_CI_AS" if base in _TEXT_TYPES else '""'
        lines.append(f'{position}\tSQLCHAR\t4\t{maximum}\t""\t{position}\tcolumn_{position}\t{collation}')
    path.write_text("\n".join(lines) + "\n", encoding="ascii")


def _bcp_text(value: object | None) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return b"1" if value else b"0"
    if isinstance(value, bytes):
        return value.hex().encode("ascii")
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="microseconds").encode("ascii")
    if isinstance(value, time):
        return value.isoformat(timespec="microseconds").encode("ascii")
    if isinstance(value, date):
        return value.isoformat().encode("ascii")
    if isinstance(value, Decimal):
        return str(value).encode("ascii")
    if isinstance(value, float):
        return repr(value).encode("ascii")
    if isinstance(value, int):
        return str(value).encode("ascii")
    if isinstance(value, str):
        return value.encode("utf-8")
    raise MssqlTypedBcpError("value_type_unsupported")


def _type_parts(target_type: str) -> tuple[str, str | None]:
    match = _TYPE.fullmatch(" ".join(str(target_type).lower().split()))
    if match is None:
        raise MssqlTypedBcpError("target_type_invalid")
    return match.group(1).strip(), match.group(2)


def _host_max_length(base: str, declared: str | None) -> int:
    if base in {"tinyint", "smallint", "int", "bigint"}:
        return 20
    if base == "bit":
        return 1
    if base in {"decimal", "numeric", "money", "smallmoney"}:
        return 50
    if base in {"real", "float"}:
        return 32
    if base == "uniqueidentifier":
        return 36
    if base == "date":
        return 10
    if base in {"datetime", "datetime2", "datetimeoffset", "smalldatetime", "time"}:
        return 40
    if base in {"binary", "varbinary", "image"}:
        return 0 if declared is None or declared == "max" else int(declared) * 2
    if base in _TEXT_TYPES:
        return 0 if declared is None or declared == "max" or base in {"text", "ntext"} else int(declared) * 4
    raise MssqlTypedBcpError("target_type_unsupported")


__all__ = ["MssqlTypedBcpError", "write_format_file", "write_length_prefixed_row"]
