"""Lossless pyodbc converter for SQL Server ``datetimeoffset`` values."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from struct import Struct
from struct import error as StructError

SQL_SS_TIMESTAMPOFFSET = -155
_DATETIMEOFFSET_STRUCT = Struct("<6hI2h")


class MSSQLDatetimeOffsetDecodeError(ValueError):
    """Raised when an ODBC ``SQL_SS_TIMESTAMPOFFSET`` payload is malformed."""

    code = "DPONE_MSSQL_DATETIMEOFFSET_DECODE_FAILED"

    def __init__(self, reason: str) -> None:
        super().__init__(f"{self.code}: {reason}")


def decode_datetimeoffset(raw: bytes) -> datetime:
    """Decode Microsoft's 20-byte ``SQL_SS_TIMESTAMPOFFSET_STRUCT``.

    Python cannot represent SQL Server's seventh 100-ns fractional digit.
    Such values fail closed instead of being silently rounded or truncated.
    """

    if not isinstance(raw, bytes | bytearray | memoryview):
        raise MSSQLDatetimeOffsetDecodeError(f"expected bytes, got {type(raw).__name__}")
    payload = bytes(raw)
    if len(payload) != _DATETIMEOFFSET_STRUCT.size:
        raise MSSQLDatetimeOffsetDecodeError(f"expected {_DATETIMEOFFSET_STRUCT.size} bytes, got {len(payload)}")
    try:
        year, month, day, hour, minute, second, fraction_ns, offset_hours, offset_minutes = (
            _DATETIMEOFFSET_STRUCT.unpack(payload)
        )
    except StructError as exc:  # pragma: no cover - length guard owns normal failures
        raise MSSQLDatetimeOffsetDecodeError("invalid binary struct") from exc
    _validate_fraction(fraction_ns)
    offset = _timezone(offset_hours, offset_minutes)
    try:
        return datetime(
            year,
            month,
            day,
            hour,
            minute,
            second,
            fraction_ns // 1_000,
            tzinfo=offset,
        )
    except ValueError as exc:
        raise MSSQLDatetimeOffsetDecodeError(str(exc)) from exc


def _validate_fraction(fraction_ns: int) -> None:
    if fraction_ns > 999_999_999:
        raise MSSQLDatetimeOffsetDecodeError(f"fraction nanoseconds out of range: {fraction_ns}")
    if fraction_ns % 100:
        raise MSSQLDatetimeOffsetDecodeError(f"fraction is not at SQL Server 100ns resolution: {fraction_ns}")
    if fraction_ns % 1_000:
        raise MSSQLDatetimeOffsetDecodeError(
            f"fraction is not representable by Python datetime microseconds: {fraction_ns}"
        )


def _timezone(offset_hours: int, offset_minutes: int) -> timezone:
    if not -14 <= offset_hours <= 14 or not -59 <= offset_minutes <= 59:
        raise MSSQLDatetimeOffsetDecodeError(f"offset out of range: hours={offset_hours}, minutes={offset_minutes}")
    if offset_hours and offset_minutes and (offset_hours > 0) != (offset_minutes > 0):
        raise MSSQLDatetimeOffsetDecodeError(f"offset sign mismatch: hours={offset_hours}, minutes={offset_minutes}")
    offset = timedelta(hours=offset_hours, minutes=offset_minutes)
    if abs(offset) > timedelta(hours=14):
        raise MSSQLDatetimeOffsetDecodeError(
            f"offset exceeds SQL Server range: hours={offset_hours}, minutes={offset_minutes}"
        )
    return timezone(offset)


__all__ = ["MSSQLDatetimeOffsetDecodeError", "SQL_SS_TIMESTAMPOFFSET", "decode_datetimeoffset"]
