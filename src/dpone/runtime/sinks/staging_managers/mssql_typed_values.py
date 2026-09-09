"""Pure MSSQL wire-to-native scalar decoding for typed file ingestion."""

from __future__ import annotations

import math
import re
import struct
import sys
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation, localcontext
from uuid import UUID

from dpone.runtime.support.bulk_text_codec import BulkTextCodec

_TYPE = re.compile(r"^\s*([a-z0-9 ]+?)(?:\s*\(\s*(max|-?\d+)(?:\s*,\s*(-?\d+))?\s*\))?\s*$", re.I)
_INTEGER_BOUNDS = {
    "tinyint": (0, 255),
    "smallint": (-(2**15), 2**15 - 1),
    "int": (-(2**31), 2**31 - 1),
    "bigint": (-(2**63), 2**63 - 1),
}
_SUPPORTED_BASES = frozenset(
    {
        *_INTEGER_BOUNDS,
        "binary",
        "bit",
        "char",
        "date",
        "datetime",
        "datetime2",
        "datetimeoffset",
        "decimal",
        "float",
        "image",
        "money",
        "nchar",
        "ntext",
        "numeric",
        "nvarchar",
        "real",
        "smalldatetime",
        "smallmoney",
        "text",
        "time",
        "uniqueidentifier",
        "varbinary",
        "varchar",
    }
)


class MssqlTypedValueError(ValueError):
    """Stable pre-business-DML failure for one invalid typed wire scalar."""

    def __init__(self, code: str, *, column: str) -> None:
        self.code = f"mssql_typed_ingest.{code}"
        self.column = column
        super().__init__(f"{self.code}:{column}")


class MssqlTypedValueDecoder:
    """Decode the certified PostgreSQL text wire into DB-API values."""

    def __init__(self, codec: BulkTextCodec) -> None:
        self._codec = codec

    def decode(self, raw: bytes, *, column: str, target_type: str) -> object | None:
        require_typed_target_type(target_type, column=column)
        if raw == b"":
            return None
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise MssqlTypedValueError("utf8_invalid", column=column) from exc
        base, first, second = _type_parts(target_type, column=column)
        try:
            if base in {"nvarchar", "nchar", "ntext"}:
                return _unicode_text(self._codec.decode(text), base, first, column=column)
            if base in {"varchar", "char", "text"}:
                return _ansi_text(self._codec.decode(text), base, first, column=column)
            if base in _INTEGER_BOUNDS:
                return _integer(text, base, column=column)
            if base == "bit":
                return _bit(text, column=column)
            if base in {"decimal", "numeric"}:
                return _decimal(text, int(first or 18), int(second or 0), column=column)
            if base in {"money", "smallmoney"}:
                return _money(text, small=base == "smallmoney", column=column)
            if base in {"real", "float"}:
                return _floating(
                    text,
                    real=base == "real" or (base == "float" and first is not None and int(first) <= 24),
                    column=column,
                )
            if base == "uniqueidentifier":
                return str(UUID(text))
            if base in {"binary", "varbinary", "image"}:
                decoded = self._codec.decode(text)
                return _binary(decoded, base, first, column=column)
            if base == "date":
                return date.fromisoformat(text)
            if base in {"datetime2", "datetime", "smalldatetime"}:
                timestamp_value = datetime.fromisoformat(text)
                if timestamp_value.tzinfo is not None:
                    raise ValueError("timezone is not allowed")
                _require_temporal_scale(timestamp_value.microsecond, _scale(base, first), column=column)
                _require_legacy_datetime_exact(timestamp_value, base=base, column=column)
                return timestamp_value
            if base == "datetimeoffset":
                offset_value = datetime.fromisoformat(text.replace("Z", "+00:00"))
                if offset_value.tzinfo is None or offset_value.utcoffset() is None:
                    raise ValueError("timezone is required")
                _require_temporal_scale(offset_value.microsecond, _scale(base, first), column=column)
                _require_datetimeoffset_exact(offset_value, column=column)
                # Length-prefixed BCP consumes canonical ISO text and performs
                # the native datetimeoffset conversion while bulk loading.
                return offset_value.isoformat(sep=" ", timespec="microseconds")
            if base == "time":
                time_value = time.fromisoformat(text)
                if time_value.tzinfo is not None:
                    raise ValueError("timezone is not allowed")
                _require_temporal_scale(time_value.microsecond, _scale(base, first), column=column)
                # Preserve all six source microseconds in the character host
                # field consumed by native BCP conversion.
                return time_value.isoformat(timespec="microseconds")
        except MssqlTypedValueError:
            raise
        except (InvalidOperation, OverflowError, TypeError, ValueError) as exc:
            raise MssqlTypedValueError("value_invalid", column=column) from exc
        raise MssqlTypedValueError("target_type_unsupported", column=column)


def require_typed_target_type(target_type: str, *, column: str) -> None:
    """Reject an unsupported native target before staging is created."""

    base, first, second = _type_parts(target_type, column=column)
    if base not in _SUPPORTED_BASES:
        raise MssqlTypedValueError("target_type_unsupported", column=column)
    sized_limits = {
        "binary": 8_000,
        "char": 8_000,
        "nchar": 4_000,
        "nvarchar": 4_000,
        "varbinary": 8_000,
        "varchar": 8_000,
    }
    if base in sized_limits:
        if first is None or (first == "max" and base in {"binary", "char", "nchar"}):
            raise MssqlTypedValueError("target_type_invalid", column=column)
        if first != "max" and not 1 <= int(first) <= sized_limits[base]:
            raise MssqlTypedValueError("target_type_invalid", column=column)
    elif base in {"decimal", "numeric", "datetime2", "datetimeoffset", "time", "float"}:
        pass
    elif first is not None or second is not None:
        raise MssqlTypedValueError("target_type_invalid", column=column)
    if base in {"decimal", "numeric"}:
        if first == "max":
            raise MssqlTypedValueError("target_type_invalid", column=column)
        precision = int(first or 18)
        scale = int(second or 0)
        if precision < 1 or precision > 38 or not 0 <= scale <= precision:
            raise MssqlTypedValueError("target_type_invalid", column=column)
    if base in {"datetime2", "datetimeoffset", "time"}:
        if first == "max":
            raise MssqlTypedValueError("target_type_invalid", column=column)
        _require_temporal_scale(0, _scale(base, first), column=column)
    if base == "float" and first is not None:
        if first == "max":
            raise MssqlTypedValueError("target_type_invalid", column=column)
        bits = int(first)
        if not 1 <= bits <= 53:
            raise MssqlTypedValueError("target_type_invalid", column=column)


def _type_parts(target_type: str, *, column: str) -> tuple[str, str | None, str | None]:
    normalized = " ".join(str(target_type).strip().lower().split())
    match = _TYPE.fullmatch(normalized)
    if match is None:
        raise MssqlTypedValueError("target_type_invalid", column=column)
    return match.group(1).strip(), match.group(2), match.group(3)


def _unicode_text(value: str, base: str, length: str | None, *, column: str) -> str:
    if "\x00" in value:
        raise MssqlTypedValueError("text_nul_unsupported", column=column)
    if length is not None and length != "max":
        units = len(value.encode("utf-16le")) // 2
        if units > int(length):
            raise MssqlTypedValueError("value_too_long", column=column)
        if base == "nchar" and units != int(length):
            raise MssqlTypedValueError("fixed_width_padding_loss", column=column)
    if base == "nchar" and length is None:
        raise MssqlTypedValueError("target_type_invalid", column=column)
    return value


def _ansi_text(value: str, base: str, length: str | None, *, column: str) -> str:
    # Without the exact target collation code page, non-ASCII conversion cannot
    # be proven lossless. PostgreSQL's default mapping is Unicode, so this is a
    # fail-closed explicit-override boundary rather than a silent fallback.
    if any(ord(character) > 127 or character == "\x00" for character in value):
        raise MssqlTypedValueError("ansi_text_unverifiable", column=column)
    if length is not None and length != "max" and len(value) > int(length):
        raise MssqlTypedValueError("value_too_long", column=column)
    if base == "char" and length is not None and len(value) != int(length):
        raise MssqlTypedValueError("fixed_width_padding_loss", column=column)
    if base == "char" and length is None:
        raise MssqlTypedValueError("target_type_invalid", column=column)
    return value


def _integer(value: str, base: str, *, column: str) -> int:
    if re.fullmatch(r"-?(?:0|[1-9][0-9]*)", value) is None:
        raise MssqlTypedValueError("value_invalid", column=column)
    parsed = int(value)
    lower, upper = _INTEGER_BOUNDS[base]
    if not lower <= parsed <= upper:
        raise MssqlTypedValueError("value_out_of_range", column=column)
    return parsed


def _bit(value: str, *, column: str) -> bool:
    if value not in {"0", "1"}:
        raise MssqlTypedValueError("value_invalid", column=column)
    return value == "1"


def _decimal(value: str, precision: int, scale: int, *, column: str) -> Decimal:
    parsed = Decimal(value)
    if not parsed.is_finite() or precision < 1 or not 0 <= scale <= precision:
        raise MssqlTypedValueError("value_invalid", column=column)
    quantum = Decimal(1).scaleb(-scale)
    decimal_tuple = parsed.as_tuple()
    exponent = decimal_tuple.exponent
    if not isinstance(exponent, int):
        raise MssqlTypedValueError("value_invalid", column=column)
    digits = len(decimal_tuple.digits)
    with localcontext() as context:
        context.prec = max(precision + 2, digits + abs(exponent) + 2)
        quantized = parsed.quantize(quantum)
        out_of_range = abs(quantized) >= Decimal(10) ** (precision - scale)
    if quantized != parsed or out_of_range:
        raise MssqlTypedValueError("value_out_of_range", column=column)
    return quantized


def _money(value: str, *, small: bool, column: str) -> Decimal:
    parsed = _decimal(value, 10 if small else 19, 4, column=column)
    lower, upper = (
        (Decimal("-214748.3648"), Decimal("214748.3647"))
        if small
        else (Decimal("-922337203685477.5808"), Decimal("922337203685477.5807"))
    )
    if not lower <= parsed <= upper:
        raise MssqlTypedValueError("value_out_of_range", column=column)
    return parsed


def _floating(value: str, *, real: bool, column: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or (parsed == 0.0 and value.lstrip().startswith("-")):
        raise MssqlTypedValueError("value_lossy", column=column)
    if real:
        try:
            projected = struct.unpack("!f", struct.pack("!f", parsed))[0]
        except OverflowError as exc:
            raise MssqlTypedValueError("value_out_of_range", column=column) from exc
        if not math.isfinite(projected) or (parsed != 0.0 and projected == 0.0):
            raise MssqlTypedValueError("value_lossy", column=column)
        return projected
    if 0.0 < abs(parsed) < sys.float_info.min:
        # SQL Server's character-to-float conversion collapses float(53)
        # subnormals even though the native IEEE storage could represent them.
        raise MssqlTypedValueError("value_lossy", column=column)
    return parsed


def _binary(value: str, base: str, length: str | None, *, column: str) -> bytes:
    if len(value) % 2 or re.fullmatch(r"[0-9a-fA-F]*", value) is None:
        raise MssqlTypedValueError("binary_hex_invalid", column=column)
    parsed = bytes.fromhex(value)
    if length is not None and length != "max" and len(parsed) > int(length):
        raise MssqlTypedValueError("value_too_long", column=column)
    if base == "binary" and length is not None and len(parsed) != int(length):
        raise MssqlTypedValueError("fixed_width_padding_loss", column=column)
    return parsed


def _require_legacy_datetime_exact(value: datetime, *, base: str, column: str) -> None:
    if base == "datetime":
        if value < datetime(1753, 1, 1) or value.microsecond % 10_000:
            raise MssqlTypedValueError("value_lossy", column=column)
    elif base == "smalldatetime":
        if not datetime(1900, 1, 1) <= value <= datetime(2079, 6, 6, 23, 59):
            raise MssqlTypedValueError("value_out_of_range", column=column)
        if value.second or value.microsecond:
            raise MssqlTypedValueError("value_lossy", column=column)


def _require_datetimeoffset_exact(value: datetime, *, column: str) -> None:
    offset = value.utcoffset()
    if offset is None or offset % timedelta(minutes=1) or abs(offset) > timedelta(hours=14):
        raise MssqlTypedValueError("value_out_of_range", column=column)
    try:
        value.astimezone(timezone.utc)  # noqa: UP017 - project mypy target predates datetime.UTC
    except (OverflowError, ValueError) as exc:
        raise MssqlTypedValueError("value_out_of_range", column=column) from exc


def _scale(base: str, declared: str | None) -> int:
    defaults = {"datetime": 3, "smalldatetime": 0, "datetime2": 7, "datetimeoffset": 7, "time": 7}
    return int(declared) if declared is not None else defaults[base]


def _require_temporal_scale(microsecond: int, scale: int, *, column: str) -> None:
    if not 0 <= scale <= 7:
        raise MssqlTypedValueError("target_type_invalid", column=column)
    if scale < 6 and microsecond % (10 ** (6 - scale)):
        raise MssqlTypedValueError("value_lossy", column=column)


__all__ = [
    "MssqlTypedValueDecoder",
    "MssqlTypedValueError",
    "require_typed_target_type",
]
