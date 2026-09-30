"""Shared ClickHouse binary value encoding helpers."""

from __future__ import annotations

import re
import struct
import uuid
from base64 import b64encode
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from datetime import time as dt_time
from decimal import Decimal
from typing import Any

from dpone.contracts import clickhouse_scalar_wire as wire
from dpone.runtime.support.type_mapping.mssql_clickhouse import (
    MssqlClickHouseTypeMapper,
    MssqlClickHouseTypePolicy,
    clickhouse_type_for_mssql,
)

_EPOCH_DATE = date(1970, 1, 1)
_EPOCH_DATETIME = datetime(1970, 1, 1, tzinfo=UTC)
# Preserve historical imports while depending on one cohesive wire boundary.
encode_temporal_integer = wire.encode_temporal_integer
_decimal_precision_scale = wire._decimal_precision_scale


@dataclass(frozen=True, slots=True)
class ClickHouseColumnSpec:
    """Resolved source and ClickHouse type pair for binary encoders."""

    name: str
    source_type: str
    clickhouse_type: str


def resolve_columns(
    schema: Sequence[tuple[str, str]],
    *,
    schema_kind: str = "mssql",
    type_policy: MssqlClickHouseTypePolicy | None = None,
    target_schema: Sequence[tuple[str, str]] | None = None,
) -> tuple[ClickHouseColumnSpec, ...]:
    """Resolve source schema to ClickHouse binary column specs."""

    if target_schema is not None:
        if len(schema) != len(target_schema):
            raise ValueError("source schema and target_schema must have the same column count")
        return tuple(
            ClickHouseColumnSpec(str(name), str(source_type), str(target_type))
            for (name, source_type), (_, target_type) in zip(schema, target_schema, strict=True)
        )
    return tuple(
        ClickHouseColumnSpec(str(name), str(dtype), _clickhouse_type(name, dtype, schema_kind, type_policy))
        for name, dtype in schema
    )


def encode_clickhouse_value(
    value: Any,
    clickhouse_type: str,
    source_type: str,
    policy: MssqlClickHouseTypePolicy,
) -> bytes:
    """Encode one ClickHouse binary value including Nullable prefix."""

    nullable, inner_type = unwrap_nullable(clickhouse_type)
    if nullable:
        if value is None:
            return b"\x01"
        return b"\x00" + encode_clickhouse_non_null(value, inner_type, source_type, policy)
    if value is None:
        raise ValueError(f"NULL cannot be encoded for non-nullable ClickHouse type {clickhouse_type!r}.")
    return encode_clickhouse_non_null(value, inner_type, source_type, policy)


def encode_clickhouse_non_null(
    value: Any,
    clickhouse_type: str,
    source_type: str,
    policy: MssqlClickHouseTypePolicy,
) -> bytes:
    """Encode one non-null ClickHouse binary value."""

    root = root_type(clickhouse_type)
    if root == "bool":
        return struct.pack("<B", 1 if _bool(value) else 0)
    if root in {"int8", "uint8", "int16", "uint16", "int32", "uint32", "int64", "uint64"}:
        return _pack_integer(_integer_value(value), root)
    if root == "float32":
        return struct.pack("<f", float(value))
    if root == "float64":
        return struct.pack("<d", float(value))
    if root == "string":
        return encode_clickhouse_string(_string_value(value, source_type, policy))
    if root == "fixedstring":
        return _encode_fixed_string(_string_value(value, source_type, policy), clickhouse_type)
    if root == "uuid":
        return encode_clickhouse_uuid(value)
    if root == "date":
        return encode_temporal_integer((_date(value) - _EPOCH_DATE).days, root)
    if root == "date32":
        return encode_temporal_integer((_date(value) - _EPOCH_DATE).days, root)
    if root == "datetime":
        return encode_temporal_integer(_datetime_seconds(value), root)
    if root == "datetime64":
        return encode_temporal_integer(_datetime64_ticks(value, scale(clickhouse_type)), root, scale(clickhouse_type))
    if root.startswith("decimal"):
        return _encode_decimal(value, clickhouse_type)
    raise ValueError(f"ClickHouse binary type is not supported yet: {clickhouse_type!r}.")


def default_non_null_value(clickhouse_type: str) -> Any:
    """Return a deterministic nested value for NULL cells in Native Nullable columns."""

    root = root_type(clickhouse_type)
    if root in {"string", "fixedstring", "uuid"}:
        return "" if root != "uuid" else "00000000-0000-0000-0000-000000000000"
    if root in {"date", "date32"}:
        return _EPOCH_DATE
    if root in {"datetime", "datetime64"}:
        return _EPOCH_DATETIME
    if root == "bool":
        return False
    return 0


def encode_clickhouse_string(value: Any) -> bytes:
    """Encode ClickHouse length-prefixed binary string."""

    if isinstance(value, bytes):
        payload = value
    elif isinstance(value, bytearray):
        payload = bytes(value)
    else:
        payload = str(value).encode("utf-8")
    return wire.encode_string(payload)


def encode_clickhouse_uuid(value: Any) -> bytes:
    return wire.encode_uuid(uuid.UUID(str(value)))


def var_uint(value: int) -> bytes:
    return wire.var_uint(int(value))


def unwrap_nullable(clickhouse_type: str) -> tuple[bool, str]:
    value = str(clickhouse_type).strip()
    if "lowcardinality(" in value.lower():
        raise ValueError(
            "ClickHouse LowCardinality Native encoding is not supported; "
            "use the underlying scalar target type or a certified dictionary encoder."
        )
    if value.lower().startswith("nullable(") and value.endswith(")"):
        return True, value[len("Nullable(") : -1].strip()
    return False, value


def root_type(clickhouse_type: str) -> str:
    return str(clickhouse_type).strip().split("(", 1)[0].strip().lower()


def scale(clickhouse_type: str) -> int:
    match = re.search(r"\(\s*(\d+)", clickhouse_type)
    return min(max(int(match.group(1)), 0), 9) if match else 0


def _clickhouse_type(
    name: str,
    dtype: str,
    schema_kind: str,
    type_policy: MssqlClickHouseTypePolicy | None,
) -> str:
    if schema_kind == "clickhouse":
        return str(dtype)
    if schema_kind != "mssql":
        raise ValueError(f"Unsupported ClickHouse binary schema_kind: {schema_kind}")
    if type_policy is not None:
        return MssqlClickHouseTypeMapper(type_policy).resolve_column(str(name), str(dtype)).clickhouse_type
    return clickhouse_type_for_mssql(dtype)


def _integer_value(value: Any) -> Any:
    if isinstance(value, dt_time):
        return value.hour * 3600 + value.minute * 60 + value.second
    return value


def _string_value(value: Any, source_type: str, policy: MssqlClickHouseTypePolicy) -> Any:
    if _is_binary_source(source_type) and isinstance(value, bytes | bytearray):
        raw = bytes(value)
        if policy.binary_encoding == "base64":
            return b64encode(raw).decode("ascii")
        if policy.binary_encoding == "hex":
            return raw.hex()
    if _is_datetimeoffset_source(source_type) and isinstance(value, datetime):
        return _datetimeoffset_text(value, source_type)
    if _is_time_source(source_type) and isinstance(value, dt_time):
        return _time_text(value, source_type)
    return value


def _is_binary_source(source_type: str) -> bool:
    normalized = str(source_type).strip().lower()
    return any(token in normalized for token in ("binary", "varbinary", "rowversion", "timestamp", "image"))


def _is_datetimeoffset_source(source_type: str) -> bool:
    return str(source_type).strip().lower().startswith("datetimeoffset")


def _is_time_source(source_type: str) -> bool:
    normalized = re.sub(r"\s+nullable\b", "", str(source_type).strip().lower())
    return normalized == "time" or normalized.startswith("time(")


def _datetimeoffset_text(value: datetime, source_type: str) -> str:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None:
        raise ValueError("MSSQL datetimeoffset text encoding requires an offset-aware value.")
    match = re.search(r"\((\d+)\)", str(source_type))
    source_scale = min(max(int(match.group(1)), 0), 7) if match else 7
    ticks = value.microsecond * 10
    ticks += int(getattr(value, "submicrosecond_100ns", 0))
    fractional = f".{ticks:07d}"[: source_scale + 1] if source_scale else ""
    offset_minutes = int(offset.total_seconds() // 60)
    sign = "+" if offset_minutes >= 0 else "-"
    offset_hours, offset_remainder = divmod(abs(offset_minutes), 60)
    return f"{value:%Y-%m-%d %H:%M:%S}{fractional} {sign}{offset_hours:02d}:{offset_remainder:02d}"


def _time_text(value: dt_time, source_type: str) -> str:
    match = re.search(r"\((\d+)\)", str(source_type))
    source_scale = min(max(int(match.group(1)), 0), 7) if match else 7
    ticks = value.microsecond * 10 + int(getattr(value, "submicrosecond_100ns", 0))
    fractional = f".{ticks:07d}"[: source_scale + 1] if source_scale else ""
    return f"{value:%H:%M:%S}{fractional}"


def _pack_integer(value: Any, root: str) -> bytes:
    return wire.pack_integer(int(value), root)


def _encode_decimal(value: Any, clickhouse_type: str) -> bytes:
    # Preserve declaration validation before source-value coercion.
    _decimal_precision_scale(clickhouse_type)
    decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    return wire.encode_decimal(decimal_value, clickhouse_type)


def _encode_fixed_string(value: Any, clickhouse_type: str) -> bytes:
    length = _fixed_string_length(clickhouse_type)
    payload = value if isinstance(value, bytes | bytearray) else str(value).encode("utf-8")
    raw = bytes(payload)
    return wire.encode_fixed_string(raw, length)


def _fixed_string_length(clickhouse_type: str) -> int:
    match = re.search(r"\((\d+)\)", clickhouse_type)
    if not match:
        raise ValueError(f"FixedString length is required: {clickhouse_type!r}.")
    return int(match.group(1))


def _datetime64_ticks(value: Any, clickhouse_scale: int) -> int:
    dt = _datetime(value)
    delta = dt - _EPOCH_DATETIME
    seconds = delta.days * 86400 + delta.seconds
    fractional = (
        dt.microsecond * (10 ** (clickhouse_scale - 6))
        if clickhouse_scale >= 6
        else dt.microsecond // (10 ** (6 - clickhouse_scale))
    )
    if clickhouse_scale >= 7:
        fractional += int(getattr(value, "submicrosecond_100ns", 0)) * (10 ** (clickhouse_scale - 7))
    return seconds * (10**clickhouse_scale) + fractional


def _datetime_seconds(value: Any) -> int:
    delta = _datetime(value) - _EPOCH_DATETIME
    return delta.days * 86400 + delta.seconds


def _datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = datetime(
            value.year,
            value.month,
            value.day,
            value.hour,
            value.minute,
            value.second,
            value.microsecond,
            tzinfo=value.tzinfo,
            fold=value.fold,
        )
    elif isinstance(value, date):
        dt = datetime(value.year, value.month, value.day)
    else:
        dt = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def _date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value).strip()[:10])


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def validate_clickhouse_value_fidelity(value: Any, clickhouse_type: str) -> None:
    """Reject lossy scalar coercions in opt-in bounded typed streams.

    Legacy unbounded encoders retain their historical integer and timestamp
    truncation behavior. Decimal declarations and precision use the canonical
    encoder parser for both paths. The shared temporal encoder also enforces ClickHouse calendar ranges before
    transmission.
    """
    if value is None:
        return
    _, inner = unwrap_nullable(clickhouse_type)
    root = root_type(inner)
    if root in {"int8", "uint8", "int16", "uint16", "int32", "uint32", "int64", "uint64"}:
        converted = _integer_value(value)
        if isinstance(value, dt_time) and (value.microsecond or getattr(value, "submicrosecond_100ns", 0)):
            raise ValueError("ClickHouse integer conversion would lose temporal precision")
        if isinstance(converted, float | Decimal) and converted != int(converted):
            raise ValueError("ClickHouse integer conversion would lose precision")
    if root in {"date", "date32", "datetime", "datetime64"}:
        dt = _datetime(value)
        precision = 0
        if root == "datetime64":
            match = re.fullmatch(r"DateTime64\(\s*([0-9])(?:\s*,\s*'[^']+')?\s*\)", inner, re.IGNORECASE)
            if match is None:
                raise ValueError("ClickHouse DateTime64 precision must be between zero and nine")
            precision = scale(inner)
        extra = getattr(value, "submicrosecond_100ns", 0)
        if isinstance(extra, bool) or not isinstance(extra, int) or not 0 <= extra <= 9:
            raise ValueError("ClickHouse timestamp submicrosecond precision is invalid")
        if precision < 6 and dt.microsecond % (10 ** (6 - precision)):
            raise ValueError("ClickHouse timestamp conversion would lose precision")
        if precision < 7 and extra:
            raise ValueError("ClickHouse timestamp conversion would lose precision")
        if isinstance(value, str):
            fractional = re.search(r"[T ]\d{2}:\d{2}:\d{2}\.(\d+)", value)
            if fractional and any(char != "0" for char in fractional[1][min(precision, 6) :]):
                raise ValueError("ClickHouse timestamp text conversion would lose precision")
        if root in {"date", "date32"} and (dt.hour or dt.minute or dt.second):
            raise ValueError("ClickHouse date conversion would lose temporal precision")
