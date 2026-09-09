"""MSSQL bcp native wire layout and decoder."""

from __future__ import annotations

import re
import struct
import uuid
from collections.abc import Iterator, Sequence
from datetime import UTC, date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, BinaryIO

from dpone.runtime.native_wire_models import (
    NATIVE_WIRE_SCHEMA_VERSION,
    NativeWireColumnLayout,
    SourceNativeWireContract,
    stable_hash,
)
from dpone.runtime.support.type_mapping.mssql_clickhouse import MssqlClickHouseTypeMapper, MssqlClickHouseTypePolicy

_SQL_SERVER_EPOCH = date(1900, 1, 1)
_DATETIME2_EPOCH = date(1, 1, 1)


class _MssqlDateTime2(datetime):
    """Datetime-compatible value that retains SQL Server's seventh fractional digit."""

    __slots__ = ("submicrosecond_100ns",)

    def __new__(
        cls,
        value: datetime | int,
        *args: Any,
        submicrosecond_100ns: int = 0,
        **kwargs: Any,
    ) -> _MssqlDateTime2:
        if isinstance(value, datetime):
            instance = super().__new__(
                cls,
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
        else:
            instance = super().__new__(cls, value, *args, **kwargs)
        instance.submicrosecond_100ns = int(submicrosecond_100ns)
        return instance


class _MssqlTime(time):
    """Time-compatible value that retains SQL Server's seventh fractional digit."""

    __slots__ = ("submicrosecond_100ns",)

    def __new__(cls, value: time, submicrosecond_100ns: int = 0) -> _MssqlTime:
        instance = super().__new__(
            cls,
            value.hour,
            value.minute,
            value.second,
            value.microsecond,
            tzinfo=value.tzinfo,
            fold=value.fold,
        )
        instance.submicrosecond_100ns = int(submicrosecond_100ns)
        return instance


class MssqlBcpNativeDecoder:
    """Decode a bounded SQL Server bcp native file into typed Python rows."""

    def __init__(self, contract: SourceNativeWireContract) -> None:
        if contract.source_format != "mssql-bcp-native":
            raise ValueError("MssqlBcpNativeDecoder requires mssql-bcp-native source format")
        if contract.blockers:
            raise ValueError("; ".join(contract.blockers))
        self._contract = contract

    def iter_rows(self, path: str | Path) -> Iterator[dict[str, Any]]:
        with Path(path).open("rb") as handle:
            while True:
                first = handle.peek(1)[:1] if hasattr(handle, "peek") else _peek_one(handle)
                if not first:
                    return
                yield self._read_row(handle)

    def _read_row(self, handle: BinaryIO) -> dict[str, Any]:
        row: dict[str, Any] = {}
        for column in self._contract.columns:
            row[column.name] = _read_column(handle, column)
        return row


def build_mssql_bcp_native_contract(
    *,
    schema: Sequence[tuple[str, str]],
    query: str,
    bcp_version: str | None = None,
    type_policy: MssqlClickHouseTypePolicy | None = None,
    target_format: str = "RowBinary",
) -> SourceNativeWireContract:
    """Build the decoder contract for a native bcp queryout artifact."""

    type_mapper = MssqlClickHouseTypeMapper(type_policy or MssqlClickHouseTypePolicy())
    layouts = tuple(_layout(name, dtype, type_mapper=type_mapper) for name, dtype in schema)
    blockers = tuple(
        f"mssql_bcp_native_unsupported_type:{layout.source_type}"
        for layout in layouts
        if layout.storage_type == "unsupported"
    )
    layout_payload = tuple(layout.to_dict() for layout in layouts)
    return SourceNativeWireContract(
        schema_version=NATIVE_WIRE_SCHEMA_VERSION,
        source_system="mssql",
        source_format="mssql-bcp-native",
        target_format=str(target_format),
        columns=layouts,
        schema_hash=stable_hash(tuple((str(name), str(dtype)) for name, dtype in schema)),
        query_hash=stable_hash(str(query)),
        type_layout_hash=stable_hash(layout_payload),
        bcp_version=bcp_version,
        warnings=("native_wire_query_is_not_sink_escaped",),
        blockers=blockers,
    )


def _layout(name: str, dtype: str, *, type_mapper: MssqlClickHouseTypeMapper) -> NativeWireColumnLayout:
    normalized = _normalize_type(dtype)
    root = _root(normalized)
    nullable = _is_nullable(dtype)
    precision, scale = _precision_scale(normalized)
    target_type = type_mapper.resolve_column(str(name), str(dtype)).clickhouse_type

    if root in {"varchar", "nvarchar", "char", "nchar", "varbinary", "binary"}:
        max_type = "max" in normalized
        encoding = "utf-16le" if root in {"nvarchar", "nchar"} else "utf-8"
        return NativeWireColumnLayout(
            name=str(name),
            source_type=str(dtype),
            target_type=target_type,
            nullable=nullable,
            storage_type=root,
            prefix_width=8 if max_type else 2,
            encoding=encoding,
        )
    fixed = _fixed_length(root, precision, scale)
    if fixed is not None:
        return NativeWireColumnLayout(
            name=str(name),
            source_type=str(dtype),
            target_type=target_type,
            nullable=nullable,
            storage_type=root,
            prefix_width=1 if nullable else 0,
            fixed_length=fixed,
            precision=precision,
            scale=scale,
        )
    return NativeWireColumnLayout(
        name=str(name),
        source_type=str(dtype),
        target_type=target_type,
        nullable=nullable,
        storage_type="unsupported",
    )


def _read_column(handle: BinaryIO, layout: NativeWireColumnLayout) -> Any:
    length = layout.fixed_length
    if layout.prefix_width:
        indicator = int.from_bytes(_read_exact(handle, layout.prefix_width), byteorder="little", signed=True)
        if indicator == -1:
            return None
        length = indicator
    if length is None:
        raise ValueError(f"native_wire_missing_length:{layout.name}")
    payload = _read_exact(handle, length)
    return _decode_payload(payload, layout)


def _decode_payload(payload: bytes, layout: NativeWireColumnLayout) -> Any:
    storage = layout.storage_type
    if storage == "bit":
        return bool(payload[0])
    if storage == "tinyint":
        return struct.unpack("<B", payload)[0]
    if storage == "smallint":
        return struct.unpack("<h", payload)[0]
    if storage == "int":
        return struct.unpack("<i", payload)[0]
    if storage == "bigint":
        return struct.unpack("<q", payload)[0]
    if storage == "real":
        return struct.unpack("<f", payload)[0]
    if storage == "float":
        return struct.unpack("<d", payload)[0]
    if storage in {"money", "smallmoney"}:
        scale = Decimal("10000")
        value = _decode_money(payload) if storage == "money" else struct.unpack("<i", payload)[0]
        return Decimal(value) / scale
    if storage in {"decimal", "numeric"}:
        return _decode_decimal(payload, int(layout.scale or 0))
    if storage in {"varchar", "nvarchar", "char", "nchar"}:
        return payload.decode(layout.encoding or "utf-8")
    if storage in {"varbinary", "binary"}:
        return payload
    if storage == "date":
        days = int.from_bytes(payload, byteorder="little", signed=False)
        return _DATETIME2_EPOCH + timedelta(days=days)
    if storage == "time":
        return _decode_time(payload, _scale_or_default(layout.scale))
    if storage == "datetime2":
        return _decode_datetime2(payload, _scale_or_default(layout.scale))
    if storage == "datetimeoffset":
        return _decode_datetimeoffset(payload, _scale_or_default(layout.scale))
    if storage == "datetime":
        return _decode_datetime(payload)
    if storage == "smalldatetime":
        days, minutes = struct.unpack("<HH", payload)
        return datetime.combine(_SQL_SERVER_EPOCH + timedelta(days=days), time()) + timedelta(minutes=minutes)
    if storage == "uniqueidentifier":
        return str(uuid.UUID(bytes_le=payload))
    raise ValueError(f"mssql_bcp_native_unsupported_type:{layout.source_type}")


def _decode_decimal(payload: bytes, scale: int) -> Decimal:
    if len(payload) == 19:
        scale = payload[1]
        sign = 1 if payload[2] == 1 else -1
        magnitude = int.from_bytes(payload[3:], byteorder="little", signed=False)
        return Decimal(sign * magnitude).scaleb(-scale)
    sign = 1 if payload[0] == 1 else -1
    magnitude = int.from_bytes(payload[1:], byteorder="little", signed=False)
    return Decimal(sign * magnitude).scaleb(-scale)


def _decode_money(payload: bytes) -> int:
    high = int.from_bytes(payload[:4], byteorder="little", signed=True)
    low = int.from_bytes(payload[4:], byteorder="little", signed=False)
    return (high << 32) + low


def _decode_time(payload: bytes, scale: int) -> time:
    ticks = int.from_bytes(payload, byteorder="little", signed=False)
    microseconds = ticks * (10 ** (6 - scale)) if scale <= 6 else ticks // (10 ** (scale - 6))
    value = (datetime.combine(_DATETIME2_EPOCH, time()) + timedelta(microseconds=microseconds)).time()
    remainder = ticks % 10 if scale == 7 else 0
    return _MssqlTime(value, remainder)


def _decode_datetime2(payload: bytes, scale: int) -> datetime:
    time_length = _time_length(scale)
    value_time = _decode_time(payload[:time_length], scale)
    days = int.from_bytes(payload[time_length:], byteorder="little", signed=False)
    value = datetime.combine(_DATETIME2_EPOCH + timedelta(days=days), value_time)
    remainder = int.from_bytes(payload[:time_length], byteorder="little", signed=False) % 10 if scale == 7 else 0
    return _MssqlDateTime2(value, submicrosecond_100ns=remainder)


def _decode_datetimeoffset(payload: bytes, scale: int) -> datetime:
    time_length = _time_length(scale)
    expected_length = time_length + 5
    if len(payload) != expected_length:
        raise ValueError(f"mssql_bcp_native_invalid_datetimeoffset_length:{len(payload)}")
    encoded_utc = _decode_datetime2(payload[: time_length + 3], scale)
    offset_minutes = struct.unpack("<h", payload[time_length + 3 :])[0]
    utc_value = datetime(
        encoded_utc.year,
        encoded_utc.month,
        encoded_utc.day,
        encoded_utc.hour,
        encoded_utc.minute,
        encoded_utc.second,
        encoded_utc.microsecond,
        tzinfo=UTC,
    )
    aware = utc_value.astimezone(timezone(timedelta(minutes=offset_minutes)))
    return _MssqlDateTime2(
        aware,
        submicrosecond_100ns=getattr(encoded_utc, "submicrosecond_100ns", 0),
    )


def _decode_datetime(payload: bytes) -> datetime:
    days, ticks = struct.unpack("<ii", payload)
    milliseconds = (ticks * 1000 + 150) // 300
    return datetime.combine(_SQL_SERVER_EPOCH + timedelta(days=days), time()) + timedelta(milliseconds=milliseconds)


def _fixed_length(root: str, precision: int | None, scale: int | None) -> int | None:
    fixed = {
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
    }
    if root in fixed:
        return fixed[root]
    if root == "time":
        return _time_length(_scale_or_default(scale))
    if root == "datetime2":
        return _time_length(_scale_or_default(scale)) + 3
    if root == "datetimeoffset":
        return _time_length(_scale_or_default(scale)) + 5
    if root in {"decimal", "numeric"}:
        return 19
    return None


def _time_length(scale: int) -> int:
    return 3 if scale <= 2 else 4 if scale <= 4 else 5


def _scale_or_default(scale: int | None) -> int:
    return 7 if scale is None else scale


def _read_exact(handle: BinaryIO, length: int) -> bytes:
    payload = handle.read(length)
    if len(payload) != length:
        raise EOFError("mssql_bcp_native_unexpected_eof")
    return payload


def _peek_one(handle: BinaryIO) -> bytes:
    position = handle.tell()
    payload = handle.read(1)
    handle.seek(position)
    return payload


def _normalize_type(dtype: str) -> str:
    return re.sub(r"\s+nullable\b", "", str(dtype).strip().lower())


def _root(dtype: str) -> str:
    return dtype.split("(", 1)[0].strip()


def _is_nullable(dtype: str) -> bool:
    return "nullable" in str(dtype).lower() or str(dtype).strip().lower().startswith("null")


def _precision_scale(dtype: str) -> tuple[int | None, int | None]:
    match = re.search(r"\((\d+)\s*,\s*(\d+)\)", dtype)
    if match:
        return int(match.group(1)), int(match.group(2))
    scale_match = re.search(r"\((\d+)\)", dtype)
    if _root(dtype) in {"time", "datetime2", "datetimeoffset"} and scale_match:
        return None, int(scale_match.group(1))
    return None, None


__all__ = ["MssqlBcpNativeDecoder", "build_mssql_bcp_native_contract"]
