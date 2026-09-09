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
from dpone.runtime.native_wire_mssql_framing import fixed_length, native_prefix_width, read_native_payload, time_length
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
        validate_mssql_native_contract(contract)
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
        for ordinal, column in enumerate(self._contract.columns):
            row[column.name] = _read_column(handle, column, ordinal)
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
    if root in {"time", "datetime2", "datetimeoffset"}:
        time_length(7 if scale is None else scale)
        scale = 7  # Native BCP uses a physical 100ns payload at every declared scale.
    target_type = type_mapper.resolve_column(str(name), str(dtype)).clickhouse_type

    if root in {"varchar", "nvarchar", "char", "nchar", "varbinary", "binary"} and not (
        root == "char" and not nullable
    ):
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
    fixed = fixed_length(root, scale, precision)
    if fixed is not None:
        return NativeWireColumnLayout(
            name=str(name),
            source_type=str(dtype),
            target_type=target_type,
            nullable=nullable,
            storage_type=root,
            prefix_width=native_prefix_width(root, nullable=nullable),
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


def validate_mssql_native_contract(contract: SourceNativeWireContract) -> None:
    """Reject stale or inconsistent physical metadata before either reader opens a file."""
    if (
        contract.schema_version != NATIVE_WIRE_SCHEMA_VERSION
        or contract.source_system != "mssql"
        or contract.source_format != "mssql-bcp-native"
        or contract.blockers
        or not contract.columns
    ):
        raise ValueError("native_wire_invalid_layout:profile_or_columns:re_export_required")
    names = [column.name for column in contract.columns]
    if any(not name for name in names) or len(set(names)) != len(names):
        raise ValueError("native_wire_invalid_layout:column_identity")
    mapper = MssqlClickHouseTypeMapper(MssqlClickHouseTypePolicy())
    for ordinal, column in enumerate(contract.columns):
        expected = _layout(column.name, column.source_type, type_mapper=mapper)
        fields = ("nullable", "storage_type", "prefix_width", "fixed_length", "precision", "scale", "encoding")
        if expected.storage_type == "unsupported" or any(
            type(getattr(column, key)) is not type(getattr(expected, key))
            or getattr(column, key) != getattr(expected, key)
            for key in fields
        ):
            raise ValueError(f"native_wire_invalid_layout:ordinal={ordinal}:re_export_required")
    if contract.type_layout_hash != stable_hash(tuple(column.to_dict() for column in contract.columns)):
        raise ValueError("native_wire_invalid_layout:hash:re_export_required")
    if contract.schema_hash != stable_hash(tuple((column.name, column.source_type) for column in contract.columns)):
        raise ValueError("native_wire_invalid_layout:schema_hash:re_export_required")


def _read_column(handle: BinaryIO, layout: NativeWireColumnLayout, ordinal: int = 0) -> Any:
    payload = read_native_payload(handle, layout, ordinal)
    return None if payload is None else _decode_payload(payload, layout)


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
        return struct.unpack("<f" if len(payload) == 4 else "<d", payload)[0]
    if storage in {"money", "smallmoney"}:
        value = _decode_money(payload) if storage == "money" else struct.unpack("<i", payload)[0]
        return _scaled_decimal(value, 4)
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
    # Tuple construction is exact even when the caller's Decimal context has precision 28.
    magnitude = int.from_bytes(payload[3:], byteorder="little", signed=False)
    return _scaled_decimal(magnitude * (1 if payload[2] else -1), scale)


def _scaled_decimal(value: int, scale: int) -> Decimal:
    return Decimal((int(value < 0), tuple(map(int, str(abs(value)))), -scale))


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


def _time_length(scale: int) -> int:
    return time_length(scale)


def _scale_or_default(scale: int | None) -> int:
    return 7 if scale is None else scale


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
    if _root(dtype) in {"decimal", "numeric"}:
        return (int(scale_match.group(1)) if scale_match else 18), 0
    if _root(dtype) == "float" and scale_match:
        return int(scale_match.group(1)), None
    if _root(dtype) in {"time", "datetime2", "datetimeoffset"} and scale_match:
        return None, int(scale_match.group(1))
    return None, None


__all__ = ["MssqlBcpNativeDecoder", "build_mssql_bcp_native_contract"]
