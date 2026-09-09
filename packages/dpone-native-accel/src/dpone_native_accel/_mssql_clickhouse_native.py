"""Fused MSSQL BCP native to ClickHouse Native block encoder."""

from __future__ import annotations

import re
import struct
import uuid
from base64 import b64encode
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, BinaryIO

from ._clickhouse_temporal_encoding import encode_temporal_integer
from ._mssql_native_framing import read_payload, validate_column, validate_contract, validate_target
from ._mssql_native_numeric import encode_decimal, encode_numeric

_DAYS_TO_UNIX_EPOCH = (date(1970, 1, 1) - date(1, 1, 1)).days
_DAYS_TO_SQL_SERVER_EPOCH = (date(1970, 1, 1) - date(1900, 1, 1)).days
_UNSUPPORTED_COMPLEX_ROOTS = frozenset({"array", "map", "tuple", "nested", "enum8", "enum16", "lowcardinality"})


@dataclass(frozen=True, slots=True)
class ColumnLayout:
    name: str
    storage_type: str
    clickhouse_type: str
    nullable: bool
    prefix_width: int
    fixed_length: int | None
    precision: int | None
    scale: int | None
    encoding: str
    binary_encoding: str = "none"
    source_type: str = ""

    @classmethod
    def from_contract(
        cls,
        raw: Mapping[str, Any],
        target_type: str | None,
        *,
        binary_encoding: str,
    ) -> ColumnLayout:
        validate_column(raw)
        validate_target(raw, str(target_type or raw["target_type"]))
        return cls(
            source_type=str(raw["source_type"]),
            name=str(raw["name"]),
            storage_type=str(raw["storage_type"]).lower(),
            clickhouse_type=str(target_type or raw["target_type"]),
            nullable=bool(raw.get("nullable")),
            prefix_width=int(raw.get("prefix_width") or 0),
            fixed_length=int(raw["fixed_length"]) if raw.get("fixed_length") is not None else None,
            precision=int(raw["precision"]) if raw.get("precision") is not None else None,
            scale=int(raw["scale"]) if raw.get("scale") is not None else None,
            encoding=str(raw.get("encoding") or "utf-8"),
            binary_encoding=binary_encoding,
        )


class MssqlBcpClickHouseNativeBackend:
    """Encode supported MSSQL native rows into ClickHouse Native blocks."""

    def __init__(
        self,
        *,
        artifact_path: Path,
        columns: Sequence[ColumnLayout],
        block_rows: int,
        block_bytes: int | None,
    ) -> None:
        self._artifact_path = artifact_path
        self._columns = tuple(columns)
        self._block_rows = max(1, block_rows)
        self._block_bytes = block_bytes

    @classmethod
    def from_request(cls, request: Mapping[str, Any]) -> MssqlBcpClickHouseNativeBackend:
        contract = _mapping(request.get("native_wire_contract"))
        validate_contract(contract)
        if "schema" in request and tuple(tuple(item) for item in request["schema"]) != tuple(
            (raw["name"], raw["source_type"]) for raw in contract["columns"]
        ):
            raise ValueError("native_wire_source_schema_mismatch:re-export_with_matching_schema")
        bulk = _mapping(request.get("bulk_wire_contract"))
        target_schema = tuple((str(name), str(dtype)) for name, dtype in request.get("clickhouse_schema") or ())
        if target_schema and tuple(name for name, _ in target_schema) != tuple(
            raw["name"] for raw in contract["columns"]
        ):
            raise ValueError("native_wire_invalid_layout:target_columns")
        target_by_position = tuple(dtype for _, dtype in target_schema)
        raw_columns = tuple(_mapping(item) for item in contract.get("columns") or ())
        type_policy = request.get("type_policy")
        binary_encoding = (
            str(type_policy.get("binary_encoding") or "none").lower() if isinstance(type_policy, Mapping) else "none"
        )
        columns = tuple(
            ColumnLayout.from_contract(
                raw,
                target_by_position[index] if index < len(target_by_position) else None,
                binary_encoding=binary_encoding,
            )
            for index, raw in enumerate(raw_columns)
        )
        return cls(
            artifact_path=Path(str(request["artifact_path"])),
            columns=columns,
            block_rows=int(bulk.get("block_rows") or 65_536),
            block_bytes=_parse_bytes(bulk.get("block_bytes")),
        )

    def iter_blocks(self) -> Iterator[bytes]:
        for payload, _ in self.iter_batches():
            yield payload

    def iter_batches(self) -> Iterator[tuple[bytes, int]]:
        with self._artifact_path.open("rb") as handle:
            while True:
                block = _BlockBuilder(self._columns)
                estimated_bytes = 0
                while len(block) < self._block_rows and not _eof(handle):
                    values = [_read_cell(handle, column, index) for index, column in enumerate(self._columns)]
                    estimated_bytes += sum(len(value or b"") + 1 for value in values)
                    block.append(values)
                    if self._block_bytes is not None and estimated_bytes >= self._block_bytes:
                        break
                if len(block) == 0:
                    return
                yield block.to_bytes(), len(block)


class _BlockBuilder:
    def __init__(self, columns: Sequence[ColumnLayout]) -> None:
        self._columns = tuple(columns)
        self._null_maps = [bytearray() for _ in columns]
        self._data = [bytearray() for _ in columns]
        self._rows = 0

    def __len__(self) -> int:
        return self._rows

    def append(self, values: Sequence[bytes | None]) -> None:
        for index, value in enumerate(values):
            column = self._columns[index]
            nullable, inner_type = _unwrap_nullable(column.clickhouse_type)
            if nullable:
                self._null_maps[index].append(1 if value is None else 0)
                self._data[index].extend(
                    _default_value(inner_type) if value is None else _encode_value(value, column, inner_type)
                )
            elif value is None:
                raise ValueError(f"native_acceleration_null_for_non_nullable:{column.name}")
            else:
                self._data[index].extend(_encode_value(value, column, inner_type))
        self._rows += 1

    def to_bytes(self) -> bytes:
        payload = bytearray()
        payload.extend(_var_uint(len(self._columns)))
        payload.extend(_var_uint(self._rows))
        for index, column in enumerate(self._columns):
            payload.extend(_ch_string(column.name))
            payload.extend(_ch_string(column.clickhouse_type))
            payload.extend(self._null_maps[index])
            payload.extend(self._data[index])
        return bytes(payload)


def _read_cell(handle: BinaryIO, column: ColumnLayout, ordinal: int = 0) -> bytes | None:
    return read_payload(handle, column, ordinal)


def _encode_value(payload: bytes, column: ColumnLayout, clickhouse_type: str) -> bytes:
    root = _root_type(clickhouse_type)
    storage = column.storage_type
    if root in _UNSUPPORTED_COMPLEX_ROOTS or root.startswith("enum"):
        raise ValueError(f"native_acceleration_unsupported_clickhouse_type:{clickhouse_type}")
    if storage == "time":
        return _encode_time(payload, column, root)
    if root == "bool":
        return encode_numeric(payload, column, root)
    if root in {"int8", "uint8", "int16", "uint16", "int32", "uint32", "int64", "uint64", "float32", "float64"}:
        return encode_numeric(payload, column, root)
    if root.startswith("decimal"):
        return encode_decimal(payload, column, clickhouse_type)
    if root in {"date", "date32"}:
        return _encode_date(payload, root)
    if root in {"datetime", "datetime64"}:
        return _encode_datetime(payload, column, clickhouse_type)
    if root == "uuid":
        return _encode_uuid(payload)
    if root == "string":
        value = _string_payload(payload, column)
        return _var_uint(len(value)) + value
    if root == "fixedstring":
        return _encode_fixed_string(payload, column, clickhouse_type)
    raise ValueError(f"native_acceleration_unsupported_clickhouse_type:{clickhouse_type}")


def _default_value(clickhouse_type: str) -> bytes:
    root = _root_type(clickhouse_type)
    widths = {
        "bool": 1,
        "int8": 1,
        "uint8": 1,
        "int16": 2,
        "uint16": 2,
        "int32": 4,
        "uint32": 4,
        "int64": 8,
        "uint64": 8,
        "float32": 4,
        "float64": 8,
    }
    if root == "string":
        return b"\x00"
    if root == "fixedstring":
        return b"\x00" * _fixed_string_length(clickhouse_type)
    if root == "uuid":
        return b"\x00" * 16
    if root == "date":
        return b"\x00" * 2
    if root == "date32":
        return b"\x00" * 4
    if root == "datetime":
        return b"\x00" * 4
    if root == "datetime64":
        return b"\x00" * 8
    if root.startswith("decimal"):
        precision, _ = _decimal_precision_scale(clickhouse_type)
        return b"\x00" * _decimal_width(precision)
    if root in widths:
        return b"\x00" * widths[root]
    if root in _UNSUPPORTED_COMPLEX_ROOTS or root.startswith("enum"):
        raise ValueError(f"native_acceleration_unsupported_clickhouse_type:{clickhouse_type}")
    raise ValueError(f"native_acceleration_unsupported_clickhouse_type:{clickhouse_type}")


def _encode_date(payload: bytes, target_root: str) -> bytes:
    days = int.from_bytes(payload, byteorder="little", signed=False) - _DAYS_TO_UNIX_EPOCH
    return encode_temporal_integer(days, target_root)


def _encode_datetime(payload: bytes, column: ColumnLayout, clickhouse_type: str) -> bytes:
    target_scale = _scale(clickhouse_type) if _root_type(clickhouse_type) == "datetime64" else 0
    ticks = _datetime_ticks(payload, column, target_scale)
    return encode_temporal_integer(ticks, _root_type(clickhouse_type), target_scale)


def _encode_time(payload: bytes, column: ColumnLayout, target_root: str) -> bytes:
    source_scale = int(column.scale if column.scale is not None else 7)
    ticks = int.from_bytes(payload, byteorder="little", signed=False)
    if target_root == "uint32":
        return struct.pack("<I", ticks // (10**source_scale))
    if target_root == "string":
        display_scale = _declared_temporal_scale(column)
        ticks = _rescale_ticks(ticks, source_scale, display_scale)
        source_scale = display_scale
        seconds, fraction = divmod(ticks, 10**source_scale)
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        text = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        if source_scale:
            text += f".{fraction:0{source_scale}d}"
        encoded = text.encode("ascii")
        return _var_uint(len(encoded)) + encoded
    raise ValueError(f"native_acceleration_unsupported_time_target:{column.name}:{target_root}")


def _datetime_ticks(payload: bytes, column: ColumnLayout, target_scale: int) -> int:
    storage = column.storage_type
    if storage == "datetime2":
        time_length = len(payload) - 3
        source_scale = int(column.scale) if column.scale is not None else 7
        time_ticks = int.from_bytes(payload[:time_length], byteorder="little", signed=False)
        days = int.from_bytes(payload[time_length:], byteorder="little", signed=False) - _DAYS_TO_UNIX_EPOCH
        ticks = days * 86400 * (10**source_scale) + time_ticks
        return _rescale_ticks(ticks, source_scale, target_scale)
    if storage == "datetimeoffset":
        time_ticks, days, _offset_minutes, source_scale = _decode_datetimeoffset_payload(payload, column)
        utc_ticks = days * 86400 * (10**source_scale) + time_ticks
        return _rescale_ticks(utc_ticks, source_scale, target_scale)
    if storage == "datetime":
        days, sql_ticks = struct.unpack("<ii", payload)
        milliseconds = (sql_ticks * 1000 + 150) // 300
        ticks = (days - _DAYS_TO_SQL_SERVER_EPOCH) * 86400 * 1000 + milliseconds
        return _rescale_ticks(ticks, 3, target_scale)
    if storage == "smalldatetime":
        days, minutes = struct.unpack("<HH", payload)
        ticks = ((days - _DAYS_TO_SQL_SERVER_EPOCH) * 86400 + minutes * 60) * (10**target_scale)
        return ticks
    raise ValueError(f"native_acceleration_unsupported_datetime_source:{column.storage_type}")


def _datetimeoffset_text(payload: bytes, column: ColumnLayout) -> str:
    time_ticks, days, offset_minutes, source_scale = _decode_datetimeoffset_payload(payload, column)
    ticks_per_second = 10**source_scale
    ticks_per_day = 86400 * ticks_per_second
    local_ticks = days * ticks_per_day + time_ticks + offset_minutes * 60 * ticks_per_second
    local_days, day_ticks = divmod(local_ticks, ticks_per_day)
    seconds, fraction = divmod(day_ticks, ticks_per_second)
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    local_date = date(1970, 1, 1) + timedelta(days=local_days)
    display_scale = _declared_temporal_scale(column)
    fraction = _rescale_ticks(fraction, source_scale, display_scale)
    fractional = f".{fraction:0{display_scale}d}" if display_scale else ""
    sign = "+" if offset_minutes >= 0 else "-"
    absolute_offset = abs(offset_minutes)
    offset_hours, offset_remainder = divmod(absolute_offset, 60)
    return (
        f"{local_date.isoformat()} {hours:02d}:{minutes:02d}:{seconds:02d}{fractional} "
        f"{sign}{offset_hours:02d}:{offset_remainder:02d}"
    )


def _decode_datetimeoffset_payload(payload: bytes, column: ColumnLayout) -> tuple[int, int, int, int]:
    source_scale = int(column.scale) if column.scale is not None else 7
    time_length = _time_length(source_scale)
    expected_length = time_length + 5
    if len(payload) != expected_length:
        raise ValueError(f"native_acceleration_invalid_datetimeoffset_length:{column.name}:{len(payload)}")
    time_ticks = int.from_bytes(payload[:time_length], byteorder="little", signed=False)
    days = int.from_bytes(payload[time_length : time_length + 3], byteorder="little", signed=False)
    days -= _DAYS_TO_UNIX_EPOCH
    offset_minutes = struct.unpack("<h", payload[time_length + 3 :])[0]
    return time_ticks, days, offset_minutes, source_scale


def _string_payload(payload: bytes, column: ColumnLayout) -> bytes:
    """Apply the value policy before either variable or fixed string framing."""
    if column.storage_type == "datetimeoffset":
        return _datetimeoffset_text(payload, column).encode("ascii")
    if column.storage_type in {"binary", "varbinary"}:
        if column.binary_encoding == "hex":
            return payload.hex().encode("ascii")
        if column.binary_encoding == "base64":
            return b64encode(payload)
    if column.storage_type in {"nvarchar", "nchar"}:
        return payload.decode(column.encoding).encode("utf-8")
    return payload


def _declared_temporal_scale(column: ColumnLayout) -> int:
    match = re.search(r"\((\d+)\)", column.source_type)
    return int(match[1]) if match else 7


def _encode_fixed_string(payload: bytes, column: ColumnLayout, clickhouse_type: str) -> bytes:
    value = _string_payload(payload, column)
    length = _fixed_string_length(clickhouse_type)
    if len(value) > length:
        raise ValueError(f"native_acceleration_fixed_string_oversize:{column.name}:{len(value)}:{length}")
    return value + b"\x00" * (length - len(value))


def _encode_uuid(payload: bytes) -> bytes:
    raw = uuid.UUID(bytes_le=payload).bytes
    return raw[:8][::-1] + raw[8:][::-1]


def _rescale_ticks(value: int, source_scale: int, target_scale: int) -> int:
    if target_scale > source_scale:
        return value * (10 ** (target_scale - source_scale))
    if target_scale < source_scale:
        return value // (10 ** (source_scale - target_scale))
    return value


def _money_integer(payload: bytes) -> int:
    high = int.from_bytes(payload[:4], byteorder="little", signed=True)
    low = int.from_bytes(payload[4:], byteorder="little", signed=False)
    return (high << 32) + low


def _decimal_precision_scale(clickhouse_type: str) -> tuple[int, int]:
    match = re.search(r"\((\d+)\s*,\s*(\d+)\)", clickhouse_type)
    if not match:
        return 38, 9
    return int(match.group(1)), int(match.group(2))


def _decimal_width(precision: int) -> int:
    return 4 if precision <= 9 else 8 if precision <= 18 else 16 if precision <= 38 else 32


def _scale(clickhouse_type: str) -> int:
    match = re.search(r"\((\d+)", clickhouse_type)
    return min(max(int(match.group(1)), 0), 9) if match else 0


def _time_length(scale: int) -> int:
    return 3 if scale <= 2 else 4 if scale <= 4 else 5


def _fixed_string_length(clickhouse_type: str) -> int:
    match = re.search(r"\((\d+)\)", clickhouse_type)
    if not match:
        raise ValueError(f"native_acceleration_fixed_string_length_missing:{clickhouse_type}")
    return int(match.group(1))


def _unwrap_nullable(clickhouse_type: str) -> tuple[bool, str]:
    value = clickhouse_type.strip()
    if value.lower().startswith("nullable(") and value.endswith(")"):
        return True, value[len("Nullable(") : -1].strip()
    return False, value


def _root_type(clickhouse_type: str) -> str:
    return clickhouse_type.strip().split("(", 1)[0].strip().lower()


def _ch_string(value: str) -> bytes:
    payload = value.encode("utf-8")
    return _var_uint(len(payload)) + payload


def _var_uint(value: int) -> bytes:
    output = bytearray()
    current = int(value)
    while current >= 0x80:
        output.append((current & 0x7F) | 0x80)
        current >>= 7
    output.append(current)
    return bytes(output)


def _eof(handle: BinaryIO) -> bool:
    position = handle.tell()
    payload = handle.read(1)
    handle.seek(position)
    return not payload


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("native_acceleration_invalid_request")
    return value


def _parse_bytes(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    units = {"kib": 1024, "mib": 1024**2, "gib": 1024**3, "kb": 1000, "mb": 1000**2, "gb": 1000**3}
    suffix = "".join(re.findall(r"[A-Za-z]+", text)).lower()
    number = text[: len(text) - len(suffix)] if suffix else text
    return int(float(number.strip()) * units.get(suffix, 1))
