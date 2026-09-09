"""Validate the standalone provider's native-wire boundary before optimized reads.

Core owns layout production. This independent package rejects malformed external
requests and is certified against the same framing vectors without importing core.
"""

from __future__ import annotations

import math
import re
import struct
from collections.abc import Mapping
from hashlib import sha256
from typing import Any, BinaryIO, Protocol

_WIDTHS = {
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
    "decimal": 19,
    "numeric": 19,
}
_VARIABLE = frozenset({"char", "varchar", "nchar", "nvarchar", "binary", "varbinary"})


class Field(Protocol):
    """Only the framing facts consumed by a bounded field read."""

    @property
    def storage_type(self) -> str: ...
    @property
    def source_type(self) -> str: ...
    @property
    def prefix_width(self) -> int: ...
    @property
    def fixed_length(self) -> int | None: ...
    @property
    def nullable(self) -> bool: ...
    @property
    def precision(self) -> int | None: ...
    @property
    def scale(self) -> int | None: ...


def validate_contract(contract: Mapping[str, Any]) -> None:
    """Reject incompatible layouts even if an old layout has a consistent digest."""
    if (
        contract.get("schema_version") != "dpone.native_transfer.native_wire.v1"
        or contract.get("source_format") != "mssql-bcp-native"
        or contract.get("source_system") != "mssql"
        or contract.get("target_format") != "Native"
        or contract.get("blockers")
    ):
        raise ValueError("native_wire_invalid_layout:profile:re_export_required")
    columns = contract.get("columns")
    if not isinstance(columns, (tuple, list)) or not columns:
        raise ValueError("native_wire_invalid_layout:columns")
    names = []
    for raw in columns:
        if not isinstance(raw, Mapping):
            raise ValueError("native_wire_invalid_layout:column")
        validate_column(raw)
        names.append(raw["name"])
    if len(set(names)) != len(names):
        raise ValueError("native_wire_invalid_layout:duplicate_columns")
    expected = "sha256:" + sha256(repr(tuple(dict(raw) for raw in columns)).encode("utf-8")).hexdigest()
    schema = tuple((raw["name"], raw["source_type"]) for raw in columns)
    schema_hash = "sha256:" + sha256(repr(schema).encode("utf-8")).hexdigest()
    if expected != contract.get("type_layout_hash") or schema_hash != contract.get("schema_hash"):
        raise ValueError("native_wire_invalid_layout:hash:re_export_required")


def validate_column(raw: Mapping[str, Any]) -> None:
    """Validate primitives and the admitted external native framing profile."""
    for key in ("name", "source_type", "storage_type", "target_type"):
        if not isinstance(raw.get(key), str) or not raw[key]:
            raise ValueError("native_wire_invalid_layout:column_identity")
    if type(raw.get("nullable")) is not bool or type(raw.get("prefix_width")) is not int:
        raise ValueError("native_wire_invalid_layout:primitive")
    for key in ("fixed_length", "precision", "scale"):
        if raw.get(key) is not None and type(raw[key]) is not int:
            raise ValueError("native_wire_invalid_layout:primitive")
    source = re.sub(r"\s+nullable\b", "", raw["source_type"].strip().lower())
    storage = source.split("(", 1)[0].strip()
    nullable = "nullable" in raw["source_type"].lower()
    if storage == "char" and not nullable:
        raise ValueError("native_wire_unsupported_unprefixed_char:use_row_stream_or_nullable_projection")
    if raw["storage_type"] != storage or raw["nullable"] != nullable:
        raise ValueError("native_wire_invalid_layout:source_type")
    pair = re.search(r"\((\d+)\s*,\s*(\d+)\)", source)
    single = re.search(r"\((\d+)\)", source)
    precision, scale = None, None
    if pair:
        precision, scale = int(pair[1]), int(pair[2])
    elif storage in {"decimal", "numeric"}:
        precision, scale = int(single[1]) if single else 18, 0
    elif storage == "float" and single:
        precision = int(single[1])
    elif storage in {"time", "datetime2", "datetimeoffset"} and single:
        scale = int(single[1])
    if storage in {"time", "datetime2", "datetimeoffset"}:
        if scale is not None and not 0 <= scale <= 7:
            raise ValueError("native_wire_invalid_layout:scale")
        scale = 7
    encoding = ("utf-16le" if storage in {"nchar", "nvarchar"} else "utf-8") if storage in _VARIABLE else None
    if raw.get("precision") != precision or raw.get("scale") != scale or raw.get("encoding") != encoding:
        raise ValueError("native_wire_invalid_layout:metadata")
    width = _WIDTHS.get(storage)
    if storage == "float" and precision is not None:
        if not 1 <= precision <= 53:
            raise ValueError("native_wire_invalid_layout:float_precision")
        width = 4 if precision <= 24 else 8
    prefix = int(nullable or storage in {"bit", "uniqueidentifier", "decimal", "numeric"})
    if storage in _VARIABLE:
        prefix = 8 if "max" in source else 2
    elif storage in {"time", "datetime2", "datetimeoffset"}:
        scale = 7 if raw.get("scale") is None else raw["scale"]
        if not 0 <= scale <= 7:
            raise ValueError("native_wire_invalid_layout:scale")
        width = (3 if scale <= 2 else 4 if scale <= 4 else 5) + {"time": 0, "datetime2": 3, "datetimeoffset": 5}[
            storage
        ]
    elif width is None:
        raise ValueError("native_wire_invalid_layout:unsupported_type")
    if raw["prefix_width"] != prefix or raw.get("fixed_length") != width:
        raise ValueError("native_wire_invalid_layout:framing:re_export_required")


def read_payload(handle: BinaryIO, column: Field, ordinal: int = 0) -> bytes | None:
    """Reject invalid lengths before passthrough/encoding or allocation."""
    context = f"type={column.storage_type}:ordinal={ordinal}:offset={handle.tell()}:format=native_wire.v1"
    length = column.fixed_length
    if column.prefix_width:
        length = int.from_bytes(_read_exact(handle, column.prefix_width, context), "little", signed=True)
        if length == -1:
            if not column.nullable:
                raise ValueError(f"native_wire_unexpected_null:{context}")
            return None
    if length is None or length < 0 or (column.fixed_length is not None and length != column.fixed_length):
        raise ValueError(f"native_wire_invalid_length:{context}")
    if column.fixed_length is None:
        match = re.search(r"\((\d+)\)", column.source_type)
        if match and length > int(match[1]) * {"nvarchar": 2, "nchar": 2, "varchar": 4, "char": 4}.get(
            column.storage_type, 1
        ):
            raise ValueError(f"native_wire_invalid_length:{context}")
    payload = _read_exact(handle, length, context)
    if column.storage_type in {"decimal", "numeric"}:
        precision = 18 if column.precision is None else column.precision
        scale = 0 if column.scale is None else column.scale
        if (
            payload[0] != precision
            or payload[1] != scale
            or payload[2] not in (0, 1)
            or not 1 <= precision <= 38
            or not 0 <= scale <= precision
            or int.from_bytes(payload[3:], "little") >= 10**precision
        ):
            raise ValueError(f"native_wire_invalid_decimal:{context}")
    if column.storage_type == "bit" and payload[0] not in (0, 1):
        raise ValueError(f"native_wire_invalid_bit:{context}")
    _validate_scalar_domain(payload, column, context)
    return payload


def _validate_scalar_domain(payload: bytes, field: Field, context: str) -> None:
    """Reject malformed values rather than wrapping days or interpreting invalid text."""
    storage = field.storage_type
    if storage in {"real", "float"} and not math.isfinite(
        struct.unpack("<f" if len(payload) == 4 else "<d", payload)[0]
    ):
        raise ValueError(f"native_wire_invalid_float:{context}")
    if storage in {"nchar", "nvarchar", "char", "varchar"}:
        try:
            payload.decode("utf-16le" if storage in {"nchar", "nvarchar"} else "utf-8")
        except UnicodeError:
            raise ValueError(f"native_wire_invalid_text:{context}") from None
    scale = 7 if field.scale is None else field.scale
    if storage in {"time", "datetime2", "datetimeoffset"}:
        width = 3 if scale <= 2 else 4 if scale <= 4 else 5
        ticks = int.from_bytes(payload[:width], "little")
        day_ticks = 86400 * 10**scale
        declared = re.search(r"\((\d+)\)", field.source_type)
        declared_scale = int(declared[1]) if declared else 7
        if ticks >= day_ticks or ticks % (10 ** (7 - declared_scale)):
            raise ValueError(f"native_wire_invalid_temporal:{context}")
        if storage != "time":
            days = int.from_bytes(payload[width : width + 3], "little")
            if days > 3652058:
                raise ValueError(f"native_wire_invalid_temporal:{context}")
            if storage == "datetimeoffset":
                offset = int.from_bytes(payload[-2:], "little", signed=True)
                local = days * day_ticks + ticks + offset * 60 * 10**scale
                if abs(offset) > 840 or not 0 <= local < 3652059 * day_ticks:
                    raise ValueError(f"native_wire_invalid_temporal:{context}")
    elif storage == "date" and int.from_bytes(payload, "little") > 3652058:
        raise ValueError(f"native_wire_invalid_temporal:{context}")
    elif storage == "datetime":
        days = int.from_bytes(payload[:4], "little", signed=True)
        ticks = int.from_bytes(payload[4:], "little", signed=True)
        if not -53690 <= days <= 2958463 or not 0 <= ticks < 25920000:
            raise ValueError(f"native_wire_invalid_temporal:{context}")
    elif storage == "smalldatetime" and int.from_bytes(payload[2:], "little") >= 1440:
        raise ValueError(f"native_wire_invalid_temporal:{context}")


def _read_exact(handle: BinaryIO, length: int, context: str) -> bytes:
    if length > 65536:
        position = handle.tell()
        end = handle.seek(0, 2)
        handle.seek(position)
        if length > end - position:
            raise EOFError(f"native_acceleration_unexpected_eof:{context}")
    payload = bytearray()
    while len(payload) < length:
        part = handle.read(min(length - len(payload), 65536))
        if not part:
            raise EOFError(f"native_acceleration_unexpected_eof:{context}")
        payload.extend(part)
    return bytes(payload)


def validate_target(raw: Mapping[str, Any], target: str) -> None:
    """Reject mappings this provider cannot represent, including raw numeric-as-text."""
    value = target.strip().lower()
    if value.startswith("nullable(") and value.endswith(")"):
        value = value[9:-1]
    root = value.split("(", 1)[0]
    storage = raw["storage_type"]
    integers = {"int8", "uint8", "int16", "uint16", "int32", "uint32", "int64", "uint64"}
    if storage in {"bit", "tinyint", "smallint", "int", "bigint"}:
        allowed = integers | ({"bool"} if storage == "bit" else set())
    elif storage in {"real", "float"}:
        allowed = {"float64"} | ({"float32"} if raw["fixed_length"] == 4 else set())
    elif storage in {"decimal", "numeric", "money", "smallmoney"}:
        allowed = {"decimal"}
    elif storage == "date":
        allowed = {"date", "date32"}
    elif storage in {"datetime", "smalldatetime", "datetime2"}:
        allowed = {"datetime", "datetime64"}
    elif storage == "datetimeoffset":
        allowed = {"datetime64", "string"}
    elif storage == "time":
        allowed = {"string", "uint32"}
    elif storage == "uniqueidentifier":
        allowed = {"uuid"}
    else:
        allowed = {"string", "fixedstring"}
    if root not in allowed:
        raise ValueError("native_wire_unsupported_target_mapping")
