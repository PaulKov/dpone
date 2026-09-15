"""Strict, side-effect-free decoder for version-one physical catalog tuples.

This boundary accepts detached tuples only. Acquisition must enforce its own
limits *before* materialization and explicitly adapt driver representations to
exact bool/UUID values and lossless char(27) timestamps. Decoding cannot establish
visibility, transaction consistency, cross-call header equality, COUNT_BIG
execution, supported dependencies, or physical admission. Unknown property codes
are preserved; a later admission boundary must fail closed on unknown semantics.
"""

from __future__ import annotations

import re
from dataclasses import Field, fields
from datetime import datetime
from types import MappingProxyType
from typing import Any
from uuid import UUID

from dpone.contracts.dbt_mssql_physical_catalog_rows import (
    CatalogRow,
    ColumnRow,
    CountRow,
    DependencyRow,
    ForbiddenPropertyRow,
    HeaderRow,
    IndexColumnRow,
    IndexRow,
    PartitionRow,
    TableRow,
)

_ROW_TYPES = MappingProxyType(
    {
        "HEADER": HeaderRow,
        "TABLE": TableRow,
        "COLUMN": ColumnRow,
        "INDEX": IndexRow,
        "INDEX_COLUMN": IndexColumnRow,
        "PARTITION": PartitionRow,
        "DEPENDENCY": DependencyRow,
        "FORBIDDEN_PROPERTY": ForbiddenPropertyRow,
        "COUNT": CountRow,
    }
)
_SINGLETONS = frozenset({"HEADER", "TABLE", "COUNT"})
_INTEGER_RANGES = MappingProxyType(
    {
        "tinyint": (0, 255),
        "smallint": (-32768, 32767),
        "int": (-2147483648, 2147483647),
        "bigint": (-9223372036854775808, 9223372036854775807),
    }
)
_TIMESTAMP = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{7}")


class CatalogWireError(ValueError):
    """A malformed or incomplete resultset; messages name fields, not values."""


def _integer(value: object, name: str, lower: int, upper: int | None = None) -> int:
    if type(value) is not int or value < lower or (upper is not None and value > upper):
        raise CatalogWireError(f"{name} requires an exact integer within bounds")
    return value


def _timestamp(value: str, name: str) -> None:
    if _TIMESTAMP.fullmatch(value) is None:
        raise CatalogWireError(f"{name} requires canonical char(27) timestamp text")
    try:
        # Validate only whole seconds; never parse/round the retained fraction.
        datetime(
            int(value[:4]), int(value[5:7]), int(value[8:10]), int(value[11:13]), int(value[14:16]), int(value[17:19])
        )
    except ValueError:
        raise CatalogWireError(f"{name} has invalid Gregorian date or clock fields") from None


def _text(value: object, name: str, sql: str, max_definition_bytes: int) -> None:
    if type(value) is not str:
        raise CatalogWireError(f"{name} requires exact text")
    try:
        byte_count = len(value.encode("utf-16-le"))
    except UnicodeEncodeError:
        raise CatalogWireError(f"{name} requires valid UTF-16 text") from None
    if sql == "nvarchar(max)":
        if byte_count > max_definition_bytes:
            raise CatalogWireError(f"{name} exceeds definition byte budget")
        return
    width = int(sql.split("(")[1][:-1])
    if not value or byte_count // 2 > width:
        raise CatalogWireError(f"{name} exceeds SQL text width or is empty")
    if not sql.startswith("nvarchar") and not value.isascii():
        raise CatalogWireError(f"{name} requires ASCII text")
    if sql.startswith("char(") and len(value) != width:
        raise CatalogWireError(f"{name} requires exact char width")
    if sql == "char(27)":
        _timestamp(value, name)
    if name == "direction" and value not in {"INBOUND", "OUTBOUND"}:
        raise CatalogWireError("direction requires INBOUND or OUTBOUND")


def _detail(value: object, descriptor: Field[Any], max_rows: int, max_definition_bytes: int) -> None:
    name = descriptor.name
    if value is None:
        if descriptor.metadata["nullable"]:
            return
        raise CatalogWireError(f"{name} cannot be NULL on data rows")
    sql = descriptor.metadata["sql"]
    if sql in _INTEGER_RANGES:
        lower, upper = _INTEGER_RANGES[sql]
        if name.endswith("_count"):
            lower, upper = 0, min(upper, max_rows)
        elif name == "row_count_exact":
            lower = 0
        _integer(value, name, lower, upper)
    elif sql == "bit":
        if type(value) is not bool:
            raise CatalogWireError(f"{name} requires exact bool")
    elif sql == "uniqueidentifier":
        if type(value) is not UUID:
            raise CatalogWireError(f"{name} requires exact UUID")
    else:
        _text(value, name, sql, max_definition_bytes)


def decode_catalog_result(
    rows: object,
    *,
    expected_kind: str,
    expected_object_id: int,
    max_rows: int,
    max_definition_bytes: int,
) -> tuple[CatalogRow, ...]:
    """Decode one complete resultset or raise :class:`CatalogWireError`.

    Both budgets are required positive exact ints. ``max_rows`` bounds data
    rows, including HEADER collection-count claims; definition limits measure
    UTF-16LE bytes. No truncation or coercion occurs. A complete zero marker
    returns ``()`` only for collection kinds. HEADER, TABLE and COUNT require
    one data row. Returned frozen DTOs retain all envelope and detail values.

    The SQL producer must emit fields in dataclass declaration order, preceded
    by version/kind/object/ordinal/count. Errors identify the failing field or
    invariant without echoing catalog names or definitions.
    """
    _integer(max_rows, "max_rows", 1)
    _integer(max_definition_bytes, "max_definition_bytes", 1)
    _integer(expected_object_id, "expected_object_id", 1, 2147483647)
    if type(expected_kind) is not str or expected_kind not in _ROW_TYPES:
        raise CatalogWireError("expected_kind is unknown")
    row_type = _ROW_TYPES[expected_kind]
    descriptors = fields(row_type)
    if type(rows) is not tuple or not rows:
        raise CatalogWireError("resultset requires a nonempty exact tuple")
    if len(rows) > max_rows:
        raise CatalogWireError("resultset exceeds data row budget")
    result: list[CatalogRow] = []
    for ordinal, row in enumerate(rows, 1):
        if type(row) is not tuple or len(row) != len(descriptors):
            raise CatalogWireError("row requires exact tuple and ordered column count")
        _integer(row[0], "wire_version", 1, 1)
        if type(row[1]) is not str or row[1] != expected_kind:
            raise CatalogWireError("row_kind differs from expected kind")
        _integer(row[2], "object_id", expected_object_id, expected_object_id)
        row_ordinal = _integer(row[3], "row_ordinal", 0, 2147483647)
        row_count = _integer(row[4], "row_count", 0, min(max_rows, 2147483647))
        if row_count == 0:
            if (
                expected_kind in _SINGLETONS
                or len(rows) != 1
                or row_ordinal != 0
                or any(value is not None for value in row[5:])
            ):
                raise CatalogWireError("invalid empty collection marker")
            return ()
        if row_count != len(rows) or row_ordinal != ordinal:
            raise CatalogWireError("resultset count or contiguous ordinal order differs")
        if expected_kind in _SINGLETONS and row_count != 1:
            raise CatalogWireError("singleton kind requires exactly one data row")
        for descriptor, value in zip(descriptors[5:], row[5:], strict=True):
            _detail(value, descriptor, max_rows, max_definition_bytes)
        result.append(row_type(**dict(zip((field.name for field in descriptors), row, strict=True))))
    return tuple(result)
