"""Versioned typed content identity shared by snapshot producers and observers.

V1 hashes sorted canonical JSON rows including their declared scalar types and
NULLs. Sorting preserves duplicate multiplicity and removes storage order. Decimal
values never pass through binary floats or the ambient Decimal context. This is
a cryptographic digest, not a claim of live reconciliation. Historical unspecified
content hashes cannot be reinterpreted as this version.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from decimal import Decimal, localcontext
from hashlib import sha256
from heapq import heappush, heapreplace
from uuid import UUID

from dpone.contracts.composition_clickhouse_dispatch import ClickHouseDispatchColumn
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

CONTENT_VERSION = "dpone.composition-snapshot-typed-multiset.v1"


def snapshot_scalar(value: object, kind: str) -> object:
    """Normalize one lossless supported scalar; reject implicit coercions."""
    if kind.startswith("Nullable(") and kind.endswith(")"):
        return None if value is None else snapshot_scalar(value, kind[9:-1])
    if kind == "String" and type(value) is str:
        return value
    integer = re.fullmatch(r"(U?)Int(8|16|32|64|128|256)", kind)
    if integer and type(value) is int:
        bits = int(integer[2])
        low = 0 if integer[1] else -(2 ** (bits - 1))
        high = 2**bits if integer[1] else 2 ** (bits - 1)
        if low <= value < high:
            return str(value)
    decimal = re.fullmatch(r"Decimal\(([1-9][0-9]?), ?(0|[1-9][0-9]?)\)", kind)
    if decimal and isinstance(value, Decimal) and value.is_finite():
        precision, scale = map(int, decimal.groups())
        with localcontext() as context:
            context.prec = 160
            if abs(value) < Decimal(10) ** (precision - scale):
                quantized = value.quantize(Decimal(1).scaleb(-scale))
                if value == quantized:
                    return format(abs(quantized) if quantized == 0 else quantized, f".{scale}f")
    if kind == "UUID" and isinstance(value, str | UUID):
        try:
            return str(UUID(str(value)))
        except ValueError:
            pass
    raise CompositionAdmissionError("snapshot_materialization_scalar")


def require_snapshot_columns(columns: tuple[ClickHouseDispatchColumn, ...]) -> None:
    """The initial observer supports bounded integer, decimal, UUID and text schemas."""
    if type(columns) is not tuple or not 1 <= len(columns) <= 64 or len({c.name for c in columns}) != len(columns):
        raise CompositionAdmissionError("snapshot_materialization_schema")
    for column in columns:
        column.__post_init__()
        kind = (
            column.type_name.removeprefix("Nullable(").removesuffix(")")
            if column.type_name.startswith("Nullable(")
            else column.type_name
        )
        if not (
            kind in {"String", "UUID"}
            or re.fullmatch(r"U?Int(8|16|32|64|128|256)|Decimal\([1-9][0-9]?, ?[0-9]{1,2}\)", kind)
        ):
            raise CompositionAdmissionError("snapshot_materialization_schema")


def snapshot_content_sha256(
    columns: tuple[ClickHouseDispatchColumn, ...], rows: Iterable[Sequence[object]], *, max_rows: int, max_bytes: int
) -> str:
    """Hash a bounded multiset using only typed observed/source values."""
    require_snapshot_columns(columns)
    if type(max_rows) is not int or max_rows < 1 or type(max_bytes) is not int or max_bytes < 1:
        raise CompositionAdmissionError("snapshot_materialization_budget")
    encoded: list[bytes] = []
    size = 0
    for row in rows:
        if len(encoded) >= max_rows or isinstance(row, str | bytes) or len(row) != len(columns):
            raise CompositionAdmissionError("snapshot_materialization_rows")
        if any(isinstance(value, str | bytes) and len(value) > max_bytes for value in row):
            raise CompositionAdmissionError("snapshot_materialization_budget")
        item = canonical_json_bytes(
            [snapshot_scalar(value, column.type_name) for value, column in zip(row, columns, strict=True)]
        )
        size += len(item)
        if size > max_bytes:
            raise CompositionAdmissionError("snapshot_materialization_budget")
        encoded.append(item)
    digest = sha256(canonical_json_bytes([CONTENT_VERSION, [(c.name, c.type_name) for c in columns]]))
    for item in sorted(encoded):
        digest.update(len(item).to_bytes(8, "big"))
        digest.update(item)
    return "sha256:" + digest.hexdigest()


# Pinned ClickHouse JSONCompact observation profile, shared with its transport
# and paging composition root. These limits do not change the V1 content digest.
SNAPSHOT_MATERIALIZATION_RESPONSE_BYTES = 1024 * 1024
SNAPSHOT_MATERIALIZATION_PAGE_ROWS = 64
_MATERIALIZATION_FRAME_BYTES = 65536
_MATERIALIZATION_CELL_BYTES = 16
_MATERIALIZATION_ROW_BYTES = 16
_MATERIALIZATION_CATALOG_RESPONSES = 32


def snapshot_materialization_catalog_budget(*, max_source_bytes: int, max_rows: int) -> int:
    """Bound an observation's escaped content, all pages, and fixed catalog reads.

    Source canonical JSON bounds unescaped scalar UTF8 bytes. Allow six bytes per
    input byte, 16 framing bytes per cell/row for up to 64 columns, and 64 KiB per
    JSONCompact page (including the possible final empty page). The current
    observer issues fewer than 32 non-content requests, each independently capped
    at 1 MiB. Keep this fixed-query allowance and page profile in sync with the
    observer when changing its query plan. This is a budget, not live evidence.
    """
    if any(type(value) is not int or value < 1 for value in (max_source_bytes, max_rows)):
        raise CompositionAdmissionError("snapshot_materialization_budget")
    pages = max_rows // SNAPSHOT_MATERIALIZATION_PAGE_ROWS + 1
    return (
        6 * max_source_bytes
        + max_rows * (_MATERIALIZATION_ROW_BYTES + 64 * _MATERIALIZATION_CELL_BYTES)
        + pages * _MATERIALIZATION_FRAME_BYTES
        + _MATERIALIZATION_CATALOG_RESPONSES * SNAPSHOT_MATERIALIZATION_RESPONSE_BYTES
    )


def require_snapshot_materialization_pages(
    columns: tuple[ClickHouseDispatchColumn, ...], rows: Iterable[Sequence[object]]
) -> None:
    """Reject source data that cannot fit the pinned independent observer pages.

    Run on already bounded source rows before Native encoding, CREATE or CAPTURED.
    JSON escaping costs at most six bytes per UTF8 byte (including controls and
    Unicode escapes). Add 16 bytes per cell and row for punctuation/pretty output.
    The 64-KiB frame covers <=64 ASCII column names of <=128 bytes, their observed
    String/Nullable(String) metadata, counters and statistics. Keep only the 64
    largest row bounds, so the check holds for any ClickHouse row ordering without
    constructing escaped payloads or retaining another copy of the source rows.
    """
    require_snapshot_columns(columns)
    largest: list[int] = []
    total = 0
    for row in rows:
        if isinstance(row, str | bytes) or len(row) != len(columns):
            raise CompositionAdmissionError("snapshot_materialization_rows")
        size = _MATERIALIZATION_ROW_BYTES
        for value, column in zip(row, columns, strict=True):
            normalized = snapshot_scalar(value, column.type_name)
            try:
                scalar_bytes = 4 if normalized is None else 6 * len(str(normalized).encode("utf-8"))
            except UnicodeError:
                raise CompositionAdmissionError("snapshot_materialization_scalar") from None
            size += _MATERIALIZATION_CELL_BYTES + scalar_bytes
        if len(largest) < SNAPSHOT_MATERIALIZATION_PAGE_ROWS:
            heappush(largest, size)
            total += size
        elif size > largest[0]:
            total += size - heapreplace(largest, size)
        if total + _MATERIALIZATION_FRAME_BYTES > SNAPSHOT_MATERIALIZATION_RESPONSE_BYTES:
            raise CompositionAdmissionError("snapshot_materialization_page_budget")


_VISIBILITY_META = [
    {"name": name, "type": kind}
    for name, kind in (("observer", "String"), ("service", "String"), ("revokes", "UInt64"), ("complete", "UInt64"))
]


def catalog_visibility_original(body: bytes | None, *, expected_service_id: str, expected_observer: str) -> bytes:
    """Validate closed visibility facts and produce their stable V1 evidence.

    ``None`` represents an unavailable transport original. Only one bounded row
    proving the pinned observer/service, zero partial revokes and complete direct
    global grants is accepted. UInt64 canonical strings and integers denote the
    same fact; booleans and other spellings fail closed. Query statistics are
    deliberately excluded: elapsed time is not an authority fact and would make
    unchanged visibility appear to drift between independent observations.
    """
    try:
        if type(body) is not bytes or not 1 <= len(body) <= 8192:
            raise ValueError
        value = strict_json_object(body)
        if (
            value.get("meta") != _VISIBILITY_META
            or type(value.get("rows")) is not int
            or value["rows"] != 1
            or not set(value) <= {"meta", "data", "rows", "statistics"}
            or type(value.get("data")) is not list
            or len(value["data"]) != 1
        ):
            raise ValueError
        row = value["data"][0]
        if type(row) is not list or len(row) != 4 or row[:2] != [expected_observer, expected_service_id]:
            raise ValueError
        for actual, expected in zip(row[2:], (0, 1), strict=True):
            if not (type(actual) is int and actual == expected or type(actual) is str and actual == str(expected)):
                raise ValueError
        return canonical_json_bytes(
            {
                "schema": "dpone.composition-catalog-visibility.v1",
                "observer": row[0],
                "service": row[1],
                "revokes": 0,
                "complete": 1,
            }
        )
    except Exception:
        raise CompositionAdmissionError("snapshot_catalog_visibility") from None
