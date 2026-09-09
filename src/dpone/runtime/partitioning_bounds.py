"""Typed range-bound helpers for Spark-class JDBC partitioning."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any


class PartitionBoundKind(str, Enum):  # noqa: UP042
    """Source-neutral partition boundary families."""

    NUMERIC = "numeric"
    DATE = "date"
    DATETIME = "datetime"
    DATETIME_OFFSET = "datetimeoffset"
    ROWVERSION = "rowversion"
    TIME_CYCLIC = "time_cyclic"


@dataclass(frozen=True, slots=True)
class PartitionBoundaryResolution:
    """Resolved boundary type and source precision hints."""

    kind: PartitionBoundKind
    source_type: str = ""
    scale: int | None = None
    warning_code: str | None = None


class PartitionBoundaryTypeResolver:
    """Resolve source metadata and Python DB values into boundary families."""

    @classmethod
    def resolve(
        cls,
        *,
        source_type: str | None = None,
        source_system: str | None = None,
        boundary_type: str | None = None,
        sample_values: tuple[Any, ...] = (),
    ) -> PartitionBoundaryResolution:
        requested = _clean(boundary_type)
        if requested and requested != "auto":
            return cls._from_type_name(requested, source_system=source_system)

        source = _clean(source_type)
        if source:
            return cls._from_type_name(source, source_system=source_system)

        for value in sample_values:
            if value is None:
                continue
            inferred = cls._from_value(value)
            if inferred is not None:
                return inferred
        return PartitionBoundaryResolution(PartitionBoundKind.NUMERIC)

    @classmethod
    def _from_type_name(cls, type_name: str, *, source_system: str | None) -> PartitionBoundaryResolution:
        normalized = _clean(type_name)
        if source_system and _clean(source_system) == "mssql" and normalized in {"timestamp", "rowversion"}:
            return PartitionBoundaryResolution(
                PartitionBoundKind.ROWVERSION,
                source_type=normalized,
                warning_code="mssql_timestamp_is_rowversion",
            )
        if normalized in {"rowversion", "bytea_boundary"}:
            return PartitionBoundaryResolution(PartitionBoundKind.ROWVERSION, source_type=normalized)
        if normalized == "date":
            return PartitionBoundaryResolution(PartitionBoundKind.DATE, source_type=normalized)
        if normalized in {"datetime", "smalldatetime"} or normalized.startswith("datetime2"):
            return PartitionBoundaryResolution(
                PartitionBoundKind.DATETIME,
                source_type=normalized,
                scale=_temporal_scale(normalized),
            )
        if normalized in {"datetimeoffset", "timestamptz", "timestamp with time zone"}:
            return PartitionBoundaryResolution(
                PartitionBoundKind.DATETIME_OFFSET,
                source_type=normalized,
                scale=_temporal_scale(normalized),
            )
        if normalized in {"timestamp", "timestamp without time zone", "datetime64"}:
            return PartitionBoundaryResolution(
                PartitionBoundKind.DATETIME,
                source_type=normalized,
                scale=_temporal_scale(normalized),
            )
        if normalized == "time":
            return PartitionBoundaryResolution(PartitionBoundKind.TIME_CYCLIC, source_type=normalized)
        return PartitionBoundaryResolution(PartitionBoundKind.NUMERIC, source_type=normalized)

    @classmethod
    def _from_value(cls, value: Any) -> PartitionBoundaryResolution | None:
        if isinstance(value, datetime):
            if value.tzinfo is not None and value.utcoffset() is not None:
                return PartitionBoundaryResolution(PartitionBoundKind.DATETIME_OFFSET, scale=6)
            return PartitionBoundaryResolution(PartitionBoundKind.DATETIME, scale=6)
        if isinstance(value, date):
            return PartitionBoundaryResolution(PartitionBoundKind.DATE)
        if isinstance(value, bytes | bytearray):
            return PartitionBoundaryResolution(PartitionBoundKind.ROWVERSION)
        if isinstance(value, int | float | Decimal):
            return PartitionBoundaryResolution(PartitionBoundKind.NUMERIC)
        if isinstance(value, str):
            stripped = value.strip()
            if _DATE_RE.fullmatch(stripped):
                return PartitionBoundaryResolution(PartitionBoundKind.DATE)
            if "T" in stripped or ":" in stripped:
                try:
                    parsed = _parse_datetime(stripped)
                except ValueError:
                    return None
                if parsed.tzinfo is not None and parsed.utcoffset() is not None:
                    return PartitionBoundaryResolution(
                        PartitionBoundKind.DATETIME_OFFSET, scale=_fractional_scale(stripped)
                    )
                return PartitionBoundaryResolution(PartitionBoundKind.DATETIME, scale=_fractional_scale(stripped))
            try:
                Decimal(stripped)
            except Exception:
                return None
            return PartitionBoundaryResolution(PartitionBoundKind.NUMERIC)
        return None


def normalize_partition_bound(value: Any, resolution: PartitionBoundaryResolution) -> Any:
    """Normalize a source bound value without losing its semantic family."""

    if value is None:
        return None
    if resolution.kind == PartitionBoundKind.DATE:
        return _parse_date(value)
    if resolution.kind in {PartitionBoundKind.DATETIME, PartitionBoundKind.DATETIME_OFFSET}:
        parsed = _parse_datetime(value)
        if resolution.kind == PartitionBoundKind.DATETIME_OFFSET and parsed.tzinfo is not None:
            return parsed.astimezone(timezone.utc)  # noqa: UP017
        return parsed
    if resolution.kind == PartitionBoundKind.ROWVERSION:
        return _rowversion_to_int(value)
    if resolution.kind == PartitionBoundKind.TIME_CYCLIC:
        raise ValueError("time-only partition boundaries are cyclic and require an explicit certified policy.")
    return _normalize_numeric(value)


def format_datetime_literal(value: datetime, *, scale: int | None) -> str:
    """Format datetime literals up to SQL Server datetime2(7) precision."""

    base = value.replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%S")
    digits = max(0, min(7, int(scale if scale is not None else 6)))
    if digits == 0:
        return base
    fraction = f"{value.microsecond:06d}" + "0"
    return f"{base}.{fraction[:digits]}"


def format_rowversion_literal(value: Any) -> str:
    numeric = _rowversion_to_int(value)
    width = max(16, ((numeric.bit_length() + 7) // 8) * 2)
    return "0x" + f"{numeric:0{width}X}"


def _normalize_numeric(value: Any) -> int | float | Decimal:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int | float | Decimal):
        return value
    text = str(value).strip()
    if re.fullmatch(r"[-+]?\d+", text):
        return int(text)
    return Decimal(text)


def _parse_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value).strip())


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    text = str(value).strip().replace("Z", "+00:00")
    if "." in text:
        head, tail = text.split(".", 1)
        zone = ""
        match = re.search(r"([+-]\d\d:\d\d)$", tail)
        if match:
            zone = match.group(1)
            tail = tail[: -len(zone)]
        text = f"{head}.{tail[:6].ljust(6, '0')}{zone}"
    return datetime.fromisoformat(text)


def _rowversion_to_int(value: Any) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, bytes | bytearray):
        return int.from_bytes(value, byteorder="big", signed=False)
    text = str(value).strip()
    if text.lower().startswith("0x"):
        return int(text[2:], 16)
    return int(text)


def _temporal_scale(type_name: str) -> int | None:
    normalized = _clean(type_name)
    match = re.search(r"\((\d+)\)", normalized)
    if match:
        return int(match.group(1))
    if normalized == "datetime2":
        return 7
    if normalized == "datetimeoffset":
        return 7
    if normalized == "datetime":
        return 3
    if normalized == "smalldatetime":
        return 0
    return None


def _fractional_scale(value: str) -> int | None:
    match = re.search(r"\.(\d+)", value)
    if not match:
        return 0
    return min(7, len(match.group(1)))


def _clean(value: str | None) -> str:
    return str(value or "").strip().lower()


_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
