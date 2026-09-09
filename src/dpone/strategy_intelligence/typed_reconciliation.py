"""Typed row hashing for source-target reconciliation evidence."""

from __future__ import annotations

import hashlib
import json
import re
from base64 import b64decode, b64encode
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from dpone._compat import UTC
from dpone.runtime.support.temporal_fidelity import is_offset_timestamp_type
from dpone.runtime.support.type_mapping.mssql_clickhouse import MssqlClickHouseTypePolicy


@dataclass(frozen=True, slots=True)
class TypedColumnSpec:
    """Column metadata used to build stable typed row hashes."""

    name: str
    source_type: str


class TypedRowHashService:
    """Hash rows using type-aware canonical serialization."""

    def __init__(
        self,
        columns: Sequence[TypedColumnSpec],
        *,
        policy: MssqlClickHouseTypePolicy | None = None,
    ) -> None:
        self._columns = tuple(columns)
        self._policy = policy or MssqlClickHouseTypePolicy()

    def hash_rows(self, rows: Sequence[Sequence[Any]]) -> str:
        digest = hashlib.sha256()
        for row in rows:
            values = [
                _canonical_value(value, self._columns[index].source_type, self._policy)
                for index, value in enumerate(row[: len(self._columns)])
            ]
            digest.update(json.dumps(values, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode())
            digest.update(b"\n")
        return "sha256:" + digest.hexdigest()


def _canonical_value(value: Any, source_type: str, policy: MssqlClickHouseTypePolicy) -> dict[str, Any]:
    if value is None:
        return {"type": "null", "value": None}
    normalized = _normalize_type(source_type)
    base = _strip_size(normalized)
    if _is_exact_decimal(normalized) or base in {"money", "smallmoney"}:
        return {"type": "decimal", "value": _decimal_text(value, _decimal_scale(normalized, base))}
    if base in {"bigint", "int", "smallint", "tinyint"}:
        return {"type": "integer", "value": str(int(value))}
    if base == "bit":
        return {"type": "boolean", "value": bool(value)}
    if base == "uniqueidentifier":
        return {"type": "uuid", "value": str(value).lower()}
    if base in {"date"}:
        return {"type": "date", "value": _date_text(value)}
    if is_offset_timestamp_type(normalized):
        return {"type": "offset_timestamp", "value": _offset_timestamp_text(value, _datetime_scale(normalized), policy)}
    if base in {"datetime", "smalldatetime"} or normalized.startswith("datetime2"):
        return {"type": "timestamp", "value": _timestamp_text(value, _datetime_scale(normalized))}
    if isinstance(value, bytes | bytearray):
        return {"type": "binary", "value": _binary_text(value, policy)}
    if base in {"binary", "varbinary", "image", "rowversion", "timestamp"}:
        return {"type": "binary", "value": _binary_text(value, policy)}
    if _is_time_type(normalized):
        return {"type": "time", "value": _time_text(value, policy)}
    return {"type": "string", "value": str(value)}


def _decimal_text(value: Any, scale: int) -> str:
    if isinstance(value, Decimal):
        decimal = value
    elif isinstance(value, float):
        decimal = Decimal(format(value, ".15g"))
    else:
        decimal = Decimal(str(value))
    quantized = decimal.quantize(Decimal(1).scaleb(-scale)) if scale >= 0 else decimal
    return format(quantized, "f")


def _date_text(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)[:10]


def _binary_text(value: Any, policy: MssqlClickHouseTypePolicy) -> str:
    if isinstance(value, bytes | bytearray):
        raw = bytes(value)
        return b64encode(raw).decode("ascii") if policy.binary_encoding == "base64" else raw.hex()
    text = str(value).strip()
    if policy.binary_encoding == "base64":
        return b64encode(b64decode(text)).decode("ascii")
    return text.lower().removeprefix("0x")


def _time_text(value: Any, policy: MssqlClickHouseTypePolicy) -> str:
    if policy.time_encoding != "seconds_since_midnight":
        return str(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, time):
        return str(value.hour * 3600 + value.minute * 60 + value.second)
    text = str(value).strip()
    parts = text.split(":", 2)
    if len(parts) < 3:
        return text
    seconds = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(float(parts[2]))
    return str(seconds)


def _offset_timestamp_text(value: Any, scale: int, policy: MssqlClickHouseTypePolicy) -> dict[str, Any] | str:
    mode = policy.temporal.offset_timestamp_mode
    text = str(value).strip().replace(" ", "T")
    aware = _aware_datetime(value)
    if mode == "preserve_text":
        return _timestamp_text(value, scale)
    if mode == "fixed_timezone":
        converted = aware.astimezone(ZoneInfo(policy.temporal.target_timezone)).replace(tzinfo=None) if aware else value
        return _timestamp_text(converted, scale)
    if mode == "preserve_offset":
        offset_minutes = _offset_minutes(value)
        utc_text = _timestamp_text(aware.astimezone(UTC).replace(tzinfo=None) if aware else text, scale)
        return {"instant": utc_text, "offset_minutes": offset_minutes}
    return _timestamp_text(aware.astimezone(UTC).replace(tzinfo=None) if aware else text, scale)


def _aware_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else None
    text = str(value).strip().replace(" ", "T")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _offset_minutes(value: Any) -> int | None:
    if isinstance(value, datetime) and value.tzinfo is not None:
        offset = value.utcoffset()
        return int(offset.total_seconds() // 60) if offset is not None else None
    text = str(value).strip()
    suffix = _timezone_suffix(text)
    if not suffix:
        return None
    sign = -1 if suffix.startswith("-") else 1
    hours, minutes = suffix[1:].split(":", 1)
    return sign * (int(hours) * 60 + int(minutes))


def _timestamp_text(value: Any, scale: int) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(UTC).replace(tzinfo=None)
        text = value.isoformat()
    else:
        text = str(value).strip().replace(" ", "T")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    main, fraction, suffix = _split_timestamp(text)
    fraction = (fraction + ("0" * scale))[:scale] if scale else ""
    return f"{main}.{fraction}{suffix}" if fraction else f"{main}{suffix}"


def _split_timestamp(text: str) -> tuple[str, str, str]:
    if "." not in text:
        suffix = _timezone_suffix(text)
        if suffix:
            return text[: -len(suffix)], "", suffix
        return text, "", ""
    main, tail = text.split(".", 1)
    suffix = ""
    for marker in ("+", "-"):
        if marker in tail:
            fraction, suffix_tail = tail.split(marker, 1)
            suffix = marker + suffix_tail
            break
    else:
        fraction = tail
    return main, "".join(ch for ch in fraction if ch.isdigit()), suffix


def _timezone_suffix(text: str) -> str:
    match = re.search(r"([+-]\d{2}:\d{2})$", text)
    return match.group(1) if match else ""


def _normalize_type(source_type: str) -> str:
    normalized = str(source_type or "").strip().lower().replace(" nullable", "")
    if normalized.startswith("nullable(") and normalized.endswith(")"):
        return normalized[len("nullable(") : -1].strip()
    return normalized


def _strip_size(normalized: str) -> str:
    return re.sub(r"\(.*\)$", "", normalized).strip()


def _is_exact_decimal(normalized: str) -> bool:
    return bool(re.match(r"^(decimal|numeric)\(\d+\s*,\s*\d+\)$", normalized))


def _is_time_type(normalized: str) -> bool:
    return normalized == "time" or normalized.startswith("time(")


def _decimal_scale(normalized: str, base: str) -> int:
    if base == "money":
        return 4
    if base == "smallmoney":
        return 4
    match = re.match(r"^(decimal|numeric)\(\d+\s*,\s*(\d+)\)$", normalized)
    if not match:
        return 0
    return int(match.group(2))


def _datetime_scale(normalized: str) -> int:
    if normalized == "datetime":
        return 3
    if normalized == "smalldatetime":
        return 0
    match = re.search(r"\((\d+)\)", normalized)
    return min(max(int(match.group(1)), 0), 9) if match else 7


__all__ = ["TypedColumnSpec", "TypedRowHashService"]
