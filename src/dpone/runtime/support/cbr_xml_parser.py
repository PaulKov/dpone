"""Helpers for parsing XML responses from the public CBR API."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import date, datetime
from typing import Any

from dpone._compat import UTC


def resolve_date_option(value: Any) -> date | None:
    """Resolves a date option from YAML/runtime values.

    Supported values:
    - ``date``
    - ``datetime``
    - ISO date string ``YYYY-MM-DD``
    - ``today`` / ``today()``
    """

    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        raw = value.strip().lower()
        if raw in {"today", "today()"}:
            return datetime.now(UTC).date()
        return datetime.strptime(value.strip(), "%Y-%m-%d").date()
    raise ValueError(f"Unsupported CBR date option: {value!r}")


def _safe_strip(value: str | None) -> str:
    return (value or "").strip()


def _parse_int(value: str | None) -> int | None:
    raw = _safe_strip(value)
    if not raw:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _parse_float(value: str | None) -> float | None:
    raw = _safe_strip(value)
    if not raw:
        return None
    try:
        return float(raw.replace(",", "."))
    except (TypeError, ValueError):
        return None


def parse_xml_daily(xml_text: str, loaded_at_utc: datetime) -> list[dict[str, Any]]:
    """Parses XML from ``XML_daily.asp`` into normalized rows."""

    root = ET.fromstring(xml_text)
    as_of_date_raw = root.attrib.get("Date")
    as_of_date: str | None = None
    if as_of_date_raw:
        try:
            as_of_date = datetime.strptime(as_of_date_raw, "%d.%m.%Y").date().isoformat()
        except ValueError:
            as_of_date = None

    loaded_at_str = loaded_at_utc.replace(microsecond=0).isoformat()
    rows: list[dict[str, Any]] = []
    for valute in root.findall("Valute"):
        raw_value = _safe_strip(valute.findtext("Value"))
        value = _parse_float(raw_value)
        nominal = _parse_int(valute.findtext("Nominal"))
        vunit_rate: float | None = None
        if value is not None and nominal not in (None, 0):
            vunit_rate = value / nominal

        rows.append(
            {
                "as_of_date": as_of_date,
                "valute_id": valute.attrib.get("ID"),
                "num_code": _safe_strip(valute.findtext("NumCode")),
                "char_code": _safe_strip(valute.findtext("CharCode")),
                "nominal": nominal,
                "name": _safe_strip(valute.findtext("Name")),
                "value": value,
                "vunit_rate": vunit_rate,
                "raw_value": raw_value,
                "loaded_at": loaded_at_str,
            }
        )
    return rows


def deduplicate_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deduplicates rows by ``(as_of_date, valute_id)`` keeping the latest occurrence."""

    dedup: dict[tuple[Any, Any], dict[str, Any]] = {}
    for row in rows:
        dedup[(row.get("as_of_date"), row.get("valute_id"))] = row
    return list(dedup.values())


__all__ = ["resolve_date_option", "parse_xml_daily", "deduplicate_rows"]
