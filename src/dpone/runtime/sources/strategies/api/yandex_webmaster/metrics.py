from __future__ import annotations

from datetime import date, datetime
from typing import Any

from dpone.runtime.connectors.api.yandex_webmaster_resources import get_yandex_webmaster_resource
from dpone.runtime.sources.strategies.api.yandex_webmaster.dates import (
    MOSCOW_TZ,
    parse_yandex_datetime,
    to_msk_date,
)


def aggregate_indexing_history(payload: dict[str, Any]) -> dict[date, dict[str, int | None]]:
    result: dict[date, dict[str, int | None]] = {}
    indicators = payload.get("indicators", {}) or {}
    status_map = {
        "HTTP_2XX": "http_2xx",
        "HTTP_3XX": "http_3xx",
        "HTTP_4XX": "http_4xx",
    }

    for source_status, target_field in status_map.items():
        for item in indicators.get(source_status, []) or []:
            day = to_msk_date(item["date"])
            value = int(item["value"])
            bucket = result.setdefault(
                day,
                {
                    "http_2xx": None,
                    "http_3xx": None,
                    "http_4xx": None,
                },
            )
            current_value = bucket[target_field]
            if current_value is None or value > current_value:
                bucket[target_field] = value
    return result


def aggregate_in_search_history(
    payload: dict[str, Any],
    *,
    daily_agg: str = "last",
) -> dict[date, dict[str, int | None]]:
    result: dict[date, dict[str, int | None]] = {}
    history = payload.get("history", []) or []

    if daily_agg not in {"last", "max"}:
        raise ValueError(f"Unsupported pages_in_search_daily_agg='{daily_agg}'. Use 'last' or 'max'.")

    if daily_agg == "last":
        temp: dict[date, tuple[datetime, int]] = {}
        for item in history:
            dt = parse_yandex_datetime(item["date"]).astimezone(MOSCOW_TZ)
            day = dt.date()
            value = int(item["value"])
            current = temp.get(day)
            if current is None or dt > current[0]:
                temp[day] = (dt, value)
        for day, (_, value) in temp.items():
            result[day] = {"pages_in_search": value}
        return result

    temp_max: dict[date, int] = {}
    for item in history:
        day = to_msk_date(item["date"])
        value = int(item["value"])
        current = temp_max.get(day)
        if current is None or value > current:
            temp_max[day] = value
    for day, value in temp_max.items():
        result[day] = {"pages_in_search": value}
    return result


def aggregate_search_events(payload: dict[str, Any]) -> dict[date, dict[str, int | None]]:
    result: dict[date, dict[str, int | None]] = {}
    indicators = payload.get("indicators", {}) or {}
    event_map = {
        "APPEARED_IN_SEARCH": "appeared_in_search",
        "REMOVED_FROM_SEARCH": "removed_from_search",
    }

    for source_event, target_field in event_map.items():
        for item in indicators.get(source_event, []) or []:
            day = to_msk_date(item["date"])
            value = int(item["value"])
            bucket = result.setdefault(
                day,
                {
                    "appeared_in_search": None,
                    "removed_from_search": None,
                },
            )
            current_value = bucket[target_field]
            bucket[target_field] = value if current_value is None else current_value + value
    return result


def validate_unique_dates(rows: list[dict[str, Any]]) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for row in rows:
        day = str(row["date"])
        if day in seen:
            duplicates.add(day)
        seen.add(day)
    if duplicates:
        dup_list = ", ".join(sorted(duplicates))
        raise ValueError(f"Yandex Webmaster extract produced duplicate dates: {dup_list}")


def validate_unique_keys(rows: list[dict[str, Any]], *, key_columns: tuple[str, ...]) -> None:
    seen: set[tuple[str, ...]] = set()
    duplicates: set[tuple[str, ...]] = set()
    for row in rows:
        key = tuple(str(row.get(column)) for column in key_columns)
        if key in seen:
            duplicates.add(key)
        seen.add(key)
    if duplicates:
        dup_list = ", ".join(f"({', '.join(item)})" for item in sorted(duplicates)[:5])
        raise ValueError(f"Yandex Webmaster extract produced duplicate keys for {key_columns}: {dup_list}")


def merge_daily_metrics(
    *,
    indexing: dict[date, dict[str, int | None]],
    in_search: dict[date, dict[str, int | None]],
    events: dict[date, dict[str, int | None]],
) -> list[dict[str, Any]]:
    all_days = sorted(set(indexing) | set(in_search) | set(events))
    rows: list[dict[str, Any]] = []
    for day in all_days:
        row = {
            "date": day.isoformat(),
            "http_2xx": None,
            "http_3xx": None,
            "http_4xx": None,
            "pages_in_search": None,
            "appeared_in_search": None,
            "removed_from_search": None,
        }
        row.update(indexing.get(day, {}))
        row.update(in_search.get(day, {}))
        row.update(events.get(day, {}))
        rows.append(row)
    validate_unique_dates(rows)
    return rows


def ensure_host_metrics_daily_resource(resource: str | None) -> str:
    resolved = str(resource or "host_metrics_daily")
    if resolved != "host_metrics_daily":
        raise ValueError("Yandex Webmaster canonical runtime supports only options.resource='host_metrics_daily'")
    return resolved


def resolve_yandex_webmaster_resource(resource: str | None):
    return get_yandex_webmaster_resource(str(resource or "host_metrics_daily"))
