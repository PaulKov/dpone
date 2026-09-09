from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from dpone.runtime.sources.strategies.api.yandex_webmaster.dates import (
    MOSCOW_TZ,
    parse_yandex_datetime,
    to_msk_date,
    yesterday_msk,
)
from dpone.runtime.sources.strategies.api.yandex_webmaster.regions import (
    YANDEX_WEBMASTER_DEFAULT_DEVICE_TYPES,
    YANDEX_WEBMASTER_DEFAULT_REGIONS,
    YANDEX_WEBMASTER_REGION_NAME_BY_ID,
    YandexWebmasterRegion,
)


def parse_date_option(value: Any) -> date | None:
    if value in (None, "", "null"):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def parse_csv_list(value: Any) -> tuple[str, ...]:
    if value in (None, "", "null"):
        return ()
    if isinstance(value, str):
        return tuple(item.strip() for item in value.split(",") if item.strip())
    return tuple(str(item).strip() for item in value if str(item).strip())


def parse_device_types(value: Any) -> tuple[str, ...]:
    raw = parse_csv_list(value)
    device_types = raw or YANDEX_WEBMASTER_DEFAULT_DEVICE_TYPES
    return tuple(item.upper() for item in device_types)


def parse_int_list_option(value: Any) -> tuple[int, ...]:
    raw = parse_csv_list(value)
    if raw:
        return tuple(int(item) for item in raw)
    if value in (None, "", "null"):
        return ()
    if isinstance(value, int):
        return (value,)
    return tuple(int(item) for item in value)


def resolve_window(
    *,
    options: dict[str, Any],
    default_days: int,
) -> tuple[date, date]:
    explicit_start = parse_date_option(options.get("start_date"))
    explicit_end = parse_date_option(options.get("end_date"))
    single_day = parse_date_option(options.get("day"))

    if explicit_start is not None:
        from_day = explicit_start
        to_day = explicit_end or explicit_start
    elif single_day is not None:
        from_day = single_day
        to_day = single_day
    else:
        to_day = yesterday_msk()
        from_day = to_day - timedelta(days=max(default_days - 1, 0))

    if from_day > to_day:
        raise ValueError(f"Yandex Webmaster extract window is invalid: {from_day} > {to_day}")
    return from_day, to_day


def build_date_replace_predicate(*, date_from: date, date_to: date, column: str = "date") -> str:
    if date_from == date_to:
        return f"{column} = DATE '{date_from.isoformat()}'"
    return f"{column} BETWEEN DATE '{date_from.isoformat()}' AND DATE '{date_to.isoformat()}'"


def resolve_regions(region_ids: Any) -> tuple[YandexWebmasterRegion, ...]:
    parsed = parse_int_list_option(region_ids)
    if not parsed:
        return YANDEX_WEBMASTER_DEFAULT_REGIONS
    return tuple(
        YandexWebmasterRegion(
            region_id=item, region_name=YANDEX_WEBMASTER_REGION_NAME_BY_ID.get(item, f"Region {item}")
        )
        for item in parsed
    )


def _within_window(day: date, *, date_from: date, date_to: date) -> bool:
    return date_from <= day <= date_to


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


__all__ = [
    "MOSCOW_TZ",
    "YANDEX_WEBMASTER_DEFAULT_DEVICE_TYPES",
    "build_date_replace_predicate",
    "parse_csv_list",
    "parse_date_option",
    "parse_device_types",
    "parse_int_list_option",
    "parse_yandex_datetime",
    "resolve_regions",
    "resolve_window",
    "to_msk_date",
    "yesterday_msk",
]
