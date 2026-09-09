from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

MOSCOW_TZ = ZoneInfo("Europe/Moscow")


def yesterday_msk() -> date:
    return datetime.now(MOSCOW_TZ).date() - timedelta(days=1)


def parse_yandex_datetime(value: str) -> datetime:
    formats = (
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S,%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d",
    )
    for fmt in formats:
        try:
            dt = datetime.strptime(value, fmt)
            if fmt == "%Y-%m-%d":
                return dt.replace(tzinfo=MOSCOW_TZ)
            return dt
        except ValueError:
            continue
    raise ValueError(f"Unsupported Yandex datetime format: {value}")


def to_msk_date(value: str) -> date:
    return parse_yandex_datetime(value).astimezone(MOSCOW_TZ).date()


__all__ = ["MOSCOW_TZ", "parse_yandex_datetime", "to_msk_date", "yesterday_msk"]
