"""Конвертация timezone для инкрементальных загрузок.

Используется для ClickHouse и PostgreSQL когда source хранит данные
в локальном timezone, а BigQuery — в UTC.
"""

import re
from datetime import UTC, date, datetime
from typing import Any
from zoneinfo import ZoneInfo

import pytz


class TimezoneConverter:
    """
    Конвертирует datetime между UTC и ClickHouse timezone.

    **Назначение:**
    - BigQuery хранит TIMESTAMP в UTC
    - ClickHouse может хранить DateTime64 в другой timezone (например, Europe/Moscow)
    - Для корректного инкремента нужно конвертировать UTC → ClickHouse timezone
    """

    def __init__(self, target_tz: str = "Europe/Moscow"):
        """
        Args:
            target_tz: Целевая timezone для ClickHouse (по умолчанию Europe/Moscow)
        """
        self.target_tz = pytz.timezone(target_tz)
        self.utc_tz = pytz.utc

    def utc_to_clickhouse(self, dt: datetime) -> datetime:
        """
        Конвертирует UTC datetime в ClickHouse timezone.
        """
        # Если timezone не указан, считаем что это UTC
        if dt.tzinfo is None:
            dt = self.utc_tz.localize(dt)

        # Конвертируем в ClickHouse timezone
        return dt.astimezone(self.target_tz)

    def parse_and_convert(self, value: Any, from_tz: str | None = None) -> datetime:
        """
        Парсит значение (строка, datetime, date) и конвертирует в ClickHouse timezone.
        """
        source_tz = pytz.timezone(from_tz) if from_tz else self.utc_tz

        # Если уже datetime объект
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = source_tz.localize(value)
            return value.astimezone(self.target_tz)

        # Если date объект - конвертируем в datetime (начало дня)
        if isinstance(value, date):
            dt = datetime.combine(value, datetime.min.time())
            dt = source_tz.localize(dt)
            return dt.astimezone(self.target_tz)

        # Если строка - парсим
        if isinstance(value, str):
            # Убираем timezone суффиксы (+00:00, Z)
            clean_str = value.replace("+00:00", "").replace("Z", "")

            # Пробуем разные форматы
            for fmt in [
                "%Y-%m-%d %H:%M:%S.%f",  # С микросекундами
                "%Y-%m-%d %H:%M:%S",  # Без микросекунд
                "%Y-%m-%d",  # Только дата
            ]:
                try:
                    dt = datetime.strptime(clean_str.strip(), fmt)
                    dt = source_tz.localize(dt)
                    return dt.astimezone(self.target_tz)
                except ValueError:
                    continue

            # Если не удалось распарсить, пробуем fromisoformat
            try:
                dt = datetime.fromisoformat(clean_str.strip())
                if dt.tzinfo is None:
                    dt = source_tz.localize(dt)
                return dt.astimezone(self.target_tz)
            except ValueError:
                pass

        raise ValueError(f"Не удалось распарсить значение: {value} (тип: {type(value)})")

    def format_for_clickhouse(self, dt: datetime, include_milliseconds: bool = True) -> str:
        """
        Форматирует datetime для использования в ClickHouse запросе.
        """
        if include_milliseconds:
            return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        else:
            return dt.strftime("%Y-%m-%d %H:%M:%S")

    def get_current_date_in_tz(self) -> date:
        """
        Возвращает текущую дату в ClickHouse timezone.
        """
        return datetime.now(self.target_tz).date()

    def convert_and_format(
        self,
        value: Any,
        include_microseconds: bool = True,
    ) -> str | None:
        """
        Конвертирует UTC timestamp в target timezone и форматирует для SQL WHERE clause.

        Args:
            value: Timestamp из BigQuery (в UTC)
            include_microseconds: Включать микросекунды в результат

        Returns:
            Отформатированная строка или None если не удалось распарсить
        """
        dt = self.parse_timestamp(value)

        if dt is None:
            return None

        # Если datetime naive — считаем что это UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))

        # Конвертируем в target timezone
        dt = dt.astimezone(self.target_tz)

        # Форматируем без timezone info (naive timestamp для PostgreSQL/ClickHouse)
        if include_microseconds:
            return dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        else:
            return dt.strftime("%Y-%m-%d %H:%M:%S")

    def parse_timestamp(self, value: Any) -> datetime | None:
        """
        Парсит timestamp из различных форматов.

        Поддерживает:
        - datetime объекты
        - ISO форматы с/без timezone
        - Форматы с микросекундами
        """
        if isinstance(value, datetime):
            return value

        if isinstance(value, date) and not isinstance(value, datetime):
            return datetime.combine(value, datetime.min.time())

        if not isinstance(value, str):
            return None

        cleaned = re.sub(r"\s*[+-]\d{2}:?\d{2}$", "", value)
        cleaned = cleaned.replace("Z", "").strip()

        # Пробуем разные форматы
        formats = [
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(cleaned, fmt)
            except ValueError:
                continue

        # Пробуем fromisoformat как fallback
        try:
            return datetime.fromisoformat(cleaned)
        except ValueError:
            return None


def format_timestamp_for_sql(
    value: Any,
    source_timezone: str | None = None,
) -> str:
    """
    Форматирует timestamp для использования в PostgreSQL WHERE clause.

    """
    converter = TimezoneConverter(target_tz="UTC")
    dt = converter.parse_timestamp(value)

    if dt is None:
        return str(value)

    if dt.tzinfo is None:
        from zoneinfo import ZoneInfo

        dt = dt.replace(tzinfo=ZoneInfo("UTC"))

    if source_timezone:
        from zoneinfo import ZoneInfo

        try:
            local_tz = ZoneInfo(source_timezone)
            local_dt = dt.astimezone(local_tz)
            return local_dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        except Exception:
            pass

    return dt.strftime("%Y-%m-%d %H:%M:%S.%f+00:00")


def to_unix_timestamp(value: Any) -> int:
    """
    Конвертирует datetime/date/int в unix timestamp (секунды с 1970-01-01).

    Args:
        value: datetime, date, int или строка ISO формата

    Returns:
        Unix timestamp (int)

    """
    # Если уже int — возвращаем как есть
    if isinstance(value, int):
        return value

    # Если datetime
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return int(value.timestamp())

    # Если date (но не datetime)
    if isinstance(value, date):
        dt = datetime.combine(value, datetime.min.time(), tzinfo=UTC)
        return int(dt.timestamp())

    # Если строка — парсим
    if isinstance(value, str):
        converter = TimezoneConverter(target_tz="UTC")
        dt = converter.parse_timestamp(value)
        if dt is not None:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return int(dt.timestamp())

    raise ValueError(f"Не удалось конвертировать в unix timestamp: {value} (тип: {type(value)})")
