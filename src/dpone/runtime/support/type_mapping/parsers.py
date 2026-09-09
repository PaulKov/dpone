"""Source type parsers for dpone runtime type mapping."""

from __future__ import annotations

import re

from dpone.runtime.support.type_mapping.models import CanonicalType, ParsedType, TimestampConfig


class _PgTypeParser:
    """Простой парсер PostgreSQL-подобных типов.

    Примеры распознавания:
    - int, int4, integer, serial, bigserial → INTEGER
    - float4, float8, double precision → FLOAT
    - numeric, numeric(10,2), decimal → NUMERIC
    - varchar, varchar(100), character varying(50), text → STRING
    - timestamp, timestamptz → TIMESTAMP
    - date → DATE
    - time → TIME
    - bool, boolean → BOOLEAN
    - json, jsonb → JSON
    - int[], text[], varchar(10)[] → ARRAY<…>
    - иное → UNKNOWN
    """

    _RE_NUMERIC = re.compile(r"^(numeric|decimal)\b", re.IGNORECASE)
    _RE_VARCHAR = re.compile(r"^(varchar|character varying)\b", re.IGNORECASE)
    _RE_TIMESTAMP = re.compile(r"^timestamp\b", re.IGNORECASE)
    _RE_TIME = re.compile(r"^time\b", re.IGNORECASE)

    def parse(self, db_type: str) -> ParsedType:
        t = (db_type or "").strip().lower()

        # Массивы: int[], text[] …
        if t.endswith("[]"):
            elem_str = t[:-2]
            elem_parsed = self.parse(elem_str)
            return ParsedType(base=CanonicalType.ARRAY, element=elem_parsed)

        # Целые
        if (
            t in {"int", "int2", "int4", "int8", "integer", "bigint", "smallint", "serial", "bigserial"}
            or "serial" in t
            or "int" in t
        ):
            return ParsedType(CanonicalType.INTEGER)

        # Вещественные
        if t in {"float", "float4", "float8", "double", "double precision", "real"} or "float" in t or "double" in t:
            return ParsedType(CanonicalType.FLOAT)

        # NUMERIC/DECIMAL
        if self._RE_NUMERIC.match(t):
            return ParsedType(CanonicalType.NUMERIC)

        # Строковые
        if t in {"text", "char", "character"} or self._RE_VARCHAR.match(t) or "varchar" in t:
            return ParsedType(CanonicalType.STRING)

        # Дата/время
        if self._RE_TIMESTAMP.match(t) or "timestamptz" in t:
            return ParsedType(CanonicalType.TIMESTAMP)
        if t == "date":
            return ParsedType(CanonicalType.DATE)
        if self._RE_TIME.match(t):
            return ParsedType(CanonicalType.TIME)

        # Булево
        if t in {"bool", "boolean"}:
            return ParsedType(CanonicalType.BOOLEAN)

        # JSON
        if t in {"json", "jsonb"}:
            return ParsedType(CanonicalType.JSON)

        # По умолчанию
        return ParsedType(CanonicalType.UNKNOWN)


class _ClickHouseTypeParser:
    """Парсер ClickHouse типов для корректного маппинга в BigQuery.

    Поддерживает:
    - Целые: Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64
    - Вещественные: Float32, Float64
    - Decimal: Decimal(P, S), Decimal32, Decimal64, Decimal128, Decimal256
    - Строковые: String, FixedString(N)
    - Дата/время: Date, Date32, DateTime, DateTime64
    - Булево: Bool, Boolean
    - JSON: String (Object('json'))
    - Array: Array(T) → RECORD<list ARRAY<STRUCT<item STRING>>>
    - Nullable: Nullable(T)
    - LowCardinality: LowCardinality(T)

    Примеры:
    - String → STRING
    - Array(String) → RECORD<list ARRAY<STRUCT<item STRING>>>
    - Nullable(String) → STRING (nullable игнорируется в BigQuery)
    - LowCardinality(String) → STRING
    """

    # Regex patterns для сложных типов
    _RE_NULLABLE = re.compile(r"^Nullable\((.*)\)$", re.IGNORECASE)
    _RE_LOW_CARDINALITY = re.compile(r"^LowCardinality\((.*)\)$", re.IGNORECASE)
    _RE_ARRAY = re.compile(r"^Array\((.*)\)$", re.IGNORECASE)
    _RE_FIXED_STRING = re.compile(r"^FixedString\(\d+\)$", re.IGNORECASE)
    _RE_DECIMAL = re.compile(r"^Decimal(\d+)?\((\d+),\s*(\d+)\)$", re.IGNORECASE)
    _RE_DATETIME64 = re.compile(r"^DateTime64\(\d+.*\)$", re.IGNORECASE)

    def parse(self, db_type: str) -> ParsedType:
        """Парсит ClickHouse тип в канонический."""
        t = (db_type or "").strip()

        # Nullable(T) → T (BigQuery поддерживает nullable по умолчанию)
        nullable_match = self._RE_NULLABLE.match(t)
        if nullable_match:
            inner_type = nullable_match.group(1).strip()
            return self.parse(inner_type)

        # LowCardinality(T) → T
        low_card_match = self._RE_LOW_CARDINALITY.match(t)
        if low_card_match:
            inner_type = low_card_match.group(1).strip()
            return self.parse(inner_type)

        # Array(T)
        array_match = self._RE_ARRAY.match(t)
        if array_match:
            elem_type = array_match.group(1).strip()
            elem_parsed = self.parse(elem_type)
            return ParsedType(base=CanonicalType.ARRAY, element=elem_parsed)

        # Теперь проверяем базовые типы (case-insensitive)
        t_lower = t.lower()

        # Целые числа
        if t_lower in {"int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64"}:
            return ParsedType(CanonicalType.INTEGER)

        # Вещественные
        if t_lower in {"float32", "float64"}:
            return ParsedType(CanonicalType.FLOAT)

        # Decimal
        if t_lower.startswith("decimal"):
            return ParsedType(CanonicalType.NUMERIC)

        # Строковые
        if t_lower == "string":
            return ParsedType(CanonicalType.STRING)

        # FixedString → BYTES (для совместимости с Parquet, который экспортирует это как BYTES)
        if self._RE_FIXED_STRING.match(t):
            return ParsedType(CanonicalType.BYTES)

        # Дата
        if t_lower in {"date", "date32"}:
            return ParsedType(CanonicalType.DATE)

        # Дата/время
        if t_lower.startswith("datetime"):
            return ParsedType(CanonicalType.TIMESTAMP)

        # Булево
        if t_lower in {"bool", "boolean"}:
            return ParsedType(CanonicalType.BOOLEAN)

        # По умолчанию - STRING (безопасный fallback)
        return ParsedType(CanonicalType.STRING)


class _PythonTypeParser:
    """Парсер Python типов для API данных.

    Определяет тип данных на основе ЗНАЧЕНИЯ:
    1. Python тип (int, float, bool, datetime, etc.)
    2. Формат строки (ISO datetime, RFC 2822 → TIMESTAMP)

    Используется для API источников (Omnidesk, AppFlyer, etc.).
    """

    # ISO datetime regex (YYYY-MM-DD с опциональным временем)
    _RE_ISO_DATETIME = re.compile(
        r"^\d{4}-\d{2}-\d{2}"  # YYYY-MM-DD
        r"([T\s]\d{2}:\d{2}"  # T или пробел + HH:MM
        r"(:\d{2})?"  # :SS (опционально)
        r"(\.\d{1,6})?"  # .microseconds (опционально)
        r"(Z|[+-]\d{2}:?\d{2})?"  # timezone (опционально)
        r")?$"
    )

    # RFC 2822 datetime regex (Sun, 07 Nov 2021 04:49:18 +0300)
    _RE_RFC2822_DATETIME = re.compile(
        r"^[A-Za-z]{3},\s+"  # Day name + comma
        r"\d{1,2}\s+"  # Day
        r"[A-Za-z]{3}\s+"  # Month name
        r"\d{4}\s+"  # Year
        r"\d{2}:\d{2}(:\d{2})?\s*"  # Time HH:MM(:SS)
        r"([+-]\d{4})?$"  # Timezone +0300
    )

    def parse_value(self, value) -> ParsedType:
        """
        Определяет тип по Python значению.

        Логика основана на ТИПЕ и ФОРМАТЕ значения, не на имени колонки.

        Args:
            value: Python значение (int, str, bool, None, datetime, etc.)

        Returns:
            ParsedType с каноническим типом
        """
        from datetime import date, datetime

        # None → STRING (безопасный default)
        if value is None:
            return ParsedType(CanonicalType.STRING)

        # bool должен проверяться ДО int (bool это подкласс int в Python)
        if isinstance(value, bool):
            return ParsedType(CanonicalType.BOOLEAN)

        if isinstance(value, int):
            return ParsedType(CanonicalType.INTEGER)

        if isinstance(value, float):
            return ParsedType(CanonicalType.FLOAT)

        if isinstance(value, datetime):
            return ParsedType(CanonicalType.TIMESTAMP)

        if isinstance(value, date):
            return ParsedType(CanonicalType.DATE)

        if isinstance(value, str):
            # Определяем по формату строки
            if self._looks_like_timestamp(value):
                return ParsedType(CanonicalType.TIMESTAMP)
            return ParsedType(CanonicalType.STRING)

        if isinstance(value, list | tuple):
            return ParsedType(CanonicalType.ARRAY)

        if isinstance(value, dict):
            return ParsedType(CanonicalType.JSON)

        # Fallback
        return ParsedType(CanonicalType.STRING)

    def _looks_like_timestamp(self, value: str) -> bool:
        """Проверяет, похожа ли строка на datetime (ISO или RFC 2822)."""
        if not value or len(value) < 10:
            return False
        v = value.strip()
        # Проверяем ISO формат: 2024-01-15, 2024-01-15 10:30:00
        if self._RE_ISO_DATETIME.match(v):
            return True
        # Проверяем RFC 2822 формат: Sun, 07 Nov 2021 04:49:18 +0300
        if self._RE_RFC2822_DATETIME.match(v):
            return True
        return False

    def parse_timestamp(self, value):
        """
        Парсит значение в datetime объект.

        Поддерживает:
        - datetime объекты (passthrough)
        - ISO строки: "2024-01-15T10:30:00", "2024-01-15 10:30:00"
        - Только дата: "2024-01-15"
        - RFC 2822: "Sun, 07 Nov 2021 04:49:18 +0300" (Omnidesk API)
        - С timezone: "2024-01-15T10:30:00+03:00", "2024-01-15T10:30:00Z"

        Args:
            value: Строка или datetime

        Returns:
            datetime объект или None если не удалось распарсить
        """
        from datetime import datetime
        from email.utils import parsedate_to_datetime

        if value is None:
            return None

        if isinstance(value, datetime):
            return value

        if isinstance(value, str):
            value = value.strip()
            if not value or value == "-":
                return None

            # Сначала проверяем формат через regex
            if not self._looks_like_timestamp(value):
                return None

            # RFC 2822 формат (Omnidesk API): "Sun, 07 Nov 2021 04:49:18 +0300"
            if self._RE_RFC2822_DATETIME.match(value):
                try:
                    # parsedate_to_datetime возвращает timezone-aware datetime
                    dt = parsedate_to_datetime(value)
                    # Убираем timezone для совместимости с BigQuery TIMESTAMP
                    return dt.replace(tzinfo=None)
                except (ValueError, TypeError):
                    pass

            # ISO форматы
            for fmt in TimestampConfig.DATETIME_FORMATS:
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue

            # Не удалось распарсить
            return None

        return None


__all__ = ["_ClickHouseTypeParser", "_PgTypeParser", "_PythonTypeParser"]
