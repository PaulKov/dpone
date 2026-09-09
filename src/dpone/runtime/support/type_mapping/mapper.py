"""Compatibility mapper facade implemented with parser and dialect services."""

from __future__ import annotations

import re

from dpone.runtime.support.type_mapping.dialects import _BigQueryDialect, _PostgresDialect
from dpone.runtime.support.type_mapping.models import CanonicalType
from dpone.runtime.support.type_mapping.parsers import _ClickHouseTypeParser, _PgTypeParser, _PythonTypeParser

# Already-BigQuery type names (optionally parameterized) must not be re-parsed as Postgres.
_BQ_NATIVE_TYPE = re.compile(
    r"^(?P<base>INT64|INTEGER|FLOAT64|FLOAT|NUMERIC|BIGNUMERIC|STRING|BYTES|BOOL|BOOLEAN|"
    r"DATE|DATETIME|TIME|TIMESTAMP|JSON)"
    r"(?:\((?P<precision>\d+)\s*,\s*(?P<scale>\d+)\))?$",
    re.IGNORECASE,
)
_BQ_BASE_ALIASES = {
    "INTEGER": "INT64",
    "FLOAT": "FLOAT64",
    "BOOLEAN": "BOOL",
}


class DataTypeMapper:
    """Фасад для маппинга типов между различными СУБД.

    Поддерживает автоматическое определение типа источника:
    - PostgreSQL типы (через _PgTypeParser)
    - ClickHouse типы (через _ClickHouseTypeParser)
    - Python типы для API (через _PythonTypeParser)
    """

    _pg_parser = _PgTypeParser()
    _ch_parser = _ClickHouseTypeParser()
    _py_parser = _PythonTypeParser()

    @classmethod
    def _detect_source_db(cls, db_type: str) -> str:
        """Автоопределение типа источника по характерным признакам."""
        t = (db_type or "").strip()

        # ClickHouse характерные типы
        ch_markers = [
            "Nullable(",
            "LowCardinality(",
            "Array(",
            "Int8",
            "Int16",
            "Int32",
            "Int64",
            "UInt8",
            "UInt16",
            "UInt32",
            "UInt64",
            "Float32",
            "Float64",
            "FixedString(",
            "DateTime64",
            "DateTime(",
            "Decimal32",
            "Decimal64",
            "Decimal128",
            "Decimal256",
        ]

        for marker in ch_markers:
            if marker in t:
                return "clickhouse"

        # По умолчанию считаем PostgreSQL
        return "postgres"

    @classmethod
    def normalize(cls, db_type: str, source_db: str = "auto") -> CanonicalType:
        """Нормализует тип в канонический."""
        if source_db == "auto":
            source_db = cls._detect_source_db(db_type)

        if source_db == "clickhouse":
            return cls._ch_parser.parse(db_type).base
        return cls._pg_parser.parse(db_type).base

    @classmethod
    def to_bigquery(cls, db_type: str, source_db: str = "auto") -> str:
        """Конвертирует тип в BigQuery."""
        passthrough = _passthrough_bigquery_type(db_type)
        if passthrough is not None:
            return passthrough
        if source_db == "auto":
            source_db = cls._detect_source_db(db_type)

        if source_db == "clickhouse":
            parsed = cls._ch_parser.parse(db_type)
        else:
            parsed = cls._pg_parser.parse(db_type)

        return _BigQueryDialect.to_type(parsed)

    @classmethod
    def to_postgres(cls, db_type: str, source_db: str = "auto") -> str:
        """Конвертирует тип в PostgreSQL."""
        if source_db == "auto":
            source_db = cls._detect_source_db(db_type)

        if source_db == "clickhouse":
            parsed = cls._ch_parser.parse(db_type)
        else:
            parsed = cls._pg_parser.parse(db_type)

        return _PostgresDialect.to_type(parsed)

    @classmethod
    def from_clickhouse_to_bigquery(cls, db_type: str) -> str:
        """Явная конвертация ClickHouse → BigQuery (для читаемости кода)."""
        return cls.to_bigquery(db_type, source_db="clickhouse")

    @classmethod
    def to_bigquery_schema_field(cls, column_name: str, db_type: str, source_db: str = "auto"):
        """Создает BigQuery SchemaField с корректной обработкой массивов."""
        from google.cloud import bigquery

        bq_type = cls.to_bigquery(db_type, source_db=source_db)
        raw_db_type = (db_type or "").strip()

        if raw_db_type.upper().startswith("REPEATED "):
            repeated_type = raw_db_type.split(None, 1)[1].strip().upper()
            return bigquery.SchemaField(column_name, repeated_type, mode="REPEATED")

        # Обрабатываем RECORD<list ARRAY<STRUCT<item STRING>>> для ClickHouse Array
        if bq_type.startswith("RECORD<") and "list ARRAY<STRUCT<item STRING>>" in bq_type:
            # Создаем nested структуру:
            # RECORD {
            #   list: ARRAY<STRUCT<item STRING>> REPEATED
            # }
            item_field = bigquery.SchemaField("item", "STRING", mode="NULLABLE")
            list_field = bigquery.SchemaField("list", "RECORD", mode="REPEATED", fields=[item_field])
            return bigquery.SchemaField(column_name, "RECORD", mode="NULLABLE", fields=[list_field])
        else:
            return bigquery.SchemaField(column_name, bq_type, mode="NULLABLE")

    @staticmethod
    def bq_field_type_to_pyarrow(bq_field_type: str):
        import pyarrow as pa

        mapping = {
            "STRING": pa.string(),
            "BYTES": pa.binary(),
            "INTEGER": pa.int64(),
            "INT64": pa.int64(),
            "FLOAT": pa.float64(),
            "FLOAT64": pa.float64(),
            "BOOLEAN": pa.bool_(),
            "BOOL": pa.bool_(),
            "DATE": pa.date32(),
            "TIME": pa.time64("us"),
            "TIMESTAMP": pa.timestamp("us", tz="UTC"),
            "DATETIME": pa.timestamp("us"),
            "NUMERIC": pa.decimal128(38, 9),
            "BIGNUMERIC": pa.decimal256(76, 38) if hasattr(pa, "decimal256") else pa.decimal128(38, 9),
        }
        return mapping.get((bq_field_type or "").upper(), pa.string())

    # =========================================================================
    # Python/API Type Methods
    # =========================================================================

    @classmethod
    def from_python_value(cls, value) -> CanonicalType:
        """
        Определяет канонический тип из Python значения.

        Определение основано на ТИПЕ и ФОРМАТЕ значения:
        - datetime → TIMESTAMP
        - ISO строка "2024-01-15..." → TIMESTAMP
        - int → INTEGER
        - и т.д.

        Args:
            value: Python значение (int, str, bool, None, datetime, etc.)

        Returns:
            CanonicalType

        """
        return cls._py_parser.parse_value(value).base

    @classmethod
    def python_to_bigquery(cls, value) -> str:
        """
        Конвертирует Python значение в BigQuery тип.

        Args:
            value: Python значение

        Returns:
            BigQuery тип (STRING, INT64, TIMESTAMP, etc.)

        """
        parsed = cls._py_parser.parse_value(value)
        return _BigQueryDialect.to_type(parsed)

    @classmethod
    def python_to_postgres(cls, value) -> str:
        """
        Конвертирует Python значение в PostgreSQL тип.

        Args:
            value: Python значение

        Returns:
            PostgreSQL тип (text, integer, timestamp, etc.)
        """
        parsed = cls._py_parser.parse_value(value)
        return _PostgresDialect.to_type(parsed)

    @classmethod
    def detect_schema_from_record(cls, record: dict, target_db: str = "bigquery") -> list:
        """
        Определяет схему из Python dict (одной записи).

        Определение типа основано на ЗНАЧЕНИИ, не на имени колонки.

        Args:
            record: Словарь с данными (например из API)
            target_db: Целевая БД ('bigquery' или 'postgres')

        Returns:
            Список кортежей (column_name, db_type)

        """
        schema = []
        for column_name, value in record.items():
            if target_db == "bigquery":
                db_type = cls.python_to_bigquery(value)
            else:
                db_type = cls.python_to_postgres(value)
            schema.append((column_name, db_type))
        return schema

    # =========================================================================
    # Timestamp Parsing (для API коннекторов)
    # =========================================================================

    @classmethod
    def is_timestamp_value(cls, value) -> bool:
        """
        Проверяет, является ли значение датой.

        Проверяет по ФОРМАТУ значения:
        - datetime объект → True
        - ISO строка "2024-01-15..." → True
        - Иное → False

        Args:
            value: Любое Python значение

        Returns:
            True если значение — дата

        """
        from datetime import datetime

        if isinstance(value, datetime):
            return True
        if isinstance(value, str):
            return cls._py_parser._looks_like_timestamp(value)
        return False

    @classmethod
    def parse_timestamp(cls, value):
        """
        Парсит значение в datetime объект.

        Поддерживает:
        - datetime объекты (passthrough)
        - ISO строки: "2024-01-15T10:30:00", "2024-01-15 10:30:00"
        - Только дата: "2024-01-15"
        - С timezone: "2024-01-15T10:30:00Z", "2024-01-15T10:30:00+03:00"

        Args:
            value: Строка или datetime

        Returns:
            datetime объект или None

        Example:
            >>> DataTypeMapper.parse_timestamp("2024-01-15 10:30:00")
            datetime(2024, 1, 15, 10, 30, 0)
        """
        return cls._py_parser.parse_timestamp(value)

    @classmethod
    def normalize_record_timestamps(cls, record: dict) -> dict:
        """
        Нормализует все timestamp-подобные строки в записи.

        Определяет по ФОРМАТУ значения (ISO строка → datetime).
        Не зависит от имён колонок.

        Args:
            record: Словарь с данными

        Returns:
            Новый словарь с конвертированными датами
        """
        result = {}
        for key, value in record.items():
            if value is not None and cls.is_timestamp_value(value):
                parsed = cls.parse_timestamp(value)
                result[key] = parsed if parsed is not None else value
            else:
                result[key] = value
        return result

    # =========================================================================
    # Value Normalization (для API коннекторов → DB)
    # =========================================================================

    @classmethod
    def normalize_value_for_db(cls, value, null_markers: tuple = ("-", "")) -> any:
        """
        Нормализует значение из API для записи в БД.

        Операции:
        1. Заменяет null-маркеры (например '-', '') на None
        2. Сериализует list/dict в JSON строку (для PostgreSQL TEXT колонок)
        3. Парсит строки вида "['...']" как Python literals → JSON

        Args:
            value: Значение из API
            null_markers: Кортеж значений, которые считаются NULL

        Returns:
            Нормализованное значение

        """
        import ast
        import json

        # Null markers
        if value in null_markers:
            return None

        # Python list/dict → JSON string
        if isinstance(value, list | dict):
            try:
                return json.dumps(value, ensure_ascii=False)
            except (TypeError, ValueError):
                return str(value)

        # Строка, похожая на Python list literal → парсим и сериализуем в JSON
        if isinstance(value, str):
            stripped = value.strip()

            # Python list literal: "['a', 'b']" → '["a", "b"]'
            if stripped.startswith("[") and stripped.endswith("]"):
                try:
                    parsed = ast.literal_eval(stripped)
                    if isinstance(parsed, list):
                        return json.dumps(parsed, ensure_ascii=False)
                except (ValueError, SyntaxError):
                    pass

            # Python dict literal: "{'a': 1}" → '{"a": 1}'
            if stripped.startswith("{") and stripped.endswith("}"):
                try:
                    parsed = ast.literal_eval(stripped)
                    if isinstance(parsed, dict):
                        return json.dumps(parsed, ensure_ascii=False)
                except (ValueError, SyntaxError):
                    pass

        return value

    @classmethod
    def normalize_record_for_db(cls, record: dict, null_markers: tuple = ("-", "")) -> dict:
        """
        Полная нормализация записи из API для записи в БД.

        Операции:
        1. Нормализация значений (null markers, list/dict → JSON)
        2. Конвертация timestamp строк в datetime объекты

        Args:
            record: Словарь с данными из API
            null_markers: Кортеж значений, которые считаются NULL

        Returns:
            Нормализованный словарь

        """
        # Шаг 1: Нормализация значений (null markers, list/dict → JSON)
        normalized = {k: cls.normalize_value_for_db(v, null_markers) for k, v in record.items()}

        # Шаг 2: Конвертация timestamp строк в datetime
        return cls.normalize_record_timestamps(normalized)


def _passthrough_bigquery_type(db_type: str) -> str | None:
    """Return a staging-safe BigQuery type when *db_type* is already BigQuery-native."""

    match = _BQ_NATIVE_TYPE.fullmatch((db_type or "").strip())
    if match is None:
        return None
    base = _BQ_BASE_ALIASES.get(match.group("base").upper(), match.group("base").upper())
    # google.cloud.bigquery.SchemaField expects bare type names (precision/scale via kwargs).
    return base


__all__ = ["DataTypeMapper"]
