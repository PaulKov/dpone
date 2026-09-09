"""Canonical type models for dpone runtime type mapping."""

from __future__ import annotations

from dataclasses import dataclass

from dpone._compat import StrEnum


class CanonicalType(StrEnum):
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    NUMERIC = "NUMERIC"
    STRING = "STRING"
    TIMESTAMP = "TIMESTAMP"
    DATE = "DATE"
    TIME = "TIME"
    BOOLEAN = "BOOLEAN"
    JSON = "JSON"
    ARRAY = "ARRAY"
    BYTES = "BYTES"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class ParsedType:
    base: CanonicalType
    element: ParsedType | None = None

    def is_array(self) -> bool:
        return self.base == CanonicalType.ARRAY


class TimestampConfig:
    """
    Конфигурация для парсинга timestamp значений.

    Определение типа основано на ФОРМАТЕ ЗНАЧЕНИЯ, а не на имени колонки.
    Это более надёжный подход, не зависящий от naming conventions.
    """

    DATETIME_FORMATS: tuple = (
        "%Y-%m-%dT%H:%M:%S.%f",  # ISO с микросекундами
        "%Y-%m-%d %H:%M:%S.%f",  # Space с микросекундами
        "%Y-%m-%dT%H:%M:%S%z",  # ISO с timezone
        "%Y-%m-%d %H:%M:%S%z",  # Space с timezone
        "%Y-%m-%dT%H:%M:%SZ",  # ISO UTC
        "%Y-%m-%d %H:%M:%SZ",  # Space UTC
        "%Y-%m-%dT%H:%M:%S",  # ISO без timezone
        "%Y-%m-%d %H:%M:%S",  # Space без timezone
        "%Y-%m-%d",  # Только дата
    )


__all__ = ["CanonicalType", "ParsedType", "TimestampConfig"]
