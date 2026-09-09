"""Sample-based source row profiling for type inference."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from typing import Any

from dpone.type_system.models import ColumnProfile, TypeInferenceOptions


class SampleTypeProfiler:
    """Profile row samples without deciding target-specific types."""

    def __init__(self, options: TypeInferenceOptions | None = None) -> None:
        self.options = options or TypeInferenceOptions()

    def profile_rows(self, rows: Iterable[Mapping[str, Any]]) -> dict[str, ColumnProfile]:
        materialized = list(rows)[: self.options.max_sample_rows]
        counters: dict[str, _ColumnAccumulator] = {}
        all_columns = {str(key) for row in materialized for key in row}
        for column in all_columns:
            counters[column] = _ColumnAccumulator(column)
        for row in materialized:
            for column in all_columns:
                counters[column].observe(row.get(column), empty_string_is_null=self.options.empty_string_is_null)
        return {name: accumulator.to_profile(len(materialized)) for name, accumulator in counters.items()}


class _ColumnAccumulator:
    def __init__(self, name: str) -> None:
        self.name = name
        self.null_count = 0
        self.empty_string_count = 0
        self.max_length: int | None = None
        self.values: set[Any] = set()
        self.observed_types: Counter[str] = Counter()

    def observe(self, value: Any, *, empty_string_is_null: bool) -> None:
        if value is None:
            self.null_count += 1
            return
        if value == "":
            self.empty_string_count += 1
            if empty_string_is_null:
                self.null_count += 1
                return
        observed = _observed_type(value)
        self.observed_types[observed] += 1
        try:
            self.values.add(value)
        except TypeError:
            self.values.add(repr(value))
        if isinstance(value, str):
            length = len(value)
            self.max_length = length if self.max_length is None else max(self.max_length, length)

    def to_profile(self, row_count: int) -> ColumnProfile:
        non_null = max(row_count - self.null_count, 0)
        distinct_count = len(self.values)
        return ColumnProfile(
            name=self.name,
            row_count=row_count,
            null_count=self.null_count,
            empty_string_count=self.empty_string_count,
            distinct_count=distinct_count,
            distinct_ratio=(distinct_count / non_null) if non_null else 0.0,
            observed_types=tuple(sorted(self.observed_types)),
            max_length=self.max_length,
        )


def _observed_type(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, Decimal):
        return "decimal"
    if isinstance(value, float):
        return "float"
    if isinstance(value, datetime):
        return "timestamp"
    if isinstance(value, date):
        return "date"
    if isinstance(value, time):
        return "time"
    if isinstance(value, bytes | bytearray):
        return "binary"
    if isinstance(value, list | tuple):
        return "array"
    if isinstance(value, dict):
        return "json"
    if isinstance(value, str):
        if _is_decimal_string(value):
            return "decimal"
        if _looks_like_timestamp(value):
            return "timestamp"
        return "string"
    return "string"


def _is_decimal_string(value: str) -> bool:
    text = value.strip()
    if not text:
        return False
    try:
        Decimal(text)
    except InvalidOperation:
        return False
    return any(ch.isdigit() for ch in text)


def _looks_like_timestamp(value: str) -> bool:
    text = value.strip()
    if len(text) < 10:
        return False
    if len(text) >= 10 and text[4:5] == "-" and text[7:8] == "-":
        return True
    return False
