"""Closed, immutable inputs to protected ClickHouse content observation.

These values describe data, never permission to mutate or a certified observation.
The first profile deliberately supports less than the server's SQL/type language.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class ScalarType:
    """Parsed allowlisted scalar; argument is width or scale, never SQL."""

    name: str
    root: str
    argument: int = 0
    nullable: bool = False


def scalar_type(value: str) -> ScalarType:
    """Normalize only supported server aliases; consume the complete declaration."""
    if type(value) is not str:
        raise ValueError("unsupported_observation_type")
    nullable = value.startswith("Nullable(") and value.endswith(")")
    inner = value[9:-1].strip() if nullable else value.strip()
    simple = {"Bool", "Float32", "Float64", "String", "UUID", "Date", "Date32"}
    simple.update(f"{sign}Int{width}" for sign in ("", "U") for width in (8, 16, 32, 64))
    argument = 0
    if inner in simple:
        name, root = inner, inner
    elif match := re.fullmatch(r"FixedString\(\s*([1-9][0-9]*)\s*\)", inner):
        argument = int(match[1])
        name, root = f"FixedString({argument})", "FixedString"
    elif re.fullmatch(r"DateTime\(\s*'UTC'\s*\)", inner):
        name, root = "DateTime('UTC')", "DateTime"
    elif match := re.fullmatch(r"DateTime64\(\s*([0-6])\s*,\s*'UTC'\s*\)", inner):
        argument = int(match[1])
        name, root = f"DateTime64({argument}, 'UTC')", "DateTime64"
    else:
        alias = re.fullmatch(r"Decimal(32|64|128)\(\s*([0-9]+)\s*\)", inner)
        generic = re.fullmatch(r"Decimal\(\s*(9|18|38)\s*,\s*([0-9]+)\s*\)", inner)
        if alias:
            precision, argument = {"32": 9, "64": 18, "128": 38}[alias[1]], int(alias[2])
        elif generic:
            precision, argument = int(generic[1]), int(generic[2])
        else:
            raise ValueError("unsupported_observation_type")
        if argument > precision:
            raise ValueError("unsupported_observation_decimal_scale")
        name, root = f"Decimal({precision}, {argument})", "Decimal"
    return ScalarType(f"Nullable({name})" if nullable else name, root, argument, nullable)


@dataclass(frozen=True)
class CandidateColumn:
    """Exact UTF-8 column name and normalized scalar declaration."""

    name: str
    type_name: str

    def __post_init__(self) -> None:
        if type(self.name) is not str or not self.name or "\x00" in self.name:
            raise ValueError("invalid_candidate_column_name")
        self.name.encode("utf-8")
        object.__setattr__(self, "type_name", scalar_type(self.type_name).name)


@dataclass(frozen=True)
class CandidateDesign:
    """Plain MergeTree on local default storage with column-only keys.

    An empty tuple is an explicit empty key, not an unspecified server default.
    Primary key must be a sorting prefix. Partition keys cannot be nullable.
    """

    columns: tuple[CandidateColumn, ...]
    sorting_key: tuple[str, ...]
    primary_key: tuple[str, ...]
    partition_key: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            type(self.columns) is not tuple
            or not self.columns
            or any(type(c) is not CandidateColumn for c in self.columns)
        ):
            raise ValueError("candidate_columns_required")
        names = {column.name: column for column in self.columns}
        if len(names) != len(self.columns):
            raise ValueError("duplicate_candidate_column")
        for key in (self.sorting_key, self.primary_key, self.partition_key):
            if type(key) is not tuple or any(type(name) is not str or name not in names for name in key):
                raise ValueError("unsupported_candidate_key")
            if len(set(key)) != len(key):
                raise ValueError("duplicate_candidate_key_column")
        if self.primary_key != self.sorting_key[: len(self.primary_key)]:
            raise ValueError("candidate_primary_key_not_sorting_prefix")
        for name in self.partition_key:
            scalar = scalar_type(names[name].type_name)
            if scalar.nullable or not (scalar.root.startswith(("Int", "UInt")) or scalar.root in {"Date", "Date32"}):
                raise ValueError("unsupported_candidate_partition_type")


@dataclass(frozen=True)
class ObservationLimits:
    """Explicit encoded-byte/row budgets, not bounds on source objects or RSS."""

    max_columns: int
    max_batch_rows: int
    max_batch_bytes: int
    max_row_bytes: int
    max_partitions: int
    max_scan_rows: int
    max_scan_bytes: int
    request_seconds: float

    def __post_init__(self) -> None:
        for value in (
            self.max_columns,
            self.max_batch_rows,
            self.max_batch_bytes,
            self.max_row_bytes,
            self.max_partitions,
            self.max_scan_rows,
            self.max_scan_bytes,
        ):
            if type(value) is not int or not 0 < value <= (1 << 63) - 1:
                raise ValueError("observation_limit_requires_positive_integer")
        if type(self.request_seconds) not in (int, float) or not 0 < self.request_seconds < math.inf:
            raise ValueError("observation_timeout_requires_positive_finite_number")
        try:
            seconds = float(self.request_seconds)
        except OverflowError:
            raise ValueError("observation_timeout_requires_positive_finite_number") from None
        if not math.isfinite(seconds):
            raise ValueError("observation_timeout_requires_positive_finite_number")
        object.__setattr__(self, "request_seconds", seconds)
        if self.max_row_bytes > self.max_batch_bytes:
            raise ValueError("observation_row_limit_exceeds_batch_limit")


@dataclass(frozen=True)
class MultisetState:
    """Persist the 256-bit sum as canonical JSON text, never a SQLite integer."""

    count: int
    total: int
    encoded_bytes: int
    nulls: tuple[int, ...]

    def __post_init__(self) -> None:
        if any(type(v) is not int or v < 0 for v in (self.count, self.total, self.encoded_bytes)):
            raise ValueError("invalid_multiset_counter")
        if self.total >= 1 << 256 or type(self.nulls) is not tuple:
            raise ValueError("invalid_multiset_sum_or_width")
        if any(type(n) is not int or not 0 <= n <= self.count for n in self.nulls):
            raise ValueError("invalid_multiset_null_count")
        if self.count == 0 and (self.total or self.encoded_bytes):
            raise ValueError("invalid_empty_multiset")


@dataclass(frozen=True)
class TypedTableEvidence:
    """Bounded complete scan evidence; hash parity remains probabilistic."""

    rows: int
    content_digest: str
    partitions: tuple[str, ...]
    encoded_bytes: int
