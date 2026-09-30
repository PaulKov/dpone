"""Deterministic synthetic inputs for the SqlClient live certification matrix."""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256

_SEED = 20260927
_TYPE_CYCLE = ("bigint", "float(53)", "nvarchar(max)", "datetime2(6)")


@dataclass(frozen=True, slots=True)
class SqlClientLiveCase:
    """One versioned schema and lazy deterministic row generator."""

    fixture_id: str
    columns: tuple[tuple[str, str], ...]

    @property
    def schema_sha256(self) -> str:
        payload = json.dumps(self.columns, ensure_ascii=True, separators=(",", ":"))
        return sha256(payload.encode()).hexdigest()

    def rows(self, count: int) -> Iterator[tuple[object, ...]]:
        if type(count) is not int or count < 0:
            raise ValueError("mssql_sqlclient.invalid_fixture_rows")
        text_indexes = tuple(
            index for index, (_name, declared) in enumerate(self.columns) if declared.startswith("nvarchar")
        )
        text_ordinals = {column_index: ordinal for ordinal, column_index in enumerate(text_indexes)}
        for row_index in range(count):
            source_index = row_index - 1 if row_index > 0 and row_index % 13 == 0 else row_index
            yield tuple(
                _value(
                    source_index,
                    column_index,
                    target_type,
                    nullable=" nullable" in declared,
                    text_ordinal=text_ordinals.get(column_index),
                    text_count=len(text_indexes),
                )
                for column_index, (_name, declared) in enumerate(self.columns)
                for target_type in (declared.removesuffix(" nullable"),)
            )

    def descriptor(self) -> dict[str, object]:
        """Return the closed shareable fixture authority."""
        return {
            "schema_version": 1,
            "fixture_id": self.fixture_id,
            "generator_version": "mssql-sqlclient-synthetic-v1",
            "seed": _SEED,
            "row_counts": [10_000, 1_000_000],
            "business_column_count": len(self.columns),
            "schema_sha256": self.schema_sha256,
            "max_row_bytes": 1_048_576,
            "columns": [{"name": name, "type": declared} for name, declared in self.columns],
            "rules": {
                "null": "nullable column j is null when (source_row+i) % 11 == 0",
                "duplicate": "every thirteenth row repeats its predecessor",
                "text_lengths": [0, 1, 31, 255, 4095, 65535],
                "text_skew": {
                    "common_cycle": [0, 1, 31, 255],
                    "one_4095_value_every_rows": 257,
                    "one_65535_value_every_rows": 65536,
                },
            },
            "source": "deterministic-synthetic-only",
        }


def _value(
    row_index: int,
    column_index: int,
    target_type: str,
    *,
    nullable: bool,
    text_ordinal: int | None,
    text_count: int,
) -> object:
    if nullable and (row_index + column_index) % 11 == 0:
        return None
    token = row_index * 131 + column_index * 17 + _SEED
    if target_type == "bigint":
        return token if row_index % 2 == 0 else -token
    if target_type == "float(53)":
        return float((token % 1_000_003) - 500_001) / 16.0
    if target_type == "nvarchar(max)":
        if text_ordinal is None or text_count < 1:
            raise ValueError("mssql_sqlclient.invalid_fixture_text_layout")
        size = _text_size(row_index, column_index, text_ordinal, text_count)
        unit = f"s{_SEED:x}-{row_index:x}-{column_index:x}-λ"
        return (unit * (size // len(unit) + 1))[:size]
    if target_type == "datetime2(6)":
        return datetime(2000, 1, 1) + timedelta(microseconds=token % 631_139_040_000_000)
    raise ValueError("mssql_sqlclient.unsupported_fixture_type")


def _text_size(row_index: int, column_index: int, text_ordinal: int, text_count: int) -> int:
    if row_index % 65_536 == 65_535 and text_ordinal == (row_index // 65_536) % text_count:
        return 65_535
    if row_index % 257 == 256 and text_ordinal == (row_index // 257) % text_count:
        return 4_095
    common = (0, 1, 31, 255)
    return common[(row_index + column_index) % len(common)]


def sqlclient_live_cases() -> tuple[SqlClientLiveCase, ...]:
    """Return the narrow and exactly-100-column certified profiles."""
    narrow = SqlClientLiveCase(
        "narrow-sqlclient-v1",
        (
            ("event_id", "bigint"),
            ("metric", "float(53) nullable"),
            ("payload", "nvarchar(max) nullable"),
            ("occurred_at", "datetime2(6)"),
        ),
    )
    wide_columns = tuple(
        (f"c{index:03d}", target_type + (" nullable" if index % 2 else ""))
        for index in range(100)
        for target_type in (_TYPE_CYCLE[index % len(_TYPE_CYCLE)],)
    )
    return narrow, SqlClientLiveCase("wide100-sqlclient-v1", wide_columns)


def sqlclient_live_case(fixture_id: str) -> SqlClientLiveCase:
    """Resolve only a checked profile identifier."""
    try:
        return next(case for case in sqlclient_live_cases() if case.fixture_id == fixture_id)
    except StopIteration as error:
        raise ValueError("mssql_sqlclient.unknown_fixture") from error


__all__ = ["SqlClientLiveCase", "sqlclient_live_case", "sqlclient_live_cases"]
