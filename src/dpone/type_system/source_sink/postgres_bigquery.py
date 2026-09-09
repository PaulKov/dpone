"""PostgreSQL -> BigQuery type mapping profile."""

from __future__ import annotations

import re
from dataclasses import dataclass

# BigQuery parameterized decimal bounds (same policy as mysql_to_bigquery_analytics_v1).
_BQ_NUMERIC_MAX_PRECISION = 38
_BQ_NUMERIC_MAX_SCALE = 9
_BQ_NUMERIC_MAX_INTEGER_DIGITS = 29
_BQ_BIGNUMERIC_MAX_PRECISION = 76
_BQ_BIGNUMERIC_MAX_SCALE = 38
_BQ_BIGNUMERIC_MAX_INTEGER_DIGITS = 38


@dataclass(frozen=True, slots=True)
class PostgresBigQueryTypeDecision:
    """One explainable PostgreSQL -> BigQuery type decision."""

    source_type: str
    target_type: str
    transfer_representation: str
    compatible: bool = True
    requires_explicit_contract: bool = False
    lossless: bool = True
    warning: str | None = None


class PostgresBigQueryTypeMapper:
    """Pair-specific type decisions for planning, docs, schema compare, and extract DDL."""

    def resolve(self, source_type: str) -> PostgresBigQueryTypeDecision:
        normalized = _normalize(source_type)

        if match := re.fullmatch(r"(?:numeric|decimal)\((\d+),(\d+)\)", normalized):
            return _decimal_decision(source_type, int(match.group(1)), int(match.group(2)))
        if match := re.fullmatch(r"(?:character varying|varchar|character|char)\((\d+)\)", normalized):
            del match
            return PostgresBigQueryTypeDecision(
                source_type=source_type,
                target_type="STRING",
                transfer_representation="CSV text",
            )
        if normalized in _EXACT:
            target_type, representation = _EXACT[normalized]
            return PostgresBigQueryTypeDecision(
                source_type=source_type,
                target_type=target_type,
                transfer_representation=representation,
                requires_explicit_contract=normalized in {"uuid", "array", "user-defined"},
                lossless=normalized not in {"real", "uuid"},
                warning=(
                    "PostgreSQL UUID lands as STRING; declare schema_contract when consumers require typed UUID."
                    if normalized == "uuid"
                    else None
                ),
            )
        if _requires_contract(normalized):
            return PostgresBigQueryTypeDecision(
                source_type=source_type,
                target_type="STRING",
                transfer_representation="CSV text",
                requires_explicit_contract=True,
                lossless=False,
                warning="PostgreSQL array/composite/geometric/user-defined types require explicit schema_contract.",
            )
        return PostgresBigQueryTypeDecision(
            source_type=source_type,
            target_type="STRING",
            transfer_representation="CSV text",
            requires_explicit_contract=True,
            lossless=False,
            warning="Unknown or custom PostgreSQL type requires explicit schema_contract for production loads.",
        )


def _fits_bq_numeric(precision: int, scale: int) -> bool:
    return (
        0 <= scale <= _BQ_NUMERIC_MAX_SCALE
        and scale <= precision <= _BQ_NUMERIC_MAX_PRECISION
        and precision <= scale + _BQ_NUMERIC_MAX_INTEGER_DIGITS
    )


def _fits_bq_bignumeric(precision: int, scale: int) -> bool:
    return (
        0 <= scale <= _BQ_BIGNUMERIC_MAX_SCALE
        and scale <= precision <= _BQ_BIGNUMERIC_MAX_PRECISION
        and precision <= scale + _BQ_BIGNUMERIC_MAX_INTEGER_DIGITS
    )


def _decimal_decision(source_type: str, precision: int, scale: int) -> PostgresBigQueryTypeDecision:
    if _fits_bq_numeric(precision, scale):
        return PostgresBigQueryTypeDecision(
            source_type=source_type,
            target_type="NUMERIC",
            transfer_representation="decimal CSV text",
        )
    if _fits_bq_bignumeric(precision, scale):
        return PostgresBigQueryTypeDecision(
            source_type=source_type,
            target_type="BIGNUMERIC",
            transfer_representation="decimal CSV text",
            warning=(
                "PostgreSQL NUMERIC exceeds BigQuery NUMERIC parameterized limits; mapped to BIGNUMERIC. "
                "Declare schema_contract when consumers require an explicit numeric domain."
            ),
        )
    return PostgresBigQueryTypeDecision(
        source_type=source_type,
        target_type="STRING",
        transfer_representation="decimal CSV text",
        compatible=False,
        requires_explicit_contract=True,
        lossless=False,
        warning=(
            "PostgreSQL NUMERIC exceeds BigQuery BIGNUMERIC parameterized limits; require schema_contract "
            "(STRING or custom cast) before production loads."
        ),
    )


_EXACT: dict[str, tuple[str, str]] = {
    "smallint": ("INT64", "integer CSV text"),
    "integer": ("INT64", "integer CSV text"),
    "bigint": ("INT64", "integer CSV text"),
    "int2": ("INT64", "integer CSV text"),
    "int4": ("INT64", "integer CSV text"),
    "int8": ("INT64", "integer CSV text"),
    "real": ("FLOAT64", "float CSV text"),
    "double precision": ("FLOAT64", "float CSV text"),
    "float4": ("FLOAT64", "float CSV text"),
    "float8": ("FLOAT64", "float CSV text"),
    "boolean": ("BOOL", "boolean CSV text"),
    "bool": ("BOOL", "boolean CSV text"),
    "date": ("DATE", "ISO date CSV text"),
    "time": ("TIME", "time CSV text"),
    "time without time zone": ("TIME", "time CSV text"),
    "time with time zone": ("TIME", "time CSV text"),
    "timestamp": ("DATETIME", "datetime CSV text"),
    "timestamp without time zone": ("DATETIME", "datetime CSV text"),
    "timestamp with time zone": ("TIMESTAMP", "timestamp CSV text"),
    "timestamptz": ("TIMESTAMP", "timestamp CSV text"),
    "text": ("STRING", "CSV text"),
    "character varying": ("STRING", "CSV text"),
    "varchar": ("STRING", "CSV text"),
    "character": ("STRING", "CSV text"),
    "char": ("STRING", "CSV text"),
    "json": ("JSON", "JSON CSV text"),
    "jsonb": ("JSON", "JSON CSV text"),
    "bytea": ("BYTES", "base64 CSV text"),
    "uuid": ("STRING", "UUID CSV text"),
}


def _normalize(value: str) -> str:
    normalized = " ".join(str(value).strip().lower().split())
    normalized = re.sub(r"\s+nullable$", "", normalized)
    return normalized


def _requires_contract(normalized: str) -> bool:
    return (
        normalized.startswith("array")
        or normalized.endswith("[]")
        or normalized in {"user-defined", "array", "geometry", "geography"}
        or normalized.startswith(("geometry", "geography", "point", "polygon", "line", "circle", "box", "path"))
    )


__all__ = ["PostgresBigQueryTypeDecision", "PostgresBigQueryTypeMapper"]
