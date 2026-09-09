"""MSSQL -> BigQuery type mapping profile (``mssql_to_bigquery_analytics_v1``)."""

from __future__ import annotations

import re
from dataclasses import dataclass

# BigQuery parameterized decimal bounds (same policy as mysql/postgres BQ profiles).
_BQ_NUMERIC_MAX_PRECISION = 38
_BQ_NUMERIC_MAX_SCALE = 9
_BQ_NUMERIC_MAX_INTEGER_DIGITS = 29
_BQ_BIGNUMERIC_MAX_PRECISION = 76
_BQ_BIGNUMERIC_MAX_SCALE = 38
_BQ_BIGNUMERIC_MAX_INTEGER_DIGITS = 38


@dataclass(frozen=True, slots=True)
class MSSQLBigQueryTypeDecision:
    """One explainable MSSQL -> BigQuery type decision."""

    source_type: str
    target_type: str
    transfer_representation: str
    compatible: bool = True
    requires_explicit_contract: bool = False
    lossless: bool = True
    warning: str | None = None


class MSSQLBigQueryTypeMapper:
    """Pair-specific type decisions for planning, docs, schema compare, and extract DDL."""

    def resolve(self, source_type: str) -> MSSQLBigQueryTypeDecision:
        normalized = _normalize(source_type)

        if match := re.fullmatch(r"(?:decimal|numeric)\((\d+),(\d+)\)", normalized):
            return _decimal_decision(source_type, int(match.group(1)), int(match.group(2)))
        if match := re.fullmatch(r"n?varchar\((max|\d+)\)", normalized):
            del match
            return MSSQLBigQueryTypeDecision(
                source_type=source_type,
                target_type="STRING",
                transfer_representation="CSV text",
            )
        if match := re.fullmatch(r"n?char\((\d+)\)", normalized):
            del match
            return MSSQLBigQueryTypeDecision(
                source_type=source_type,
                target_type="STRING",
                transfer_representation="CSV text",
            )
        if re.fullmatch(r"(?:var)?binary\((?:max|\d+)\)", normalized):
            return MSSQLBigQueryTypeDecision(
                source_type=source_type,
                target_type="BYTES",
                transfer_representation="base64 CSV text",
            )
        if re.fullmatch(r"datetime2(?:\(\d+\))?", normalized):
            return MSSQLBigQueryTypeDecision(
                source_type=source_type,
                target_type="DATETIME",
                transfer_representation="datetime CSV text",
            )
        if re.fullmatch(r"datetimeoffset(?:\(\d+\))?", normalized):
            return MSSQLBigQueryTypeDecision(
                source_type=source_type,
                target_type="TIMESTAMP",
                transfer_representation="timestamp CSV text",
            )
        if re.fullmatch(r"time(?:\(\d+\))?", normalized):
            return MSSQLBigQueryTypeDecision(
                source_type=source_type,
                target_type="TIME",
                transfer_representation="time CSV text",
            )
        if normalized in _EXACT:
            target_type, representation, lossless, warning = _EXACT[normalized]
            return MSSQLBigQueryTypeDecision(
                source_type=source_type,
                target_type=target_type,
                transfer_representation=representation,
                lossless=lossless,
                requires_explicit_contract=warning is not None and "schema_contract" in (warning or ""),
                warning=warning,
            )
        if _requires_contract(normalized):
            return MSSQLBigQueryTypeDecision(
                source_type=source_type,
                target_type="STRING",
                transfer_representation="CSV text",
                requires_explicit_contract=True,
                lossless=False,
                warning="MSSQL spatial/hierarchyid/sql_variant types require explicit schema_contract.",
            )
        return MSSQLBigQueryTypeDecision(
            source_type=source_type,
            target_type="STRING",
            transfer_representation="CSV text",
            requires_explicit_contract=True,
            lossless=False,
            warning="Unknown or custom MSSQL type requires explicit schema_contract for production loads.",
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


def _decimal_decision(source_type: str, precision: int, scale: int) -> MSSQLBigQueryTypeDecision:
    if _fits_bq_numeric(precision, scale):
        return MSSQLBigQueryTypeDecision(
            source_type=source_type,
            target_type="NUMERIC",
            transfer_representation="decimal CSV text",
        )
    if _fits_bq_bignumeric(precision, scale):
        return MSSQLBigQueryTypeDecision(
            source_type=source_type,
            target_type="BIGNUMERIC",
            transfer_representation="decimal CSV text",
            warning=(
                "MSSQL DECIMAL exceeds BigQuery NUMERIC parameterized limits; mapped to BIGNUMERIC. "
                "Declare schema_contract when consumers require an explicit numeric domain."
            ),
        )
    return MSSQLBigQueryTypeDecision(
        source_type=source_type,
        target_type="STRING",
        transfer_representation="decimal CSV text",
        compatible=False,
        requires_explicit_contract=True,
        lossless=False,
        warning=(
            "MSSQL DECIMAL exceeds BigQuery BIGNUMERIC parameterized limits; require schema_contract "
            "(STRING or custom cast) before production loads."
        ),
    )


# target_type, transfer_representation, lossless, warning
_EXACT: dict[str, tuple[str, str, bool, str | None]] = {
    "tinyint": ("INT64", "integer CSV text", True, None),
    "smallint": ("INT64", "integer CSV text", True, None),
    "int": ("INT64", "integer CSV text", True, None),
    "bigint": ("INT64", "integer CSV text", True, None),
    "bit": ("BOOL", "boolean CSV text", True, None),
    "real": ("FLOAT64", "float CSV text", False, None),
    "float": ("FLOAT64", "float CSV text", True, None),
    "money": ("NUMERIC", "decimal CSV text", True, None),
    "smallmoney": ("NUMERIC", "decimal CSV text", True, None),
    "date": ("DATE", "ISO date CSV text", True, None),
    "datetime": ("DATETIME", "datetime CSV text", True, None),
    "smalldatetime": (
        "DATETIME",
        "datetime CSV text",
        False,
        "MSSQL smalldatetime is mapped to DATETIME; declare schema_contract when second-level precision matters.",
    ),
    "uniqueidentifier": (
        "STRING",
        "UUID CSV text",
        False,
        "MSSQL uniqueidentifier lands as STRING; declare schema_contract when consumers require typed UUID.",
    ),
    "xml": (
        "STRING",
        "XML CSV text",
        False,
        "MSSQL XML lands as STRING; declare schema_contract for typed XML consumers.",
    ),
    "text": ("STRING", "CSV text", True, None),
    "ntext": ("STRING", "CSV text", True, None),
    "image": ("BYTES", "base64 CSV text", True, None),
    "rowversion": ("BYTES", "base64 CSV text", True, None),
    "timestamp": ("BYTES", "base64 CSV text", True, None),
}


def _normalize(value: str) -> str:
    normalized = " ".join(str(value).strip().lower().split())
    return re.sub(r"\s+nullable$", "", normalized).strip()


def _requires_contract(normalized: str) -> bool:
    return normalized.startswith(("geography", "geometry", "hierarchyid", "sql_variant"))


__all__ = ["MSSQLBigQueryTypeDecision", "MSSQLBigQueryTypeMapper"]
