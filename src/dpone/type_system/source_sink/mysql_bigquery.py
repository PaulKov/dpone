"""MySQL -> BigQuery type mapping profile."""

from __future__ import annotations

import re
from dataclasses import dataclass

# BigQuery parameterized decimal bounds:
# NUMERIC: precision <= 38, scale <= 9, and precision <= scale + 29
# BIGNUMERIC: precision <= 76, scale <= 38, and precision <= scale + 38
_BQ_NUMERIC_MAX_PRECISION = 38
_BQ_NUMERIC_MAX_SCALE = 9
_BQ_NUMERIC_MAX_INTEGER_DIGITS = 29
_BQ_BIGNUMERIC_MAX_PRECISION = 76
_BQ_BIGNUMERIC_MAX_SCALE = 38
_BQ_BIGNUMERIC_MAX_INTEGER_DIGITS = 38


@dataclass(frozen=True)
class MySQLBigQueryTypeDecision:
    """One explainable MySQL -> BigQuery type decision."""

    source_type: str
    target_type: str
    transfer_representation: str
    compatible: bool = True
    requires_explicit_contract: bool = False
    lossless: bool = True
    warning: str | None = None


class MySQLBigQueryTypeMapper:
    """Focused pair-specific type decisions for planning, docs and schema comparison."""

    def resolve(self, source_type: str) -> MySQLBigQueryTypeDecision:
        raw = " ".join(str(source_type).strip().lower().split())
        unsigned = bool(re.search(r"\bunsigned\b", raw))
        normalized = _normalize(raw)

        if unsigned:
            return _unsigned_decision(source_type, normalized)

        if match := re.fullmatch(r"decimal\((\d+),(\d+)\)", normalized):
            return _decimal_decision(source_type, int(match.group(1)), int(match.group(2)))
        if match := re.fullmatch(r"numeric\((\d+),(\d+)\)", normalized):
            return _decimal_decision(source_type, int(match.group(1)), int(match.group(2)))
        if match := re.fullmatch(r"varchar\((\d+)\)", normalized):
            return MySQLBigQueryTypeDecision(
                source_type=source_type,
                target_type="STRING",
                transfer_representation="CSV text",
            )
        if match := re.fullmatch(r"char\((\d+)\)", normalized):
            return MySQLBigQueryTypeDecision(
                source_type=source_type,
                target_type="STRING",
                transfer_representation="CSV text",
            )
        if normalized in _EXACT:
            target_type, representation = _EXACT[normalized]
            return MySQLBigQueryTypeDecision(
                source_type=source_type,
                target_type=target_type,
                transfer_representation=representation,
                requires_explicit_contract=normalized == "year",
                lossless=normalized not in {"float", "year"},
                warning=(
                    "MySQL YEAR maps to INT64; declare schema_contract when consumers require year semantics."
                    if normalized == "year"
                    else None
                ),
            )
        if _requires_contract(normalized):
            return MySQLBigQueryTypeDecision(
                source_type=source_type,
                target_type="STRING",
                transfer_representation="CSV text",
                requires_explicit_contract=True,
                lossless=False,
                warning="MySQL ENUM/SET/spatial types require explicit schema_contract.",
            )
        return MySQLBigQueryTypeDecision(
            source_type=source_type,
            target_type="STRING",
            transfer_representation="CSV text",
            requires_explicit_contract=True,
            lossless=False,
            warning="Unknown or custom MySQL type requires explicit schema_contract for production loads.",
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


def _decimal_decision(source_type: str, precision: int, scale: int) -> MySQLBigQueryTypeDecision:
    # Emit bare BigQuery type names so staging SchemaField / DataTypeMapper round-trip.
    if _fits_bq_numeric(precision, scale):
        return MySQLBigQueryTypeDecision(
            source_type=source_type,
            target_type="NUMERIC",
            transfer_representation="decimal CSV text",
        )
    if _fits_bq_bignumeric(precision, scale):
        return MySQLBigQueryTypeDecision(
            source_type=source_type,
            target_type="BIGNUMERIC",
            transfer_representation="decimal CSV text",
            warning=(
                "MySQL DECIMAL exceeds BigQuery NUMERIC parameterized limits; mapped to BIGNUMERIC. "
                "Declare schema_contract when consumers require an explicit numeric domain."
            ),
        )
    return MySQLBigQueryTypeDecision(
        source_type=source_type,
        target_type="STRING",
        transfer_representation="decimal CSV text",
        compatible=False,
        requires_explicit_contract=True,
        lossless=False,
        warning=(
            "MySQL DECIMAL exceeds BigQuery BIGNUMERIC parameterized limits; require schema_contract "
            "(STRING or custom cast) before production loads."
        ),
    )


def _unsigned_decision(source_type: str, normalized: str) -> MySQLBigQueryTypeDecision:
    base = re.sub(r"\s*\(\d+\)\s*$", "", normalized)
    wider = {
        "tinyint": ("INT64", False),
        "smallint": ("INT64", False),
        "mediumint": ("INT64", False),
        "int": ("INT64", False),
        "integer": ("INT64", False),
        "bigint": ("NUMERIC", True),
    }.get(base)
    if wider is None:
        return MySQLBigQueryTypeDecision(
            source_type=source_type,
            target_type="NUMERIC",
            transfer_representation="unsigned numeric CSV text",
            requires_explicit_contract=True,
            lossless=False,
            warning="Unsigned MySQL numeric type requires explicit schema_contract.",
        )
    target_type, needs_contract = wider
    return MySQLBigQueryTypeDecision(
        source_type=source_type,
        target_type=target_type,
        transfer_representation="unsigned integer CSV text",
        requires_explicit_contract=needs_contract,
        lossless=not needs_contract,
        warning=(
            "MySQL BIGINT UNSIGNED exceeds signed INT64; use NUMERIC via schema_contract."
            if needs_contract
            else f"MySQL UNSIGNED {base} maps to BigQuery {target_type}."
        ),
    )


_EXACT: dict[str, tuple[str, str]] = {
    "tinyint": ("INT64", "integer CSV text"),
    "tinyint(1)": ("BOOL", "boolean CSV text"),
    "smallint": ("INT64", "integer CSV text"),
    "mediumint": ("INT64", "integer CSV text"),
    "int": ("INT64", "integer CSV text"),
    "integer": ("INT64", "integer CSV text"),
    "bigint": ("INT64", "integer CSV text"),
    "float": ("FLOAT64", "float CSV text"),
    "double": ("FLOAT64", "float CSV text"),
    "double precision": ("FLOAT64", "float CSV text"),
    "bit": ("BOOL", "boolean CSV text"),
    "bool": ("BOOL", "boolean CSV text"),
    "boolean": ("BOOL", "boolean CSV text"),
    "date": ("DATE", "ISO date CSV text"),
    "datetime": ("DATETIME", "datetime CSV text"),
    "timestamp": ("TIMESTAMP", "timestamp CSV text"),
    "time": ("TIME", "time CSV text"),
    "year": ("INT64", "integer CSV text"),
    "text": ("STRING", "CSV text"),
    "tinytext": ("STRING", "CSV text"),
    "mediumtext": ("STRING", "CSV text"),
    "longtext": ("STRING", "CSV text"),
    "blob": ("BYTES", "binary CSV text"),
    "tinyblob": ("BYTES", "binary CSV text"),
    "mediumblob": ("BYTES", "binary CSV text"),
    "longblob": ("BYTES", "binary CSV text"),
    "binary": ("BYTES", "binary CSV text"),
    "varbinary": ("BYTES", "binary CSV text"),
    "json": ("JSON", "JSON CSV text"),
}


def _normalize(value: str) -> str:
    normalized = " ".join(str(value).strip().lower().split())
    normalized = re.sub(r"\s+nullable$", "", normalized)
    normalized = re.sub(r"\s+unsigned$", "", normalized)
    # Preserve MySQL boolean convention; strip other COLUMN_TYPE display widths.
    if normalized == "tinyint(1)":
        return normalized
    if match := re.fullmatch(r"(tinyint|smallint|mediumint|int|integer|bigint)\(\d+\)", normalized):
        return match.group(1)
    if match := re.fullmatch(r"(binary|varbinary)\(\d+\)", normalized):
        return match.group(1)
    if match := re.fullmatch(r"bit\((\d+)\)", normalized):
        return "bit" if match.group(1) == "1" else normalized
    return normalized


def _requires_contract(normalized: str) -> bool:
    return (
        normalized.startswith("enum(")
        or normalized.startswith("set(")
        or normalized.startswith(("geometry", "point", "linestring", "polygon", "multipoint"))
    )


__all__ = ["MySQLBigQueryTypeDecision", "MySQLBigQueryTypeMapper"]
