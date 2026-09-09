"""MySQL -> ClickHouse type mapping profile."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class MySQLClickHouseTypeDecision:
    """One explainable MySQL -> ClickHouse type decision."""

    source_type: str
    target_type: str
    transfer_representation: str
    compatible: bool = True
    requires_explicit_contract: bool = False
    lossless: bool = True
    warning: str | None = None


class MySQLClickHouseTypeMapper:
    """Focused pair-specific type decisions for planning, docs and schema comparison."""

    def resolve(self, source_type: str) -> MySQLClickHouseTypeDecision:
        raw = " ".join(str(source_type).strip().lower().split())
        unsigned = bool(re.search(r"\bunsigned\b", raw))
        normalized = _normalize(raw)

        if unsigned:
            return _unsigned_decision(source_type, normalized)

        if match := re.fullmatch(r"decimal\((\d+),(\d+)\)", normalized):
            return MySQLClickHouseTypeDecision(
                source_type=source_type,
                target_type=f"Decimal({match.group(1)}, {match.group(2)})",
                transfer_representation="decimal TSV text",
            )
        if match := re.fullmatch(r"numeric\((\d+),(\d+)\)", normalized):
            return MySQLClickHouseTypeDecision(
                source_type=source_type,
                target_type=f"Decimal({match.group(1)}, {match.group(2)})",
                transfer_representation="decimal TSV text",
            )
        if match := re.fullmatch(r"varchar\((\d+)\)", normalized):
            return MySQLClickHouseTypeDecision(
                source_type=source_type,
                target_type="String",
                transfer_representation="escaped TSV text",
            )
        if match := re.fullmatch(r"char\((\d+)\)", normalized):
            return MySQLClickHouseTypeDecision(
                source_type=source_type,
                target_type="String",
                transfer_representation="escaped TSV text",
            )
        if normalized in _EXACT:
            target_type, representation = _EXACT[normalized]
            return MySQLClickHouseTypeDecision(
                source_type=source_type,
                target_type=target_type,
                transfer_representation=representation,
                requires_explicit_contract=normalized == "year",
                lossless=normalized not in {"float", "year"},
                warning=(
                    "MySQL YEAR maps to Int16; declare schema_contract when consumers require year semantics."
                    if normalized == "year"
                    else None
                ),
            )
        if _requires_contract(normalized):
            return MySQLClickHouseTypeDecision(
                source_type=source_type,
                target_type="String",
                transfer_representation="escaped TSV text",
                requires_explicit_contract=True,
                lossless=False,
                warning="MySQL ENUM/SET/spatial types require explicit schema_contract.",
            )
        return MySQLClickHouseTypeDecision(
            source_type=source_type,
            target_type="String",
            transfer_representation="escaped TSV text",
            requires_explicit_contract=True,
            lossless=False,
            warning="Unknown or custom MySQL type requires explicit schema_contract for production loads.",
        )


def _unsigned_decision(source_type: str, normalized: str) -> MySQLClickHouseTypeDecision:
    base = re.sub(r"\s*\(\d+\)\s*$", "", normalized)
    wider = {
        "tinyint": ("UInt8", False),
        "smallint": ("UInt16", False),
        "mediumint": ("UInt32", False),
        "int": ("UInt32", False),
        "integer": ("UInt32", False),
        "bigint": ("UInt64", False),
    }.get(base)
    if wider is None:
        return MySQLClickHouseTypeDecision(
            source_type=source_type,
            target_type="String",
            transfer_representation="unsigned numeric TSV text",
            requires_explicit_contract=True,
            lossless=False,
            warning="Unsigned MySQL numeric type requires explicit schema_contract.",
        )
    target_type, needs_contract = wider
    return MySQLClickHouseTypeDecision(
        source_type=source_type,
        target_type=target_type,
        transfer_representation="unsigned integer TSV text",
        requires_explicit_contract=needs_contract,
        lossless=not needs_contract,
        warning=f"MySQL UNSIGNED {base} maps to ClickHouse {target_type}.",
    )


_EXACT: dict[str, tuple[str, str]] = {
    "tinyint": ("Int8", "integer TSV text"),
    "tinyint(1)": ("Bool", "0/1 TSV"),
    "smallint": ("Int16", "integer TSV text"),
    "mediumint": ("Int32", "integer TSV text"),
    "int": ("Int32", "integer TSV text"),
    "integer": ("Int32", "integer TSV text"),
    "bigint": ("Int64", "integer TSV text"),
    "float": ("Float32", "float TSV text"),
    "double": ("Float64", "float TSV text"),
    "double precision": ("Float64", "float TSV text"),
    "bit": ("Bool", "0/1 TSV"),
    "bool": ("Bool", "0/1 TSV"),
    "boolean": ("Bool", "0/1 TSV"),
    "date": ("Date", "ISO date TSV"),
    "datetime": ("DateTime64(6)", "ClickHouse-safe timestamp text"),
    "timestamp": ("DateTime64(6)", "ClickHouse-safe timestamp text"),
    "time": ("String", "time text"),
    "year": ("Int16", "integer TSV text"),
    "text": ("String", "escaped TSV text"),
    "tinytext": ("String", "escaped TSV text"),
    "mediumtext": ("String", "escaped TSV text"),
    "longtext": ("String", "escaped TSV text"),
    "blob": ("String", "binary hex TSV text"),
    "tinyblob": ("String", "binary hex TSV text"),
    "mediumblob": ("String", "binary hex TSV text"),
    "longblob": ("String", "binary hex TSV text"),
    "binary": ("String", "binary hex TSV text"),
    "varbinary": ("String", "binary hex TSV text"),
    "json": ("String", "JSON text"),
}


def _normalize(value: str) -> str:
    normalized = " ".join(str(value).strip().lower().split())
    normalized = re.sub(r"\s+nullable$", "", normalized)
    normalized = re.sub(r"\s+unsigned$", "", normalized)
    return normalized


def _requires_contract(normalized: str) -> bool:
    return (
        normalized.startswith("enum(")
        or normalized.startswith("set(")
        or normalized.startswith(("geometry", "point", "linestring", "polygon", "multipoint"))
    )


__all__ = ["MySQLClickHouseTypeDecision", "MySQLClickHouseTypeMapper"]
