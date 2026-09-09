"""MySQL -> Postgres type mapping profile."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class MySQLPostgresTypeDecision:
    """One explainable MySQL -> Postgres type decision."""

    source_type: str
    target_type: str
    transfer_representation: str
    compatible: bool = True
    requires_explicit_contract: bool = False
    lossless: bool = True
    warning: str | None = None


class MySQLPostgresTypeMapper:
    """Focused pair-specific type decisions for planning, docs and schema comparison."""

    def resolve(self, source_type: str) -> MySQLPostgresTypeDecision:
        raw = " ".join(str(source_type).strip().lower().split())
        unsigned = bool(re.search(r"\bunsigned\b", raw))
        normalized = _normalize(raw)

        if unsigned:
            return _unsigned_decision(source_type, normalized)

        if match := re.fullmatch(r"decimal\((\d+),(\d+)\)", normalized):
            return MySQLPostgresTypeDecision(
                source_type=source_type,
                target_type=f"numeric({match.group(1)},{match.group(2)})",
                transfer_representation="decimal CSV text",
            )
        if match := re.fullmatch(r"numeric\((\d+),(\d+)\)", normalized):
            return MySQLPostgresTypeDecision(
                source_type=source_type,
                target_type=f"numeric({match.group(1)},{match.group(2)})",
                transfer_representation="decimal CSV text",
            )
        if match := re.fullmatch(r"varchar\((\d+)\)", normalized):
            return MySQLPostgresTypeDecision(
                source_type=source_type,
                target_type=f"varchar({match.group(1)})",
                transfer_representation="CSV text",
            )
        if match := re.fullmatch(r"char\((\d+)\)", normalized):
            return MySQLPostgresTypeDecision(
                source_type=source_type,
                target_type=f"char({match.group(1)})",
                transfer_representation="CSV text",
            )
        if normalized in _EXACT:
            target_type, representation = _EXACT[normalized]
            return MySQLPostgresTypeDecision(
                source_type=source_type,
                target_type=target_type,
                transfer_representation=representation,
                requires_explicit_contract=normalized == "year",
                lossless=normalized not in {"float", "year"},
                warning=(
                    "MySQL YEAR is mapped to smallint; declare schema_contract when consumers require year semantics."
                    if normalized == "year"
                    else None
                ),
            )
        if _requires_contract(normalized):
            return MySQLPostgresTypeDecision(
                source_type=source_type,
                target_type="text",
                transfer_representation="json/text via CSV",
                requires_explicit_contract=True,
                lossless=False,
                warning="MySQL ENUM/SET/spatial types require explicit schema_contract.",
            )
        return MySQLPostgresTypeDecision(
            source_type=source_type,
            target_type="text",
            transfer_representation="text via CSV",
            requires_explicit_contract=True,
            lossless=False,
            warning="Unknown or custom MySQL type requires explicit schema_contract for production loads.",
        )


def _unsigned_decision(source_type: str, normalized: str) -> MySQLPostgresTypeDecision:
    """Map unsigned MySQL integers without silently truncating into signed Postgres ranges."""

    base = re.sub(r"\s*\(\d+\)\s*$", "", normalized)
    wider = {
        "tinyint": ("smallint", False),
        "smallint": ("integer", False),
        "mediumint": ("integer", False),
        "int": ("bigint", False),
        "integer": ("bigint", False),
        "bigint": ("numeric(20,0)", True),
    }.get(base)
    if wider is None:
        return MySQLPostgresTypeDecision(
            source_type=source_type,
            target_type="numeric",
            transfer_representation="unsigned numeric CSV text",
            requires_explicit_contract=True,
            lossless=False,
            warning="Unsigned MySQL numeric type requires explicit schema_contract.",
        )
    target_type, needs_contract = wider
    return MySQLPostgresTypeDecision(
        source_type=source_type,
        target_type=target_type,
        transfer_representation="unsigned integer CSV text",
        requires_explicit_contract=needs_contract,
        lossless=not needs_contract,
        warning=(
            "MySQL BIGINT UNSIGNED exceeds signed Postgres BIGINT; use numeric(20,0) via schema_contract."
            if needs_contract
            else f"MySQL UNSIGNED {base} maps to wider signed Postgres {target_type}."
        ),
    )


_EXACT: dict[str, tuple[str, str]] = {
    "tinyint": ("smallint", "integer CSV text"),
    "tinyint(1)": ("boolean", "boolean CSV text"),
    "smallint": ("smallint", "integer CSV text"),
    "mediumint": ("integer", "integer CSV text"),
    "int": ("integer", "integer CSV text"),
    "integer": ("integer", "integer CSV text"),
    "bigint": ("bigint", "integer CSV text"),
    "float": ("real", "float CSV text"),
    "double": ("double precision", "float CSV text"),
    "double precision": ("double precision", "float CSV text"),
    "bit": ("boolean", "boolean CSV text"),
    "bool": ("boolean", "boolean CSV text"),
    "boolean": ("boolean", "boolean CSV text"),
    "date": ("date", "ISO date CSV text"),
    "datetime": ("timestamp", "timestamp CSV text"),
    "timestamp": ("timestamp", "timestamp CSV text"),
    "time": ("time", "time CSV text"),
    "year": ("smallint", "integer CSV text"),
    "text": ("text", "CSV text"),
    "tinytext": ("text", "CSV text"),
    "mediumtext": ("text", "CSV text"),
    "longtext": ("text", "CSV text"),
    "blob": ("bytea", "binary hex CSV text"),
    "tinyblob": ("bytea", "binary hex CSV text"),
    "mediumblob": ("bytea", "binary hex CSV text"),
    "longblob": ("bytea", "binary hex CSV text"),
    "binary": ("bytea", "binary hex CSV text"),
    "varbinary": ("bytea", "binary hex CSV text"),
    "json": ("jsonb", "JSON CSV text"),
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


__all__ = ["MySQLPostgresTypeDecision", "MySQLPostgresTypeMapper"]
