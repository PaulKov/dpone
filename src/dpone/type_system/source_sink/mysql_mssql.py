"""MySQL -> MSSQL type mapping profile."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class MySQLMssqlTypeDecision:
    """One explainable MySQL -> MSSQL type decision."""

    source_type: str
    target_type: str
    transfer_representation: str
    compatible: bool = True
    requires_explicit_contract: bool = False
    lossless: bool = True
    warning: str | None = None


class MySQLMssqlTypeMapper:
    """Focused pair-specific type decisions for planning, docs and schema comparison."""

    def resolve(self, source_type: str) -> MySQLMssqlTypeDecision:
        raw = " ".join(str(source_type).strip().lower().split())
        unsigned = bool(re.search(r"\bunsigned\b", raw))
        normalized = _normalize(raw)

        if unsigned:
            return _unsigned_decision(source_type, normalized)

        if match := re.fullmatch(r"decimal\((\d+),(\d+)\)", normalized):
            return MySQLMssqlTypeDecision(
                source_type=source_type,
                target_type=f"decimal({match.group(1)},{match.group(2)})",
                transfer_representation="decimal text",
            )
        if match := re.fullmatch(r"numeric\((\d+),(\d+)\)", normalized):
            return MySQLMssqlTypeDecision(
                source_type=source_type,
                target_type=f"decimal({match.group(1)},{match.group(2)})",
                transfer_representation="decimal text",
            )
        if match := re.fullmatch(r"varchar\((\d+)\)", normalized):
            width = int(match.group(1))
            target = f"nvarchar({min(width, 4000)})" if width <= 4000 else "nvarchar(max)"
            return MySQLMssqlTypeDecision(
                source_type=source_type,
                target_type=target,
                transfer_representation="BulkTextCodec text",
            )
        if match := re.fullmatch(r"char\((\d+)\)", normalized):
            return MySQLMssqlTypeDecision(
                source_type=source_type,
                target_type=f"nchar({match.group(1)})",
                transfer_representation="BulkTextCodec text",
            )
        if normalized in _EXACT:
            target_type, representation = _EXACT[normalized]
            return MySQLMssqlTypeDecision(
                source_type=source_type,
                target_type=target_type,
                transfer_representation=representation,
                requires_explicit_contract=normalized == "json",
                lossless=normalized != "json",
                warning=(
                    "MySQL JSON lands as nvarchar(max); declare schema_contract when consumers require typed JSON."
                    if normalized == "json"
                    else None
                ),
            )
        if _requires_contract(normalized):
            return MySQLMssqlTypeDecision(
                source_type=source_type,
                target_type="nvarchar(max)",
                transfer_representation="json/text via BulkTextCodec",
                requires_explicit_contract=True,
                lossless=False,
                warning="MySQL ENUM/SET/spatial types require explicit schema_contract.",
            )
        return MySQLMssqlTypeDecision(
            source_type=source_type,
            target_type="nvarchar(max)",
            transfer_representation="text via BulkTextCodec",
            requires_explicit_contract=True,
            lossless=False,
            warning="Unknown or custom MySQL type requires explicit schema_contract for production loads.",
        )


def _unsigned_decision(source_type: str, normalized: str) -> MySQLMssqlTypeDecision:
    """Map unsigned MySQL integers without silently truncating into signed MSSQL ranges."""

    base = re.sub(r"\s*\(\d+\)\s*$", "", normalized)
    wider = {
        "tinyint": ("smallint", False),
        "smallint": ("int", False),
        "mediumint": ("int", False),
        "int": ("bigint", False),
        "integer": ("bigint", False),
        "bigint": ("numeric(20,0)", True),
    }.get(base)
    if wider is None:
        return MySQLMssqlTypeDecision(
            source_type=source_type,
            target_type="numeric(38,0)",
            transfer_representation="unsigned numeric text",
            requires_explicit_contract=True,
            lossless=False,
            warning="Unsigned MySQL numeric type requires explicit schema_contract.",
        )
    target_type, needs_contract = wider
    return MySQLMssqlTypeDecision(
        source_type=source_type,
        target_type=target_type,
        transfer_representation="unsigned integer text",
        requires_explicit_contract=needs_contract,
        lossless=not needs_contract,
        warning=(
            "MySQL BIGINT UNSIGNED exceeds signed MSSQL BIGINT; use numeric(20,0) via schema_contract."
            if needs_contract
            else f"MySQL UNSIGNED {base} maps to wider signed MSSQL {target_type}."
        ),
    )


_EXACT: dict[str, tuple[str, str]] = {
    "tinyint": ("smallint", "integer text"),
    "tinyint(1)": ("bit", "0/1 text"),
    "smallint": ("smallint", "integer text"),
    "mediumint": ("int", "integer text"),
    "int": ("int", "integer text"),
    "integer": ("int", "integer text"),
    "bigint": ("bigint", "integer text"),
    "float": ("real", "float text"),
    "double": ("float", "float text"),
    "double precision": ("float", "float text"),
    "bit": ("bit", "0/1 text"),
    "bool": ("bit", "0/1 text"),
    "boolean": ("bit", "0/1 text"),
    "date": ("date", "ISO date text"),
    "datetime": ("datetime2(6)", "timestamp text"),
    "timestamp": ("datetime2(6)", "timestamp text"),
    "time": ("time(6)", "time text"),
    "year": ("smallint", "integer text"),
    "text": ("nvarchar(max)", "BulkTextCodec text"),
    "tinytext": ("nvarchar(max)", "BulkTextCodec text"),
    "mediumtext": ("nvarchar(max)", "BulkTextCodec text"),
    "longtext": ("nvarchar(max)", "BulkTextCodec text"),
    "blob": ("varbinary(max)", "binary hex text"),
    "tinyblob": ("varbinary(max)", "binary hex text"),
    "mediumblob": ("varbinary(max)", "binary hex text"),
    "longblob": ("varbinary(max)", "binary hex text"),
    "binary": ("varbinary(max)", "binary hex text"),
    "varbinary": ("varbinary(max)", "binary hex text"),
    "json": ("nvarchar(max)", "json text via BulkTextCodec"),
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


__all__ = ["MySQLMssqlTypeDecision", "MySQLMssqlTypeMapper"]
