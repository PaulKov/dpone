"""MSSQL -> Postgres type mapping profile."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class MSSQLPostgresTypeDecision:
    """One explainable MSSQL -> Postgres type decision."""

    source_type: str
    target_type: str
    transfer_representation: str
    compatible: bool = True
    requires_explicit_contract: bool = False
    lossless: bool = True
    warning: str | None = None


class MSSQLPostgresTypeMapper:
    """Focused pair-specific type decisions for planning, docs and schema comparison."""

    def resolve(self, source_type: str) -> MSSQLPostgresTypeDecision:
        normalized = _normalize(source_type)

        if match := re.fullmatch(r"(?:decimal|numeric)\((\d+),(\d+)\)", normalized):
            return MSSQLPostgresTypeDecision(
                source_type=source_type,
                target_type=f"numeric({match.group(1)},{match.group(2)})",
                transfer_representation="decimal CSV text",
            )
        if match := re.fullmatch(r"nvarchar\((max|\d+)\)", normalized):
            size = match.group(1)
            target = "text" if size == "max" else f"varchar({size})"
            return MSSQLPostgresTypeDecision(
                source_type=source_type,
                target_type=target,
                transfer_representation="CSV text",
            )
        if match := re.fullmatch(r"varchar\((max|\d+)\)", normalized):
            size = match.group(1)
            target = "text" if size == "max" else f"varchar({size})"
            return MSSQLPostgresTypeDecision(
                source_type=source_type,
                target_type=target,
                transfer_representation="CSV text",
            )
        if match := re.fullmatch(r"nchar\((\d+)\)", normalized):
            return MSSQLPostgresTypeDecision(
                source_type=source_type,
                target_type=f"char({match.group(1)})",
                transfer_representation="CSV text",
            )
        if match := re.fullmatch(r"char\((\d+)\)", normalized):
            return MSSQLPostgresTypeDecision(
                source_type=source_type,
                target_type=f"char({match.group(1)})",
                transfer_representation="CSV text",
            )
        if re.fullmatch(r"(?:var)?binary\((?:max|\d+)\)", normalized):
            return MSSQLPostgresTypeDecision(
                source_type=source_type,
                target_type="bytea",
                transfer_representation="binary hex CSV text",
            )
        if re.fullmatch(r"datetime2(?:\(\d+\))?", normalized):
            return MSSQLPostgresTypeDecision(
                source_type=source_type,
                target_type="timestamp",
                transfer_representation="timestamp CSV text",
            )
        if re.fullmatch(r"datetimeoffset(?:\(\d+\))?", normalized):
            return MSSQLPostgresTypeDecision(
                source_type=source_type,
                target_type="timestamp with time zone",
                transfer_representation="timestamptz CSV text",
            )
        if re.fullmatch(r"time(?:\(\d+\))?", normalized):
            return MSSQLPostgresTypeDecision(
                source_type=source_type,
                target_type="time",
                transfer_representation="time CSV text",
            )
        if normalized in _EXACT:
            target_type, representation, lossless, warning = _EXACT[normalized]
            return MSSQLPostgresTypeDecision(
                source_type=source_type,
                target_type=target_type,
                transfer_representation=representation,
                lossless=lossless,
                requires_explicit_contract=warning is not None and "schema_contract" in warning,
                warning=warning,
            )
        if _requires_contract(normalized):
            return MSSQLPostgresTypeDecision(
                source_type=source_type,
                target_type="text",
                transfer_representation="text via CSV",
                requires_explicit_contract=True,
                lossless=False,
                warning="MSSQL spatial/hierarchyid/sql_variant types require explicit schema_contract.",
            )
        return MSSQLPostgresTypeDecision(
            source_type=source_type,
            target_type="text",
            transfer_representation="text via CSV",
            requires_explicit_contract=True,
            lossless=False,
            warning="Unknown or custom MSSQL type requires explicit schema_contract for production loads.",
        )


# target_type, transfer_representation, lossless, warning
_EXACT: dict[str, tuple[str, str, bool, str | None]] = {
    "tinyint": ("smallint", "integer CSV text", True, None),
    "smallint": ("smallint", "integer CSV text", True, None),
    "int": ("integer", "integer CSV text", True, None),
    "bigint": ("bigint", "integer CSV text", True, None),
    "bit": ("boolean", "boolean CSV text", True, None),
    "real": ("real", "float CSV text", True, None),
    "float": ("double precision", "float CSV text", True, None),
    "money": ("numeric(19,4)", "decimal CSV text", True, None),
    "smallmoney": ("numeric(10,4)", "decimal CSV text", True, None),
    "date": ("date", "ISO date CSV text", True, None),
    "datetime": ("timestamp", "timestamp CSV text", True, None),
    "smalldatetime": (
        "timestamp",
        "timestamp CSV text",
        False,
        "MSSQL smalldatetime is mapped to timestamp; declare schema_contract when second-level precision matters.",
    ),
    "uniqueidentifier": ("uuid", "uuid CSV text", True, None),
    "xml": ("text", "XML CSV text", False, "MSSQL XML lands as text; declare schema_contract for typed XML consumers."),
    "text": ("text", "CSV text", True, None),
    "ntext": ("text", "CSV text", True, None),
    "image": ("bytea", "binary hex CSV text", True, None),
    "rowversion": ("bytea", "binary hex CSV text", True, None),
    "timestamp": ("bytea", "binary hex CSV text", True, None),
}


def _normalize(value: str) -> str:
    normalized = " ".join(str(value).strip().lower().split())
    return re.sub(r"\s+nullable$", "", normalized).strip()


def _requires_contract(normalized: str) -> bool:
    return normalized.startswith(("geography", "geometry", "hierarchyid", "sql_variant"))


__all__ = ["MSSQLPostgresTypeDecision", "MSSQLPostgresTypeMapper"]
