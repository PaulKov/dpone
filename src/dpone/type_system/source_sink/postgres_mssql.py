"""PostgreSQL -> MSSQL type mapping profile."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class PostgresMssqlTypeDecision:
    """One explainable PostgreSQL -> MSSQL type decision."""

    source_type: str
    target_type: str
    transfer_representation: str
    compatible: bool = True
    requires_explicit_contract: bool = False
    warning: str | None = None


class PostgresMssqlTypeMapper:
    """Focused pair-specific type decisions for planning, docs and schema comparison."""

    def resolve(self, source_type: str) -> PostgresMssqlTypeDecision:
        semantic_type = _semantic_source_type(source_type)
        bounded_character = _bounded_character(semantic_type)
        if bounded_character is not None:
            _family, length = bounded_character
            # PostgreSQL counts Unicode code points while SQL Server nvarchar(n)
            # counts UTF-16 code units.  A supplementary code point consumes two
            # SQL Server units, so 2*n is the only declaration-wide guarantee.
            target_length = length * 2
            if target_length > 4000:
                return PostgresMssqlTypeDecision(
                    source_type=source_type,
                    target_type="nvarchar(max)",
                    transfer_representation="exact text via BulkTextCodec",
                    requires_explicit_contract=True,
                    warning=(
                        "PostgreSQL character width cannot be represented by a bounded "
                        "SQL Server Unicode type for every supplementary-plane value."
                    ),
                )
            return PostgresMssqlTypeDecision(
                source_type=source_type,
                # nvarchar avoids adding SQL Server fixed-width padding that is
                # not part of PostgreSQL's exported value representation.
                target_type=f"nvarchar({target_length})",
                transfer_representation="BulkTextCodec text",
            )
        normalized = _normalize(semantic_type)
        if match := re.fullmatch(r"(?:numeric|decimal)\((\d+)(?:,(-?\d+))?\)", normalized):
            return _numeric_decision(source_type, int(match.group(1)), int(match.group(2) or 0))
        if temporal := _bounded_temporal(normalized):
            source_family, scale = temporal
            target_family = {
                "timestamp without time zone": "datetime2",
                "timestamp": "datetime2",
                "timestamp with time zone": "datetimeoffset",
                "timestamptz": "datetimeoffset",
                "time without time zone": "time",
                "time": "time",
            }[source_family]
            return PostgresMssqlTypeDecision(
                source_type=source_type,
                target_type=f"{target_family}({scale})",
                transfer_representation="offset timestamp text"
                if target_family == "datetimeoffset"
                else "timestamp text"
                if target_family == "datetime2"
                else "time text",
            )
        if normalized in _EXACT:
            target_type, representation = _EXACT[normalized]
            return PostgresMssqlTypeDecision(
                source_type=source_type,
                target_type=target_type,
                transfer_representation=representation,
            )
        if _requires_explicit_text_landing(normalized):
            return PostgresMssqlTypeDecision(
                source_type=source_type,
                target_type="nvarchar(max)",
                transfer_representation="exact text via BulkTextCodec",
                requires_explicit_contract=True,
                warning="PostgreSQL type requires an explicit textual/physical contract for MSSQL.",
            )
        return PostgresMssqlTypeDecision(
            source_type=source_type,
            target_type="nvarchar(max)",
            transfer_representation="text via BulkTextCodec",
            requires_explicit_contract=True,
            warning="Unknown or custom PostgreSQL type requires explicit schema_contract for production loads.",
        )


_EXACT: dict[str, tuple[str, str]] = {
    "smallint": ("smallint", "integer text"),
    "int2": ("smallint", "integer text"),
    "integer": ("int", "integer text"),
    "int": ("int", "integer text"),
    "int4": ("int", "integer text"),
    "bigint": ("bigint", "integer text"),
    "int8": ("bigint", "integer text"),
    "real": ("real", "float text"),
    "float4": ("real", "float text"),
    "double precision": ("float", "float text"),
    "float8": ("float", "float text"),
    "boolean": ("bit", "0/1 text"),
    "bool": ("bit", "0/1 text"),
    "text": ("nvarchar(max)", "BulkTextCodec text"),
    "character varying": ("nvarchar(max)", "BulkTextCodec text"),
    "varchar": ("nvarchar(max)", "BulkTextCodec text"),
    # PostgreSQL CHARACTER without a length is CHARACTER(1); two UTF-16 units
    # are required for one arbitrary Unicode code point.
    "character": ("nvarchar(2)", "BulkTextCodec text"),
    "char": ("nvarchar(2)", "BulkTextCodec text"),
    "uuid": ("uniqueidentifier", "uuid text"),
    "bytea": ("varbinary(max)", "hex text via character BCP"),
    "date": ("date", "ISO date text"),
    "timestamp without time zone": ("datetime2(6)", "timestamp text"),
    "timestamp": ("datetime2(6)", "timestamp text"),
    "timestamp with time zone": ("datetimeoffset(6)", "offset timestamp text"),
    "timestamptz": ("datetimeoffset(6)", "offset timestamp text"),
    "time without time zone": ("time(6)", "time text"),
    "time": ("time(6)", "time text"),
    "json": ("nvarchar(max)", "json text via BulkTextCodec"),
    "jsonb": ("nvarchar(max)", "json text via BulkTextCodec"),
}


def _normalize(value: str) -> str:
    normalized = " ".join(str(value).strip().lower().split())
    normalized = re.sub(r"\s+nullable$", "", normalized)
    normalized = re.sub(r"character varying\(\d+\)", "character varying", normalized)
    normalized = re.sub(r"varchar\(\d+\)", "varchar", normalized)
    normalized = re.sub(r"character\(\d+\)", "character", normalized)
    normalized = re.sub(r"char\(\d+\)", "char", normalized)
    return normalized


def _semantic_source_type(value: str) -> str:
    """Remove dpone catalog metadata while retaining it in decision.source_type."""

    return re.sub(r"\s+\[dpone_pg=\{.*\}\]\s*$", "", str(value).strip())


def _bounded_character(value: str) -> tuple[str, int] | None:
    """Preserve PostgreSQL declared character width in the MSSQL contract."""

    normalized = " ".join(str(value).strip().lower().split())
    match = re.fullmatch(r"(character varying|varchar|character|char)\s*\(\s*(\d+)\s*\)", normalized)
    if match is None:
        return None
    length = int(match.group(2))
    if length < 1:
        return None
    return ("fixed" if match.group(1) in {"character", "char"} else "varying", length)


def _numeric_decision(source_type: str, precision: int, scale: int) -> PostgresMssqlTypeDecision:
    """Map PostgreSQL's extended numeric shape to valid SQL Server decimal.

    PostgreSQL permits scale greater than precision and, on supported server
    versions, negative scale. SQL Server requires ``0 <= scale <= precision``.
    Widening to ``decimal(max(p,s),s)`` or ``decimal(p-s,0)`` preserves every
    representable source value; shapes above precision 38 require approved text.
    """

    target_scale = max(scale, 0)
    target_precision = max(precision, scale) if scale >= 0 else precision - scale
    if precision < 1 or target_precision > 38:
        return PostgresMssqlTypeDecision(
            source_type=source_type,
            target_type="nvarchar(max)",
            transfer_representation="exact numeric text via BulkTextCodec",
            requires_explicit_contract=True,
            warning="PostgreSQL numeric domain exceeds SQL Server decimal(38,s).",
        )
    return PostgresMssqlTypeDecision(
        source_type=source_type,
        target_type=f"decimal({target_precision},{target_scale})",
        transfer_representation="decimal text",
    )


def _bounded_temporal(normalized: str) -> tuple[str, int] | None:
    patterns = (
        (r"timestamp\((\d+)\) without time zone", "timestamp without time zone"),
        (r"timestamp\((\d+)\) with time zone", "timestamp with time zone"),
        (r"timestamp\((\d+)\)", "timestamp"),
        (r"timestamptz\((\d+)\)", "timestamptz"),
        (r"time\((\d+)\) without time zone", "time without time zone"),
        (r"time\((\d+)\)", "time"),
    )
    for pattern, family in patterns:
        match = re.fullmatch(pattern, normalized)
        if match is not None:
            scale = int(match.group(1))
            return (family, scale) if 0 <= scale <= 6 else None
    return None


def _requires_explicit_text_landing(normalized: str) -> bool:
    return (
        normalized.endswith("[]")
        or normalized.startswith(("array:", "domain:", "enum:", "custom:", "user-defined:"))
        or "range" in normalized
        or normalized.startswith(("geometry", "geography", "raster", "spatial:", "composite:"))
        or normalized
        in {
            "bit",
            "bit varying",
            "varbit",
            "interval",
            "money",
            "xml",
            "jsonpath",
            "aclitem",
            '"char"',
            "name",
            "bpchar",
            "refcursor",
            "inet",
            "cidr",
            "macaddr",
            "macaddr8",
            "oid",
            "xid",
            "xid8",
            "cid",
            "tid",
            "regclass",
            "regcollation",
            "regconfig",
            "regdictionary",
            "regnamespace",
            "regoper",
            "regoperator",
            "regproc",
            "regprocedure",
            "regrole",
            "regtype",
            "tsvector",
            "tsquery",
            "pg_lsn",
            "pg_snapshot",
            "txid_snapshot",
            "point",
            "line",
            "lseg",
            "box",
            "path",
            "polygon",
            "circle",
            "time with time zone",
            "timetz",
        }
        or normalized.startswith(("bit(", "bit varying(", "varbit(", "interval "))
    )


__all__ = ["PostgresMssqlTypeDecision", "PostgresMssqlTypeMapper"]
