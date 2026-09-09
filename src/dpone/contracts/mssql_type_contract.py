"""Canonical MSSQL type classes shared by planning and runtime."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

_EXPLICIT_CONTRACT_TYPES = frozenset(
    {
        "geography",
        "geometry",
        "hierarchyid",
        "sql_variant",
        "xml",
    }
)

_PARAMETERLESS_PHYSICAL_TYPES = frozenset(
    {
        "bigint",
        "bit",
        "date",
        "datetime",
        "float",
        "int",
        "money",
        "real",
        "smalldatetime",
        "smallint",
        "smallmoney",
        "tinyint",
        "uniqueidentifier",
    }
)
_CHARACTER_LIMITS = {
    "char": 8000,
    "nchar": 4000,
    "nvarchar": 4000,
    "varchar": 8000,
}
_BINARY_LIMITS = {"binary": 8000, "varbinary": 8000}


@dataclass(frozen=True, slots=True)
class MssqlCatalogColumn:
    """Exact SQL Server catalog column used by schema-evolution consumers."""

    name: str
    dtype: str
    nullable: bool
    collation: str | None = None


def normalized_mssql_base_type(source_type: str) -> str:
    """Return one lowercase base type without nullability or size parameters."""

    normalized = str(source_type or "").strip().lower()
    normalized = normalized.replace(" nullable", "").strip()
    if normalized.startswith("nullable(") and normalized.endswith(")"):
        normalized = normalized[len("nullable(") : -1].strip()
    return re.sub(r"\(.*\)$", "", normalized).strip()


def mssql_type_requires_explicit_contract(source_type: str) -> bool:
    """Return whether automatic physical mapping is unsafe for this type."""

    normalized = str(source_type or "").strip().lower()
    return normalized_mssql_base_type(normalized) in _EXPLICIT_CONTRACT_TYPES or "user-defined" in normalized


def normalize_mssql_physical_type(value: str) -> str:
    """Validate and canonicalize an insertable SQL Server column type.

    This parser is deliberately strict for generated/runtime DDL. It prevents
    permissive legacy type mapping from turning a misspelled override into
    ``nvarchar(max)`` and rejects declarations SQL Server cannot create.
    """

    normalized = re.sub(r"\s+", "", str(value or "").strip().lower())
    if normalized in _PARAMETERLESS_PHYSICAL_TYPES:
        return normalized

    decimal = re.fullmatch(r"(decimal|numeric)\((\d+),(\d+)\)", normalized)
    if decimal:
        precision, scale = int(decimal.group(2)), int(decimal.group(3))
        if not 1 <= precision <= 38 or not 0 <= scale <= precision:
            raise ValueError("MSSQL decimal precision or scale is invalid")
        return f"{decimal.group(1)}({precision},{scale})"

    floating = re.fullmatch(r"float\((\d+)\)", normalized)
    if floating:
        precision = int(floating.group(1))
        if not 1 <= precision <= 53:
            raise ValueError("MSSQL float precision must be between 1 and 53")
        return f"float({precision})"

    temporal = re.fullmatch(r"(datetime2|datetimeoffset|time)(?:\((\d+)\))?", normalized)
    if temporal:
        scale = 7 if temporal.group(2) is None else int(temporal.group(2))
        if not 0 <= scale <= 7:
            raise ValueError("MSSQL temporal scale must be between 0 and 7")
        return f"{temporal.group(1)}({scale})"

    sized = re.fullmatch(r"(nvarchar|varchar|nchar|char|varbinary|binary)\((max|\d+)\)", normalized)
    if sized:
        family, raw_length = sized.group(1), sized.group(2)
        if raw_length == "max":
            if family not in {"nvarchar", "varchar", "varbinary"}:
                raise ValueError(f"MSSQL {family} does not support MAX")
            return f"{family}(max)"
        length = int(raw_length)
        limit = (_CHARACTER_LIMITS | _BINARY_LIMITS)[family]
        if not 1 <= length <= limit:
            raise ValueError(f"MSSQL {family} length must be between 1 and {limit}")
        return f"{family}({length})"

    raise ValueError(f"unsupported MSSQL physical type: {value}")


def mssql_logical_type(source_type: str) -> dict[str, Any]:
    """Map MSSQL metadata to the logical schema-contract vocabulary."""

    normalized = str(source_type or "").strip().lower()
    base = normalized_mssql_base_type(normalized)
    if not base:
        raise ValueError("MSSQL source type is required")
    if mssql_type_requires_explicit_contract(normalized):
        raise ValueError(f"MSSQL source type requires an explicit contract: {base}")
    decimal = re.fullmatch(
        r"(?:decimal|numeric)\(\s*(\d+)\s*,\s*(\d+)\s*\)",
        _without_nullability(normalized),
    )
    if decimal:
        precision = int(decimal.group(1))
        scale = int(decimal.group(2))
        if not 1 <= precision <= 38 or not 0 <= scale <= precision:
            raise ValueError("MSSQL decimal precision or scale is invalid")
        return {
            "type": "decimal",
            "precision": precision,
            "scale": scale,
        }
    if base == "money":
        return {"type": "decimal", "precision": 19, "scale": 4}
    if base == "smallmoney":
        return {"type": "decimal", "precision": 10, "scale": 4}
    if base in {"decimal", "numeric"}:
        raise ValueError("MSSQL decimal contract requires explicit precision and scale")
    if base == "bigint":
        return {"type": "bigint"}
    if base in {"int", "integer", "smallint", "tinyint"}:
        return {"type": "integer"}
    if base in {"bit", "boolean", "bool"}:
        return {"type": "boolean"}
    if base == "uniqueidentifier":
        return {"type": "uuid"}
    if base == "date":
        return {"type": "date"}
    if base == "datetimeoffset":
        return {"type": "timestamp with time zone"}
    if base in {"datetime", "datetime2", "smalldatetime"}:
        return {"type": "timestamp"}
    if base == "time":
        return {"type": "time"}
    if base == "real":
        return {"type": "float32"}
    if base in {"float", "double", "double precision"}:
        float_precision = _float_precision(normalized)
        return {"type": "float32" if float_precision is not None and float_precision <= 24 else "float64"}
    if base in {"binary", "varbinary", "image", "rowversion", "timestamp"}:
        return {"type": "binary"}
    if base in {
        "char",
        "varchar",
        "nchar",
        "nvarchar",
        "text",
        "ntext",
        "sysname",
    }:
        return {"type": "string"}
    raise ValueError(f"unsupported MSSQL source type: {source_type}")


def _without_nullability(source_type: str) -> str:
    normalized = source_type.replace(" nullable", "").strip()
    if normalized.startswith("nullable(") and normalized.endswith(")"):
        return normalized[len("nullable(") : -1].strip()
    return normalized


def _float_precision(source_type: str) -> int | None:
    match = re.fullmatch(r"float\(\s*(\d+)\s*\)", _without_nullability(source_type))
    if match is None:
        return None
    precision = int(match.group(1))
    if not 1 <= precision <= 53:
        raise ValueError("MSSQL float precision must be between 1 and 53")
    return precision


__all__ = [
    "MssqlCatalogColumn",
    "mssql_logical_type",
    "mssql_type_requires_explicit_contract",
    "normalize_mssql_physical_type",
    "normalized_mssql_base_type",
]
