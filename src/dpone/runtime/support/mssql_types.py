"""SQL Server DDL type mapping.

One mapper backs SQL Server staging DDL and schema-evolution comparisons for
every ``* -> mssql`` route. Policy (documented in
``docs/type-mapping-matrix.md``):

- **ClickHouse dialect** (recognized case-sensitive spellings): strict
  allowlist via :mod:`dpone.type_system.source_sink.clickhouse_mssql`. Every
  type is either mapped width/precision-exact or raises
  :class:`MSSQLTypeMappingError` with the reason and a source-side workaround
  — no silent fallback.
- **Generic/PostgreSQL dialect** (lowercase spellings): exact table plus
  parameterized families; unknown types keep the documented safe-fallback to
  ``nvarchar(max)`` but emit a warning so the drift is visible in logs.

Mapped ClickHouse types round-trip through
``dpone.readiness.schema_type_compatibility`` (idempotent chunk re-runs).
"""

from __future__ import annotations

import logging
import re

from dpone.type_system.source_sink.clickhouse_mssql import classify_clickhouse_type

logger = logging.getLogger(__name__)


class MSSQLTypeMappingError(ValueError):
    """Deterministic configuration error for unsupported source types."""


class MSSQLTypeMapper:
    """Maps common source/runtime types to SQL Server DDL types."""

    _EXACT = {
        "integer": "int",
        "int": "int",
        "int4": "int",
        "bigint": "bigint",
        "int8": "bigint",
        "smallint": "smallint",
        "boolean": "bit",
        "bool": "bit",
        "bit": "bit",
        "text": "nvarchar(max)",
        "string": "nvarchar(max)",
        "json": "nvarchar(max)",
        "jsonb": "nvarchar(max)",
        "uuid": "uniqueidentifier",
        "uniqueidentifier": "uniqueidentifier",
        "date": "date",
        "timestamp": "datetime2",
        "timestamp without time zone": "datetime2",
        "timestamp with time zone": "datetimeoffset",
        "datetime": "datetime2",
        "datetime2": "datetime2",
        "datetimeoffset": "datetimeoffset",
        "float": "float",
        "float8": "float",
        "double precision": "float",
        "real": "real",
        # MySQL dialect aligned with mysql_to_mssql_native_v1 / MySQLMssqlTypeMapper.
        "tinyint": "smallint",
        "tinyint(1)": "bit",
        "mediumint": "int",
        "year": "smallint",
        "time": "time(6)",
        "blob": "varbinary(max)",
        "tinyblob": "varbinary(max)",
        "mediumblob": "varbinary(max)",
        "longblob": "varbinary(max)",
        "tinytext": "nvarchar(max)",
        "mediumtext": "nvarchar(max)",
        "longtext": "nvarchar(max)",
    }

    @classmethod
    def is_clickhouse_type(cls, dtype: str) -> bool:
        """Return whether the case-sensitive spelling belongs to ClickHouse."""

        return classify_clickhouse_type(dtype) is not None

    @classmethod
    def to_mssql(cls, dtype: str) -> str:
        decision = classify_clickhouse_type(dtype)
        if decision is not None:
            if decision.mssql_type is None:
                raise MSSQLTypeMappingError(
                    f"ClickHouse type {decision.source_type!r} is not supported by the MSSQL sink: "
                    f"{decision.reason}. Workaround: {decision.workaround}."
                )
            return decision.mssql_type
        return cls._generic_to_mssql(str(dtype).strip())

    @classmethod
    def _generic_to_mssql(cls, dtype: str) -> str:
        # Extract schemas often append " nullable"; SQL Server DDL must not keep that token.
        normalized = re.sub(r"\s+nullable$", "", dtype.lower()).strip()
        if normalized in cls._EXACT:
            return cls._EXACT[normalized]
        if normalized.startswith(("varchar", "nvarchar", "char", "nchar", "varbinary", "binary")):
            return normalized
        if normalized.startswith(("datetime2", "datetimeoffset", "smalldatetime", "time(")):
            return normalized
        if re.fullmatch(r"float\(\d+\)", normalized):
            return normalized
        if normalized.startswith(("decimal", "numeric")):
            return re.sub(r"\s+", "", normalized)
        if normalized.startswith(("int", "uint")):
            return "bigint"
        if normalized.startswith(("float", "double")):
            return "float"
        mysql_target = cls._mysql_pair_target(normalized)
        if mysql_target is not None:
            return mysql_target
        logger.warning(
            "Unknown source type %r for the MSSQL sink; using the documented safe-fallback nvarchar(max). "
            "Declare an explicit schema_contract to pin the target type.",
            dtype,
        )
        return "nvarchar(max)"

    @classmethod
    def _mysql_pair_target(cls, dtype: str) -> str | None:
        """Apply mysql_to_mssql_native_v1 when the generic table has no hit."""

        from dpone.type_system.source_sink.mysql_mssql import MySQLMssqlTypeMapper

        decision = MySQLMssqlTypeMapper().resolve(dtype)
        warning = decision.warning or ""
        if "Unknown or custom MySQL type" in warning:
            return None
        return decision.target_type


__all__ = ["MSSQLTypeMapper", "MSSQLTypeMappingError"]
