"""SQL Server object-name parsing and rendering primitives."""

from __future__ import annotations

import re
from dataclasses import dataclass

_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True, slots=True)
class MSSQLObjectName:
    """Immutable SQL Server table identity.

    Runtime code may pass either a two-part ``schema.table`` pair or a
    three-part ``database.schema.table`` identity. The compact
    ``database.schema`` schema form is accepted to preserve legacy route
    refresh config strings and to keep existing connector call sites small.
    """

    schema: str
    table: str
    database: str | None = None

    @classmethod
    def from_parts(
        cls,
        *,
        schema: str,
        table: str,
        database: str | None = None,
        strict: bool = False,
    ) -> MSSQLObjectName:
        db, normalized_schema = _normalize_schema(schema=schema, database=database, strict=strict)
        _validate_part(normalized_schema, "schema", strict=strict)
        _validate_part(table, "table", strict=strict)
        if db is not None:
            _validate_part(db, "database", strict=strict)
        return cls(database=db, schema=normalized_schema, table=str(table).strip())

    @classmethod
    def from_dataset(cls, value: str, *, strict: bool = True) -> MSSQLObjectName:
        parts = tuple(part.strip() for part in str(value or "").split("."))
        if len(parts) not in {2, 3} or any(not part for part in parts):
            raise ValueError(f"Unsafe MSSQL dataset identifier: {value}")
        if len(parts) == 2:
            return cls.from_parts(schema=parts[0], table=parts[1], strict=strict)
        return cls.from_parts(database=parts[0], schema=parts[1], table=parts[2], strict=strict)

    @property
    def schema_label(self) -> str:
        if self.database:
            return f"{self.database}.{self.schema}"
        return self.schema

    @property
    def dataset(self) -> str:
        return f"{self.schema_label}.{self.table}"

    def quoted(self) -> str:
        parts = (self.database, self.schema, self.table) if self.database else (self.schema, self.table)
        return ".".join(quote_mssql_identifier(part) for part in parts if part)

    def information_schema_table(self, table: str) -> str:
        prefix = f"{quote_mssql_identifier(self.database)}." if self.database else ""
        return f"{prefix}INFORMATION_SCHEMA.{table}"

    @property
    def sp_rename_object_name(self) -> str:
        """Two-part object identity expected by ``sp_rename`` inside the target database."""

        return f"{self.schema}.{self.table}"

    @property
    def execution_scope_prefix(self) -> str:
        """Database scope prefix for ``sys.sp_executesql`` when identity is cross-database."""

        return f"{quote_mssql_identifier(self.database)}." if self.database else ""

    def with_table(self, table: str) -> MSSQLObjectName:
        return MSSQLObjectName(database=self.database, schema=self.schema, table=str(table).strip())


def quote_mssql_identifier(value: str) -> str:
    text = str(value)
    if not text:
        raise ValueError("MSSQL identifier must not be empty")
    return "[" + text.replace("]", "]]") + "]"


def safe_mssql_identifier(value: str) -> bool:
    return bool(_SAFE_IDENTIFIER_RE.fullmatch(str(value or "")))


def mssql_dataset_is_safe(value: str) -> bool:
    try:
        MSSQLObjectName.from_dataset(value, strict=True)
    except ValueError:
        return False
    return True


def mssql_ensure_schema_statement(schema: str) -> tuple[str, tuple[str, ...]]:
    name = MSSQLObjectName.from_parts(schema=schema, table="__dpone_schema_probe")
    schema_sql = quote_mssql_identifier(name.schema)
    if name.database:
        database_sql = quote_mssql_identifier(name.database)
        return (
            f"""
            IF NOT EXISTS (SELECT 1 FROM {database_sql}.sys.schemas WHERE name = ?)
            BEGIN
                EXEC {database_sql}.sys.sp_executesql N'CREATE SCHEMA {schema_sql}'
            END
            """,
            (name.schema,),
        )
    return (
        f"IF SCHEMA_ID(?) IS NULL EXEC('CREATE SCHEMA {schema_sql}')",
        (name.schema,),
    )


def mssql_sp_rename_statement(name: MSSQLObjectName, new_table: str) -> str:
    """Render a database-scoped ``sp_rename`` statement for a normalized table identity."""

    old_literal = _quote_sql_literal(name.sp_rename_object_name)
    new_literal = _quote_sql_literal(str(new_table).strip())
    return (
        f"EXEC {name.execution_scope_prefix}sys.sp_executesql "
        f"N'EXEC sp_rename @objname, @newname', "
        f"N'@objname nvarchar(776), @newname sysname', "
        f"N'{old_literal}', N'{new_literal}'"
    )


def _quote_sql_literal(value: str) -> str:
    return str(value).replace("'", "''")


def _normalize_schema(*, schema: str, database: str | None, strict: bool) -> tuple[str | None, str]:
    raw_schema = str(schema or "").strip()
    raw_database = str(database or "").strip() or None
    if not raw_schema:
        raise ValueError("MSSQL schema must not be empty")
    schema_parts = tuple(part.strip() for part in raw_schema.split("."))
    if len(schema_parts) == 1:
        return raw_database, raw_schema
    if len(schema_parts) == 2 and raw_database == schema_parts[0]:
        return raw_database, schema_parts[1]
    if len(schema_parts) == 2 and raw_database is None:
        return schema_parts[0], schema_parts[1]
    if strict or len(schema_parts) > 2:
        raise ValueError(f"Unsafe MSSQL schema identifier: {schema}")
    return raw_database, raw_schema


def _validate_part(value: str, part: str, *, strict: bool) -> None:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"MSSQL {part} must not be empty")
    if "." in text:
        raise ValueError(f"MSSQL {part} must not contain dots")
    if strict and not safe_mssql_identifier(text):
        raise ValueError(f"Unsafe MSSQL {part} identifier: {value}")


__all__ = [
    "MSSQLObjectName",
    "mssql_dataset_is_safe",
    "mssql_ensure_schema_statement",
    "mssql_sp_rename_statement",
    "quote_mssql_identifier",
    "safe_mssql_identifier",
]
