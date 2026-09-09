"""Side-effect-free DDL rendering for schema-evolution plans."""

from __future__ import annotations


def add_column_sql(
    dialect: str,
    table: str,
    column: str,
    dtype: str,
    *,
    nullable: bool,
    collation: str | None = None,
) -> str:
    if dialect == "mssql":
        return (
            f"ALTER TABLE {table} ADD {quote_identifier(dialect, column)} {render_type(dialect, dtype)}"
            f"{_mssql_collation(collation)} "
            f"{'NULL' if nullable else 'NOT NULL'}"
        )
    if dialect in {"postgres", "clickhouse", "bigquery"}:
        return f"ALTER TABLE {table} ADD COLUMN {quote_identifier(dialect, column)} {render_type(dialect, dtype)}"
    raise ValueError(f"Unsupported schema evolution dialect: {dialect}")


def alter_column_sql(
    dialect: str,
    table: str,
    column: str,
    dtype: str,
    *,
    nullable: bool,
    collation: str | None = None,
) -> str:
    if dialect == "mssql":
        return (
            f"ALTER TABLE {table} ALTER COLUMN {quote_identifier(dialect, column)} {render_type(dialect, dtype)}"
            f"{_mssql_collation(collation)} "
            f"{'NULL' if nullable else 'NOT NULL'}"
        )
    if dialect == "postgres":
        return (
            f"ALTER TABLE {table} ALTER COLUMN {quote_identifier(dialect, column)} TYPE {render_type(dialect, dtype)}"
        )
    if dialect == "clickhouse":
        return f"ALTER TABLE {table} MODIFY COLUMN {quote_identifier(dialect, column)} {render_type(dialect, dtype)}"
    if dialect == "bigquery":
        return (
            f"ALTER TABLE {table} ALTER COLUMN {quote_identifier(dialect, column)} "
            f"SET DATA TYPE {render_type(dialect, dtype)}"
        )
    raise ValueError(f"Unsupported schema evolution dialect: {dialect}")


def normalize_dialect(dialect: str) -> str:
    normalized = dialect.lower().replace("-", "_")
    normalized = {
        "sqlserver": "mssql",
        "sql_server": "mssql",
        "postgresql": "postgres",
        "pg": "postgres",
        "ch": "clickhouse",
        "bq": "bigquery",
    }.get(normalized, normalized)
    if normalized not in {"mssql", "postgres", "clickhouse", "bigquery"}:
        raise ValueError(f"Unsupported schema evolution dialect: {dialect}")
    return normalized


def quote_qualified_table(dialect: str, name: str) -> str:
    if name.startswith("`") and name.endswith("`"):
        return name
    parts = [part.strip('"[]`') for part in name.split(".")]
    if dialect == "bigquery":
        return f"`{'.'.join(parts)}`"
    return ".".join(quote_identifier(dialect, part) for part in parts)


def quote_identifier(dialect: str, name: str) -> str:
    if dialect == "mssql":
        return "[" + name.replace("]", "]]") + "]"
    if dialect == "postgres":
        return '"' + name.replace('"', '""') + '"'
    if dialect in {"clickhouse", "bigquery"}:
        return "`" + name.replace("`", "``") + "`"
    raise ValueError(f"Unsupported schema evolution dialect: {dialect}")


def render_type(dialect: str, dtype: str) -> str:
    if dialect == "mssql":
        try:
            from dpone.runtime.support.mssql_types import MSSQLTypeMapper

            return MSSQLTypeMapper.to_mssql(dtype)
        except Exception:
            return dtype
    if dialect == "clickhouse":
        return _to_clickhouse_type(dtype)
    if dialect == "bigquery":
        return _to_bigquery_type(dtype)
    return dtype


def _mssql_collation(value: str | None) -> str:
    if value is None:
        return ""
    normalized = str(value).strip()
    if not normalized or not all(character.isalnum() or character == "_" for character in normalized):
        raise ValueError("MSSQL schema-evolution collation is invalid")
    return f" COLLATE {normalized}"


def _to_clickhouse_type(dtype: str) -> str:
    normalized = str(dtype).lower()
    if "bigint" in normalized or normalized in {"int8", "int64"}:
        return "Int64"
    if "smallint" in normalized:
        return "Int16"
    if "tinyint" in normalized:
        return "Int8"
    if "int" in normalized:
        return "Int32"
    if "decimal" in normalized or "numeric" in normalized:
        return "Decimal(38, 10)"
    if "float" in normalized or "double" in normalized or "real" in normalized:
        return "Float64"
    if normalized == "date":
        return "Date"
    if "time" in normalized or "date" in normalized:
        return "DateTime64(6)"
    if "bool" in normalized or normalized == "bit":
        return "UInt8"
    return "String"


def _to_bigquery_type(dtype: str) -> str:
    normalized = str(dtype).lower()
    if "bigint" in normalized or normalized in {"int8", "int64"} or "int" in normalized:
        return "INT64"
    if "decimal" in normalized or "numeric" in normalized:
        return "BIGNUMERIC"
    if "float" in normalized or "double" in normalized or "real" in normalized:
        return "FLOAT64"
    if normalized == "date":
        return "DATE"
    if "time" in normalized or "date" in normalized:
        return "TIMESTAMP"
    if "bool" in normalized or normalized == "bit":
        return "BOOL"
    if normalized in {"bytes", "bytea", "binary", "varbinary"}:
        return "BYTES"
    if normalized in {"json", "jsonb"}:
        return "JSON"
    return "STRING"


__all__ = ["add_column_sql", "alter_column_sql", "normalize_dialect", "quote_qualified_table"]
