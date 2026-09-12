"""Wide PostgreSQL fixtures for postgres→MSSQL pair-profile live certification.

``bytea`` lands as hex on character ``mssql-delimited`` BCP and is decoded with
``CONVERT(varbinary, …, 2)`` into the target binary contract.
"""

from __future__ import annotations

from dataclasses import dataclass

from dpone.adapters.nonproduction_postgres_fixture_recipe import postgres_fixture_columns
from dpone.adapters.nonproduction_postgres_fixture_rows import postgres_seed_rows
from dpone.type_system.source_sink.postgres_mssql import PostgresMssqlTypeMapper

WIDE_TABLE = "postgres_to_mssql_wide"
SOURCE_SCHEMA = "dpone_src"
WIDE_COLUMN_TARGET = 128

_ENUM_TYPE = "dpone_wide_enum"

_SPECS: list[tuple[str, str, str, str]] = [
    (column.name, column.pg_ddl, column.pg_type, column.seed_sql) for column in postgres_fixture_columns()
]


@dataclass(frozen=True, slots=True)
class WideColumn:
    name: str
    pg_ddl: str
    pg_type: str
    seed_sql: str
    expected_mssql_type: str


def wide_columns() -> list[WideColumn]:
    mapper = PostgresMssqlTypeMapper()
    columns: list[WideColumn] = []
    for name, ddl, pg_type, seed in _SPECS:
        columns.append(
            WideColumn(
                name=name,
                pg_ddl=f'"{name}" {ddl}',
                pg_type=pg_type,
                seed_sql=seed,
                expected_mssql_type=mapper.resolve(pg_type).target_type,
            )
        )
    return columns


def create_wide_postgres_table(postgres, *, table: str = WIDE_TABLE, schema: str = SOURCE_SCHEMA) -> list[WideColumn]:
    columns = wide_columns()
    postgres.execute_query(
        f"""
        DO $dpone$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_type AS t
                INNER JOIN pg_namespace AS n ON n.oid = t.typnamespace
                WHERE n.nspname = '{schema}' AND t.typname = '{_ENUM_TYPE}'
            ) THEN
                EXECUTE 'CREATE TYPE "{schema}"."{_ENUM_TYPE}" AS ENUM (''alpha'', ''beta'')';
            END IF;
        END
        $dpone$
        """
    )
    enum_ddl = f'"{schema}"."{_ENUM_TYPE}"'
    ddl_cols = ",\n            ".join(col.pg_ddl.replace('"__DPONE_ENUM_TYPE__"', enum_ddl) for col in columns)
    postgres.execute_query(f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE')
    postgres.execute_query(
        f"""
        CREATE TABLE "{schema}"."{table}" (
            {ddl_cols},
            PRIMARY KEY ("id")
        )
        """
    )
    names = ", ".join(f'"{col.name}"' for col in columns)
    enum_cast = f'"{schema}"."{_ENUM_TYPE}"'
    initial_rows = postgres_seed_rows("initial")
    values = ", ".join(value.replace("__DPONE_ENUM_CAST__", enum_cast) for value in initial_rows[0])
    postgres.execute_query(f'INSERT INTO "{schema}"."{table}" ({names}) VALUES ({values})')
    values2 = [value.replace("__DPONE_ENUM_CAST__", enum_cast) for value in initial_rows[1]]
    postgres.execute_query(f'INSERT INTO "{schema}"."{table}" ({names}) VALUES ({", ".join(values2)})')
    return columns


def insert_wide_watermark_row(
    postgres, *, table: str = WIDE_TABLE, schema: str = SOURCE_SCHEMA, row_id: int = 3
) -> None:
    columns = wide_columns()
    names = ", ".join(f'"{col.name}"' for col in columns)
    values = list(postgres_seed_rows("watermark_3")[0])
    enum_cast = f'"{schema}"."{_ENUM_TYPE}"'
    for index, col in enumerate(columns):
        values[index] = str(row_id) if col.name == "id" else values[index].replace("__DPONE_ENUM_CAST__", enum_cast)
    postgres.execute_query(f'INSERT INTO "{schema}"."{table}" ({names}) VALUES ({", ".join(values)})')


__all__ = [
    "SOURCE_SCHEMA",
    "WIDE_TABLE",
    "WIDE_COLUMN_TARGET",
    "WideColumn",
    "create_wide_postgres_table",
    "insert_wide_watermark_row",
    "wide_columns",
]
