"""Wide PostgreSQL fixtures for postgres→ClickHouse pair-profile live certification."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.type_system.source_sink.postgres_clickhouse import PostgresClickHouseTypeMapper

WIDE_TABLE = "postgres_to_clickhouse_wide"
SOURCE_SCHEMA = "dpone_src"

_SPECS: list[tuple[str, str, str, str]] = [
    ("id", "INTEGER NOT NULL", "integer", "1"),
    ("business_date", "DATE NOT NULL", "date", "DATE '2026-07-21'"),
    (
        "updated_at",
        "TIMESTAMP WITHOUT TIME ZONE NOT NULL",
        "timestamp without time zone",
        "TIMESTAMP '2026-07-21 10:00:00'",
    ),
    ("c_smallint", "SMALLINT NULL", "smallint", "70"),
    ("c_bigint", "BIGINT NULL", "bigint", "7000000000"),
    ("c_decimal", "NUMERIC(18,4) NULL", "numeric(18,4)", "12.3456"),
    ("c_bool", "BOOLEAN NULL", "boolean", "TRUE"),
    ("c_float", "REAL NULL", "real", "1.25"),
    ("c_double", "DOUBLE PRECISION NULL", "double precision", "2.5"),
    ("c_date", "DATE NULL", "date", "DATE '2026-01-15'"),
    (
        "c_datetime",
        "TIMESTAMP WITHOUT TIME ZONE NULL",
        "timestamp without time zone",
        "TIMESTAMP '2026-01-15 12:30:00'",
    ),
    (
        "c_timestamp",
        "TIMESTAMP WITH TIME ZONE NULL",
        "timestamp with time zone",
        "TIMESTAMPTZ '2026-01-15 12:30:00+00'",
    ),
    ("c_time", "TIME WITHOUT TIME ZONE NULL", "time without time zone", "TIME '12:30:00'"),
    ("c_varchar", "VARCHAR(255) NULL", "character varying(255)", "'alpha,comma'"),
    ("c_text", "TEXT NULL", "text", "'long text value'"),
    ("c_json", "JSONB NULL", "jsonb", "'{\"k\": 1}'::jsonb"),
    ("c_uuid", "UUID NULL", "uuid", "'11111111-1111-1111-1111-111111111111'::uuid"),
    ("c_bytea", "BYTEA NULL", "bytea", "'\\x010203'::bytea"),
    ("c_name", "VARCHAR(64) NOT NULL", "character varying(64)", "'row-one'"),
]


@dataclass(frozen=True, slots=True)
class WideColumn:
    name: str
    pg_ddl: str
    pg_type: str
    seed_sql: str
    expected_ch_type: str


def wide_columns() -> list[WideColumn]:
    mapper = PostgresClickHouseTypeMapper()
    columns: list[WideColumn] = []
    for name, ddl, pg_type, seed in _SPECS:
        columns.append(
            WideColumn(
                name=name,
                pg_ddl=f'"{name}" {ddl}',
                pg_type=pg_type,
                seed_sql=seed,
                expected_ch_type=mapper.resolve(pg_type).target_type,
            )
        )
    return columns


def create_wide_postgres_table(postgres, *, table: str = WIDE_TABLE, schema: str = SOURCE_SCHEMA) -> list[WideColumn]:
    columns = wide_columns()
    ddl_cols = ",\n            ".join(col.pg_ddl for col in columns)
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
    values = ", ".join(col.seed_sql for col in columns)
    postgres.execute_query(f'INSERT INTO "{schema}"."{table}" ({names}) VALUES ({values})')
    values2 = []
    for col in columns:
        if col.name == "id":
            values2.append("2")
        elif col.name == "updated_at":
            values2.append("TIMESTAMP '2026-07-21 11:00:00'")
        elif col.name == "c_name":
            values2.append("'row-two'")
        elif col.name == "c_varchar":
            values2.append("'beta'")
        elif col.name == "c_bool":
            values2.append("FALSE")
        else:
            values2.append(col.seed_sql)
    postgres.execute_query(f'INSERT INTO "{schema}"."{table}" ({names}) VALUES ({", ".join(values2)})')
    return columns


def insert_wide_watermark_row(
    postgres, *, table: str = WIDE_TABLE, schema: str = SOURCE_SCHEMA, row_id: int = 3
) -> None:
    columns = wide_columns()
    names = ", ".join(f'"{col.name}"' for col in columns)
    values = []
    for col in columns:
        if col.name == "id":
            values.append(str(row_id))
        elif col.name == "updated_at":
            values.append("TIMESTAMP '2026-07-21 12:00:00'")
        elif col.name == "c_name":
            values.append("'row-three'")
        else:
            values.append(col.seed_sql)
    postgres.execute_query(f'INSERT INTO "{schema}"."{table}" ({names}) VALUES ({", ".join(values)})')


__all__ = [
    "SOURCE_SCHEMA",
    "WIDE_TABLE",
    "WideColumn",
    "create_wide_postgres_table",
    "insert_wide_watermark_row",
    "wide_columns",
]
