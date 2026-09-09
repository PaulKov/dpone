"""Wide MSSQL fixtures for mssql→mssql identity-profile live certification."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.type_system.source_sink.profiles import build_default_profiles

WIDE_TABLE = "mssql_to_mssql_wide"
SOURCE_SCHEMA = "dpone_src"

# Identity types that the shared MSSQLTypeMapper preserves without MySQL/CH
# dialect collisions (tinyint→smallint, bare datetime→DateTime hijack remain
# hermetic-only under mssql_to_mssql_identity_v1).
_SPECS: list[tuple[str, str, str, str]] = [
    ("id", "int NOT NULL", "int", "1"),
    ("business_date", "date NOT NULL", "date", "'2026-07-21'"),
    ("updated_at", "datetime2(3) NOT NULL", "datetime2(3)", "'2026-07-21 10:00:00.000'"),
    ("c_smallint", "smallint NULL", "smallint", "70"),
    ("c_bigint", "bigint NULL", "bigint", "7000000000"),
    ("c_decimal", "decimal(18,4) NULL", "decimal(18,4)", "12.3456"),
    ("c_bool", "bit NULL", "bit", "1"),
    ("c_float", "float NULL", "float", "1.25"),
    ("c_real", "real NULL", "real", "2.5"),
    ("c_date", "date NULL", "date", "'2026-01-15'"),
    ("c_datetime2", "datetime2(3) NULL", "datetime2(3)", "'2026-01-15 12:30:00.000'"),
    ("c_time", "time(0) NULL", "time(0)", "'12:30:00'"),
    ("c_varchar", "nvarchar(255) NULL", "nvarchar(255)", "N'alpha,comma'"),
    ("c_text", "nvarchar(max) NULL", "nvarchar(max)", "N'long text value'"),
    ("c_uuid", "uniqueidentifier NULL", "uniqueidentifier", "'11111111-1111-1111-1111-111111111111'"),
    ("c_varbinary", "varbinary(16) NULL", "varbinary(16)", "0x010203"),
    ("c_name", "nvarchar(64) NOT NULL", "nvarchar(64)", "N'row-one'"),
]


@dataclass(frozen=True, slots=True)
class WideColumn:
    name: str
    mssql_ddl: str
    mssql_type: str
    seed_sql: str
    expected_mssql_type: str


def _identity_resolve(mssql_type: str) -> str:
    profile = build_default_profiles()[("mssql", "mssql")]
    return profile.resolve(mssql_type).target_type


def wide_columns() -> list[WideColumn]:
    columns: list[WideColumn] = []
    for name, ddl, mssql_type, seed in _SPECS:
        columns.append(
            WideColumn(
                name=name,
                mssql_ddl=f"[{name}] {ddl}",
                mssql_type=mssql_type,
                seed_sql=seed,
                expected_mssql_type=_identity_resolve(mssql_type),
            )
        )
    return columns


def create_wide_mssql_table(mssql, *, table: str = WIDE_TABLE, schema: str = SOURCE_SCHEMA) -> list[WideColumn]:
    columns = wide_columns()
    ddl_cols = ",\n            ".join(col.mssql_ddl for col in columns)
    mssql.execute_query(f"DROP TABLE IF EXISTS [{schema}].[{table}]")
    mssql.execute_query(
        f"""
        CREATE TABLE [{schema}].[{table}] (
            {ddl_cols},
            CONSTRAINT [PK_{table}] PRIMARY KEY ([id])
        )
        """
    )
    names = ", ".join(f"[{col.name}]" for col in columns)
    values = ", ".join(col.seed_sql for col in columns)
    mssql.execute_query(f"INSERT INTO [{schema}].[{table}] ({names}) VALUES ({values})")
    values2 = []
    for col in columns:
        if col.name == "id":
            values2.append("2")
        elif col.name == "updated_at":
            values2.append("'2026-07-21 11:00:00.000'")
        elif col.name == "c_name":
            values2.append("N'row-two'")
        elif col.name == "c_varchar":
            values2.append("N'beta'")
        elif col.name == "c_bool":
            values2.append("0")
        else:
            values2.append(col.seed_sql)
    mssql.execute_query(f"INSERT INTO [{schema}].[{table}] ({names}) VALUES ({', '.join(values2)})")
    return columns


def insert_wide_watermark_row(mssql, *, table: str = WIDE_TABLE, schema: str = SOURCE_SCHEMA, row_id: int = 3) -> None:
    columns = wide_columns()
    names = ", ".join(f"[{col.name}]" for col in columns)
    values = []
    for col in columns:
        if col.name == "id":
            values.append(str(row_id))
        elif col.name == "updated_at":
            values.append("'2026-07-21 12:00:00.000'")
        elif col.name == "c_name":
            values.append("N'row-three'")
        else:
            values.append(col.seed_sql)
    mssql.execute_query(f"INSERT INTO [{schema}].[{table}] ({names}) VALUES ({', '.join(values)})")


__all__ = [
    "SOURCE_SCHEMA",
    "WIDE_TABLE",
    "WideColumn",
    "create_wide_mssql_table",
    "insert_wide_watermark_row",
    "wide_columns",
]
