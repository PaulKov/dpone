from __future__ import annotations

import os

from tests.integration.mssql.mssql_clickhouse_live_support import IntegrationLogger

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.clickhouse import ClickHouseSink
from dpone.runtime.sources.strategies.mssql.mssql_strategies import MSSQLFullExtractStrategy
from dpone.runtime.support.type_mapping.mssql_clickhouse import MssqlClickHouseTypeMapper, MssqlClickHouseTypePolicy

TYPE_POLICY = MssqlClickHouseTypePolicy(binary_encoding="hex", time_encoding="seconds_since_midnight")
TYPE_MAPPER = MssqlClickHouseTypeMapper(TYPE_POLICY)
DIRECTIONS = (
    ("nullable_to_not_nullable", True, False),
    ("nullable_to_nullable", True, True),
    ("not_nullable_to_nullable", False, True),
    ("not_nullable_to_not_nullable", False, False),
)

# SQL Server rowversion/timestamp is generated, not user-insertable, and cannot
# exercise nullable source directions as an ordinary column. Unit contracts cover it.
LIVE_TYPES: tuple[tuple[str, str, str], ...] = (
    ("tinyint", "tinyint", "CAST(7 AS tinyint)"),
    ("smallint", "smallint", "CAST(123 AS smallint)"),
    ("int", "int", "CAST(12345 AS int)"),
    ("bigint", "bigint", "CAST(123456789012 AS bigint)"),
    ("decimal", "decimal(18,4)", "CAST(123.4567 AS decimal(18,4))"),
    ("numeric", "numeric(38,9)", "CAST(123.456789123 AS numeric(38,9))"),
    ("money", "money", "CAST(123.45 AS money)"),
    ("smallmoney", "smallmoney", "CAST(12.34 AS smallmoney)"),
    ("float", "float", "CAST(123.456 AS float)"),
    ("real", "real", "CAST(12.5 AS real)"),
    ("bit", "bit", "CAST(1 AS bit)"),
    ("uuid", "uniqueidentifier", "CAST('11111111-1111-1111-1111-111111111111' AS uniqueidentifier)"),
    ("date", "date", "CAST('2026-06-18' AS date)"),
    ("datetime", "datetime", "CAST('2026-06-18T12:34:56.123' AS datetime)"),
    ("datetime2", "datetime2(7)", "CAST('2026-06-18T12:34:56.1234567' AS datetime2(7))"),
    ("smalldatetime", "smalldatetime", "CAST('2026-06-18T12:35:00' AS smalldatetime)"),
    (
        "datetimeoffset",
        "datetimeoffset(7)",
        "CAST('2026-06-18T12:34:56.1234567+03:00' AS datetimeoffset(7))",
    ),
    ("time", "time(7)", "CAST('12:34:56.1234567' AS time(7))"),
    ("nvarchar", "nvarchar(510)", "N'hello-unicode'"),
    ("varchar", "varchar(max)", "CAST('hello-varchar' AS varchar(max))"),
    ("varbinary", "varbinary(max)", "CONVERT(varbinary(max), 0x00010203)"),
)


def create_source_table(mssql, *, schema: str, table: str, columns_sql: list[str]) -> None:
    mssql.execute_query(
        f"""
        CREATE TABLE [{schema}].[{table}] (
            {", ".join(columns_sql)}
        )
        """
    )


def insert_source_rows(mssql, *, schema: str, table: str, rows_sql: list[list[str]]) -> None:
    for insert_row in rows_sql:
        mssql.execute_query(
            f"""
            INSERT INTO [{schema}].[{table}]
            VALUES ({", ".join(insert_row)})
            """
        )


def load_and_assert(
    *,
    mssql,
    clickhouse_connector,
    clickhouse_settings,
    schema: str,
    source_table: str,
    target_table: str,
    expected_target_types: dict[str, str],
    physical_target_types: dict[str, str] | None,
) -> None:
    cfg = load_config(
        schema=schema,
        source_table=source_table,
        target_schema=clickhouse_settings.database,
        target_table=target_table,
        physical_target_types=physical_target_types,
        clickhouse_settings=clickhouse_settings,
    )
    extract = MSSQLFullExtractStrategy(mssql, IntegrationLogger(), sink_connector=clickhouse_connector).extract(
        cfg,
        None,
    )
    result = ClickHouseSink(clickhouse_connector).load(
        cfg,
        LoadPayload(artifact=extract.artifact, schema=extract.schema),
    )

    assert result.inserted_rows == 2
    actual_types = clickhouse_column_types(clickhouse_connector, clickhouse_settings.database, target_table)
    assert canonical_clickhouse_types(actual_types) == canonical_clickhouse_types(expected_target_types)


def physical_source_columns_sql(slug: str, source_type: str) -> list[str]:
    columns: list[str] = []
    for direction, source_nullable, _target_nullable in DIRECTIONS:
        nullability = "NULL" if source_nullable else "NOT NULL"
        columns.append(f"[{slug}_{direction}] {source_type} {nullability}")
    return columns


def physical_insert_rows_sql(value_sql: str) -> list[list[str]]:
    rows: list[list[str]] = []
    for row_index in range(2):
        values: list[str] = []
        for direction, _source_nullable, _target_nullable in DIRECTIONS:
            values.append("NULL" if row_index == 1 and direction == "nullable_to_nullable" else value_sql)
        rows.append(values)
    return rows


def auto_source_columns_sql(slug: str, source_type: str) -> list[str]:
    return [f"[{slug}_nullable] {source_type} NULL", f"[{slug}_not_nullable] {source_type} NOT NULL"]


def auto_insert_rows_sql(value_sql: str) -> list[list[str]]:
    rows: list[list[str]] = []
    for row_index in range(2):
        rows.append(["NULL" if row_index == 1 else value_sql, value_sql])
    return rows


def expected_physical_target_types(slug: str, source_type: str) -> dict[str, str]:
    expected: dict[str, str] = {}
    base_type = TYPE_MAPPER.resolve_column(slug, source_type).clickhouse_type
    for direction, _source_nullable, target_nullable in DIRECTIONS:
        expected[f"{slug}_{direction}"] = with_clickhouse_nullability(base_type, nullable=target_nullable)
    return expected


def expected_auto_target_types(slug: str, source_type: str) -> dict[str, str]:
    base_type = TYPE_MAPPER.resolve_column(slug, source_type).clickhouse_type
    return {
        f"{slug}_nullable": with_clickhouse_nullability(base_type, nullable=True),
        f"{slug}_not_nullable": with_clickhouse_nullability(base_type, nullable=False),
    }


def load_config(
    *,
    schema: str,
    source_table: str,
    target_schema: str,
    target_table: str,
    physical_target_types: dict[str, str] | None,
    clickhouse_settings,
    nullability: dict | None = None,
) -> LoadConfig:
    options = {
        "extract_mode": "bcp_queryout",
        "mssql_export_mode": "bcp",
        "type_fidelity": {"binary_encoding": "hex", "time_encoding": "seconds_since_midnight"},
        "bulk": {"mode": "bcp", "bcp": {"batch_size": 10, "packet_size": 65535, "timeout_seconds": 120}},
        "clickhouse_bulk": {
            "mode": "http",
            "http": {
                "host": clickhouse_settings.host,
                "port": int(os.getenv("DPONE_IT_CH_HTTP_PORT", "58123")),
                "database": clickhouse_settings.database,
                "user": clickhouse_settings.user,
                "password": clickhouse_settings.password,
            },
        },
    }
    if physical_target_types is not None:
        options["physical_design"] = {
            "columns": {
                column: {"target_type": {"clickhouse": target_type}}
                for column, target_type in physical_target_types.items()
            }
        }
    if nullability is not None:
        physical_design = options.setdefault("physical_design", {})
        storage = physical_design.setdefault("storage", {})
        clickhouse = storage.setdefault("clickhouse", {})
        clickhouse["nullability"] = nullability
    return LoadConfig(
        source_conn_id="mssql-it",
        target_conn_id="clickhouse-it",
        source_schema=schema,
        source_table=source_table,
        target_schema=target_schema,
        target_table=target_table,
        load_strategy=LoadStrategy.FULL_REFRESH,
        batch_size=10,
        options=options,
    )


def clickhouse_column_types(connector, database: str, table: str) -> dict[str, str]:
    return {
        row["name"]: row["type"]
        for row in connector.get_records(
            f"""
            SELECT name, type
            FROM system.columns
            WHERE database = '{database}'
              AND table = '{table}'
            """,
            as_dict=True,
        )
    }


def canonical_clickhouse_types(types: dict[str, str]) -> dict[str, str]:
    return {column: canonical_clickhouse_type(clickhouse_type) for column, clickhouse_type in types.items()}


def canonical_clickhouse_type(clickhouse_type: str) -> str:
    return clickhouse_type.replace(", ", ",")


def assert_one_null(connector, database: str, table: str, column: str) -> None:
    null_count = connector.get_records(
        f"""
        SELECT count()
        FROM `{database}`.`{table}`
        WHERE isNull(`{column}`)
        """
    )[0][0]
    assert null_count == 1


def with_clickhouse_nullability(clickhouse_type: str, *, nullable: bool) -> str:
    if nullable and not clickhouse_type.startswith("Nullable("):
        return f"Nullable({clickhouse_type})"
    if not nullable and clickhouse_type.startswith("Nullable(") and clickhouse_type.endswith(")"):
        return clickhouse_type[len("Nullable(") : -1]
    return clickhouse_type
