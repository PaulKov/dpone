from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class MssqlClickHouseWideColumn:
    name: str
    mssql_type: str
    clickhouse_type: str
    insert_expression: str
    export_expression: str
    type_hint: str


@dataclass(frozen=True, slots=True)
class PostgresMssqlWideColumn:
    name: str
    postgres_expression: str
    mssql_type: str
    export_expression: str
    type_hint: str


def build_mssql_clickhouse_columns(column_count: int) -> list[MssqlClickHouseWideColumn]:
    base = [
        MssqlClickHouseWideColumn("order_id", "int NOT NULL", "Int32", "CAST(gs AS int)", "src.[order_id]", "int"),
        MssqlClickHouseWideColumn(
            "customer_id", "bigint", "Int64", "CAST(gs * 1000 AS bigint)", "src.[customer_id]", "bigint"
        ),
        MssqlClickHouseWideColumn(
            "amount",
            "decimal(18,4)",
            "Decimal(18,4)",
            "CAST((gs % 100000) / 10000.0 AS decimal(18,4))",
            "src.[amount]",
            "decimal(18,4)",
        ),
        MssqlClickHouseWideColumn(
            "amount_high",
            "numeric(38,9)",
            "Decimal(38,9)",
            "CAST((gs % 1000000) / 1000000000.0 AS numeric(38,9))",
            "src.[amount_high]",
            "numeric(38,9)",
        ),
        MssqlClickHouseWideColumn("is_active", "bit", "Bool", "CAST(gs % 2 AS bit)", "src.[is_active]", "bit"),
        MssqlClickHouseWideColumn(
            "trace_id",
            "uniqueidentifier",
            "UUID",
            "CONVERT(uniqueidentifier, HASHBYTES('MD5', CONVERT(varchar(32), gs)))",
            "src.[trace_id]",
            "uniqueidentifier",
        ),
        MssqlClickHouseWideColumn(
            "created_at",
            "datetime2(3)",
            "DateTime64(3)",
            "DATEADD(millisecond, gs % 1000000, CONVERT(datetime2(3), '2026-01-01T00:00:00'))",
            "CONVERT(varchar(23), src.[created_at], 121) AS [created_at]",
            "datetime2(3)",
        ),
        MssqlClickHouseWideColumn(
            "business_date",
            "date",
            "Date",
            "DATEADD(day, gs % 365, CONVERT(date, '2026-01-01'))",
            "CONVERT(varchar(10), src.[business_date], 23) AS [business_date]",
            "date",
        ),
        MssqlClickHouseWideColumn(
            "business_time",
            "time(0)",
            "UInt32",
            "DATEADD(second, gs % 86400, CONVERT(time(0), '00:00:00'))",
            "DATEDIFF(second, CONVERT(time(0), '00:00:00'), src.[business_time]) AS [business_time]",
            "time(7)",
        ),
        MssqlClickHouseWideColumn(
            "payload_hex",
            "varbinary(16)",
            "String",
            "HASHBYTES('MD5', CONVERT(varchar(32), gs))",
            "CONVERT(varchar(32), src.[payload_hex], 2) AS [payload_hex]",
            "varbinary(max)",
        ),
        MssqlClickHouseWideColumn(
            "description",
            "nvarchar(200)",
            "String",
            "N'description-' + CONVERT(nvarchar(40), gs)",
            "src.[description]",
            "string",
        ),
        MssqlClickHouseWideColumn(
            "float_value",
            "float",
            "Float64",
            "CAST(gs % 100000 AS float)",
            "src.[float_value]",
            "float",
        ),
    ]
    return _fill_mssql_clickhouse(base, column_count)


def build_postgres_mssql_columns(column_count: int) -> list[PostgresMssqlWideColumn]:
    base = [
        PostgresMssqlWideColumn("order_id", "gs::integer", "int NOT NULL", "order_id", "int"),
        PostgresMssqlWideColumn(
            "is_active", "(gs % 2 = 0)", "bit NULL", "CASE WHEN is_active THEN 1 ELSE 0 END", "bit"
        ),
        PostgresMssqlWideColumn("int_value", "(gs * 13)::integer", "int NULL", "int_value", "int"),
        PostgresMssqlWideColumn("big_value", "(gs * 1000)::bigint", "bigint NULL", "big_value", "bigint"),
        PostgresMssqlWideColumn(
            "amount",
            "(gs::numeric / 10000.0)::numeric(18,4)",
            "decimal(18,4) NULL",
            "amount",
            "decimal(18,4)",
        ),
        PostgresMssqlWideColumn(
            "created_at",
            "timestamp '2026-01-01 00:00:00' + gs * interval '1 second'",
            "datetime2(6) NULL",
            "to_char(created_at, 'YYYY-MM-DD HH24:MI:SS.US')",
            "datetime2(6)",
        ),
        PostgresMssqlWideColumn(
            "business_date",
            "date '2026-01-01' + (gs % 365)::int",
            "date NULL",
            "business_date",
            "date",
        ),
        PostgresMssqlWideColumn(
            "business_time",
            "time '00:00:00' + (gs % 86400) * interval '1 second'",
            "time(6) NULL",
            "business_time",
            "time(6)",
        ),
        PostgresMssqlWideColumn(
            "trace_id",
            "('00000000-0000-0000-0000-' || lpad((gs % 999999999999)::text, 12, '0'))::uuid",
            "uniqueidentifier NULL",
            "trace_id::text",
            "uniqueidentifier",
        ),
        PostgresMssqlWideColumn(
            "payload_hex",
            "decode(md5(gs::text), 'hex')",
            "varbinary(max) NULL",
            "encode(payload_hex, 'hex')",
            "varbinary(max)",
        ),
        PostgresMssqlWideColumn(
            "description",
            "'description-' || gs::text",
            "nvarchar(200) NULL",
            "description",
            "string",
        ),
        PostgresMssqlWideColumn(
            "float_value",
            "(gs % 100000)::double precision",
            "float NULL",
            "float_value",
            "float",
        ),
    ]
    return _fill_postgres_mssql(base, column_count)


def mssql_clickhouse_query_template(columns: Sequence[MssqlClickHouseWideColumn]) -> str:
    select_sql = ", ".join(_mssql_select(column.export_expression, column.name) for column in columns)
    return (
        f"SELECT {select_sql} "
        "FROM {source_table} AS src "
        "WHERE src.{boundary_column} BETWEEN {start} AND {end} "
        "ORDER BY src.{boundary_column}"
    )


def postgres_mssql_query_template(columns: Sequence[PostgresMssqlWideColumn]) -> str:
    select_sql = ", ".join(f'{column.export_expression} AS "{column.name}"' for column in columns)
    return (
        f"SELECT {select_sql} "
        "FROM {source_table} "
        "WHERE {boundary_column} BETWEEN {start} AND {end} "
        "ORDER BY {boundary_column}"
    )


def type_hints(columns: Sequence[Any]) -> dict[str, str]:
    return {str(column.name): str(column.type_hint) for column in columns}


def column_names(columns: Sequence[Any]) -> list[str]:
    return [str(column.name) for column in columns]


def create_mssql_wide_source(
    connector: Any, *, schema: str, table: str, rows: int, columns: Sequence[MssqlClickHouseWideColumn]
) -> None:
    connector.execute_query(f"EXEC('CREATE SCHEMA [{schema}]')")
    ddl = ",\n            ".join(f"[{column.name}] {column.mssql_type}" for column in columns)
    connector.execute_query(
        f"""
        CREATE TABLE [{schema}].[{table}] (
            {ddl},
            PRIMARY KEY ([order_id])
        )
        """
    )
    insert_columns = ", ".join(f"[{column.name}]" for column in columns)
    select_sql = ", ".join(f"{column.insert_expression} AS [{column.name}]" for column in columns)
    connector.execute_query(
        f"""
        WITH digits(n) AS (
            SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS d(n)
        ),
        source_rows AS (
            SELECT TOP ({int(rows)})
                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS gs
            FROM digits AS a
            CROSS JOIN digits AS b
            CROSS JOIN digits AS c
            CROSS JOIN digits AS d
            CROSS JOIN digits AS e
        )
        INSERT INTO [{schema}].[{table}] ({insert_columns})
        SELECT {select_sql}
        FROM source_rows
        ORDER BY gs
        """
    )


def create_clickhouse_wide_target(
    connector: Any, *, database: str, table: str, columns: Sequence[MssqlClickHouseWideColumn]
) -> None:
    ddl = ",\n            ".join(f"`{column.name}` {column.clickhouse_type}" for column in columns)
    connector.execute_query(
        f"""
        CREATE TABLE `{database}`.`{table}` (
            {ddl}
        )
        ENGINE = MergeTree
        ORDER BY order_id
        """
    )


def create_postgres_wide_source(
    connector: Any, *, schema: str, table: str, rows: int, columns: Sequence[PostgresMssqlWideColumn]
) -> None:
    connector.execute_query(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    connector.execute_query(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
    select_sql = ",\n            ".join(f'{column.postgres_expression} AS "{column.name}"' for column in columns)
    connector.execute_query(
        f"""
        CREATE UNLOGGED TABLE "{schema}"."{table}" AS
        SELECT
            {select_sql}
        FROM generate_series(1, {int(rows)}) AS gs
        """
    )
    connector.execute_query(f'ALTER TABLE "{schema}"."{table}" ADD PRIMARY KEY ("order_id")')


def create_mssql_wide_target(
    connector: Any, *, schema: str, table: str, columns: Sequence[PostgresMssqlWideColumn]
) -> None:
    connector.execute_query(f"IF SCHEMA_ID('{schema}') IS NULL EXEC('CREATE SCHEMA [{schema}]')")
    connector.execute_query(f"DROP TABLE IF EXISTS [{schema}].[{table}]")
    ddl = ",\n            ".join(f"[{column.name}] {column.mssql_type}" for column in columns)
    connector.execute_query(
        f"""
        CREATE TABLE [{schema}].[{table}] (
            {ddl}
        )
        """
    )


def add_mssql_clickhouse_evolved_column(
    connector: Any, clickhouse: Any, *, schema: str, table: str, database: str, target_table: str
) -> MssqlClickHouseWideColumn:
    column = MssqlClickHouseWideColumn(
        "evolved_score",
        "decimal(18,4) NULL",
        "Decimal(18,4)",
        "",
        "src.[evolved_score]",
        "decimal(18,4)",
    )
    connector.execute_query(f"ALTER TABLE [{schema}].[{table}] ADD [{column.name}] {column.mssql_type}")
    connector.execute_query(
        f"UPDATE [{schema}].[{table}] SET [{column.name}] = CAST(([order_id] % 10000) / 10000.0 AS decimal(18,4))"
    )
    clickhouse.execute_query(
        f"ALTER TABLE `{database}`.`{target_table}` ADD COLUMN IF NOT EXISTS `{column.name}` {column.clickhouse_type}"
    )
    return column


def add_postgres_mssql_evolved_column(
    postgres: Any, mssql: Any, *, schema: str, source_table: str, target_table: str
) -> PostgresMssqlWideColumn:
    column = PostgresMssqlWideColumn(
        "evolved_score",
        "NULL::numeric(18,4)",
        "decimal(18,4) NULL",
        "evolved_score",
        "decimal(18,4)",
    )
    postgres.execute_query(f'ALTER TABLE "{schema}"."{source_table}" ADD COLUMN "{column.name}" numeric(18,4)')
    postgres.execute_query(
        f'UPDATE "{schema}"."{source_table}" SET "{column.name}" = (("order_id" % 10000)::numeric / 10000.0)::numeric(18,4)'
    )
    mssql.execute_query(f"ALTER TABLE [{schema}].[{target_table}] ADD [{column.name}] {column.mssql_type}")
    return column


def _fill_mssql_clickhouse(
    base: list[MssqlClickHouseWideColumn],
    column_count: int,
) -> list[MssqlClickHouseWideColumn]:
    templates = (
        ("wide_int", "int", "Int32", "CAST((gs + {i}) % 2147483647 AS int)", "src.[{name}]", "int"),
        (
            "wide_decimal",
            "decimal(18,4)",
            "Decimal(18,4)",
            "CAST(((gs + {i}) % 100000) / 10000.0 AS decimal(18,4))",
            "src.[{name}]",
            "decimal(18,4)",
        ),
        ("wide_text", "nvarchar(80)", "String", "N'wide-{i}-' + CONVERT(nvarchar(40), gs)", "src.[{name}]", "string"),
        (
            "wide_date",
            "date",
            "Date",
            "DATEADD(day, (gs + {i}) % 365, CONVERT(date, '2026-01-01'))",
            "CONVERT(varchar(10), src.[{name}], 23) AS [{name}]",
            "date",
        ),
        (
            "wide_time",
            "time(0)",
            "UInt32",
            "DATEADD(second, (gs + {i}) % 86400, CONVERT(time(0), '00:00:00'))",
            "DATEDIFF(second, CONVERT(time(0), '00:00:00'), src.[{name}]) AS [{name}]",
            "time(7)",
        ),
        (
            "wide_binary",
            "varbinary(16)",
            "String",
            "HASHBYTES('MD5', CONVERT(varchar(32), gs + {i}))",
            "CONVERT(varchar(32), src.[{name}], 2) AS [{name}]",
            "varbinary(max)",
        ),
        ("wide_bit", "bit", "Bool", "CAST((gs + {i}) % 2 AS bit)", "src.[{name}]", "bit"),
        (
            "wide_datetime",
            "datetime2(3)",
            "DateTime64(3)",
            "DATEADD(millisecond, (gs + {i}) % 1000000, CONVERT(datetime2(3), '2026-01-01T00:00:00'))",
            "CONVERT(varchar(23), src.[{name}], 121) AS [{name}]",
            "datetime2(3)",
        ),
    )
    columns = list(base[:column_count])
    index = 1
    while len(columns) < column_count:
        prefix, mssql_type, clickhouse_type, insert_expression, export_expression, type_hint = templates[
            (index - 1) % len(templates)
        ]
        name = f"{prefix}_{index:03d}"
        columns.append(
            MssqlClickHouseWideColumn(
                name=name,
                mssql_type=mssql_type,
                clickhouse_type=clickhouse_type,
                insert_expression=insert_expression.format(i=index),
                export_expression=export_expression.format(name=name),
                type_hint=type_hint,
            )
        )
        index += 1
    return columns


def _fill_postgres_mssql(
    base: list[PostgresMssqlWideColumn],
    column_count: int,
) -> list[PostgresMssqlWideColumn]:
    templates = (
        ("wide_int", "(gs + {i})::integer", "int NULL", "wide_int_{i:03d}", "int"),
        (
            "wide_decimal",
            "((gs + {i})::numeric / 10000.0)::numeric(18,4)",
            "decimal(18,4) NULL",
            "wide_decimal_{i:03d}",
            "decimal(18,4)",
        ),
        ("wide_text", "'wide-{i}-' || gs::text", "nvarchar(100) NULL", "wide_text_{i:03d}", "string"),
        ("wide_date", "date '2026-01-01' + ((gs + {i}) % 365)::int", "date NULL", "wide_date_{i:03d}", "date"),
        (
            "wide_time",
            "time '00:00:00' + ((gs + {i}) % 86400) * interval '1 second'",
            "time(6) NULL",
            "wide_time_{i:03d}",
            "time(6)",
        ),
        ("wide_bit", "((gs + {i}) % 2 = 0)", "bit NULL", "CASE WHEN wide_bit_{i:03d} THEN 1 ELSE 0 END", "bit"),
        (
            "wide_datetime",
            "timestamp '2026-01-01 00:00:00' + (gs + {i}) * interval '1 second'",
            "datetime2(6) NULL",
            "to_char(wide_datetime_{i:03d}, 'YYYY-MM-DD HH24:MI:SS.US')",
            "datetime2(6)",
        ),
    )
    columns = list(base[:column_count])
    index = 1
    while len(columns) < column_count:
        prefix, pg_expr, mssql_type, export_expr, type_hint = templates[(index - 1) % len(templates)]
        name = f"{prefix}_{index:03d}"
        columns.append(
            PostgresMssqlWideColumn(
                name=name,
                postgres_expression=pg_expr.format(i=index),
                mssql_type=mssql_type,
                export_expression=export_expr.format(i=index),
                type_hint=type_hint,
            )
        )
        index += 1
    return columns


def _mssql_select(expression: str, name: str) -> str:
    if " AS " in expression.upper():
        return expression
    return f"{expression} AS [{name}]"
