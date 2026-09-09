"""Shared Docker Postgres + vendor credential gates for route live IT."""

from __future__ import annotations

import os

from tests.integration.mysql.mysql_live_support import (
    NoopLogger,
    bigquery_connector,
    bigquery_enabled,
    bq_dataset,
    clickhouse_connector,
    clickhouse_enabled,
    drop_bq_table,
    drop_clickhouse_table,
    ensure_bq_dataset,
    ensure_clickhouse_database,
    kafka_connector,
    kafka_enabled,
    skip_if_billing_blocked,
    wait_until_ready,
)


def postgres_enabled() -> bool:
    return bool(
        os.environ.get("DPONE_RUN_INTEGRATION") == "1"
        or os.environ.get("DPONE_IT_POSTGRES_HOST")
        or os.environ.get("DPONE_IT_PG_HOST")
        or os.environ.get("DPONE_IT_PG_PORT_FORWARD")
    )


def postgres_bigquery_enabled() -> bool:
    return postgres_enabled() and bigquery_enabled()


def postgres_connector():
    from dpone.runtime.connectors.postgres import PostgresConnector

    return PostgresConnector(
        host=os.environ.get("DPONE_IT_POSTGRES_HOST", os.environ.get("DPONE_IT_PG_HOST", "127.0.0.1")),
        port=int(os.environ.get("DPONE_IT_POSTGRES_PORT", os.environ.get("DPONE_IT_PG_PORT_FORWARD", "55432"))),
        database=os.environ.get("DPONE_IT_POSTGRES_DATABASE", os.environ.get("DPONE_IT_PG_DATABASE", "dpone_it")),
        user=os.environ.get("DPONE_IT_POSTGRES_USER", os.environ.get("DPONE_IT_PG_USER", "dpone")),
        password=os.environ.get("DPONE_IT_POSTGRES_PASSWORD", os.environ.get("DPONE_IT_PG_PASSWORD", "dpone")),
    )


def ensure_postgres_source_schema(postgres, *, schema: str = "public") -> None:
    postgres.execute_query(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')


def ensure_postgres_schemas(
    postgres,
    *,
    source_schema: str = "dpone_src",
    target_schema: str = "dpone_it",
    staging_schema: str = "staging",
) -> None:
    for schema in (source_schema, target_schema, staging_schema):
        postgres.execute_query(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')


def drop_postgres_table(postgres, table: str, *, schema: str = "dpone_it") -> None:
    postgres.execute_query(f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE')
    postgres.execute_query(f'DROP TABLE IF EXISTS "staging"."{table}" CASCADE')
    postgres.execute_query(f'DROP TABLE IF EXISTS "staging"."{table}_stg" CASCADE')
    postgres.execute_query(f'DROP TABLE IF EXISTS "staging"."{table}__tmp" CASCADE')


def postgres_postgres_enabled() -> bool:
    return postgres_enabled()


def mssql_enabled() -> bool:
    return bool(
        os.environ.get("DPONE_RUN_INTEGRATION") == "1"
        or os.environ.get("DPONE_IT_MSSQL_HOST")
        or os.environ.get("DPONE_IT_MSSQL_PORT_FORWARD")
    )


def postgres_mssql_enabled() -> bool:
    return postgres_enabled() and mssql_enabled()


def mssql_connector(
    *,
    database: str | None = None,
    connect_timeout: int = 10,
    query_timeout: int = 0,
):
    """Build the Docker MSSQL connector with optional bounded control timeouts."""

    from dpone.runtime.connectors.mssql import MSSQLConnector

    return MSSQLConnector(
        host=os.environ.get("DPONE_IT_MSSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("DPONE_IT_MSSQL_PORT", os.environ.get("DPONE_IT_MSSQL_PORT_FORWARD", "51433"))),
        database=database or os.environ.get("DPONE_IT_MSSQL_DATABASE", "dpone_it"),
        user=os.environ.get("DPONE_IT_MSSQL_USER", "sa"),
        password=os.environ.get("DPONE_IT_MSSQL_PASSWORD", "Dp0ne.Strong.Pw.2026!"),
        trust_server_certificate=os.environ.get("DPONE_IT_MSSQL_TRUST_SERVER_CERTIFICATE", "yes"),
        connect_timeout=connect_timeout,
        query_timeout=query_timeout,
        bcp_path=os.environ.get("DPONE_IT_MSSQL_BCP_PATH", "bcp"),
    )


def ensure_mssql_database_and_schemas(
    *,
    database: str | None = None,
    target_schema: str = "dpone_it",
    staging_schema: str = "staging",
) -> str:
    name = database or os.environ.get("DPONE_IT_MSSQL_DATABASE", "dpone_it")
    master = mssql_connector(database="master")
    wait_until_ready("mssql", lambda: master.get_records("SELECT 1"))
    master.execute_query(f"IF DB_ID(N'{name}') IS NULL CREATE DATABASE [{name}]")
    mssql = mssql_connector(database=name)
    mssql.execute_query(f"IF SCHEMA_ID(N'{target_schema}') IS NULL EXEC('CREATE SCHEMA [{target_schema}]')")
    mssql.execute_query(f"IF SCHEMA_ID(N'{staging_schema}') IS NULL EXEC('CREATE SCHEMA [{staging_schema}]')")
    return name


def drop_mssql_table(mssql, table: str, *, schema: str = "dpone_it") -> None:
    mssql.execute_query(f"DROP TABLE IF EXISTS [{schema}].[{table}]")
    mssql.execute_query(f"DROP TABLE IF EXISTS [staging].[{table}]")
    mssql.execute_query(f"DROP TABLE IF EXISTS [staging].[{table}_stg]")


def postgres_clickhouse_enabled() -> bool:
    return postgres_enabled() and clickhouse_enabled()


def postgres_kafka_enabled() -> bool:
    return postgres_enabled() and kafka_enabled()


__all__ = [
    "NoopLogger",
    "bigquery_connector",
    "bigquery_enabled",
    "bq_dataset",
    "clickhouse_connector",
    "clickhouse_enabled",
    "drop_bq_table",
    "drop_clickhouse_table",
    "drop_mssql_table",
    "drop_postgres_table",
    "ensure_bq_dataset",
    "ensure_clickhouse_database",
    "ensure_mssql_database_and_schemas",
    "ensure_postgres_schemas",
    "ensure_postgres_source_schema",
    "kafka_connector",
    "kafka_enabled",
    "mssql_connector",
    "mssql_enabled",
    "postgres_bigquery_enabled",
    "postgres_clickhouse_enabled",
    "postgres_connector",
    "postgres_enabled",
    "postgres_kafka_enabled",
    "postgres_mssql_enabled",
    "postgres_postgres_enabled",
    "skip_if_billing_blocked",
    "wait_until_ready",
]
