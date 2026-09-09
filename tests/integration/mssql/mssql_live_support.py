"""Shared Docker MSSQL + vendor credential gates for route live IT."""

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
    drop_postgres_table,
    ensure_bq_dataset,
    ensure_clickhouse_database,
    ensure_postgres_schemas,
    kafka_connector,
    kafka_enabled,
    postgres_connector,
    postgres_enabled,
    skip_if_billing_blocked,
    wait_until_ready,
)


def mssql_enabled() -> bool:
    return bool(
        os.environ.get("DPONE_RUN_INTEGRATION") == "1"
        or os.environ.get("DPONE_IT_MSSQL_HOST")
        or os.environ.get("DPONE_IT_MSSQL_PORT_FORWARD")
    )


def mssql_clickhouse_enabled() -> bool:
    return mssql_enabled() and clickhouse_enabled()


def mssql_mssql_enabled() -> bool:
    return mssql_enabled()


def mssql_postgres_enabled() -> bool:
    return mssql_enabled() and postgres_enabled()


def mssql_kafka_enabled() -> bool:
    return mssql_enabled() and kafka_enabled()


def mssql_bigquery_enabled() -> bool:
    return mssql_enabled() and bigquery_enabled()


def mssql_connector(*, database: str | None = None):
    from dpone.runtime.connectors.mssql import MSSQLConnector

    return MSSQLConnector(
        host=os.environ.get("DPONE_IT_MSSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("DPONE_IT_MSSQL_PORT", os.environ.get("DPONE_IT_MSSQL_PORT_FORWARD", "51433"))),
        database=database or os.environ.get("DPONE_IT_MSSQL_DATABASE", "dpone_it"),
        user=os.environ.get("DPONE_IT_MSSQL_USER", "sa"),
        password=os.environ.get("DPONE_IT_MSSQL_PASSWORD", "Dp0ne.Strong.Pw.2026!"),
        trust_server_certificate=os.environ.get("DPONE_IT_MSSQL_TRUST_SERVER_CERTIFICATE", "yes"),
        bcp_path=os.environ.get("DPONE_IT_MSSQL_BCP_PATH", "bcp"),
    )


def ensure_mssql_database_and_schemas(
    *,
    database: str | None = None,
    source_schema: str = "dpone_src",
    target_schema: str = "dpone_it",
    staging_schema: str = "staging",
) -> str:
    name = database or os.environ.get("DPONE_IT_MSSQL_DATABASE", "dpone_it")
    master = mssql_connector(database="master")
    wait_until_ready("mssql", lambda: master.get_records("SELECT 1"))
    master.execute_query(f"IF DB_ID(N'{name}') IS NULL CREATE DATABASE [{name}]")
    mssql = mssql_connector(database=name)
    for schema in (source_schema, target_schema, staging_schema):
        mssql.execute_query(f"IF SCHEMA_ID(N'{schema}') IS NULL EXEC('CREATE SCHEMA [{schema}]')")
    return name


def ensure_mssql_source_schema(mssql, *, schema: str = "dpone_src") -> None:
    mssql.execute_query(f"IF SCHEMA_ID(N'{schema}') IS NULL EXEC('CREATE SCHEMA [{schema}]')")


def drop_mssql_table(mssql, table: str, *, schema: str = "dpone_it") -> None:
    mssql.execute_query(f"DROP TABLE IF EXISTS [{schema}].[{table}]")
    mssql.execute_query(f"DROP TABLE IF EXISTS [staging].[{table}]")
    mssql.execute_query(f"DROP TABLE IF EXISTS [staging].[{table}_stg]")


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
    "ensure_mssql_source_schema",
    "ensure_postgres_schemas",
    "kafka_connector",
    "kafka_enabled",
    "mssql_bigquery_enabled",
    "mssql_clickhouse_enabled",
    "mssql_connector",
    "mssql_enabled",
    "mssql_kafka_enabled",
    "mssql_mssql_enabled",
    "mssql_postgres_enabled",
    "postgres_connector",
    "postgres_enabled",
    "skip_if_billing_blocked",
    "wait_until_ready",
]
