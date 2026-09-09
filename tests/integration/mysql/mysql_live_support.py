"""Shared Docker MySQL + vendor credential gates for route live IT."""

from __future__ import annotations

import json
import os
import time
from collections.abc import Callable
from pathlib import Path


def mysql_enabled() -> bool:
    return bool(
        os.environ.get("DPONE_RUN_INTEGRATION") == "1"
        or os.environ.get("DPONE_IT_MYSQL_HOST")
        or os.environ.get("DPONE_IT_MYSQL_PORT_FORWARD")
    )


def bigquery_enabled() -> bool:
    key_file = os.environ.get("BIGQUERY_DWH_SERVICE_ACCOUNT_KEY_FILE", "").strip()
    project = os.environ.get("BIGQUERY_DWH_PROJECT_ID", "").strip()
    return bool(key_file and Path(key_file).is_file() and project)


def mysql_bigquery_enabled() -> bool:
    return mysql_enabled() and bigquery_enabled()


def postgres_enabled() -> bool:
    return mysql_enabled()  # Postgres IT shares Docker compose gate with MySQL


def mysql_postgres_enabled() -> bool:
    return mysql_enabled() and postgres_enabled()


def mssql_enabled() -> bool:
    return mysql_enabled()  # MSSQL IT shares Docker compose gate with MySQL


def mysql_mssql_enabled() -> bool:
    return mysql_enabled() and mssql_enabled()


def clickhouse_enabled() -> bool:
    return mysql_enabled()  # ClickHouse IT shares Docker compose gate with MySQL


def mysql_clickhouse_enabled() -> bool:
    return mysql_enabled() and clickhouse_enabled()


def kafka_enabled() -> bool:
    return bool(
        os.environ.get("DPONE_RUN_INTEGRATION") == "1"
        or os.environ.get("DPONE_IT_KAFKA_BOOTSTRAP")
        or os.environ.get("DPONE_KAFKA_BOOTSTRAP_SERVERS")
        or os.environ.get("DPONE_IT_KAFKA_PORT_FORWARD")
    )


def mysql_kafka_enabled() -> bool:
    return mysql_enabled() and kafka_enabled()


def mysql_connector():
    from dpone.runtime.connectors.mysql import MySQLConnector

    return MySQLConnector(
        host=os.environ.get("DPONE_IT_MYSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("DPONE_IT_MYSQL_PORT_FORWARD", "53306")),
        database=os.environ.get("DPONE_IT_MYSQL_DATABASE", "dpone_it"),
        user=os.environ.get("DPONE_IT_MYSQL_USER", "dpone"),
        password=os.environ.get("DPONE_IT_MYSQL_PASSWORD", "dpone"),
    )


def bigquery_connector():
    from google.oauth2 import service_account

    from dpone.runtime.connectors.bigquery import BigQueryConnector

    key_file = os.environ["BIGQUERY_DWH_SERVICE_ACCOUNT_KEY_FILE"]
    project_id = os.environ["BIGQUERY_DWH_PROJECT_ID"]
    payload = json.loads(Path(key_file).read_text(encoding="utf-8"))
    assert payload.get("type") == "service_account"
    assert payload.get("project_id"), "service account JSON missing project_id"
    credentials = service_account.Credentials.from_service_account_file(key_file)
    return BigQueryConnector(project_id=project_id, credentials=credentials)


def bq_dataset() -> str:
    return os.environ.get("DPONE_IT_BQ_DATASET", "dpone_it_mysql")


class NoopLogger:
    def log_etl_progress(self, event: str, payload: dict[str, object]) -> None:
        del event, payload

    def log_etl_error(self, message: str, payload: dict[str, object] | None = None) -> None:
        del message, payload

    def log_data_sample(self, kind: str, rows: list[object], max_rows: int) -> None:
        del kind, rows, max_rows

    def info(self, message: str) -> None:
        del message

    def warning(self, message: str) -> None:
        del message


def skip_if_billing_blocked(exc: BaseException) -> None:
    import pytest

    text = str(exc)
    if "billingNotEnabled" in text or "Billing has not been enabled" in text:
        pytest.skip(
            "BigQuery project billing is disabled (DML/load strategies require billing). "
            "Enable billing for the project, then re-run this vendor-live IT."
        )
    raise exc


def wait_until_ready(label: str, action: Callable[[], object], *, timeout_seconds: int = 120) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            action()
            return
        except Exception as exc:  # noqa: BLE001 - readiness loop
            last_error = exc
            time.sleep(2)
    raise RuntimeError(f"{label} was not ready within {timeout_seconds}s") from last_error


def ensure_bq_dataset(bq, *, dataset: str | None = None) -> str:
    from google.cloud import bigquery

    name = dataset or bq_dataset()
    client = bq.connection
    dataset_ref = bigquery.Dataset(f"{bq.project_id}.{name}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)
    return name


def drop_bq_table(bq, table: str, *, dataset: str | None = None) -> None:
    name = dataset or bq_dataset()
    client = bq.connection
    client.delete_table(f"{bq.project_id}.{name}.{table}", not_found_ok=True)
    client.delete_table(f"{bq.project_id}.{name}.{table}__tmp", not_found_ok=True)


def postgres_connector():
    from dpone.runtime.connectors.postgres import PostgresConnector

    return PostgresConnector(
        host=os.environ.get("DPONE_IT_POSTGRES_HOST", os.environ.get("DPONE_IT_PG_HOST", "127.0.0.1")),
        port=int(os.environ.get("DPONE_IT_POSTGRES_PORT", os.environ.get("DPONE_IT_PG_PORT_FORWARD", "55432"))),
        database=os.environ.get("DPONE_IT_POSTGRES_DATABASE", os.environ.get("DPONE_IT_PG_DATABASE", "dpone_it")),
        user=os.environ.get("DPONE_IT_POSTGRES_USER", os.environ.get("DPONE_IT_PG_USER", "dpone")),
        password=os.environ.get("DPONE_IT_POSTGRES_PASSWORD", os.environ.get("DPONE_IT_PG_PASSWORD", "dpone")),
    )


def ensure_postgres_schemas(postgres, *, target_schema: str = "dpone_it", staging_schema: str = "staging") -> None:
    postgres.execute_query(f'CREATE SCHEMA IF NOT EXISTS "{target_schema}"')
    postgres.execute_query(f'CREATE SCHEMA IF NOT EXISTS "{staging_schema}"')


def drop_postgres_table(postgres, table: str, *, schema: str = "dpone_it") -> None:
    postgres.execute_query(f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE')
    postgres.execute_query(f'DROP TABLE IF EXISTS "staging"."{table}" CASCADE')
    postgres.execute_query(f'DROP TABLE IF EXISTS "staging"."{table}_stg" CASCADE')


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


def clickhouse_connector(*, database: str | None = None):
    from dpone.runtime.connectors.clickhouse import ClickHouseConnector

    return ClickHouseConnector(
        host=os.environ.get("DPONE_IT_CH_HOST", "127.0.0.1"),
        port=int(os.environ.get("DPONE_IT_CH_PORT_FORWARD", os.environ.get("DPONE_IT_CH_PORT", "59000"))),
        database=database or os.environ.get("DPONE_IT_CH_DATABASE", "dpone_it"),
        user=os.environ.get("DPONE_IT_CH_USER", "default"),
        password=os.environ.get("DPONE_IT_CH_PASSWORD", "dpone"),
    )


def ensure_clickhouse_database(*, database: str | None = None) -> str:
    name = database or os.environ.get("DPONE_IT_CH_DATABASE", "dpone_it")
    bootstrap = clickhouse_connector(database="default")
    wait_until_ready("clickhouse", lambda: bootstrap.get_records("SELECT 1"))
    bootstrap.execute_query(f"CREATE DATABASE IF NOT EXISTS `{name}`")
    return name


def kafka_connector(*, client_id: str = "dpone-mysql-kafka-it"):
    from dpone.runtime.connectors.kafka import KafkaConnector

    bootstrap = (
        os.environ.get("DPONE_IT_KAFKA_BOOTSTRAP")
        or os.environ.get("DPONE_KAFKA_BOOTSTRAP_SERVERS")
        or f"127.0.0.1:{os.environ.get('DPONE_IT_KAFKA_PORT_FORWARD', '59092')}"
    )
    return KafkaConnector(bootstrap_servers=bootstrap, client_id=client_id)


def drop_clickhouse_table(clickhouse, table: str, *, database: str | None = None) -> None:
    db = database or clickhouse.database
    clickhouse.execute_query(f"DROP TABLE IF EXISTS `{db}`.`{table}`")
    leftover = clickhouse.get_records(
        f"""
        SELECT name
        FROM system.tables
        WHERE database = '{db}' AND startsWith(name, '{table}__dpone_')
        """,
        as_dict=True,
    )
    for row in leftover:
        clickhouse.execute_query(f"DROP TABLE IF EXISTS `{db}`.`{row['name']}`")


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
    "kafka_connector",
    "kafka_enabled",
    "mssql_connector",
    "mssql_enabled",
    "mysql_bigquery_enabled",
    "mysql_clickhouse_enabled",
    "mysql_connector",
    "mysql_enabled",
    "mysql_kafka_enabled",
    "mysql_mssql_enabled",
    "mysql_postgres_enabled",
    "postgres_connector",
    "postgres_enabled",
    "skip_if_billing_blocked",
    "wait_until_ready",
]
