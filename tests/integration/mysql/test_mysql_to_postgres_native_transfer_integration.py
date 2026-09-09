"""Docker MySQL → Postgres COPY staging evidence (SKIP when services unavailable)."""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.connectors.mysql import MySQLConnector
from dpone.runtime.connectors.postgres import PostgresConnector
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.postgres import PostgresSink
from dpone.runtime.sources.strategies.mysql.mysql_full import MySQLFullExtractStrategy
from dpone.runtime.sources.strategies.mysql.mysql_incremental import MySQLIncrementalExtractStrategy

pytestmark = [pytest.mark.integration, pytest.mark.integration_mysql, pytest.mark.integration_postgres]


def _enabled() -> bool:
    return bool(
        os.environ.get("DPONE_RUN_INTEGRATION") == "1"
        or os.environ.get("DPONE_IT_MYSQL_HOST")
        or os.environ.get("DPONE_IT_MYSQL_PORT_FORWARD")
    )


def _mysql() -> MySQLConnector:
    return MySQLConnector(
        host=os.environ.get("DPONE_IT_MYSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("DPONE_IT_MYSQL_PORT_FORWARD", "53306")),
        database=os.environ.get("DPONE_IT_MYSQL_DATABASE", "dpone_it"),
        user=os.environ.get("DPONE_IT_MYSQL_USER", "dpone"),
        password=os.environ.get("DPONE_IT_MYSQL_PASSWORD", "dpone"),
    )


def _postgres() -> PostgresConnector:
    return PostgresConnector(
        host=os.environ.get("DPONE_IT_POSTGRES_HOST", os.environ.get("DPONE_IT_PG_HOST", "127.0.0.1")),
        port=int(os.environ.get("DPONE_IT_POSTGRES_PORT", os.environ.get("DPONE_IT_PG_PORT_FORWARD", "55432"))),
        database=os.environ.get("DPONE_IT_POSTGRES_DATABASE", os.environ.get("DPONE_IT_PG_DATABASE", "dpone_it")),
        user=os.environ.get("DPONE_IT_POSTGRES_USER", os.environ.get("DPONE_IT_PG_USER", "dpone")),
        password=os.environ.get("DPONE_IT_POSTGRES_PASSWORD", os.environ.get("DPONE_IT_PG_PASSWORD", "dpone")),
    )


class _Logger:
    def log_etl_progress(self, event: str, payload: dict[str, object]) -> None:
        del event, payload

    def log_data_sample(self, kind: str, rows: list[object], max_rows: int) -> None:
        del kind, rows, max_rows


def _wait_until_ready(label: str, action: Callable[[], object], *, timeout_seconds: int = 90) -> None:
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


def _prepare_mysql(mysql: MySQLConnector) -> None:
    database = mysql.database
    mysql.execute_query(f"DROP TABLE IF EXISTS `{database}`.`mysql_to_postgres_orders`")
    mysql.execute_query(
        f"""
        CREATE TABLE `{database}`.`mysql_to_postgres_orders` (
            id INT PRIMARY KEY,
            name VARCHAR(64) NOT NULL,
            updated_at DATETIME NOT NULL
        )
        """
    )
    mysql.execute_query(
        f"""
        INSERT INTO `{database}`.`mysql_to_postgres_orders` (id, name, updated_at) VALUES
            (1, 'alpha', '2026-07-21 10:00:00'),
            (2, 'beta,comma', '2026-07-21 11:00:00')
        """
    )


def _prepare_postgres(postgres: PostgresConnector) -> None:
    postgres.execute_query("CREATE SCHEMA IF NOT EXISTS dpone_it")
    postgres.execute_query("CREATE SCHEMA IF NOT EXISTS staging")
    postgres.execute_query("DROP TABLE IF EXISTS dpone_it.mysql_to_postgres_orders CASCADE")


@pytest.mark.skipif(not _enabled(), reason="MySQL/Postgres integration host not configured")
def test_mysql_to_postgres_full_refresh_csv_copy(tmp_path: Path) -> None:
    mysql = _mysql()
    _wait_until_ready("mysql", lambda: mysql.get_records("SELECT 1"))
    postgres = _postgres()
    _wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    _prepare_mysql(mysql)
    _prepare_postgres(postgres)

    config = LoadConfig(
        source_conn_id="mysql_source",
        target_conn_id="postgres_sink",
        source_schema=mysql.database,
        source_table="mysql_to_postgres_orders",
        source_database=mysql.database,
        target_schema="dpone_it",
        target_table="mysql_to_postgres_orders",
        staging_schema="staging",
        load_strategy=LoadStrategy.FULL_REFRESH,
        export_format="csv",
        compress_export=False,
        options={
            "sink_type": "postgres",
            "partition_tmp_dir": str(tmp_path),
        },
    )
    extract = MySQLFullExtractStrategy(mysql, logger=_Logger(), sink_connector=postgres).extract(config, None)
    assert getattr(extract.artifact, "format", None) == "csv"
    assert getattr(extract.artifact, "rows_exported", None) == 2
    result = PostgresSink(postgres, state_storage=None, logger=None).load(
        config, LoadPayload(artifact=extract.artifact, schema=extract.schema)
    )
    rows = postgres.get_records(
        "SELECT id, name FROM dpone_it.mysql_to_postgres_orders ORDER BY id",
        as_dict=False,
    )
    assert result.total_rows == 2
    assert rows[0][0] == 1
    assert rows[1][1] == "beta,comma"


@pytest.mark.skipif(not _enabled(), reason="MySQL/Postgres integration host not configured")
def test_mysql_to_postgres_incremental_merge_watermark(tmp_path: Path) -> None:
    """CSV FileExportArtifact → staging → delete+insert merge; watermark + row accumulation."""

    mysql = _mysql()
    _wait_until_ready("mysql", lambda: mysql.get_records("SELECT 1"))
    postgres = _postgres()
    _wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    _prepare_mysql(mysql)
    _prepare_postgres(postgres)

    config = LoadConfig(
        source_conn_id="mysql_source",
        target_conn_id="postgres_sink",
        source_schema=mysql.database,
        source_table="mysql_to_postgres_orders",
        source_database=mysql.database,
        target_schema="dpone_it",
        target_table="mysql_to_postgres_orders",
        staging_schema="staging",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        export_format="csv",
        compress_export=False,
        unique_key=["id"],
        options={
            "sink_type": "postgres",
            "incremental_column": "updated_at",
            "partition_tmp_dir": str(tmp_path),
            "technical_columns": "forbidden",
        },
    )
    strategy = MySQLIncrementalExtractStrategy(mysql, logger=_Logger(), sink_connector=postgres)
    first = strategy.extract(config, strategy.get_state(config))
    assert getattr(first.artifact, "rows_exported", None) == 2
    PostgresSink(postgres, state_storage=None, logger=None).load(
        config, LoadPayload(artifact=first.artifact, schema=first.schema)
    )
    assert int(postgres.get_records("SELECT COUNT(*) FROM dpone_it.mysql_to_postgres_orders")[0][0]) == 2

    second_state = strategy.get_state(config)
    assert second_state is not None
    second = strategy.extract(config, second_state)
    assert getattr(second.artifact, "rows_exported", None) == 0

    mysql.execute_query(
        f"INSERT INTO `{mysql.database}`.`mysql_to_postgres_orders` (id, name, updated_at) "
        "VALUES (3, 'gamma', '2026-07-21 12:00:00')"
    )
    third = strategy.extract(config, strategy.get_state(config))
    assert getattr(third.artifact, "rows_exported", None) == 1
    assert getattr(third.artifact, "format", None) == "csv"
    PostgresSink(postgres, state_storage=None, logger=None).load(
        config, LoadPayload(artifact=third.artifact, schema=third.schema)
    )
    rows = postgres.get_records("SELECT id, name FROM dpone_it.mysql_to_postgres_orders ORDER BY id", as_dict=False)
    assert len(rows) == 3
    assert rows[2][1] == "gamma"
