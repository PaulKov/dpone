"""Docker MySQL → MSSQL staging/bcp evidence (SKIP when services unavailable)."""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.connectors.mssql import MSSQLConnector
from dpone.runtime.connectors.mysql import MySQLConnector
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.mssql import MSSQLSink
from dpone.runtime.sources.strategies.mysql.mysql_full import MySQLFullExtractStrategy
from dpone.runtime.sources.strategies.mysql.mysql_incremental import MySQLIncrementalExtractStrategy

pytestmark = [pytest.mark.integration, pytest.mark.integration_mysql, pytest.mark.integration_mssql]


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


def _mssql(*, database: str | None = None) -> MSSQLConnector:
    return MSSQLConnector(
        host=os.environ.get("DPONE_IT_MSSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("DPONE_IT_MSSQL_PORT", os.environ.get("DPONE_IT_MSSQL_PORT_FORWARD", "51433"))),
        database=database or os.environ.get("DPONE_IT_MSSQL_DATABASE", "dpone_it"),
        user=os.environ.get("DPONE_IT_MSSQL_USER", "sa"),
        password=os.environ.get("DPONE_IT_MSSQL_PASSWORD", "Dp0ne.Strong.Pw.2026!"),
        trust_server_certificate=os.environ.get("DPONE_IT_MSSQL_TRUST_SERVER_CERTIFICATE", "yes"),
        bcp_path=os.environ.get("DPONE_IT_MSSQL_BCP_PATH", "bcp"),
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


def _ensure_mssql_database() -> None:
    database = os.environ.get("DPONE_IT_MSSQL_DATABASE", "dpone_it")
    master = _mssql(database="master")
    _wait_until_ready("mssql", lambda: master.get_records("SELECT 1"))
    master.execute_query(f"IF DB_ID(N'{database}') IS NULL CREATE DATABASE [{database}]")


def _prepare_mysql(mysql: MySQLConnector) -> None:
    database = mysql.database
    mysql.execute_query(f"DROP TABLE IF EXISTS `{database}`.`mysql_to_mssql_orders`")
    mysql.execute_query(
        f"""
        CREATE TABLE `{database}`.`mysql_to_mssql_orders` (
            id INT PRIMARY KEY,
            name VARCHAR(64) NOT NULL,
            updated_at DATETIME NOT NULL
        )
        """
    )
    mysql.execute_query(
        f"""
        INSERT INTO `{database}`.`mysql_to_mssql_orders` (id, name, updated_at) VALUES
            (1, 'alpha', '2026-07-21 10:00:00'),
            (2, 'beta\ttab', '2026-07-21 11:00:00')
        """
    )


def _prepare_mssql(mssql: MSSQLConnector) -> None:
    mssql.execute_query("IF SCHEMA_ID('dpone_it') IS NULL EXEC('CREATE SCHEMA [dpone_it]')")
    mssql.execute_query("IF SCHEMA_ID('staging') IS NULL EXEC('CREATE SCHEMA [staging]')")
    mssql.execute_query("DROP TABLE IF EXISTS [dpone_it].[mysql_to_mssql_orders]")


@pytest.mark.skipif(not _enabled(), reason="MySQL/MSSQL integration host not configured")
def test_mysql_to_mssql_full_refresh_bcp_staging(tmp_path: Path) -> None:
    mysql = _mysql()
    _wait_until_ready("mysql", lambda: mysql.get_records("SELECT 1"))
    _ensure_mssql_database()
    mssql = _mssql()
    _prepare_mysql(mysql)
    _prepare_mssql(mssql)

    config = LoadConfig(
        source_conn_id="mysql_source",
        target_conn_id="mssql_sink",
        source_schema=mysql.database,
        source_table="mysql_to_mssql_orders",
        source_database=mysql.database,
        target_schema="dpone_it",
        target_table="mysql_to_mssql_orders",
        staging_schema="staging",
        load_strategy=LoadStrategy.FULL_REFRESH,
        export_format="mssql-delimited",
        compress_export=False,
        options={
            "partition_tmp_dir": str(tmp_path),
            "bulk": {"mode": "bcp", "bcp": {"batch_size": 1000, "packet_size": 16384}},
        },
    )
    extract = MySQLFullExtractStrategy(mysql, logger=_Logger()).extract(config, None)
    assert getattr(extract.artifact, "rows_exported", None) == 2
    result = MSSQLSink(mssql, logger=None).load(config, LoadPayload(artifact=extract.artifact, schema=extract.schema))
    rows = mssql.get_records(
        "SELECT [id], [name] FROM [dpone_it].[mysql_to_mssql_orders] ORDER BY [id]",
        as_dict=False,
    )
    assert result.total_rows == 2
    assert rows[0][0] == 1
    assert rows[1][1] == "beta\ttab"


@pytest.mark.skipif(not _enabled(), reason="MySQL/MSSQL integration host not configured")
def test_mysql_to_mssql_incremental_merge_watermark(tmp_path: Path) -> None:
    mysql = _mysql()
    _wait_until_ready("mysql", lambda: mysql.get_records("SELECT 1"))
    _ensure_mssql_database()
    mssql = _mssql()
    _prepare_mysql(mysql)
    _prepare_mssql(mssql)

    config = LoadConfig(
        source_conn_id="mysql_source",
        target_conn_id="mssql_sink",
        source_schema=mysql.database,
        source_table="mysql_to_mssql_orders",
        source_database=mysql.database,
        target_schema="dpone_it",
        target_table="mysql_to_mssql_orders",
        staging_schema="staging",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        export_format="mssql-delimited",
        compress_export=False,
        unique_key=["id"],
        options={
            "incremental_column": "updated_at",
            "partition_tmp_dir": str(tmp_path),
            "bulk": {"mode": "bcp", "bcp": {"batch_size": 1000, "packet_size": 16384}},
        },
    )
    strategy = MySQLIncrementalExtractStrategy(mysql, logger=_Logger(), sink_connector=mssql)
    first = strategy.extract(config, strategy.get_state(config))
    MSSQLSink(mssql, logger=None).load(config, LoadPayload(artifact=first.artifact, schema=first.schema))

    # No new watermark rows → empty export after sink high-water mark.
    second_state = strategy.get_state(config)
    assert second_state is not None
    second = strategy.extract(config, second_state)
    assert getattr(second.artifact, "rows_exported", None) == 0

    mysql.execute_query(
        f"INSERT INTO `{mysql.database}`.`mysql_to_mssql_orders` (id, name, updated_at) "
        "VALUES (3, 'gamma', '2026-07-21 12:00:00')"
    )
    third = strategy.extract(config, strategy.get_state(config))
    assert getattr(third.artifact, "rows_exported", None) == 1
    MSSQLSink(mssql, logger=None).load(config, LoadPayload(artifact=third.artifact, schema=third.schema))
    rows = mssql.get_records("SELECT COUNT(*) FROM [dpone_it].[mysql_to_mssql_orders]")
    assert int(rows[0][0]) == 3
