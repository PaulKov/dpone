"""Docker MySQL → ClickHouse TabSeparated staging evidence (SKIP when services unavailable)."""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.connectors.clickhouse import ClickHouseConnector
from dpone.runtime.connectors.mysql import MySQLConnector
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.clickhouse import ClickHouseSink
from dpone.runtime.sources.strategies.mysql.mysql_full import MySQLFullExtractStrategy
from dpone.runtime.sources.strategies.mysql.mysql_incremental import MySQLIncrementalExtractStrategy

pytestmark = [pytest.mark.integration, pytest.mark.integration_mysql, pytest.mark.integration_clickhouse]


def _enabled() -> bool:
    return bool(
        os.environ.get("DPONE_RUN_INTEGRATION") == "1"
        or os.environ.get("DPONE_IT_MYSQL_HOST")
        or os.environ.get("DPONE_IT_MYSQL_PORT_FORWARD")
        or os.environ.get("DPONE_IT_CH_HOST")
        or os.environ.get("DPONE_IT_CH_PORT_FORWARD")
    )


def _mysql() -> MySQLConnector:
    return MySQLConnector(
        host=os.environ.get("DPONE_IT_MYSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("DPONE_IT_MYSQL_PORT_FORWARD", "53306")),
        database=os.environ.get("DPONE_IT_MYSQL_DATABASE", "dpone_it"),
        user=os.environ.get("DPONE_IT_MYSQL_USER", "dpone"),
        password=os.environ.get("DPONE_IT_MYSQL_PASSWORD", "dpone"),
    )


def _clickhouse() -> ClickHouseConnector:
    return ClickHouseConnector(
        host=os.environ.get("DPONE_IT_CH_HOST", "127.0.0.1"),
        port=int(os.environ.get("DPONE_IT_CH_PORT_FORWARD", os.environ.get("DPONE_IT_CH_PORT", "59000"))),
        database=os.environ.get("DPONE_IT_CH_DATABASE", "dpone_it"),
        user=os.environ.get("DPONE_IT_CH_USER", "default"),
        password=os.environ.get("DPONE_IT_CH_PASSWORD", "dpone"),
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
    mysql.execute_query(f"DROP TABLE IF EXISTS `{database}`.`mysql_to_clickhouse_orders`")
    mysql.execute_query(
        f"""
        CREATE TABLE `{database}`.`mysql_to_clickhouse_orders` (
            id INT PRIMARY KEY,
            name VARCHAR(64) NOT NULL,
            updated_at DATETIME NOT NULL
        )
        """
    )
    mysql.execute_query(
        f"""
        INSERT INTO `{database}`.`mysql_to_clickhouse_orders` (id, name, updated_at) VALUES
            (1, 'alpha', '2026-07-21 10:00:00'),
            (2, 'beta,comma', '2026-07-21 11:00:00')
        """
    )


def _prepare_clickhouse(clickhouse: ClickHouseConnector) -> None:
    clickhouse.execute_query(f"DROP TABLE IF EXISTS `{clickhouse.database}`.`mysql_to_clickhouse_orders`")


def _load_options(tmp_path: Path) -> dict[str, object]:
    return {
        "sink_type": "clickhouse",
        "partition_tmp_dir": str(tmp_path),
        "technical_columns": "forbidden",
        "physical_design": {
            "storage": {
                "clickhouse": {
                    "engine": "MergeTree",
                    "order_by": ["id"],
                }
            }
        },
    }


@pytest.mark.skipif(not _enabled(), reason="MySQL/ClickHouse integration host not configured")
def test_mysql_to_clickhouse_full_refresh_tsv(tmp_path: Path) -> None:
    mysql = _mysql()
    _wait_until_ready("mysql", lambda: mysql.get_records("SELECT 1"))
    clickhouse = _clickhouse()
    _wait_until_ready("clickhouse", lambda: clickhouse.get_records("SELECT 1"))
    _prepare_mysql(mysql)
    _prepare_clickhouse(clickhouse)

    config = LoadConfig(
        source_conn_id="mysql_source",
        target_conn_id="clickhouse_sink",
        source_schema=mysql.database,
        source_table="mysql_to_clickhouse_orders",
        source_database=mysql.database,
        target_schema=clickhouse.database,
        target_table="mysql_to_clickhouse_orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        export_format="csv",
        compress_export=False,
        options=_load_options(tmp_path),
    )
    extract = MySQLFullExtractStrategy(mysql, logger=_Logger(), sink_connector=clickhouse).extract(config, None)
    assert getattr(extract.artifact, "format", None) == "clickhouse-tsv"
    assert getattr(extract.artifact, "rows_exported", None) == 2
    result = ClickHouseSink(clickhouse, state_storage=None, logger=None).load(
        config, LoadPayload(artifact=extract.artifact, schema=extract.schema)
    )
    rows = clickhouse.get_records(
        f"SELECT id, name FROM `{clickhouse.database}`.`mysql_to_clickhouse_orders` ORDER BY id",
        as_dict=False,
    )
    assert result.total_rows == 2
    assert rows[0][0] == 1
    assert rows[1][1] == "beta,comma"


@pytest.mark.skipif(not _enabled(), reason="MySQL/ClickHouse integration host not configured")
def test_mysql_to_clickhouse_incremental_merge_watermark(tmp_path: Path) -> None:
    """clickhouse-tsv FileExportArtifact → staging → merge; watermark + row accumulation."""

    mysql = _mysql()
    _wait_until_ready("mysql", lambda: mysql.get_records("SELECT 1"))
    clickhouse = _clickhouse()
    _wait_until_ready("clickhouse", lambda: clickhouse.get_records("SELECT 1"))
    _prepare_mysql(mysql)
    _prepare_clickhouse(clickhouse)

    config = LoadConfig(
        source_conn_id="mysql_source",
        target_conn_id="clickhouse_sink",
        source_schema=mysql.database,
        source_table="mysql_to_clickhouse_orders",
        source_database=mysql.database,
        target_schema=clickhouse.database,
        target_table="mysql_to_clickhouse_orders",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        export_format="csv",
        compress_export=False,
        unique_key=["id"],
        options={
            **_load_options(tmp_path),
            "incremental_column": "updated_at",
        },
    )
    strategy = MySQLIncrementalExtractStrategy(mysql, logger=_Logger(), sink_connector=clickhouse)
    first = strategy.extract(config, strategy.get_state(config))
    assert getattr(first.artifact, "rows_exported", None) == 2
    ClickHouseSink(clickhouse, state_storage=None, logger=None).load(
        config, LoadPayload(artifact=first.artifact, schema=first.schema)
    )
    assert (
        int(clickhouse.get_records(f"SELECT COUNT(*) FROM `{clickhouse.database}`.`mysql_to_clickhouse_orders`")[0][0])
        == 2
    )

    second_state = strategy.get_state(config)
    assert second_state is not None
    second = strategy.extract(config, second_state)
    assert getattr(second.artifact, "rows_exported", None) == 0

    mysql.execute_query(
        f"INSERT INTO `{mysql.database}`.`mysql_to_clickhouse_orders` (id, name, updated_at) "
        "VALUES (3, 'gamma', '2026-07-21 12:00:00')"
    )
    third = strategy.extract(config, strategy.get_state(config))
    assert getattr(third.artifact, "rows_exported", None) == 1
    assert getattr(third.artifact, "format", None) == "clickhouse-tsv"
    ClickHouseSink(clickhouse, state_storage=None, logger=None).load(
        config, LoadPayload(artifact=third.artifact, schema=third.schema)
    )
    rows = clickhouse.get_records(
        f"SELECT id, name FROM `{clickhouse.database}`.`mysql_to_clickhouse_orders` ORDER BY id",
        as_dict=False,
    )
    assert len(rows) == 3
    assert rows[2][1] == "gamma"
