from __future__ import annotations

import os
import time
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifacts import PartitionedFileExportArtifact
from dpone.runtime.connectors.mssql import MSSQLConnector
from dpone.runtime.connectors.postgres import PostgresConnector
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.mssql import MSSQLSink
from dpone.runtime.sources.strategies.postgres.postgres_full_extract import PostgresFullExtractStrategy

pytestmark = [pytest.mark.integration_postgres, pytest.mark.integration_mssql, pytest.mark.integration_live]


def _enabled() -> bool:
    return os.environ.get("DPONE_RUN_INTEGRATION") == "1"


def _postgres() -> PostgresConnector:
    return PostgresConnector(
        host=os.environ.get("DPONE_IT_POSTGRES_HOST", os.environ.get("DPONE_IT_PG_HOST", "127.0.0.1")),
        port=int(os.environ.get("DPONE_IT_POSTGRES_PORT", os.environ.get("DPONE_IT_PG_PORT_FORWARD", "55432"))),
        database=os.environ.get("DPONE_IT_POSTGRES_DATABASE", os.environ.get("DPONE_IT_PG_DATABASE", "dpone_it")),
        user=os.environ.get("DPONE_IT_POSTGRES_USER", os.environ.get("DPONE_IT_PG_USER", "dpone")),
        password=os.environ.get("DPONE_IT_POSTGRES_PASSWORD", os.environ.get("DPONE_IT_PG_PASSWORD", "dpone")),
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
        except Exception as exc:  # noqa: BLE001 - readiness loop must tolerate transient driver errors
            last_error = exc
            time.sleep(2)
    raise RuntimeError(f"{label} was not ready within {timeout_seconds}s") from last_error


def _ensure_mssql_database() -> None:
    database = os.environ.get("DPONE_IT_MSSQL_DATABASE", "dpone_it")
    master = _mssql(database="master")
    _wait_until_ready("mssql", lambda: master.get_records("SELECT 1"))
    master.execute_query(f"IF DB_ID(N'{database}') IS NULL CREATE DATABASE [{database}]")


def _prepare_source(pg: PostgresConnector) -> None:
    pg.execute_query("CREATE SCHEMA IF NOT EXISTS dpone_it")
    pg.execute_query("DROP TABLE IF EXISTS dpone_it.pg_to_mssql_orders")
    pg.execute_query(
        """
        CREATE TABLE dpone_it.pg_to_mssql_orders (
            id integer PRIMARY KEY,
            name text,
            description text,
            amount numeric(18,4),
            created_at timestamp without time zone,
            empty_value text,
            nullable_value integer
        )
        """
    )
    pg.execute_query(
        """
        INSERT INTO dpone_it.pg_to_mssql_orders
            (id, name, description, amount, created_at, empty_value, nullable_value)
        VALUES
            (1, 'alpha', 'plain text', 10.2500, '2026-01-01 10:00:00', '', NULL),
            (2, 'таблица', E'line\\nwith\\ttab', 20.5000, '2026-01-02 11:30:00', '', 42),
            (3, NULL, NULL, NULL, NULL, NULL, NULL)
        """
    )


def _prepare_mssql(mssql: MSSQLConnector) -> None:
    mssql.execute_query("IF SCHEMA_ID('dpone_it') IS NULL EXEC('CREATE SCHEMA [dpone_it]')")
    mssql.execute_query("IF SCHEMA_ID('staging') IS NULL EXEC('CREATE SCHEMA [staging]')")
    mssql.execute_query("DROP TABLE IF EXISTS [dpone_it].[pg_to_mssql_orders]")
    mssql.execute_query("DROP TABLE IF EXISTS [dpone_it].[pg_to_mssql_orders_delta]")
    mssql.execute_query("DROP TABLE IF EXISTS [dpone_it].[pg_to_mssql_orders_partitioned]")


@pytest.mark.skipif(not _enabled(), reason="Set DPONE_RUN_INTEGRATION=1 to run local Postgres -> MSSQL integration")
def test_postgres_to_mssql_native_full_refresh_lossless_codec(tmp_path: Path) -> None:
    pg = _postgres()
    _wait_until_ready("postgres", lambda: pg.get_records("SELECT 1"))
    _ensure_mssql_database()
    mssql = _mssql()
    _prepare_source(pg)
    _prepare_mssql(mssql)
    config = LoadConfig(
        source_conn_id="postgres_source",
        target_conn_id="mssql_sink",
        source_schema="dpone_it",
        source_table="pg_to_mssql_orders",
        target_schema="dpone_it",
        target_table="pg_to_mssql_orders",
        staging_schema="staging",
        load_strategy=LoadStrategy.FULL_REFRESH,
        export_format="mssql-delimited",
        compress_export=False,
        options={
            "partition_tmp_dir": str(tmp_path),
            "bulk": {"mode": "bcp", "bcp": {"batch_size": 1000, "packet_size": 16384}},
        },
    )

    extract = PostgresFullExtractStrategy(pg, logger=_Logger()).extract(config, None)
    result = MSSQLSink(mssql, logger=None).load(config, LoadPayload(artifact=extract.artifact, schema=extract.schema))
    rows = mssql.get_records(
        """
        SELECT [id], [name], [description], [amount], [empty_value], [nullable_value]
        FROM [dpone_it].[pg_to_mssql_orders]
        ORDER BY [id]
        """
    )

    assert result.total_rows == 3
    assert rows[0][4] == ""
    assert rows[0][5] is None
    assert "line\nwith\ttab" == rows[1][2]
    assert Decimal(str(rows[1][3])) == Decimal("20.5000")
    assert rows[2][1] is None


@pytest.mark.skipif(not _enabled(), reason="Set DPONE_RUN_INTEGRATION=1 to run local Postgres -> MSSQL integration")
def test_postgres_to_mssql_incremental_merge_delete_insert(tmp_path: Path) -> None:
    pg = _postgres()
    _wait_until_ready("postgres", lambda: pg.get_records("SELECT 1"))
    _ensure_mssql_database()
    mssql = _mssql()
    _prepare_source(pg)
    _prepare_mssql(mssql)
    pg.execute_query("DROP TABLE IF EXISTS dpone_it.pg_to_mssql_orders_delta")
    pg.execute_query(
        """
        CREATE TABLE dpone_it.pg_to_mssql_orders_delta AS
        SELECT * FROM dpone_it.pg_to_mssql_orders WHERE id IN (1, 3)
        """
    )
    pg.execute_query(
        """
        INSERT INTO dpone_it.pg_to_mssql_orders_delta
            (id, name, description, amount, created_at, empty_value, nullable_value)
        VALUES
            (4, 'new row', 'inserted by merge', 40.0000, '2026-01-04 14:00:00', '', 100)
        """
    )
    mssql.execute_query(
        """
        CREATE TABLE [dpone_it].[pg_to_mssql_orders_delta] (
            [id] int NOT NULL,
            [name] nvarchar(max) NULL,
            [description] nvarchar(max) NULL,
            [amount] decimal(18,4) NULL,
            [created_at] datetime2 NULL,
            [empty_value] nvarchar(max) NULL,
            [nullable_value] int NULL
        )
        """
    )
    mssql.execute_query(
        """
        INSERT INTO [dpone_it].[pg_to_mssql_orders_delta]
            ([id], [name], [description], [amount], [created_at], [empty_value], [nullable_value])
        VALUES
            (1, N'old alpha', N'old text', 1.0000, '2025-01-01T00:00:00', N'old', 7),
            (2, N'survivor', N'not touched', 2.0000, '2025-01-02T00:00:00', N'', 8)
        """
    )
    config = LoadConfig(
        source_conn_id="postgres_source",
        target_conn_id="mssql_sink",
        source_schema="dpone_it",
        source_table="pg_to_mssql_orders_delta",
        target_schema="dpone_it",
        target_table="pg_to_mssql_orders_delta",
        staging_schema="staging",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key="id",
        merge_policy="delete_insert",
        export_format="mssql-delimited",
        compress_export=False,
        options={
            "partition_tmp_dir": str(tmp_path),
            "bulk": {"mode": "bcp", "bcp": {"batch_size": 1000, "packet_size": 16384}},
        },
    )

    extract = PostgresFullExtractStrategy(pg, logger=_Logger()).extract(config, None)
    result = MSSQLSink(mssql, logger=None).load(config, LoadPayload(artifact=extract.artifact, schema=extract.schema))
    rows = mssql.get_records(
        """
        SELECT [id], [name], [description], [amount], [empty_value], [nullable_value]
        FROM [dpone_it].[pg_to_mssql_orders_delta]
        ORDER BY [id]
        """
    )

    assert result.total_rows == 4
    assert result.updated_rows == 1
    assert result.inserted_rows == 2
    assert rows[0][0] == 1
    assert rows[0][1] == "alpha"
    assert rows[0][4] == ""
    assert rows[1][0] == 2
    assert rows[1][1] == "survivor"
    assert rows[2][0] == 3
    assert rows[2][1] is None
    assert rows[3][0] == 4
    assert rows[3][1] == "new row"


@pytest.mark.skipif(not _enabled(), reason="Set DPONE_RUN_INTEGRATION=1 to run local Postgres -> MSSQL integration")
def test_postgres_to_mssql_partitioned_full_refresh_exports_partition_metadata(tmp_path: Path) -> None:
    pg = _postgres()
    _wait_until_ready("postgres", lambda: pg.get_records("SELECT 1"))
    _ensure_mssql_database()
    mssql = _mssql()
    _prepare_source(pg)
    _prepare_mssql(mssql)
    config = LoadConfig(
        source_conn_id="postgres_source",
        target_conn_id="mssql_sink",
        source_schema="dpone_it",
        source_table="pg_to_mssql_orders",
        target_schema="dpone_it",
        target_table="pg_to_mssql_orders_partitioned",
        staging_schema="staging",
        load_strategy=LoadStrategy.FULL_REFRESH,
        export_format="mssql-delimited",
        compress_export=False,
        options={
            "partition_tmp_dir": str(tmp_path),
            "partitioning": {
                "strategy": "range",
                "column": "id",
                "bounds": "auto",
                "target_rows_per_partition": 2,
                "max_partitions": 2,
                "export_workers": 2,
                "load_workers": 2,
            },
            "bulk": {"mode": "bcp", "bcp": {"batch_size": 1000, "packet_size": 16384}},
        },
    )

    extract = PostgresFullExtractStrategy(pg, logger=_Logger()).extract(config, None)
    assert isinstance(extract.artifact, PartitionedFileExportArtifact)
    assert len(extract.artifact.partitions) == 2
    assert extract.artifact.max_workers == 2
    for partition in extract.artifact.partitions:
        assert partition.bulk_text_codec is not None
        assert getattr(partition, "transfer_partition_id")
        assert getattr(partition, "partition_bounds")["lower"] is not None
        assert getattr(partition, "partition_bounds")["upper"] is not None

    result = MSSQLSink(mssql, logger=None).load(config, LoadPayload(artifact=extract.artifact, schema=extract.schema))
    rows = mssql.get_records(
        """
        SELECT [id], [name], [description], [amount], [empty_value], [nullable_value]
        FROM [dpone_it].[pg_to_mssql_orders_partitioned]
        ORDER BY [id]
        """
    )

    assert result.total_rows == 3
    assert [row[0] for row in rows] == [1, 2, 3]
    assert rows[0][4] == ""
    assert rows[1][2] == "line\nwith\ttab"
    assert rows[2][1] is None
