from __future__ import annotations

import os
import uuid
from decimal import Decimal

import pytest
from tests.integration.mssql.mssql_clickhouse_live_support import IntegrationLogger as _Logger
from tests.integration.mssql.mssql_clickhouse_live_support import open_mssql_connector as _mssql_connector

from dpone.config import LoadConfig, LoadStrategy
from dpone.readiness.schema_evolution import ColumnDef, SchemaComparator, SchemaEvolutionPolicy
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.clickhouse import ClickHouseSink
from dpone.runtime.sources.strategies.mssql.mssql_strategies import MSSQLFullExtractStrategy

pytestmark = [pytest.mark.integration_mssql, pytest.mark.integration_clickhouse]


def test_mssql_bcp_queryout_loads_clickhouse_via_native_http(
    clickhouse_connector,
    clickhouse_settings,
) -> None:
    mssql = _mssql_connector()
    schema = f"nt_{uuid.uuid4().hex[:8]}"
    source_table = "orders"
    target_table = f"nt_orders_{uuid.uuid4().hex[:8]}"
    try:
        mssql.execute_query(f"EXEC('CREATE SCHEMA [{schema}]')")
        mssql.execute_query(
            f"""
            CREATE TABLE [{schema}].[{source_table}] (
                [order_id] int NOT NULL PRIMARY KEY,
                [status] nvarchar(50) NULL,
                [amount] decimal(18, 2) NULL,
                [note] nvarchar(200) NULL
            )
            """
        )
        mssql.execute_query(
            f"""
            INSERT INTO [{schema}].[{source_table}] ([order_id], [status], [amount], [note])
            VALUES
                (1, N'new', 10.50, N'alpha'),
                (2, N'paid', 20.00, N''),
                (3, N'paid', NULL, NULL),
                (4, N'shipped', 44.10, N'unicode Привет')
            """
        )

        load_config = LoadConfig(
            source_conn_id="mssql-it",
            target_conn_id="clickhouse-it",
            source_schema=schema,
            source_table=source_table,
            target_schema=clickhouse_settings.database,
            target_table=target_table,
            load_strategy=LoadStrategy.FULL_REFRESH,
            batch_size=2,
            options={
                "extract_mode": "bcp_queryout",
                "mssql_export_mode": "bcp",
                "bulk": _bcp_bulk_options(batch_size=2),
                "partitioning": {
                    "strategy": "auto",
                    "column": "order_id",
                    "bounds": "auto",
                    "target_rows_per_partition": 2,
                    "max_partitions": 4,
                    "export_workers": 2,
                    "load_workers": 2,
                },
                "clickhouse_bulk": _clickhouse_http_bulk_options(
                    clickhouse_settings,
                    insert_settings={
                        "async_insert": 1,
                        "max_insert_block_size": 100000,
                        "input_format_parallel_parsing": 1,
                    },
                ),
            },
        )

        extract = MSSQLFullExtractStrategy(mssql, _Logger(), sink_connector=clickhouse_connector).extract(
            load_config,
            None,
        )
        assert getattr(extract.artifact, "partitions", None)

        result = ClickHouseSink(clickhouse_connector).load(
            load_config,
            LoadPayload(artifact=extract.artifact, schema=extract.schema),
        )

        rows = clickhouse_connector.get_records(
            f"""
            SELECT order_id, status, amount, note
            FROM `{clickhouse_settings.database}`.`{target_table}`
            ORDER BY order_id
            """,
            as_dict=True,
        )
        assert result.inserted_rows == 4
        assert rows == [
            {"order_id": 1, "status": "new", "amount": Decimal("10.50"), "note": "alpha"},
            {"order_id": 2, "status": "paid", "amount": Decimal("20.00"), "note": ""},
            {"order_id": 3, "status": "paid", "amount": None, "note": None},
            {"order_id": 4, "status": "shipped", "amount": Decimal("44.10"), "note": "unicode Привет"},
        ]
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{clickhouse_settings.database}`.`{target_table}`")
        mssql.execute_query(f"DROP TABLE IF EXISTS [{schema}].[{source_table}]")
        mssql.execute_query(f"DROP SCHEMA IF EXISTS [{schema}]")
        mssql.close()


def test_mssql_datetimeoffset_modes_load_clickhouse_via_native_http(
    clickhouse_connector,
    clickhouse_settings,
) -> None:
    mssql = _mssql_connector()
    schema = f"tz_{uuid.uuid4().hex[:8]}"
    source_table = "events"
    try:
        mssql.execute_query(f"EXEC('CREATE SCHEMA [{schema}]')")
        mssql.execute_query(
            f"""
            CREATE TABLE [{schema}].[{source_table}] (
                [event_id] int NOT NULL PRIMARY KEY,
                [offset_at] datetimeoffset(7) NULL
            )
            """
        )
        mssql.execute_query(
            f"""
            INSERT INTO [{schema}].[{source_table}] ([event_id], [offset_at])
            VALUES
                (1, CAST('2026-06-09T12:30:00.1234567+03:00' AS datetimeoffset(7))),
                (2, CAST('2026-06-09T00:00:00.0000000+00:00' AS datetimeoffset(7))),
                (3, CAST('2026-06-08T18:30:00.0000000-05:30' AS datetimeoffset(7))),
                (4, NULL)
            """
        )

        for mode in ("utc_instant", "fixed_timezone", "preserve_offset", "preserve_text"):
            target_table = f"tz_events_{mode}_{uuid.uuid4().hex[:8]}"
            try:
                options = {
                    "extract_mode": "bcp_queryout",
                    "mssql_export_mode": "bcp",
                    "type_fidelity": {
                        "binary_encoding": "hex",
                        "time_encoding": "seconds_since_midnight",
                        "temporal": {
                            "offset_timestamp": {
                                "mode": mode,
                                "timezone": "Europe/Moscow",
                            }
                        },
                    },
                    "bulk": _bcp_bulk_options(batch_size=10),
                    "clickhouse_bulk": _clickhouse_http_bulk_options(clickhouse_settings),
                }
                load_config = LoadConfig(
                    source_conn_id="mssql-it",
                    target_conn_id="clickhouse-it",
                    source_schema=schema,
                    source_table=source_table,
                    target_schema=clickhouse_settings.database,
                    target_table=target_table,
                    load_strategy=LoadStrategy.FULL_REFRESH,
                    batch_size=10,
                    options=options,
                )

                extract = MSSQLFullExtractStrategy(mssql, _Logger(), sink_connector=clickhouse_connector).extract(
                    load_config,
                    None,
                )
                result = ClickHouseSink(clickhouse_connector).load(
                    load_config,
                    LoadPayload(artifact=extract.artifact, schema=extract.schema),
                )
                column_types = {
                    row["name"]: row["type"]
                    for row in clickhouse_connector.get_records(
                        f"""
                        SELECT name, type
                        FROM system.columns
                        WHERE database = '{clickhouse_settings.database}'
                          AND table = '{target_table}'
                        """,
                        as_dict=True,
                    )
                }

                assert result.inserted_rows == 4
                if mode == "utc_instant":
                    assert column_types["offset_at"] == "Nullable(DateTime64(7, 'UTC'))"
                elif mode == "fixed_timezone":
                    assert column_types["offset_at"] == "Nullable(DateTime64(7, 'Europe/Moscow'))"
                elif mode == "preserve_offset":
                    assert column_types["offset_at"] == "Nullable(DateTime64(7, 'UTC'))"
                    assert column_types["__dpone__tz_offset_minutes__offset_at"] == "Nullable(Int16)"
                    offsets = clickhouse_connector.get_records(
                        f"""
                        SELECT event_id, __dpone__tz_offset_minutes__offset_at AS offset_minutes
                        FROM `{clickhouse_settings.database}`.`{target_table}`
                        ORDER BY event_id
                        """,
                        as_dict=True,
                    )
                    assert offsets == [
                        {"event_id": 1, "offset_minutes": 180},
                        {"event_id": 2, "offset_minutes": 0},
                        {"event_id": 3, "offset_minutes": -330},
                        {"event_id": 4, "offset_minutes": None},
                    ]
                else:
                    assert column_types["offset_at"] == "Nullable(String)"
                    rows = clickhouse_connector.get_records(
                        f"""
                        SELECT event_id, offset_at
                        FROM `{clickhouse_settings.database}`.`{target_table}`
                        ORDER BY event_id
                        """,
                        as_dict=True,
                    )
                    assert rows[0]["offset_at"].endswith("+03:00")
                    assert rows[2]["offset_at"].endswith("-05:30")
            finally:
                clickhouse_connector.execute_query(
                    f"DROP TABLE IF EXISTS `{clickhouse_settings.database}`.`{target_table}`"
                )
    finally:
        mssql.execute_query(f"DROP TABLE IF EXISTS [{schema}].[{source_table}]")
        mssql.execute_query(f"DROP SCHEMA IF EXISTS [{schema}]")
        mssql.close()


def test_mssql_naive_datetime_modes_order_by_and_schema_evolution_live(
    clickhouse_connector,
    clickhouse_settings,
) -> None:
    mssql = _mssql_connector()
    schema = f"dt_{uuid.uuid4().hex[:8]}"
    source_table = "events"
    try:
        mssql.execute_query(f"EXEC('CREATE SCHEMA [{schema}]')")
        mssql.execute_query(
            f"""
            CREATE TABLE [{schema}].[{source_table}] (
                [event_id] int NOT NULL PRIMARY KEY,
                [created_dt] datetime NULL,
                [created_dt2] datetime2(7) NULL,
                [rounded_dt] smalldatetime NULL,
                [amount] decimal(18, 2) NULL,
                [name] nvarchar(100) NULL
            )
            """
        )
        mssql.execute_query(
            f"""
            INSERT INTO [{schema}].[{source_table}]
                ([event_id], [created_dt], [created_dt2], [rounded_dt], [amount], [name])
            VALUES
                (
                    1,
                    CAST('2026-06-11T12:34:56.123' AS datetime),
                    CAST('2026-06-11T12:34:56.1234567' AS datetime2(7)),
                    CAST('2026-06-11T12:35:00' AS smalldatetime),
                    10.25,
                    N'alpha'
                ),
                (2, NULL, NULL, NULL, NULL, N'')
            """
        )

        for transfer_encoding in ("epoch", "text"):
            target_table = f"dt_events_{transfer_encoding}_{uuid.uuid4().hex[:8]}"
            load_config = LoadConfig(
                source_conn_id="mssql-it",
                target_conn_id="clickhouse-it",
                source_schema=schema,
                source_table=source_table,
                target_schema=clickhouse_settings.database,
                target_table=target_table,
                load_strategy=LoadStrategy.FULL_REFRESH,
                batch_size=10,
                options={
                    "extract_mode": "bcp_queryout",
                    "mssql_export_mode": "bcp",
                    "type_fidelity": {
                        "binary_encoding": "hex",
                        "time_encoding": "seconds_since_midnight",
                        "temporal": {
                            "naive_timestamp": {
                                "mode": "datetime64",
                                "transfer_encoding": transfer_encoding,
                                "timezone": "UTC",
                            }
                        },
                    },
                    "bulk": _bcp_bulk_options(batch_size=10),
                    "physical_design": {
                        "storage": {
                            "clickhouse": {
                                "engine": "MergeTree",
                                "order_by": ["event_id"],
                            }
                        }
                    },
                    "clickhouse_bulk": _clickhouse_http_bulk_options(clickhouse_settings),
                },
            )
            try:
                extract = MSSQLFullExtractStrategy(mssql, _Logger(), sink_connector=clickhouse_connector).extract(
                    load_config,
                    None,
                )
                result = ClickHouseSink(clickhouse_connector).load(
                    load_config,
                    LoadPayload(artifact=extract.artifact, schema=extract.schema),
                )

                column_types = {
                    row["name"]: row["type"]
                    for row in clickhouse_connector.get_records(
                        f"""
                        SELECT name, type
                        FROM system.columns
                        WHERE database = '{clickhouse_settings.database}'
                          AND table = '{target_table}'
                        """,
                        as_dict=True,
                    )
                }
                sorting_key = clickhouse_connector.get_records(
                    f"""
                    SELECT sorting_key
                    FROM system.tables
                    WHERE database = '{clickhouse_settings.database}'
                      AND name = '{target_table}'
                    """,
                    as_dict=True,
                )[0]["sorting_key"]
                rows = clickhouse_connector.get_records(
                    f"""
                    SELECT
                        event_id,
                        toString(created_dt) AS created_dt,
                        toString(created_dt2) AS created_dt2,
                        toString(rounded_dt) AS rounded_dt,
                        isNull(created_dt) AS created_dt_is_null,
                        isNull(created_dt2) AS created_dt2_is_null,
                        isNull(rounded_dt) AS rounded_dt_is_null,
                        amount,
                        name
                    FROM `{clickhouse_settings.database}`.`{target_table}`
                    ORDER BY event_id
                    """,
                    as_dict=True,
                )
                source_columns = [ColumnDef(name, dtype, nullable=True) for name, dtype in extract.schema]
                target_columns = [
                    ColumnDef(name, dtype, nullable=True)
                    for name, dtype in ClickHouseSink(clickhouse_connector).get_target_schema(load_config)
                ]
                schema_plan = SchemaComparator(SchemaEvolutionPolicy()).compare(source_columns, target_columns)

                assert result.inserted_rows == 2
                assert column_types["created_dt"] == "Nullable(DateTime64(3))"
                assert column_types["created_dt2"] == "Nullable(DateTime64(7))"
                assert column_types["rounded_dt"] == "Nullable(DateTime64(0))"
                assert sorting_key == "event_id"
                assert rows[0]["created_dt"].startswith("2026-06-11 12:34:56.123")
                assert rows[0]["created_dt2"].startswith("2026-06-11 12:34:56.1234567")
                assert rows[0]["rounded_dt"] == "2026-06-11 12:35:00"
                assert rows[0]["amount"] == Decimal("10.25")
                assert rows[0]["name"] == "alpha"
                assert rows[1]["created_dt_is_null"] == 1
                assert rows[1]["created_dt2_is_null"] == 1
                assert rows[1]["rounded_dt_is_null"] == 1
                assert rows[1]["amount"] is None
                assert rows[1]["name"] == ""
                assert schema_plan.has_breaking_changes is False
                assert schema_plan.changes == []
            finally:
                clickhouse_connector.execute_query(
                    f"DROP TABLE IF EXISTS `{clickhouse_settings.database}`.`{target_table}`"
                )
    finally:
        mssql.execute_query(f"DROP TABLE IF EXISTS [{schema}].[{source_table}]")
        mssql.execute_query(f"DROP SCHEMA IF EXISTS [{schema}]")
        mssql.close()


def _bcp_bulk_options(*, batch_size: int) -> dict:
    return {
        "mode": "bcp",
        "bcp": {"batch_size": batch_size, "packet_size": 65535, "timeout_seconds": 120},
    }


def _clickhouse_http_bulk_options(clickhouse_settings, *, insert_settings: dict | None = None) -> dict:
    options = {
        "mode": "http",
        "http": {
            "host": clickhouse_settings.host,
            "port": int(os.getenv("DPONE_IT_CH_HTTP_PORT", "58123")),
            "database": clickhouse_settings.database,
            "user": clickhouse_settings.user,
            "password": clickhouse_settings.password,
        },
    }
    if insert_settings is not None:
        options["insert_settings"] = insert_settings
    return options
