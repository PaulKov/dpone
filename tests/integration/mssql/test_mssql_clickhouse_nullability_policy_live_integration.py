from __future__ import annotations

import uuid

import pytest
from tests.integration.mssql.mssql_clickhouse_live_support import IntegrationLogger, open_mssql_connector
from tests.integration.mssql.mssql_clickhouse_nullability_live_support import (
    canonical_clickhouse_types,
    clickhouse_column_types,
    create_source_table,
    insert_source_rows,
    load_config,
)

from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.clickhouse import ClickHouseSink
from dpone.runtime.sources.strategies.mssql.mssql_strategies import MSSQLFullExtractStrategy

pytestmark = [
    pytest.mark.integration_mssql,
    pytest.mark.integration_clickhouse,
    pytest.mark.integration_live,
]


def test_mssql_clickhouse_live_non_nullable_policy_defaults_real_nulls(
    clickhouse_connector,
    clickhouse_settings,
) -> None:
    mssql = open_mssql_connector()
    schema = f"nl_policy_{uuid.uuid4().hex[:8]}"
    source_table = "defaulting_source"
    target_table = f"nl_policy_default_{uuid.uuid4().hex[:8]}"

    try:
        mssql.execute_query(f"EXEC('CREATE SCHEMA [{schema}]')")
        create_source_table(
            mssql,
            schema=schema,
            table=source_table,
            columns_sql=["[amount] int NULL", "[comment] nvarchar(510) NULL"],
        )
        insert_source_rows(
            mssql,
            schema=schema,
            table=source_table,
            rows_sql=[["CAST(7 AS int)", "N'present'"], ["NULL", "NULL"]],
        )
        cfg = load_config(
            schema=schema,
            source_table=source_table,
            target_schema=clickhouse_settings.database,
            target_table=target_table,
            physical_target_types=None,
            clickhouse_settings=clickhouse_settings,
            nullability={
                "mode": "non_nullable_by_default",
                "null_handling": "default",
            },
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
        assert canonical_clickhouse_types(
            clickhouse_column_types(clickhouse_connector, clickhouse_settings.database, target_table)
        ) == {"amount": "Int32", "comment": "String"}
        defaults = clickhouse_connector.get_records(
            f"""
            SELECT count(), countIf(`amount` = 0), countIf(`comment` = '')
            FROM `{clickhouse_settings.database}`.`{target_table}`
            """
        )[0]
        assert defaults == (2, 1, 1)
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{clickhouse_settings.database}`.`{target_table}`")
        mssql.execute_query(f"DROP TABLE IF EXISTS [{schema}].[{source_table}]")
        mssql.execute_query(f"DROP SCHEMA IF EXISTS [{schema}]")
        mssql.close()


def test_mssql_clickhouse_live_non_nullable_policy_per_column_fail_fast(
    clickhouse_connector,
    clickhouse_settings,
) -> None:
    mssql = open_mssql_connector()
    schema = f"nl_policy_{uuid.uuid4().hex[:8]}"
    source_table = "failfast_source"
    target_table = f"nl_policy_fail_{uuid.uuid4().hex[:8]}"

    try:
        mssql.execute_query(f"EXEC('CREATE SCHEMA [{schema}]')")
        create_source_table(
            mssql,
            schema=schema,
            table=source_table,
            columns_sql=["[amount] int NULL", "[comment] nvarchar(510) NULL"],
        )
        insert_source_rows(
            mssql,
            schema=schema,
            table=source_table,
            rows_sql=[["NULL", "NULL"]],
        )
        cfg = load_config(
            schema=schema,
            source_table=source_table,
            target_schema=clickhouse_settings.database,
            target_table=target_table,
            physical_target_types=None,
            clickhouse_settings=clickhouse_settings,
            nullability={
                "mode": "non_nullable_by_default",
                "null_handling": "default",
                "columns": {
                    "amount": {"null_handling": "fail_fast"},
                },
            },
        )
        extract = MSSQLFullExtractStrategy(mssql, IntegrationLogger(), sink_connector=clickhouse_connector).extract(
            cfg,
            None,
        )

        with pytest.raises(ValueError, match="amount"):
            ClickHouseSink(clickhouse_connector).load(
                cfg,
                LoadPayload(artifact=extract.artifact, schema=extract.schema),
            )
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{clickhouse_settings.database}`.`{target_table}`")
        mssql.execute_query(f"DROP TABLE IF EXISTS [{schema}].[{source_table}]")
        mssql.execute_query(f"DROP SCHEMA IF EXISTS [{schema}]")
        mssql.close()
