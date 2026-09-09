from __future__ import annotations

import uuid

import pytest
from tests.integration.mssql.mssql_clickhouse_live_support import open_mssql_connector
from tests.integration.mssql.mssql_clickhouse_nullability_live_support import (
    LIVE_TYPES,
    assert_one_null,
    auto_insert_rows_sql,
    auto_source_columns_sql,
    create_source_table,
    expected_auto_target_types,
    expected_physical_target_types,
    insert_source_rows,
    load_and_assert,
    physical_insert_rows_sql,
    physical_source_columns_sql,
)

pytestmark = [
    pytest.mark.integration_mssql,
    pytest.mark.integration_clickhouse,
    pytest.mark.integration_live,
]


@pytest.mark.parametrize(("slug", "source_type", "value_sql"), LIVE_TYPES, ids=[item[0] for item in LIVE_TYPES])
def test_mssql_clickhouse_live_nullability_matrix_for_type(
    clickhouse_connector,
    clickhouse_settings,
    slug: str,
    source_type: str,
    value_sql: str,
) -> None:
    mssql = open_mssql_connector()
    schema = f"nl_{uuid.uuid4().hex[:8]}"
    physical_source_table = "physical_contract"
    auto_source_table = "auto_inference"
    physical_target_table = f"nl_phys_{slug}_{uuid.uuid4().hex[:8]}"
    auto_target_table = f"nl_auto_{slug}_{uuid.uuid4().hex[:8]}"
    expected_physical_types = expected_physical_target_types(slug, source_type)
    expected_auto_types = expected_auto_target_types(slug, source_type)

    try:
        mssql.execute_query(f"EXEC('CREATE SCHEMA [{schema}]')")
        create_source_table(
            mssql,
            schema=schema,
            table=physical_source_table,
            columns_sql=physical_source_columns_sql(slug, source_type),
        )
        insert_source_rows(
            mssql,
            schema=schema,
            table=physical_source_table,
            rows_sql=physical_insert_rows_sql(value_sql),
        )
        load_and_assert(
            mssql=mssql,
            clickhouse_connector=clickhouse_connector,
            clickhouse_settings=clickhouse_settings,
            schema=schema,
            source_table=physical_source_table,
            target_table=physical_target_table,
            expected_target_types=expected_physical_types,
            physical_target_types=expected_physical_types,
        )
        assert_one_null(
            clickhouse_connector,
            clickhouse_settings.database,
            physical_target_table,
            f"{slug}_nullable_to_nullable",
        )

        create_source_table(
            mssql,
            schema=schema,
            table=auto_source_table,
            columns_sql=auto_source_columns_sql(slug, source_type),
        )
        insert_source_rows(
            mssql,
            schema=schema,
            table=auto_source_table,
            rows_sql=auto_insert_rows_sql(value_sql),
        )
        load_and_assert(
            mssql=mssql,
            clickhouse_connector=clickhouse_connector,
            clickhouse_settings=clickhouse_settings,
            schema=schema,
            source_table=auto_source_table,
            target_table=auto_target_table,
            expected_target_types=expected_auto_types,
            physical_target_types=None,
        )
        assert_one_null(clickhouse_connector, clickhouse_settings.database, auto_target_table, f"{slug}_nullable")
    finally:
        clickhouse_connector.execute_query(
            f"DROP TABLE IF EXISTS `{clickhouse_settings.database}`.`{physical_target_table}`"
        )
        clickhouse_connector.execute_query(
            f"DROP TABLE IF EXISTS `{clickhouse_settings.database}`.`{auto_target_table}`"
        )
        mssql.execute_query(f"DROP TABLE IF EXISTS [{schema}].[{physical_source_table}]")
        mssql.execute_query(f"DROP TABLE IF EXISTS [{schema}].[{auto_source_table}]")
        mssql.execute_query(f"DROP SCHEMA IF EXISTS [{schema}]")
        mssql.close()
