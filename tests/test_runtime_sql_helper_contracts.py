from __future__ import annotations

from dpone.runtime.sql_helpers.exchange_queries import ExchangeQueries
from dpone.runtime.sql_helpers.reconciliation_queries import ReconciliationQueries
from dpone.runtime.sql_helpers.run_state_queries import RunStateQueries
from dpone.runtime.sql_helpers.technical_columns_queries import TechnicalColumnsQueries
from dpone.runtime.sql_helpers.xmin_queries import XMinStateQueries


def compact_sql(sql: str) -> str:
    return " ".join(sql.split())


def test_postgres_technical_columns_queries_quote_identifiers_and_fill_defaults() -> None:
    assert TechnicalColumnsQueries.pg_add_load_dtm_column("public", "orders") == (
        'ALTER TABLE "public"."orders" ADD COLUMN IF NOT EXISTS "__dpone__loaded_at" '
        "TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP"
    )
    assert TechnicalColumnsQueries.pg_add_delete_dtm_column("public", "orders") == (
        'ALTER TABLE "public"."orders" ADD COLUMN IF NOT EXISTS "__dpone__deleted_at" TIMESTAMP WITH TIME ZONE'
    )
    assert TechnicalColumnsQueries.pg_update_load_dtm_null_rows("public", "orders") == (
        'UPDATE "public"."orders" SET "__dpone__loaded_at" = CURRENT_TIMESTAMP WHERE "__dpone__loaded_at" IS NULL'
    )

    insert_sql = compact_sql(
        TechnicalColumnsQueries.pg_build_insert_with_technical_columns(
            "mart",
            "orders",
            "staging",
            "orders_tmp",
            ["id", "payload"],
        )
    )
    assert insert_sql == (
        'INSERT INTO "mart"."orders" ("id", "payload", "__dpone__loaded_at", "__dpone__deleted_at") '
        'SELECT "id", "payload", CURRENT_TIMESTAMP, NULL FROM "staging"."orders_tmp"'
    )

    columns, select = TechnicalColumnsQueries.pg_build_select_with_technical_columns(["id"], table_alias="src")
    assert columns == '"id", "__dpone__loaded_at", "__dpone__deleted_at"'
    assert select == 'src."id", CURRENT_TIMESTAMP, NULL'


def test_bigquery_technical_columns_queries_parse_json_and_do_not_duplicate_existing_tech_columns() -> None:
    insert_sql = compact_sql(
        TechnicalColumnsQueries.bq_build_insert_with_technical_columns(
            "demo-project",
            "mart",
            "orders",
            "staging",
            "orders_tmp",
            ["id", "payload"],
            json_columns=["payload"],
        )
    )
    assert insert_sql == (
        "INSERT INTO `demo-project.mart.orders` (`id`, `payload`, `__dpone__loaded_at`, `__dpone__deleted_at`) "
        "SELECT `id`, PARSE_JSON(`payload`), CURRENT_TIMESTAMP(), CAST(NULL AS TIMESTAMP) "
        "FROM `demo-project.staging.orders_tmp`"
    )

    columns, select = TechnicalColumnsQueries.bq_build_select_with_technical_columns(
        ["id", "payload", "__dpone__loaded_at"],
        json_columns=["payload"],
        table_alias="src",
    )

    assert columns == "`id`, `payload`, `__dpone__loaded_at`, `__dpone__deleted_at`"
    assert select == (
        "src.`id`, PARSE_JSON(src.`payload`) AS `payload`, "
        "src.`__dpone__loaded_at`, CAST(NULL AS TIMESTAMP) AS `__dpone__deleted_at`"
    )
    assert TechnicalColumnsQueries.get_technical_column_names() == ["__dpone__loaded_at", "__dpone__deleted_at"]
    assert TechnicalColumnsQueries.get_technical_column_definitions_pg() == [
        ("__dpone__loaded_at", "TIMESTAMP WITH TIME ZONE"),
        ("__dpone__deleted_at", "TIMESTAMP WITH TIME ZONE"),
    ]
    assert TechnicalColumnsQueries.get_technical_column_definitions_bq() == [
        ("__dpone__loaded_at", "TIMESTAMP"),
        ("__dpone__deleted_at", "TIMESTAMP"),
    ]


def test_exchange_queries_build_atomic_swap_names_and_bigquery_sql() -> None:
    assert ExchangeQueries.get_backup_table_name("orders") == "orders__backup"
    assert ExchangeQueries.get_tmp_table_name("orders") == "orders__tmp"
    assert compact_sql(ExchangeQueries.pg_check_table_exists()).startswith("SELECT EXISTS")
    assert ExchangeQueries.pg_rename_table("public", "old", "new") == "ALTER TABLE public.{} RENAME TO {}"
    assert ExchangeQueries.pg_create_table_as("public", "orders", "SELECT 1") == "CREATE TABLE public.{} AS {}"
    assert ExchangeQueries.pg_drop_table("public", "orders") == "DROP TABLE IF EXISTS public.{}"

    assert compact_sql(ExchangeQueries.bq_check_table_exists("project", "dataset", "orders")) == (
        "SELECT COUNT(*) > 0 FROM `project.dataset.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'orders'"
    )
    assert ExchangeQueries.bq_rename_table("project", "dataset", "orders", "orders_backup") == (
        "ALTER TABLE `project.dataset.orders` RENAME TO `orders_backup`"
    )
    assert ExchangeQueries.bq_drop_table("project", "dataset", "orders") == (
        "DROP TABLE IF EXISTS `project.dataset.orders`"
    )


def test_run_state_and_xmin_queries_include_expected_bigquery_parameters() -> None:
    merge_run_state = compact_sql(RunStateQueries.merge_run_state("project.tech.run_state"))
    update_run_state = compact_sql(RunStateQueries.update_run_state("project.tech.run_state"))
    get_run_state = compact_sql(RunStateQueries.get_run_state("project.tech.run_state"))
    delete_old_states = compact_sql(RunStateQueries.delete_old_states("project.tech.run_state"))

    assert "MERGE `project.tech.run_state` AS tgt" in merge_run_state
    assert "@dag_id AS dag_id" in merge_run_state
    assert "WHEN MATCHED THEN UPDATE SET state = src.state" in merge_run_state
    assert "UPDATE `project.tech.run_state` SET state = @state" in update_run_state
    assert "WHERE dag_id = @dag_id AND execution_date = @execution_date" in get_run_state
    assert "INTERVAL @days DAY" in delete_old_states

    merge_xmin = compact_sql(XMinStateQueries.merge_state("project.tech.xmin_state"))
    load_xmin = compact_sql(XMinStateQueries.load_state("project.tech.xmin_state"))
    delete_xmin = compact_sql(XMinStateQueries.delete_state("project.tech.xmin_state"))

    assert "MERGE `project.tech.xmin_state` AS tgt" in merge_xmin
    assert "@xmin_value AS xmin_value" in merge_xmin
    assert "wraparound_detected = src.wraparound_detected" in merge_xmin
    assert "SELECT xmin_value, is_initial, wraparound_detected" in load_xmin
    assert "DELETE FROM `project.tech.xmin_state`" in delete_xmin


def test_reconciliation_queries_handle_single_and_composite_keys() -> None:
    single_key_sql = compact_sql(
        ReconciliationQueries.bq_soft_delete_update(
            "project",
            "mart",
            "orders",
            "tech",
            "orders__rs",
            ["id"],
            "__dpone__deleted_at",
            "__dpone__loaded_at",
        )
    )
    composite_sql = compact_sql(
        ReconciliationQueries.bq_soft_delete_update(
            "project",
            "mart",
            "orders",
            "tech",
            "orders__rs",
            ["id", "tenant_id"],
            "__dpone__deleted_at",
            "__dpone__loaded_at",
        )
    )

    assert "COALESCE(CAST(`id` AS STRING), '__NULL__') IN" in single_key_sql
    assert "STRUCT(COALESCE(CAST(`id` AS STRING), '__NULL__') AS `id`" in composite_sql
    assert "COALESCE(prev.`tenant_id`, '__NULL__') = COALESCE(curr.`tenant_id`, '__NULL__')" in composite_sql

    deleted_keys = compact_sql(
        ReconciliationQueries.bq_get_deleted_keys(
            "project",
            "tech",
            "orders__rs",
            ["id", "tenant_id"],
            "__dpone__loaded_at",
        )
    )
    deleted_log = compact_sql(
        ReconciliationQueries.bq_insert_deleted_log(
            "project",
            "tech",
            "orders__deleted_log",
            "orders__rs",
            ["id"],
        )
    )

    assert "SELECT prev.`id`, prev.`tenant_id` FROM `project.tech.orders__rs` prev" in deleted_keys
    assert "LIMIT 1 OFFSET 1" in deleted_keys
    assert (
        "INSERT INTO `project.tech.orders__deleted_log` (`id`, `__dpone__loaded_at`, `__dpone__deleted_at`)"
        in deleted_log
    )
    assert "CURRENT_TIMESTAMP() AS __dpone__deleted_at" in deleted_log
