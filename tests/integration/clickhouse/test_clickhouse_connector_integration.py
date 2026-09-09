from __future__ import annotations

import csv
import os
import uuid
from pathlib import Path

import pytest

from dpone.config import LoadConfig
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.etl.lifecycle import RuntimeLifecycleService
from dpone.runtime.physical_design.table_settings import TableSettingsOptions
from dpone.runtime.sinks.clickhouse_sink import ClickHouseSink
from dpone.runtime.sinks.clickhouse_table_ddl import ClickHouseTableDdlRenderer, ClickHouseTableDesign
from dpone.runtime.sinks.load_payload import LoadPayload

pytestmark = [pytest.mark.integration, pytest.mark.integration_clickhouse]
if str(os.getenv("DPONE_RUN_INTEGRATION", "0")).strip().lower() not in {"1", "true", "yes", "on"}:
    pytest.skip("Integration tests are disabled", allow_module_level=True)

pytest.importorskip("clickhouse_driver")


def _table_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


def test_clickhouse_connector_executes_queries_reads_rows_and_streams(
    clickhouse_connector, clickhouse_settings
) -> None:
    table = _table_name("it_items")
    db = clickhouse_settings.database
    clickhouse_connector.execute_query(
        f"CREATE TABLE `{db}`.`{table}` (id Int32, name String) ENGINE = MergeTree ORDER BY id"
    )
    try:
        clickhouse_connector.execute_query(f"INSERT INTO `{db}`.`{table}` (id, name) VALUES (1, 'alice'), (2, 'bob')")

        rows = clickhouse_connector.get_records(
            f"SELECT id, name FROM `{db}`.`{table}` ORDER BY id",
            as_dict=True,
        )
        assert rows == [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]

        streamed = list(
            clickhouse_connector.get_records_streaming(
                f"SELECT id, name FROM `{db}`.`{table}` ORDER BY id",
                batch_size=1,
                as_dict=False,
            )
        )
        assert streamed == [[(1, "alice")], [(2, "bob")]]

        query = clickhouse_connector.build_select_query(db, table, ["id", "name"], limit=1, offset=1)
        assert query == f"SELECT `id`, `name` FROM `{db}`.`{table}` LIMIT 1 OFFSET 1"
        limited = clickhouse_connector.get_records(query, as_dict=True)
        assert limited == [{"id": 2, "name": "bob"}]
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{db}`.`{table}`")


def test_clickhouse_connector_imports_rows_from_csv(clickhouse_connector, clickhouse_settings, tmp_path: Path) -> None:
    table = _table_name("it_csv")
    db = clickhouse_settings.database
    clickhouse_connector.execute_query(
        f"CREATE TABLE `{db}`.`{table}` (id Int32, city String) ENGINE = MergeTree ORDER BY id"
    )

    csv_path = tmp_path / "cities.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["id", "city"])
        writer.writerow([1, "Paris"])
        writer.writerow([2, "Berlin"])

    try:
        clickhouse_connector.import_from_csv(db, table, str(csv_path), batch_rows=1)
        rows = clickhouse_connector.get_records(
            f"SELECT id, city FROM `{db}`.`{table}` ORDER BY id",
            as_dict=True,
        )
        assert rows == [{"id": 1, "city": "Paris"}, {"id": 2, "city": "Berlin"}]
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{db}`.`{table}`")


def test_clickhouse_partition_helpers_work_on_real_table(clickhouse_connector, clickhouse_settings) -> None:
    table = _table_name("it_partitions")
    db = clickhouse_settings.database
    clickhouse_connector.execute_query(
        f"CREATE TABLE `{db}`.`{table}` ("
        "id Int32, event_date Date, created_at DateTime"
        ") ENGINE = MergeTree ORDER BY (event_date, id)"
    )
    try:
        clickhouse_connector.execute_query(
            f"INSERT INTO `{db}`.`{table}` (id, event_date, created_at) VALUES "
            "(1, '2026-03-01', '2026-03-01 08:00:00'),"
            "(2, '2026-03-02', '2026-03-02 09:00:00'),"
            "(3, '2026-03-02', '2026-03-02 10:00:00'),"
            "(4, '2026-04-01', '2026-04-01 11:00:00')"
        )

        partitions = clickhouse_connector.generate_date_partitions(
            "2026-03-01",
            "2026-03-03",
            partition_by="day",
            date_column="event_date",
        )
        assert partitions == [
            ("2026-03-01", "toDate(`event_date`) = '2026-03-01'"),
            ("2026-03-02", "toDate(`event_date`) = '2026-03-02'"),
            ("2026-03-03", "toDate(`event_date`) = '2026-03-03'"),
        ]

        nonempty = clickhouse_connector.discover_nonempty_partitions(
            schema=db,
            table=table,
            date_column="event_date",
            date_from="2026-03-01",
            date_to="2026-03-31",
            partition_by="day",
        )
        assert nonempty == {"2026-03-01", "2026-03-02"}

        assert clickhouse_connector.count_rows_by_date(db, table, "event_date", "2026-03-02") == 2
        assert (
            clickhouse_connector.count_rows_by_date(
                db,
                table,
                "event_date",
                "2026-03-01",
                partition_by="month",
            )
            == 3
        )
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{db}`.`{table}`")


@pytest.mark.parametrize(
    ("case_name", "key_type", "table_settings", "expect_success"),
    [
        ("not_nullable_key_no_settings", "String", None, True),
        ("not_nullable_key_allow_nullable_key", "String", {"allow_nullable_key": 1}, True),
        ("not_nullable_key_index_granularity", "String", {"index_granularity": 4096}, True),
        ("nullable_key_no_settings", "Nullable(String)", None, False),
        ("nullable_key_allow_nullable_key_zero", "Nullable(String)", {"allow_nullable_key": 0}, False),
        ("nullable_key_allow_nullable_key_false", "Nullable(String)", {"allow_nullable_key": False}, False),
        ("nullable_key_allow_nullable_key_true", "Nullable(String)", {"allow_nullable_key": True}, True),
        (
            "nullable_key_allow_nullable_key_and_index_granularity",
            "Nullable(String)",
            {"allow_nullable_key": 1, "index_granularity": 4096},
            True,
        ),
    ],
    ids=[
        "not-null/no-settings",
        "not-null/allow-nullable-key",
        "not-null/index-granularity",
        "nullable/no-settings",
        "nullable/allow-nullable-key-zero",
        "nullable/allow-nullable-key-false",
        "nullable/allow-nullable-key-true",
        "nullable/allow-nullable-key-and-index-granularity",
    ],
)
def test_clickhouse_nullable_key_table_settings_live_matrix(
    clickhouse_connector,
    clickhouse_settings,
    case_name: str,
    key_type: str,
    table_settings: dict[str, object] | None,
    expect_success: bool,
) -> None:
    table = _table_name(f"it_{case_name}")
    db = clickhouse_settings.database
    renderer = ClickHouseTableDdlRenderer()
    columns_sql = [f"`optional_code` {key_type}", "`payload` String"]
    design = ClickHouseTableDesign(
        order_by=("optional_code",),
        table_settings=TableSettingsOptions.from_config(
            table_settings,
            field_prefix="physical_design.storage.clickhouse.table_settings",
        ),
    )
    ddl = renderer.render_create_table(
        table=f"`{db}`.`{table}`",
        columns_sql=columns_sql,
        design=design,
    )

    try:
        if not expect_success:
            with pytest.raises(Exception, match="allow_nullable_key|Nullable"):
                clickhouse_connector.execute_query(ddl)
            return

        clickhouse_connector.execute_query(ddl)
        value_sql = "NULL" if key_type.startswith("Nullable(") else "'A-001'"
        clickhouse_connector.execute_query(
            f"INSERT INTO `{db}`.`{table}` (`optional_code`, `payload`) VALUES ({value_sql}, 'ok')"
        )

        rows = clickhouse_connector.get_records(
            f"SELECT count(), countIf(isNull(`optional_code`)) FROM `{db}`.`{table}`"
        )[0]
        expected_nulls = 1 if key_type.startswith("Nullable(") else 0
        assert rows == (1, expected_nulls)
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{db}`.`{table}`")


def test_clickhouse_physical_reconciliation_auto_safe_modifies_table_setting(
    clickhouse_connector,
    clickhouse_settings,
) -> None:
    table = _table_name("it_physical_reconcile")
    db = clickhouse_settings.database
    clickhouse_connector.execute_query(
        f"CREATE TABLE `{db}`.`{table}` (`id` Int64) ENGINE = MergeTree ORDER BY `id` "
        "SETTINGS min_rows_for_wide_part = 0"
    )
    try:
        report = RuntimeLifecycleService()._apply_physical_design(
            _physical_reconciliation_config(db, table, mode="auto_safe"),
            ClickHouseSink(clickhouse_connector),
            _physical_reconciliation_payload(),
        )

        assert report is not None
        assert report["applied"] is True
        assert report["ddl"] == [f"ALTER TABLE `{db}`.`{table}` MODIFY SETTING min_rows_for_wide_part = 8192"]
        create_query = clickhouse_connector.get_records(
            f"SELECT create_table_query FROM system.tables WHERE database = '{db}' AND name = '{table}'"
        )[0][0]
        assert "min_rows_for_wide_part = 8192" in create_query
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{db}`.`{table}`")


def test_clickhouse_physical_reconciliation_blocks_order_by_drift(
    clickhouse_connector,
    clickhouse_settings,
) -> None:
    table = _table_name("it_physical_order_drift")
    db = clickhouse_settings.database
    clickhouse_connector.execute_query(
        f"CREATE TABLE `{db}`.`{table}` (`id` Int64, `other_id` Int64) ENGINE = MergeTree ORDER BY `other_id` "
        "SETTINGS min_rows_for_wide_part = 8192"
    )
    try:
        report = RuntimeLifecycleService()._apply_physical_design(
            _physical_reconciliation_config(db, table, mode="auto_safe"),
            ClickHouseSink(clickhouse_connector),
            _physical_reconciliation_payload(),
        )

        assert report is not None
        assert "physical_design.shadow_required:order_by" in report["blockers"]
        assert report["ddl"] == []
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{db}`.`{table}`")


def test_clickhouse_physical_reconciliation_noop_for_matching_design(
    clickhouse_connector,
    clickhouse_settings,
) -> None:
    table = _table_name("it_physical_noop")
    db = clickhouse_settings.database
    clickhouse_connector.execute_query(
        f"CREATE TABLE `{db}`.`{table}` (`id` Int64) ENGINE = MergeTree ORDER BY `id` "
        "SETTINGS min_rows_for_wide_part = 8192"
    )
    try:
        report = RuntimeLifecycleService()._apply_physical_design(
            _physical_reconciliation_config(db, table, mode="auto_safe"),
            ClickHouseSink(clickhouse_connector),
            _physical_reconciliation_payload(),
        )

        assert report is not None
        assert report["has_drift"] is False
        assert report["blockers"] == []
        assert report["ddl"] == []
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{db}`.`{table}`")


def _physical_reconciliation_config(database: str, table: str, *, mode: str) -> LoadConfig:
    return LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema=database,
        target_table=table,
        options={
            "sink_type": "clickhouse",
            "physical_design": {
                "apply_runtime": True,
                "reconciliation": {"mode": mode},
                "columns": {
                    "id": {
                        "target_type": {
                            "clickhouse": "Int64",
                        }
                    }
                },
                "storage": {
                    "clickhouse": {
                        "order_by": ["id"],
                        "table_settings": {"min_rows_for_wide_part": 8192},
                    }
                },
            },
        },
    )


def _physical_reconciliation_payload() -> LoadPayload:
    return LoadPayload(artifact=InMemoryRowsArtifact([{"id": 1}]), schema=[("id", "bigint")])
