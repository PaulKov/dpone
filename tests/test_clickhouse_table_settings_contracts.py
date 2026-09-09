from __future__ import annotations

from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig
from dpone.readiness.physical_design import PhysicalDesignOptions, PhysicalDesignPlanner
from dpone.runtime.physical_design.table_settings import (
    PhysicalDesignAdvisor,
    PhysicalSettingClassifier,
    PhysicalSettingRegistry,
    TableSettingsOptions,
)
from dpone.runtime.sinks.clickhouse_sql_mixin import ClickHouseSqlMixin
from dpone.runtime.sinks.clickhouse_table_ddl import ClickHouseTableDdlRenderer, ClickHouseTableDesign


def test_table_settings_options_normalizes_scalar_values_in_deterministic_order() -> None:
    options = TableSettingsOptions.from_config(
        {
            "index_granularity": 8192,
            "allow_nullable_key": True,
            "storage_policy": "hot'ssd",
        },
        field_prefix="physical_design.storage.clickhouse.table_settings",
    )

    assert list(options.settings) == ["allow_nullable_key", "index_granularity", "storage_policy"]
    assert options.settings["allow_nullable_key"] is True
    assert options.settings["storage_policy"] == "hot'ssd"


@pytest.mark.parametrize(
    ("raw", "match"),
    [
        ({"bad-name": 1}, "Invalid table setting name"),
        ({"nested": {"enabled": True}}, "must be a scalar"),
        ({"items": ["a"]}, "must be a scalar"),
        ({"empty": None}, "must be a scalar"),
    ],
)
def test_table_settings_options_rejects_unsafe_names_and_non_scalar_values(raw: dict, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        TableSettingsOptions.from_config(raw, field_prefix="physical_design.storage.clickhouse.table_settings")


def test_clickhouse_ddl_renderer_renders_table_settings_deterministically() -> None:
    ddl = ClickHouseTableDdlRenderer().render_create_table(
        table="`landing`.`orders`",
        columns_sql=["`optional_code` Nullable(String)"],
        design=ClickHouseTableDesign(
            order_by=("optional_code",),
            table_settings=TableSettingsOptions.from_config(
                {
                    "index_granularity": 8192,
                    "allow_nullable_key": True,
                    "storage_policy": "hot'ssd",
                },
                field_prefix="physical_design.storage.clickhouse.table_settings",
            ),
        ),
    )

    assert ddl.endswith("SETTINGS allow_nullable_key = 1, index_granularity = 8192, storage_policy = 'hot''ssd'")


def test_clickhouse_ddl_renderer_renders_cluster_and_distributed_access_table() -> None:
    renderer = ClickHouseTableDdlRenderer()
    design = ClickHouseTableDesign.from_options(
        {
            "physical_design": {
                "storage": {
                    "clickhouse": {
                        "engine": "ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')",
                        "cluster": "dwh",
                        "order_by": ["id"],
                        "access_table": {
                            "name": "orders_all",
                            "engine": "Distributed",
                            "sharding_key": "cityHash64(id)",
                        },
                    }
                }
            }
        }
    )

    statements = renderer.render_create_table_statements(
        table="`landing`.`orders_local`",
        columns_sql=["`id` UInt64"],
        design=design,
    )

    assert statements == (
        "CREATE TABLE `landing`.`orders_local` ON CLUSTER `dwh` (`id` UInt64) "
        "ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY (`id`)",
        "CREATE TABLE IF NOT EXISTS `landing`.`orders_all` ON CLUSTER `dwh` "
        "AS `landing`.`orders_local` "
        "ENGINE = Distributed('dwh', 'landing', 'orders_local', cityHash64(id))",
    )


@pytest.mark.parametrize(
    ("cluster_config", "expected_clause"),
    [
        (None, ""),
        ("dwh", " ON CLUSTER `dwh`"),
        ({"name": "dwh", "ddl_scope": "cluster"}, " ON CLUSTER `dwh`"),
        ({"name": "dwh", "ddl_scope": "local"}, ""),
        ({"name": "dwh", "on_cluster": True}, " ON CLUSTER `dwh`"),
        ({"name": "dwh", "on_cluster": False}, ""),
    ],
)
def test_clickhouse_cluster_contract_supports_shorthand_scope_and_legacy_on_cluster(
    cluster_config: object,
    expected_clause: str,
) -> None:
    clickhouse_options: dict[str, object] = {"order_by": ["id"]}
    if cluster_config is not None:
        clickhouse_options["cluster"] = cluster_config
    design = ClickHouseTableDesign.from_options({"physical_design": {"storage": {"clickhouse": clickhouse_options}}})
    renderer = ClickHouseTableDdlRenderer()

    database_ddl = renderer.render_create_database(database="landing", design=design)
    table_ddl = renderer.render_create_table(
        table="`landing`.`orders`",
        columns_sql=["`id` UInt64"],
        design=design,
    )

    assert database_ddl == f"CREATE DATABASE IF NOT EXISTS `landing`{expected_clause}"
    assert f"CREATE TABLE `landing`.`orders`{expected_clause}" in table_ddl


def test_clickhouse_distributed_access_table_requires_cluster_name() -> None:
    design = ClickHouseTableDesign.from_options(
        {
            "physical_design": {
                "storage": {
                    "clickhouse": {
                        "order_by": ["id"],
                        "access_table": {"name": "orders_all", "engine": "Distributed"},
                    }
                }
            }
        }
    )

    with pytest.raises(ValueError, match="access_table requires cluster name"):
        ClickHouseTableDdlRenderer().render_create_table_statements(
            table="`landing`.`orders_local`",
            columns_sql=["`id` UInt64"],
            design=design,
        )


def test_clickhouse_distributed_access_table_can_use_cluster_without_cluster_wide_ddl() -> None:
    design = ClickHouseTableDesign.from_options(
        {
            "physical_design": {
                "storage": {
                    "clickhouse": {
                        "cluster": {"name": "dwh", "ddl_scope": "local"},
                        "order_by": ["id"],
                        "access_table": {
                            "name": "orders_all",
                            "engine": "Distributed",
                            "sharding_key": "cityHash64(id)",
                        },
                    }
                }
            }
        }
    )

    statements = ClickHouseTableDdlRenderer().render_create_table_statements(
        table="`landing`.`orders_local`",
        columns_sql=["`id` UInt64"],
        design=design,
    )

    assert " ON CLUSTER " not in statements[0]
    assert statements[1] == (
        "CREATE TABLE IF NOT EXISTS `landing`.`orders_all` AS `landing`.`orders_local` "
        "ENGINE = Distributed('dwh', 'landing', 'orders_local', cityHash64(id))"
    )


def test_clickhouse_table_settings_reject_wrong_context_insert_settings() -> None:
    design = ClickHouseTableDesign(
        table_settings=TableSettingsOptions.from_config(
            {"async_insert": 1},
            field_prefix="physical_design.storage.clickhouse.table_settings",
        )
    )

    with pytest.raises(ValueError, match="clickhouse_bulk.insert_settings.async_insert"):
        ClickHouseTableDdlRenderer().render_create_table(
            table="`landing`.`orders`",
            columns_sql=["`id` Int32"],
            design=design,
        )


def test_clickhouse_setting_classifier_marks_escape_hatch_and_unknown_settings() -> None:
    options = TableSettingsOptions.from_config(
        {
            "allow_nullable_key": 1,
            "future_merge_tree_setting": "on",
        },
        field_prefix="physical_design.storage.clickhouse.table_settings",
    )
    decisions = PhysicalSettingClassifier(PhysicalSettingRegistry.clickhouse()).resolve(options)
    by_key = {decision.key: decision for decision in decisions}

    assert by_key["allow_nullable_key"].support_level == "escape_hatch"
    assert by_key["allow_nullable_key"].risk == "nullable_key_escape_hatch"
    assert by_key["allow_nullable_key"].docs_url.endswith("#allow_nullable_key")
    assert by_key["future_merge_tree_setting"].setting_class == "unknown_target_setting"
    assert by_key["future_merge_tree_setting"].support_level == "unknown"
    assert any("allow_nullable_key" in warning for warning in PhysicalDesignAdvisor().warnings(decisions))


def test_clickhouse_runtime_create_table_uses_table_settings() -> None:
    class FakeConnector:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def execute_query(self, query: str) -> None:
            self.queries.append(query)

    class Sink(ClickHouseSqlMixin):
        def __init__(self) -> None:
            self.connector = FakeConnector()
            self.logger = SimpleNamespace()

    load_config = LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        options={
            "physical_design": {
                "storage": {
                    "clickhouse": {
                        "order_by": ["optional_code"],
                        "table_settings": {"allow_nullable_key": 1},
                    }
                }
            }
        },
    )

    sink = Sink()
    sink._create_table(load_config, [("optional_code", "nvarchar(255) nullable")], if_not_exists=True)

    assert "ORDER BY (`optional_code`) SETTINGS allow_nullable_key = 1" in sink.connector.queries[-1]


def test_clickhouse_runtime_create_table_uses_on_cluster_for_database_and_table() -> None:
    class FakeConnector:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def execute_query(self, query: str) -> None:
            self.queries.append(query)

    class Sink(ClickHouseSqlMixin):
        def __init__(self) -> None:
            self.connector = FakeConnector()
            self.logger = SimpleNamespace()

    load_config = LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders_local",
        options={
            "physical_design": {
                "storage": {
                    "clickhouse": {
                        "cluster": "dwh",
                        "engine": "ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')",
                        "order_by": ["id"],
                    }
                }
            }
        },
    )

    sink = Sink()
    sink._create_table(load_config, [("id", "bigint")], if_not_exists=True)

    assert sink.connector.queries[0] == "CREATE DATABASE IF NOT EXISTS `landing` ON CLUSTER `dwh`"
    assert "CREATE TABLE IF NOT EXISTS `landing`.`orders_local` ON CLUSTER `dwh`" in sink.connector.queries[1]
    assert "ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')" in sink.connector.queries[1]


def test_clickhouse_runtime_create_table_creates_distributed_access_table_when_configured() -> None:
    class FakeConnector:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def execute_query(self, query: str) -> None:
            self.queries.append(query)

    class Sink(ClickHouseSqlMixin):
        def __init__(self) -> None:
            self.connector = FakeConnector()
            self.logger = SimpleNamespace()

    load_config = LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders_local",
        options={
            "physical_design": {
                "storage": {
                    "clickhouse": {
                        "cluster": "dwh",
                        "order_by": ["id"],
                        "access_table": {
                            "name": "orders_all",
                            "engine": "Distributed",
                            "sharding_key": "cityHash64(id)",
                        },
                    }
                }
            }
        },
    )

    sink = Sink()
    sink._create_table(load_config, [("id", "bigint")], if_not_exists=True)

    assert sink.connector.queries == [
        "CREATE DATABASE IF NOT EXISTS `landing` ON CLUSTER `dwh`",
        "CREATE TABLE IF NOT EXISTS `landing`.`orders_local` ON CLUSTER `dwh` "
        "(`id` Int64) ENGINE = MergeTree ORDER BY (`id`)",
        "CREATE TABLE IF NOT EXISTS `landing`.`orders_all` ON CLUSTER `dwh` "
        "AS `landing`.`orders_local` ENGINE = Distributed('dwh', 'landing', 'orders_local', cityHash64(id))",
    ]


def test_physical_design_plan_exposes_resolved_table_settings_and_warnings() -> None:
    plan = PhysicalDesignPlanner().plan(
        sink_type="clickhouse",
        table="landing.orders",
        source_schema=[("optional_code", "text")],
        options=PhysicalDesignOptions.from_config(
            {
                "storage": {
                    "clickhouse": {
                        "order_by": ["optional_code"],
                        "table_settings": {"allow_nullable_key": 1},
                    }
                }
            }
        ),
    )
    payload = plan.to_dict()

    assert "SETTINGS allow_nullable_key = 1" in payload["ddl"][0]
    assert payload["resolved_table_settings"][0]["key"] == "allow_nullable_key"
    assert payload["resolved_table_settings"][0]["support_level"] == "escape_hatch"
    assert any("allow_nullable_key" in warning for warning in payload["warnings"])
