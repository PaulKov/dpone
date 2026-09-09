from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft7Validator

from dpone.readiness.physical_design import PhysicalDesignOptions, PhysicalDesignPlanner
from dpone.readiness.physical_state import PhysicalTableState
from dpone.runtime.sinks.clickhouse_shadow_migration import ClickHouseShadowMigrationDialect


def test_clickhouse_physical_plan_renders_cluster_and_distributed_access_table() -> None:
    plan = PhysicalDesignPlanner().plan(
        sink_type="clickhouse",
        table="landing.orders_local",
        source_schema=[("id", "bigint"), ("status", "text")],
        options=PhysicalDesignOptions.from_config(
            {
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
        ),
    )

    assert plan.ddl == [
        "CREATE TABLE `landing`.`orders_local` ON CLUSTER `dwh` "
        "(`id` Int64, `status` String) "
        "ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY (`id`);",
        "CREATE TABLE IF NOT EXISTS `landing`.`orders_all` ON CLUSTER `dwh` "
        "AS `landing`.`orders_local` "
        "ENGINE = Distributed('dwh', 'landing', 'orders_local', cityHash64(id));",
    ]


def test_clickhouse_physical_plan_supports_distributed_access_without_cluster_wide_ddl() -> None:
    plan = PhysicalDesignPlanner().plan(
        sink_type="clickhouse",
        table="landing.orders_local",
        source_schema=[("id", "bigint")],
        options=PhysicalDesignOptions.from_config(
            {
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
        ),
    )

    assert plan.ddl == [
        "CREATE TABLE `landing`.`orders_local` (`id` Int64) ENGINE = MergeTree ORDER BY (`id`);",
        "CREATE TABLE IF NOT EXISTS `landing`.`orders_all` AS `landing`.`orders_local` "
        "ENGINE = Distributed('dwh', 'landing', 'orders_local', cityHash64(id));",
    ]


def test_clickhouse_physical_plan_blocks_distributed_access_without_cluster_name() -> None:
    with pytest.raises(ValueError, match="access_table requires cluster name"):
        PhysicalDesignPlanner().plan(
            sink_type="clickhouse",
            table="landing.orders_local",
            source_schema=[("id", "bigint")],
            options=PhysicalDesignOptions.from_config(
                {
                    "storage": {
                        "clickhouse": {
                            "order_by": ["id"],
                            "access_table": {"name": "orders_all", "engine": "Distributed"},
                        }
                    }
                }
            ),
        )


def test_manifest_schema_exposes_clickhouse_cluster_and_access_table_contract() -> None:
    config_schema = json.loads(Path("src/dpone/schema/etl-config.schema.json").read_text(encoding="utf-8"))
    batch_schema = json.loads(Path("src/dpone/schema/etl-batch-manifest.schema.json").read_text(encoding="utf-8"))

    for schema in (config_schema, batch_schema):
        sink_options = (
            schema["properties"]["sink"]["properties"]["options"]["properties"]
            if "sink" in schema.get("properties", {})
            else schema["definitions"]["process_fragment"]["properties"]["sink"]["properties"]["options"]["properties"]
        )
        clickhouse_storage = sink_options["physical_design"]["properties"]["storage"]["properties"]["clickhouse"][
            "properties"
        ]

        cluster_variants = clickhouse_storage["cluster"]["oneOf"]
        assert {"type": "string", "minLength": 1} in cluster_variants
        cluster_object = next(item for item in cluster_variants if item.get("type") == "object")
        assert cluster_object["properties"]["name"]["type"] == "string"
        assert cluster_object["properties"]["ddl_scope"]["enum"] == ["local", "cluster"]
        assert cluster_object["properties"]["on_cluster"]["default"] is False
        assert clickhouse_storage["access_table"]["properties"]["engine"]["enum"] == ["Distributed"]
        assert clickhouse_storage["access_table"]["properties"]["sharding_key"]["type"] == "string"


@pytest.mark.parametrize(
    "cluster_payload",
    [
        "dwh",
        {"name": "dwh", "ddl_scope": "cluster"},
        {"name": "dwh", "ddl_scope": "local"},
        {"name": "dwh", "on_cluster": True},
    ],
)
def test_manifest_schema_validates_clickhouse_cluster_contract_variants(cluster_payload: object) -> None:
    config_schema = json.loads(Path("src/dpone/schema/etl-config.schema.json").read_text(encoding="utf-8"))
    batch_schema = json.loads(Path("src/dpone/schema/etl-batch-manifest.schema.json").read_text(encoding="utf-8"))

    for schema in (config_schema, batch_schema):
        sink_options = (
            schema["properties"]["sink"]["properties"]["options"]["properties"]
            if "sink" in schema.get("properties", {})
            else schema["definitions"]["process_fragment"]["properties"]["sink"]["properties"]["options"]["properties"]
        )
        cluster_schema = sink_options["physical_design"]["properties"]["storage"]["properties"]["clickhouse"][
            "properties"
        ]["cluster"]

        assert Draft7Validator(cluster_schema).is_valid(cluster_payload)
        assert not Draft7Validator(cluster_schema).is_valid("")


def test_physical_table_state_preserves_cluster_contract_for_shadow_migration() -> None:
    plan = PhysicalDesignPlanner().plan(
        sink_type="clickhouse",
        table="landing.orders_local",
        source_schema=[("id", "bigint")],
        options=PhysicalDesignOptions.from_config(
            {
                "storage": {
                    "clickhouse": {
                        "cluster": "dwh",
                        "order_by": ["id"],
                    }
                }
            }
        ),
    )
    desired = PhysicalTableState.from_physical_plan(plan)

    ddl = ClickHouseShadowMigrationDialect().render_create_shadow(
        desired=desired,
        shadow_table="landing.orders_shadow",
    )

    assert desired.cluster == "dwh"
    assert desired.on_cluster is True
    assert "CREATE TABLE `landing`.`orders_shadow` ON CLUSTER `dwh`" in ddl


def test_physical_table_state_exposes_access_table_contract_in_payload() -> None:
    plan = PhysicalDesignPlanner().plan(
        sink_type="clickhouse",
        table="landing.orders_local",
        source_schema=[("id", "bigint")],
        options=PhysicalDesignOptions.from_config(
            {
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
        ),
    )

    payload = PhysicalTableState.from_physical_plan(plan).to_dict()

    assert payload["cluster"] == "dwh"
    assert payload["on_cluster"] is True
    assert payload["access_table"] == "orders_all"
    assert payload["access_table_engine"] == "Distributed"
    assert payload["access_table_sharding_key"] == "cityHash64(id)"
