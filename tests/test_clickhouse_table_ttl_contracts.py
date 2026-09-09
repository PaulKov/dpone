"""Fail-closed contracts for ClickHouse table-level TTL in physical_design."""

from __future__ import annotations

from dataclasses import replace

import pytest

from dpone.readiness.physical_design import PhysicalDesignOptions, PhysicalDesignPlanner
from dpone.readiness.physical_design_models import PhysicalReconciliationOptions
from dpone.readiness.physical_reconciliation import PhysicalDesignReconciler
from dpone.readiness.physical_state import PhysicalColumnState, PhysicalTableState
from dpone.runtime.physical_design.table_settings import TableSettingsOptions
from dpone.runtime.sinks.clickhouse_physical_reconciliation import (
    ClickHousePhysicalMigrationDialect,
    parse_clickhouse_table_ttl,
)
from dpone.runtime.sinks.clickhouse_shadow_migration import ClickHouseShadowMigrationDialect
from dpone.runtime.sinks.clickhouse_table_ddl import ClickHouseTableDdlRenderer, ClickHouseTableDesign


def test_clickhouse_ddl_renders_ttl_before_settings() -> None:
    design = ClickHouseTableDesign.from_options(
        {
            "physical_design": {
                "storage": {
                    "clickhouse": {
                        "engine": "ReplacingMergeTree(updated_at)",
                        "partition_by": "toYYYYMM(created_at)",
                        "order_by": ["project_execution_id", "id"],
                        "ttl": "created_at + toIntervalDay(7)",
                        "table_settings": {"ttl_only_drop_parts": 1},
                    }
                }
            }
        }
    )
    ddl = ClickHouseTableDdlRenderer().render_create_table(
        table="`price_calculation`.`price_calculated`",
        columns_sql=["`id` UInt64", "`created_at` DateTime"],
        design=design,
    )

    assert "TTL created_at + toIntervalDay(7)" in ddl
    assert "SETTINGS ttl_only_drop_parts = 1" in ddl
    assert ddl.index("ORDER BY") < ddl.index("TTL created_at")
    assert ddl.index("TTL created_at") < ddl.index("SETTINGS")


def test_clickhouse_ddl_omits_ttl_when_absent() -> None:
    design = ClickHouseTableDesign.from_options(
        {
            "physical_design": {
                "storage": {
                    "clickhouse": {
                        "order_by": ["id"],
                        "table_settings": {"ttl_only_drop_parts": 1},
                    }
                }
            }
        }
    )
    ddl = ClickHouseTableDdlRenderer().render_create_table(
        table="`landing`.`orders`",
        columns_sql=["`id` UInt64"],
        design=design,
    )

    assert " TTL " not in ddl
    assert ddl.endswith("SETTINGS ttl_only_drop_parts = 1")


def test_clickhouse_table_ttl_alias_is_accepted() -> None:
    design = ClickHouseTableDesign.from_options(
        {
            "physical_design": {
                "storage": {
                    "clickhouse": {
                        "order_by": ["id"],
                        "table_ttl": "created_at + toIntervalDay(7)",
                    }
                }
            }
        }
    )

    assert design.ttl == "created_at + toIntervalDay(7)"


@pytest.mark.parametrize(
    ("raw", "match"),
    [
        ("", "non-empty"),
        ("   ", "non-empty"),
        ("TTL created_at + toIntervalDay(7)", "without the TTL keyword"),
        ("ttl created_at + toIntervalDay(7)", "without the TTL keyword"),
        ("created_at + toIntervalDay(7); DROP TABLE x", ";"),
        ("created_at + toIntervalDay(7)\x00", "null bytes"),
    ],
)
def test_clickhouse_ttl_expression_is_fail_closed(raw: str, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        ClickHouseTableDesign.from_options(
            {"physical_design": {"storage": {"clickhouse": {"order_by": ["id"], "ttl": raw}}}}
        )


def test_parse_clickhouse_table_ttl_from_create_query() -> None:
    create_query = (
        "CREATE TABLE price_calculation.price_calculated (`id` UInt64, `created_at` DateTime) "
        "ENGINE = ReplacingMergeTree(updated_at) PARTITION BY toYYYYMM(created_at) "
        "ORDER BY (project_execution_id, id) TTL created_at + toIntervalDay(7) "
        "SETTINGS ttl_only_drop_parts = 1"
    )

    assert parse_clickhouse_table_ttl(create_query) == "created_at + toIntervalDay(7)"
    assert parse_clickhouse_table_ttl("CREATE TABLE t (id UInt64) ENGINE = MergeTree ORDER BY id") is None


def test_clickhouse_ttl_drift_is_shadow_required() -> None:
    desired = PhysicalTableState(
        sink_type="clickhouse",
        table="landing.orders",
        columns={"id": PhysicalColumnState(name="id", target_type="Int64", nullable=False)},
        engine="MergeTree",
        order_by=("id",),
        ttl="created_at + toIntervalDay(7)",
    )
    actual = replace(desired, ttl="created_at + toIntervalDay(14)")

    plan = PhysicalDesignReconciler().reconcile(
        desired=desired,
        actual=actual,
        options=PhysicalReconciliationOptions(mode="auto_safe"),
        dialect=ClickHousePhysicalMigrationDialect(),
    )

    assert "physical_design.shadow_required:ttl" in plan.blockers
    assert plan.ddl == ()


def test_clickhouse_shadow_create_includes_ttl() -> None:
    plan = PhysicalDesignPlanner().plan(
        sink_type="clickhouse",
        table="landing.orders",
        source_schema=[("id", "bigint"), ("created_at", "datetime")],
        options=PhysicalDesignOptions.from_config(
            {
                "storage": {
                    "clickhouse": {
                        "order_by": ["id"],
                        "ttl": "created_at + toIntervalDay(7)",
                        "table_settings": {"ttl_only_drop_parts": 1},
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

    assert desired.ttl == "created_at + toIntervalDay(7)"
    assert "TTL created_at + toIntervalDay(7)" in ddl
    assert "SETTINGS ttl_only_drop_parts = 1" in ddl


def test_physical_table_state_round_trips_ttl() -> None:
    state = PhysicalTableState(
        sink_type="clickhouse",
        table="landing.orders",
        engine="MergeTree",
        order_by=("id",),
        ttl="created_at + toIntervalDay(7)",
        table_settings={"ttl_only_drop_parts": 1},
    )

    payload = state.to_dict()
    restored = PhysicalTableState.from_mapping(payload)

    assert payload["ttl"] == "created_at + toIntervalDay(7)"
    assert restored.ttl == "created_at + toIntervalDay(7)"


def test_clickhouse_ttl_with_settings_object_construction() -> None:
    ddl = ClickHouseTableDdlRenderer().render_create_table(
        table="`landing`.`orders`",
        columns_sql=["`created_at` DateTime"],
        design=ClickHouseTableDesign(
            order_by=("created_at",),
            ttl="created_at + toIntervalDay(7)",
            table_settings=TableSettingsOptions.from_config(
                {"ttl_only_drop_parts": 1},
                field_prefix="physical_design.storage.clickhouse.table_settings",
            ),
        ),
    )

    assert "ORDER BY (`created_at`) TTL created_at + toIntervalDay(7) SETTINGS" in ddl
