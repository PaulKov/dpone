from __future__ import annotations

from dataclasses import replace

from dpone.readiness.physical_state import PhysicalColumnState, PhysicalTableState
from dpone.readiness.shadow_migration import (
    ColumnProjectionPlanner,
    ShadowMigrationPlanner,
    ShadowMigrationStrategy,
)
from dpone.runtime.sinks.clickhouse_shadow_migration import ClickHouseShadowMigrationDialect


def test_shadow_required_order_by_drift_becomes_phased_plan_when_enabled() -> None:
    desired = _desired_state(order_by=("id",))
    actual = _desired_state(order_by=())

    plan = ShadowMigrationPlanner(dialect=ClickHouseShadowMigrationDialect()).plan(
        desired=desired,
        actual=actual,
        strategy=ShadowMigrationStrategy.SHADOW,
        changes=(
            {
                "change_type": "order_by",
                "path": "order_by",
                "desired": ["id"],
                "actual": [],
            },
        ),
    )

    assert plan.blockers == ()
    assert plan.strategy == "shadow"
    assert [phase.name for phase in plan.phases] == [
        "prepare",
        "create_shadow",
        "backfill",
        "validate",
        "cutover",
        "contract",
    ]
    assert plan.shadow_table.startswith("landing.__dpone_shadow_orders_")
    assert any(
        "CREATE TABLE" in op.sql and "__dpone_shadow_orders_" in op.sql for op in plan.phase("create_shadow").operations
    )
    assert any("INSERT INTO" in op.sql and "SELECT `id`" in op.sql for op in plan.phase("backfill").operations)
    assert any("EXCHANGE TABLES" in op.sql for op in plan.phase("cutover").operations)
    assert plan.rollback["supported"] is True
    assert plan.rollback["supported_until_phase"] == "contract"


def test_shadow_planner_keeps_blocker_when_strategy_is_not_shadow() -> None:
    desired = _desired_state(order_by=("id",))
    actual = _desired_state(order_by=())

    plan = ShadowMigrationPlanner(dialect=ClickHouseShadowMigrationDialect()).plan(
        desired=desired,
        actual=actual,
        strategy=ShadowMigrationStrategy.BLOCK,
        changes=({"change_type": "order_by", "path": "order_by"},),
    )

    assert plan.phases == ()
    assert "migration.shadow_strategy_required:order_by" in plan.blockers


def test_safe_online_change_does_not_create_shadow_plan() -> None:
    desired = _desired_state()
    actual = replace(desired, table_settings={"min_rows_for_wide_part": 0})

    plan = ShadowMigrationPlanner(dialect=ClickHouseShadowMigrationDialect()).plan(
        desired=desired,
        actual=actual,
        strategy=ShadowMigrationStrategy.SHADOW,
        changes=(
            {
                "change_type": "table_setting",
                "path": "table_settings.min_rows_for_wide_part",
                "desired": 8192,
                "actual": 0,
            },
        ),
    )

    assert plan.phases == ()
    assert plan.blockers == ()


def test_column_projection_planner_renders_identity_cast_and_nullable_new_column() -> None:
    actual = _desired_state(
        columns={
            "id": PhysicalColumnState(name="id", target_type="Int64", nullable=False),
            "amount": PhysicalColumnState(name="amount", target_type="String", nullable=True),
        }
    )
    desired = _desired_state(
        columns={
            "id": PhysicalColumnState(name="id", target_type="Int64", nullable=False),
            "amount": PhysicalColumnState(name="amount", target_type="Decimal(18, 2)", nullable=True),
            "comment": PhysicalColumnState(name="comment", target_type="Nullable(String)", nullable=True),
        }
    )

    projection = ColumnProjectionPlanner().plan(desired=desired, actual=actual)

    assert projection.blockers == ()
    assert [item.expression for item in projection.items] == [
        "`id`",
        "CAST(`amount`, 'Decimal(18, 2)')",
        "NULL",
    ]


def test_column_projection_blocks_not_null_new_column_without_default_policy() -> None:
    actual = _desired_state(columns={"id": PhysicalColumnState(name="id", target_type="Int64", nullable=False)})
    desired = _desired_state(
        columns={
            "id": PhysicalColumnState(name="id", target_type="Int64", nullable=False),
            "status": PhysicalColumnState(name="status", target_type="String", nullable=False),
        }
    )

    projection = ColumnProjectionPlanner().plan(desired=desired, actual=actual)

    assert "migration.shadow_projection.missing_not_null_default:status" in projection.blockers


def test_clickhouse_shadow_dialect_uses_atomic_exchange_and_stable_validation_sql() -> None:
    desired = _desired_state(order_by=("id",))
    dialect = ClickHouseShadowMigrationDialect()

    assert dialect.render_cutover(actual_table=desired.table, shadow_table="landing.__dpone_shadow_orders_1234") == (
        "EXCHANGE TABLES `landing`.`orders` AND `landing`.`__dpone_shadow_orders_1234`"
    )
    validation = dialect.render_validation_checks(
        actual_table=desired.table,
        shadow_table="landing.__dpone_shadow_orders_1234",
        columns=tuple(desired.columns),
        key_columns=desired.order_by,
    )

    assert "SELECT count() FROM `landing`.`orders`" in validation
    assert "cityHash64" in "\n".join(validation)
    assert "GROUP BY `id` HAVING count() > 1" in "\n".join(validation)


def _desired_state(
    *,
    order_by: tuple[str, ...] = ("id",),
    columns: dict[str, PhysicalColumnState] | None = None,
) -> PhysicalTableState:
    return PhysicalTableState(
        sink_type="clickhouse",
        table="landing.orders",
        columns=columns or {"id": PhysicalColumnState(name="id", target_type="Int64", nullable=False)},
        engine="MergeTree",
        order_by=order_by,
        table_settings={"min_rows_for_wide_part": 8192},
    )
