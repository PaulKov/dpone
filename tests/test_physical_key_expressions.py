"""Physical key lists retain complete SQL expressions across introspection."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from dpone.readiness.physical_reconciliation import PhysicalDesignDriftDetector
from dpone.readiness.physical_state import PhysicalTableState
from dpone.runtime.sinks.clickhouse_physical_reconciliation import ClickHousePhysicalIntrospector


@pytest.mark.parametrize(
    "keys",
    [
        ["entity_id", "ifNull(event_time, '')"],
        ["entity_id", "coalesce(nullIf(label, 'a,b'), 'x')"],
        ["arrayElement([1, 2], 1)", "entity_id"],
        ["replaceAll(label, 'it''s,a', 'b')", "entity_id"],
        [r"replaceAll(label, 'it\'s,a', 'b')", "entity_id"],
        ["`comma,column`", "entity_id"],
        ['"comma,column"', "entity_id"],
    ],
)
def test_introspection_and_mapping_preserve_expression_boundaries(keys):
    class Connector:
        def get_records(self, query):
            if "system.columns" in query:
                return []
            return [{"sorting_key": ", ".join(keys), "primary_key": ", ".join(keys)}]

    actual = ClickHousePhysicalIntrospector(Connector()).inspect(
        SimpleNamespace(target_schema="demo", target_table="events")
    )
    desired = replace(actual, order_by=tuple(keys), primary_key=tuple(keys))
    assert PhysicalDesignDriftDetector().detect(desired, actual) == ()
    mapped = PhysicalTableState.from_mapping(
        {
            "sorting_key": ", ".join(keys),
            "primary_key": ", ".join(keys),
        }
    )
    assert (
        PhysicalDesignDriftDetector().detect(replace(mapped, order_by=tuple(keys), primary_key=tuple(keys)), mapped)
        == ()
    )


def test_real_expression_change_remains_drift():
    actual = PhysicalTableState.from_mapping({"order_by": "entity_id, ifNull(value, 'a')"})
    desired = replace(actual, order_by=("entity_id", "ifNull(value, 'b')"))
    changes = PhysicalDesignDriftDetector().detect(desired, actual)
    assert [change.change_type for change in changes] == ["order_by"]


@pytest.mark.parametrize("value", ["id, fn(value", "id, value)", "id, 'unterminated", "id,,value"])
def test_malformed_expression_list_fails_closed(value):
    with pytest.raises(ValueError, match="physical key expression list"):
        PhysicalTableState.from_mapping({"order_by": value})


@pytest.mark.parametrize("value", ["", "tuple()", None])
def test_empty_keys_remain_supported(value):
    assert PhysicalTableState.from_mapping({"order_by": value}).order_by == ()


def test_sequence_keys_are_not_resplit():
    keys = ["entity_id", "ifNull(event_time, '')"]
    assert PhysicalTableState.from_mapping({"order_by": keys}).order_by == tuple(keys)


def test_key_reordering_requires_shadow_migration():
    from dpone.readiness.physical_design_models import PhysicalReconciliationOptions
    from dpone.readiness.physical_reconciliation import PhysicalDesignReconciler
    from dpone.runtime.sinks.clickhouse_physical_reconciliation import ClickHousePhysicalMigrationDialect

    actual = PhysicalTableState.from_mapping({"order_by": "entity_id, ifNull(event_time, '')"})
    desired = replace(actual, order_by=tuple(reversed(actual.order_by)))
    plan = PhysicalDesignReconciler().reconcile(
        desired=desired,
        actual=actual,
        options=PhysicalReconciliationOptions(),
        dialect=ClickHousePhysicalMigrationDialect(),
    )
    assert plan.blockers == ("physical_design.shadow_required:order_by",)
    assert plan.ddl == ()
