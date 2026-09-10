"""Pure admission tests for the unregistered SWITCH component."""

from dataclasses import replace
from datetime import date

import pytest

from dpone.contracts.native_mssql_switch import (
    NativeSwitchBinding,
    NativeSwitchDatabase,
    NativeSwitchInterval,
    NativeSwitchObject,
    NativeSwitchSnapshot,
    NativeSwitchTable,
)
from dpone.runtime.sinks.mssql_native_switch import plan_native_switch


def switch_case(*, prepared_rows=3):
    """Synthetic catalog fixture; never live deployment authority."""
    database = NativeSwitchDatabase(7, "fixture", "database-generation")
    objects = tuple(
        NativeSwitchObject("dbo", name, ordinal, "created")
        for ordinal, name in enumerate(("target", "prepared", "old"), 101)
    )
    binding = NativeSwitchBinding(database, *objects, "invocation", 1, "mutation")
    tables = tuple(
        NativeSwitchTable(
            obj,
            binding.tag(role) if role != "target" else None,
            "shape",
            "catalog-" + role,
            "pf",
            "day",
            "date",
            True,
            (date(2026, 1, 1), date(2026, 2, 1)),
            (),
        )
        for obj, role in zip(objects, ("target", "prepared", "switch_out"), strict=True)
    )
    snapshot = NativeSwitchSnapshot(database, *tables, prepared_rows, 0, 0)
    return snapshot, NativeSwitchInterval("day", date(2026, 1, 1), date(2026, 2, 1)), binding


@pytest.mark.parametrize("rows", [0, 3])
def test_authored_window_authorizes_partition_even_with_empty_input(rows):
    snapshot, interval, binding = switch_case(prepared_rows=rows)
    result = plan_native_switch(snapshot, interval=interval, owner_binding=binding)
    assert result.reasons == ()
    assert result.plan.partition_number == 2
    assert result.plan.snapshot.prepared_rows == rows


@pytest.mark.parametrize(
    "start,end",
    [
        (None, date(2026, 2, 1)),
        (date(2026, 1, 2), date(2026, 2, 1)),
        (date(2026, 1, 1), None),
        (date(2026, 2, 1), date(2026, 1, 1)),
    ],
)
def test_rejects_non_exact_interval(start, end):
    snapshot, interval, binding = switch_case()
    result = plan_native_switch(snapshot, interval=replace(interval, start=start, end=end), owner_binding=binding)
    assert result.plan is None
    assert result.reasons == ("interval_not_one_finite_partition",)


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("switch_out_rows", 1, "switch_out_not_empty"),
        ("prepared_outside_rows", 1, "prepared_rows_outside_interval"),
        ("prepared_rows", -1, "metadata_unknown"),
    ],
)
def test_rejects_unsafe_content(field, value, reason):
    snapshot, interval, binding = switch_case()
    result = plan_native_switch(replace(snapshot, **{field: value}), interval=interval, owner_binding=binding)
    assert result.plan is None
    assert reason in result.reasons


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("owner_tag", "foreign", "owner_binding_mismatch"),
        ("layout_digest", "different", "layout_mismatch"),
        ("range_right", False, "unsupported_partition_layout"),
        ("issues", ("unsupported_dependency",), "unsupported_dependency"),
    ],
)
def test_rejects_ineligible_table(field, value, reason):
    snapshot, interval, binding = switch_case()
    snapshot = replace(snapshot, prepared=replace(snapshot.prepared, **{field: value}))
    assert reason in plan_native_switch(snapshot, interval=interval, owner_binding=binding).reasons
