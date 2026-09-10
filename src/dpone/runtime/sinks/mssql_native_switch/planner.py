"""Pure admission for one fully covered finite temporal RANGE RIGHT partition."""

from __future__ import annotations

from datetime import date, datetime, timedelta

from dpone.contracts.native_mssql_switch import (
    NativeSwitchBinding,
    NativeSwitchEligibility,
    NativeSwitchInterval,
    NativeSwitchPlan,
    NativeSwitchSnapshot,
)


def _temporal(value: object, kind: str) -> bool:
    if kind == "date":
        return type(value) is date
    return (
        kind == "datetime2"
        and isinstance(value, datetime)
        and value.tzinfo is not None
        and value.utcoffset() == timedelta(0)
    )


def plan_native_switch(
    snapshot: NativeSwitchSnapshot,
    *,
    interval: NativeSwitchInterval,
    owner_binding: NativeSwitchBinding,
) -> NativeSwitchEligibility:
    """Return a frozen plan or stable reasons; never derive scope from rows.

    Synthetic snapshots are useful for tests. Execution always reacquires a
    catalog-adapter snapshot under locks; this pure function grants no SQL rights.
    """
    reasons: set[str] = set()
    tables = (snapshot.target, snapshot.prepared, snapshot.switch_out)
    expected = (owner_binding.target, owner_binding.prepared, owner_binding.switch_out)
    if snapshot.database != owner_binding.database:
        reasons.add("database_binding_mismatch")
    if tuple(table.identity for table in tables) != expected or len({o.object_id for o in expected}) != 3:
        reasons.add("object_binding_mismatch")
    if not owner_binding.invocation_id or not owner_binding.mutation_id or owner_binding.generation < 1:
        reasons.add("owner_binding_mismatch")
    if snapshot.prepared.owner_tag != owner_binding.tag(
        "prepared"
    ) or snapshot.switch_out.owner_tag != owner_binding.tag("switch_out"):
        reasons.add("owner_binding_mismatch")
    for table in tables:
        reasons.update(table.issues)
        if not table.layout_digest or not table.catalog_digest:
            reasons.add("metadata_unknown")
        if not table.range_right or table.partition_type not in {"date", "datetime2"}:
            reasons.add("unsupported_partition_layout")
    if len({table.layout_digest for table in tables}) != 1:
        reasons.add("layout_mismatch")
    target = snapshot.target
    for table in tables:
        if (table.boundaries, table.partition_column, table.partition_type, table.partition_function) != (
            target.boundaries,
            target.partition_column,
            target.partition_type,
            target.partition_function,
        ):
            reasons.add("layout_mismatch")
    boundaries = target.boundaries
    valid = all(_temporal(value, target.partition_type) for value in boundaries)
    if not valid or len(boundaries) < 2 or any(a >= b for a, b in zip(boundaries, boundaries[1:])):
        reasons.add("unsupported_partition_layout")
    partition = None
    if valid and interval.column == target.partition_column:
        for number, (start, end) in enumerate(zip(boundaries, boundaries[1:]), 2):
            if (
                _temporal(interval.start, target.partition_type)
                and _temporal(interval.end, target.partition_type)
                and (interval.start, interval.end) == (start, end)
            ):
                partition = number
    if partition is None:
        reasons.add("interval_not_one_finite_partition")
    if any(
        type(count) is not int or count < 0
        for count in (
            snapshot.prepared_rows,
            snapshot.prepared_outside_rows,
            snapshot.switch_out_rows,
        )
    ):
        reasons.add("metadata_unknown")
    if snapshot.switch_out_rows != 0:
        reasons.add("switch_out_not_empty")
    if snapshot.prepared_outside_rows != 0:
        reasons.add("prepared_rows_outside_interval")
    if reasons or partition is None:
        return NativeSwitchEligibility(None, tuple(sorted(reasons)))
    return NativeSwitchEligibility(NativeSwitchPlan(snapshot, interval, owner_binding, partition), ())
