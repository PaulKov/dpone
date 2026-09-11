"""Immutable values and pure admission for unregistered SQL Server SWITCH.

Snapshots are catalog observations, not receipts or deployment authorization.
Only the catalog adapter produces deployment snapshots; tests may construct
synthetic values. No imports here activate a route or perform I/O.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta

TemporalBoundary = date | datetime


@dataclass(frozen=True)
class NativeSwitchDatabase:
    """Database identity includes its catalog generation, not just its name."""

    database_id: int
    name: str
    generation: str


@dataclass(frozen=True)
class NativeSwitchObject:
    """Exact catalog identity expected from invocation-owned provisioning."""

    schema: str
    name: str
    object_id: int
    created_at: str


@dataclass(frozen=True)
class NativeSwitchBinding:
    """Caller authority for target mutation and two disposable owned tables.

    The fixture/provisioner writes ``tag(role)`` as table extended property
    ``dpone.native_switch.owner.v1``. A matching string is necessary, but the
    caller must also validate its existing durable invocation/fence authority.
    """

    database: NativeSwitchDatabase
    target: NativeSwitchObject
    prepared: NativeSwitchObject
    switch_out: NativeSwitchObject
    invocation_id: str
    generation: int
    mutation_id: str

    def tag(self, role: str) -> str:
        """Canonical ownership marker; never a substitute for the target fence."""
        resource = {"prepared": self.prepared, "switch_out": self.switch_out}[role]
        return json.dumps(
            [
                1,
                self.database.database_id,
                self.database.generation,
                self.target.object_id,
                self.target.created_at,
                self.invocation_id,
                self.generation,
                self.mutation_id,
                role,
                resource.schema,
                resource.name,
                resource.object_id,
                resource.created_at,
            ],
            separators=(",", ":"),
            ensure_ascii=True,
        )


@dataclass(frozen=True)
class NativeSwitchInterval:
    """Authored half-open interval; missing bounds are explicitly ineligible."""

    column: str
    start: TemporalBoundary | None
    end: TemporalBoundary | None


@dataclass(frozen=True)
class NativeSwitchTable:
    """Strict catalog projection with separate physical-shape and drift digests.

    Layout excludes object/index names and allocation identities. Catalog binds
    the full projected record, including partition IDs and modification time.
    Unknown metadata is recorded in ``issues`` and cannot yield a plan.
    """

    identity: NativeSwitchObject
    owner_tag: str | None
    layout_digest: str
    catalog_digest: str
    partition_function: str
    partition_column: str
    partition_type: str
    range_right: bool
    boundaries: tuple[TemporalBoundary, ...]
    issues: tuple[str, ...]


@dataclass(frozen=True)
class NativeSwitchSnapshot:
    """Three catalog objects plus exact prepared/switch-out row observations."""

    database: NativeSwitchDatabase
    target: NativeSwitchTable
    prepared: NativeSwitchTable
    switch_out: NativeSwitchTable
    prepared_rows: int
    prepared_outside_rows: int
    switch_out_rows: int


@dataclass(frozen=True)
class NativeSwitchPlan:
    """Frozen replacement authority, revalidated under the caller transaction."""

    snapshot: NativeSwitchSnapshot
    interval: NativeSwitchInterval
    owner_binding: NativeSwitchBinding
    partition_number: int


@dataclass(frozen=True)
class NativeSwitchEligibility:
    """Exactly one plan on success; otherwise stable, sorted reason codes."""

    plan: NativeSwitchPlan | None
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class NativeSwitchResult:
    """Mutation counts only; this value never establishes commit success."""

    replaced_rows: int
    inserted_rows: int


class NativeSwitchRejected(ValueError):
    """Fail-closed catalog, authority or revalidation error."""

    def __init__(self, *reasons: str) -> None:
        self.reasons = tuple(sorted(set(reasons)))
        super().__init__(", ".join(self.reasons))


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
