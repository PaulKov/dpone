"""Immutable, feature-local values for unregistered SQL Server SWITCH.

Snapshots are catalog observations, not receipts or deployment authorization.
Only the catalog adapter produces deployment snapshots; tests may construct
synthetic values. No imports here activate a route or perform I/O.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime

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
