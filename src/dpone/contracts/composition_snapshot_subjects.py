"""Immutable ClickHouse snapshot subjects, observation evidence and resource ceilings.

These values validate bounded shape and exact physical identity. They perform no
catalog lookup, grant no authority and make no publication or persistence decision.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from uuid import UUID

from dpone.contracts.composition_identity import (
    CompositionAdmissionError,
    require_digest,
    require_physical_domain_identity,
    require_text,
)
from dpone.contracts.composition_physical_identity import composition_physical_guard_id


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise CompositionAdmissionError("snapshot_" + reason)


def _uuid(value: str) -> None:
    try:
        valid = str(UUID(value)) == value and UUID(value).int != 0
    except (ValueError, TypeError, AttributeError):
        valid = False
    _require(valid, "uuid")


def _integer(value: object, *, minimum: int = 0) -> None:
    _require(type(value) is int and minimum <= value <= 2**63 - 1, "counter")


@dataclass(frozen=True, slots=True)
class SnapshotTarget:
    """Protected logical slot and private generation in one enrolled database."""

    service_id: str
    physical_subject_sha256: str
    database_id: str
    database: str
    target_table: str
    generation_table: str
    write_subject_sha256: str
    enrollment_sha256: str

    def __post_init__(self) -> None:
        for value in (self.service_id, self.database_id):
            _uuid(value)
        for value in (self.physical_subject_sha256, self.write_subject_sha256, self.enrollment_sha256):
            require_digest(value)
        for value in (self.database, self.target_table, self.generation_table):
            require_text(value, maximum=128)
            _require(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*", value) is not None, "name")
        _require("." not in self.database and self.target_table != self.generation_table, "names")

    @property
    def guard_id(self) -> str:
        require_physical_domain_identity(
            connector="clickhouse", service_id=self.service_id, physical_subject_sha256=self.physical_subject_sha256
        )
        return composition_physical_guard_id(
            connector="clickhouse", service_id=self.service_id, physical_subject_sha256=self.physical_subject_sha256
        )


@dataclass(frozen=True, slots=True)
class SnapshotLimits:
    """Externally verified effective ceilings; runtime producers enforce counters."""

    max_rows: int
    max_source_bytes: int
    max_wire_bytes: int
    max_generation_bytes: int
    max_retained_bytes: int
    max_total_transient_bytes: int

    def __post_init__(self) -> None:
        for value in asdict(self).values():
            _integer(value, minimum=1)

    def require_storage(self, generation: int, old_target: int, retained: int) -> None:
        for value in (generation, old_target, retained):
            _integer(value)
        _require(
            generation <= self.max_generation_bytes
            and old_target + retained <= self.max_retained_bytes
            and generation + old_target + retained <= self.max_total_transient_bytes,
            "storage_budget",
        )


@dataclass(frozen=True, slots=True)
class SnapshotGeneration:
    """Complete independently reconciled B, including an explicit empty snapshot."""

    record_sha256: str
    source_snapshot_sha256: str
    content_sha256: str
    schema_sha256: str
    physical_sha256: str
    old_target_uuid: str
    new_generation_uuid: str
    rows: int
    source_bytes: int
    wire_bytes: int
    generation_bytes: int
    old_target_bytes: int
    retained_bytes: int

    def __post_init__(self) -> None:
        for name, value in asdict(self).items():
            if name.endswith("sha256"):
                require_digest(value)
            elif name.endswith("uuid"):
                _uuid(value)
            else:
                _integer(value)
        _require(self.old_target_uuid != self.new_generation_uuid, "uuid_pair")


@dataclass(frozen=True, slots=True)
class SnapshotPublisherClosure:
    """Subject returned only after actual publisher closure and quiescence."""

    intent_sha256: str
    closed_gates_sha256: str
    quiescence_sha256: str

    def __post_init__(self) -> None:
        for value in asdict(self).values():
            require_digest(value)


@dataclass(frozen=True, slots=True)
class SnapshotCatalogObservation:
    """Fresh complete protected catalog/content projection, never caller authority.

    Pair/schema/design tuples are ordered target-name then generation-name.
    Content and byte observations follow UUID B/A, independent of current names.
    Missing rows or unknown bytes remain None. Unsupported findings include all
    dependency, policy, writer, mutation, TTL, projection and design exclusions.
    The adapter must verify complete catalog visibility; an empty finding tuple
    alone cannot establish that it did so.
    """

    target: SnapshotTarget
    target_uuid: str | None
    generation_uuid: str | None
    database_engine: str
    table_engines: tuple[str | None, str | None]
    node_count: int
    replica_count: int
    schema_sha256: tuple[str | None, str | None]
    physical_sha256: tuple[str | None, str | None]
    unsupported_features: tuple[str, ...]
    generation_content_sha256: str | None
    generation_rows: int | None
    generation_bytes: int | None
    old_target_bytes: int | None
    retained_bytes: int | None
    catalog_evidence_sha256: str

    def __post_init__(self) -> None:
        _require(type(self.target) is SnapshotTarget, "observation_target")
        self.target.__post_init__()
        require_text(self.database_engine)
        require_digest(self.catalog_evidence_sha256)
        for value in (self.target_uuid, self.generation_uuid):
            if value is not None:
                _uuid(value)
        for values in (self.table_engines, self.schema_sha256, self.physical_sha256):
            _require(type(values) is tuple and len(values) == 2, "observation_pair")
            for value in values:
                if value is not None:
                    require_text(value)
        for value in (*self.schema_sha256, *self.physical_sha256, self.generation_content_sha256):
            if value is not None:
                require_digest(value)
        _require(type(self.unsupported_features) is tuple and len(self.unsupported_features) <= 128, "findings")
        for value in self.unsupported_features:
            require_text(value)
        _require(tuple(sorted(set(self.unsupported_features))) == self.unsupported_features, "findings")
        for counter in (self.node_count, self.replica_count):
            _integer(counter)
        for measured in (self.generation_rows, self.generation_bytes, self.old_target_bytes, self.retained_bytes):
            if measured is not None:
                _integer(measured)


# Retain the documented public lookup for reflection and pickle.
SnapshotCatalogObservation.__module__ = "dpone.contracts.composition_snapshot"
SnapshotGeneration.__module__ = "dpone.contracts.composition_snapshot"
SnapshotLimits.__module__ = "dpone.contracts.composition_snapshot"
SnapshotPublisherClosure.__module__ = "dpone.contracts.composition_snapshot"
SnapshotTarget.__module__ = "dpone.contracts.composition_snapshot"
