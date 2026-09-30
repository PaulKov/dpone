"""Pure admission for retiring a proven unpublished legacy preparation.

Facts must come from the deployment-admitted observation adapter. This module
checks their consistency; constructing a dataclass or hashing a file does not
authenticate writer exclusion or historical DDL coverage. It performs no I/O
and grants neither a publication permit nor a quality result.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import asdict, dataclass, replace
from typing import TYPE_CHECKING

from dpone.contracts import clickhouse_cluster_publication as c

if TYPE_CHECKING:
    from uuid import UUID

_SHA256 = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True, slots=True)
class RetirementReplicaObservation:
    """One attributable replica observation, including complete DDL history."""

    host: str
    authority: c.VersionedAuthorityRecord
    original_payload: bytes
    generation: c.ReplicaGeneration
    history_from: int
    history_through: int
    history_receipt_digest: str
    history_gaps: tuple[tuple[int, int], ...] = ()
    matching_ddl_entries: tuple[str, ...] = ()
    pending_requests: tuple[str, ...] = ()
    active_mutations: tuple[str, ...] = ()
    active_writers: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class PublicationFreezeObservation:
    """Deployment-owned exclusion receipt, not an operator boolean assertion.

    The observer authenticates the issuer and derives the full writer inventory,
    including manual/restarted workers. Its hold capability must keep the freeze
    valid through mutation/readback and subsequent deployment binding cutover.
    """

    issuer: str
    receipt_digest: str
    target_key: str
    binding_digest: str
    inventory_digest: str
    expected_writers: tuple[str, ...]
    excluded_writers: tuple[str, ...]
    established_at: int
    drained_at: int
    expires_at: int


@dataclass(frozen=True, slots=True)
class PublicationRetirementObservation:
    binding_digest: str
    source_location_digest: str
    prepared_at: int
    inventory: c.ClusterInventory
    replicas: tuple[RetirementReplicaObservation, ...]
    freeze: PublicationFreezeObservation


@dataclass(frozen=True, slots=True)
class PublicationRetirementPlan:
    """Bound immutable facts; never use a stored plan instead of re-observation."""

    observation: PublicationRetirementObservation

    @property
    def original(self) -> c.VersionedAuthorityRecord:
        return self.observation.replicas[0].authority

    @property
    def original_payload(self) -> bytes:
        return self.observation.replicas[0].original_payload

    @property
    def phase(self) -> str:
        return "RETIRED_UNPUBLISHED"

    @property
    def origin(self) -> str:
        return "legacy_retired"

    @property
    def payload(self) -> str:
        data = asdict(self.observation)
        # Preserve original bytes, not an approximation reconstructed from a
        # current schema. Hex also keeps arbitrary bytes unambiguous in JSON.
        for item in data["replicas"]:
            item["original_payload"] = item["original_payload"].hex()
        return c.canonical_json({"contract": "dpone.publication-retirement-plan.v1", "observation": data})

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.payload.encode()).hexdigest()

    @property
    def attempt_key(self) -> str:
        """Stable across re-observation/replanning of the same legacy operation."""
        return c.digest_payload(
            {
                "contract": "dpone.publication-retirement-operation.v1",
                "binding_digest": self.observation.binding_digest,
                "target_key": self.original.record.target_key,
                "operation_id": self.original.record.operation_id,
            }
        )

    @property
    def source_snapshot(self) -> tuple[object, ...]:
        """Original facts that renewed freeze/history observations cannot replace."""
        value = self.observation
        return (
            value.binding_digest,
            value.source_location_digest,
            value.prepared_at,
            value.inventory,
            tuple((item.host, item.authority, item.original_payload, item.generation) for item in value.replicas),
        )


def retirement_record(plan: PublicationRetirementPlan, *, write_id: UUID) -> c.AuthorityRecord:
    """Preserve lineage without granting historical quality or publication.

    The immutable provenance separately preserves all original bytes, including
    any old capsule. This terminal envelope cannot be a load-success receipt.
    """
    return replace(
        plan.original.record,
        phase=c.AuthorityPhase.RETIRED_UNPUBLISHED,
        schema_version=c.SCHEMA_VERSION,
        quality_evidence=None,
        quality_reader=None,
        authority_write_id=write_id.hex,
    )


def require_retirement_history(plan: PublicationRetirementPlan) -> None:
    """Validate a persisted plan at its observation boundary, for readback only.

    Expiry prevents mutation, not inspection of the original committed evidence.
    The service independently authenticates a new, currently held freeze and
    complete history. This function never admits a new SQL or DDL effect.
    """
    if not isinstance(plan, PublicationRetirementPlan):
        raise ValueError("retirement plan required")
    boundary = max(plan.observation.freeze.drained_at, *(item.history_through for item in plan.observation.replicas))
    plan_retirement(plan.observation, now=boundary)


def plan_retirement(observation: PublicationRetirementObservation, *, now: int) -> PublicationRetirementPlan:
    """Validate complete trusted observations; reject ambiguity before writes."""
    _time(now)
    if not isinstance(observation, PublicationRetirementObservation):
        raise ValueError("retirement observation required")
    value = observation
    _digest(value.binding_digest)
    _digest(value.source_location_digest)
    _time(value.prepared_at)
    value.inventory.validate()
    if not isinstance(value.replicas, tuple) or tuple(item.host for item in value.replicas) != value.inventory.hosts:
        raise ValueError("retirement requires every replica exactly once in inventory order")
    first = value.replicas[0]
    record = first.authority.record
    _digest(record.plan_digest)
    if record.target_key != c.digest_payload(
        {"cluster": value.inventory.cluster, "database": record.database, "target": record.target}
    ):
        raise ValueError("legacy target identity differs from the observation scope")
    if not isinstance(record.operation_id, str) or not 0 < len(record.operation_id) <= 128:
        raise ValueError("legacy operation identity required")
    if (
        type(record.dispatch_epoch) is not int
        or type(record.staged_rows) is not int
        or not 0 <= record.staged_rows < 2**63
    ):
        raise ValueError("invalid legacy count or epoch")
    if record.phase is not c.AuthorityPhase.PREPARED or record.dispatch_epoch != 0:
        raise ValueError("retirement requires an unpublished legacy preparation")
    if record.schema_version not in {c.SCHEMA_VERSION, c.QUALITY_SCHEMA_VERSION}:
        raise ValueError("unsupported legacy retirement envelope")
    if (record.schema_version == c.QUALITY_SCHEMA_VERSION) != (record.quality_evidence is not None):
        raise ValueError("legacy quality envelope version differs")
    if any(
        getattr(record, name) is not None
        for name in (
            "ddl_correlation_token",
            "ddl_entry",
            "ddl_query_digest",
            "cleanup_correlation_token",
            "cleanup_entry",
            "cleanup_query_digest",
            "quality_reader",
            "authority_write_id",
        )
    ):
        raise ValueError("retirement rejects dispatch or new-backend identity")
    record.desired.validate()
    if record.predecessor is not None:
        record.predecessor.validate()
    if (
        record.predecessor is not None and record.desired.uuid == record.predecessor.uuid
    ) or record.target == record.candidate:
        raise ValueError("retirement candidate is not isolated")
    c.require_inventory(record, value.inventory)
    _require_freeze(value, now=now)
    for replica in value.replicas:
        _require_replica(replica, value=value, first=first, now=now)
    return PublicationRetirementPlan(value)


def _require_freeze(value: PublicationRetirementObservation, *, now: int) -> None:
    freeze = value.freeze
    if not isinstance(freeze, PublicationFreezeObservation):
        raise ValueError("deployment-issued freeze observation required")
    _digest(freeze.receipt_digest)
    _time(freeze.established_at)
    _time(freeze.drained_at)
    _time(freeze.expires_at)
    if not freeze.issuer or not freeze.issuer.isprintable():
        raise ValueError("freeze issuer required")
    if (freeze.target_key, freeze.binding_digest, freeze.inventory_digest) != (
        value.replicas[0].authority.record.target_key,
        value.binding_digest,
        value.inventory.digest,
    ):
        raise ValueError("freeze scope differs from retirement")
    if not value.prepared_at <= freeze.established_at <= freeze.drained_at <= now < freeze.expires_at:
        raise ValueError("freeze interval is invalid or expired")
    expected, excluded = freeze.expected_writers, freeze.excluded_writers
    if (
        not isinstance(expected, tuple)
        or not isinstance(excluded, tuple)
        or not expected
        or any(not isinstance(item, str) or not item or not item.isprintable() for item in expected)
        or len(set(expected)) != len(expected)
        or len(set(excluded)) != len(excluded)
        or set(expected) != set(excluded)
    ):
        raise ValueError("all admitted writers must be excluded")


def _require_replica(
    replica: RetirementReplicaObservation,
    *,
    value: PublicationRetirementObservation,
    first: RetirementReplicaObservation,
    now: int,
) -> None:
    if type(replica.authority.version) is not int or not 0 < replica.authority.version < 2**63:
        raise ValueError("invalid legacy revision")
    if replica.authority != first.authority:
        raise ValueError("legacy authority differs across replicas")
    if (
        not isinstance(replica.original_payload, bytes)
        or not 0 < len(replica.original_payload) <= 1024 * 1024
        or replica.original_payload != replica.authority.record.payload.encode()
        or replica.original_payload != first.original_payload
    ):
        raise ValueError("original legacy bytes differ")
    generation, record = replica.generation, first.authority.record
    if (
        generation.host != replica.host
        or generation.target != record.predecessor
        or generation.candidate != record.desired
        or generation.target_healthy is not True
        or generation.candidate_healthy is not True
    ):
        raise ValueError("retirement generation differs or is unhealthy")
    _digest(replica.history_receipt_digest)
    _time(replica.history_from)
    _time(replica.history_through)
    if not replica.history_from <= value.prepared_at <= value.freeze.drained_at <= replica.history_through <= now:
        raise ValueError("historical DDL coverage is incomplete")
    if any(
        getattr(replica, name) != ()
        for name in (
            "history_gaps",
            "matching_ddl_entries",
            "pending_requests",
            "active_mutations",
            "active_writers",
        )
    ):
        raise ValueError("unknown history or active work prevents retirement")


def _time(value: int) -> None:
    if type(value) is not int or value < 0:
        raise ValueError("nonnegative UTC epoch required")


def _digest(value: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError("canonical observation SHA-256 required")
