"""Native recovery plans and trusted-observer facts, never dispatch authority.

These immutable values are audit/confirmation material. Only a deployment-owned
held observer can authenticate writer exclusion and complete attributable history;
constructing a value or possessing its digest grants no permission to execute.
"""

import hashlib
import re
from dataclasses import asdict, dataclass
from math import floor

from dpone.contracts import clickhouse_cluster_publication as c
from dpone.contracts.publication_preparation import NativePublicationPreparation
from dpone.contracts.publication_retirement import PublicationFreezeObservation


@dataclass(frozen=True, slots=True)
class RecoveryReplicaHistory:
    host: str
    history_from: int
    history_through: int
    history_receipt_digest: str
    gaps: tuple[tuple[int, int], ...] = ()
    matching_ddl_entries: tuple[str, ...] = ()
    pending_requests: tuple[str, ...] = ()
    active_mutations: tuple[str, ...] = ()
    active_writers: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class PreparedRecoverySafetyObservation:
    binding_digest: str
    target_key: str
    operation_id: str
    preparation_version: int
    preparation_payload_sha256: str
    inventory: c.ClusterInventory
    freeze: PublicationFreezeObservation
    histories: tuple[RecoveryReplicaHistory, ...]


@dataclass(frozen=True, slots=True)
class PreparedRecoveryPlan:
    """An exact native origin and deterministic intent; execution re-observes."""

    preparation: NativePublicationPreparation
    safety: PreparedRecoverySafetyObservation
    dispatch_token: str
    dispatch_query_digest: str

    @property
    def payload(self) -> str:
        value = asdict(self)
        value["preparation"]["prepared_at"] = self.preparation.prepared_at.isoformat()
        return c.canonical_json({"contract": "dpone.native-prepared-recovery.v1", "plan": value})

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.payload.encode()).hexdigest()


def recovery_dispatch_token(preparation: NativePublicationPreparation) -> str:
    """Deterministic intent identity, not permission to submit that intent."""
    record = preparation.prepared.record
    suffix = c.digest_payload(
        {
            "binding": preparation.binding_digest,
            "preparation": record.payload_sha256,
            "version": preparation.prepared.version,
        }
    )
    return f"dpone-recovery-v1-{record.operation_id[:20]}-publish-{record.dispatch_epoch + 1}-{suffix}"


def require_unpublished_safety(
    observation: PreparedRecoverySafetyObservation, preparation: NativePublicationPreparation, *, now: int
) -> None:
    """Reject incomplete facts from a trusted held observer before PREPARED CAS.

    SQL UTC time bounds coverage only; it does not authenticate cross-system clocks,
    history completeness, or the writer inventory. Those are observer obligations.
    """
    _time(now)
    original = preparation.prepared
    record, value = original.record, observation
    if (
        value.binding_digest,
        value.target_key,
        value.operation_id,
        value.preparation_version,
        value.preparation_payload_sha256,
    ) != (
        preparation.binding_digest,
        record.target_key,
        record.operation_id,
        original.version,
        record.payload_sha256,
    ):
        raise ValueError("recovery observation scope differs")
    _digest(value.binding_digest)
    value.inventory.validate()
    c.require_inventory(record, value.inventory)
    if record.target_key != c.digest_payload(
        {"cluster": value.inventory.cluster, "database": record.database, "target": record.target}
    ):
        raise ValueError("recovery observation target differs")
    offset = preparation.prepared_at.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise ValueError("native preparation UTC timestamp required")
    prepared_at = floor(preparation.prepared_at.timestamp())
    _time(prepared_at)
    freeze = value.freeze
    _digest(freeze.receipt_digest)
    for instant in (freeze.established_at, freeze.drained_at, freeze.expires_at):
        _time(instant)
    if not freeze.issuer or not freeze.issuer.isprintable():
        raise ValueError("deployment freeze issuer required")
    if (freeze.binding_digest, freeze.target_key, freeze.inventory_digest) != (
        value.binding_digest,
        record.target_key,
        value.inventory.digest,
    ) or not prepared_at <= freeze.established_at <= freeze.drained_at <= now < freeze.expires_at:
        raise ValueError("recovery freeze scope or interval differs")
    expected, excluded = freeze.expected_writers, freeze.excluded_writers
    if (
        not isinstance(expected, tuple)
        or not isinstance(excluded, tuple)
        or not expected
        or any(not isinstance(item, str) or not item or not item.isprintable() for item in expected + excluded)
        or len(set(expected)) != len(expected)
        or len(set(excluded)) != len(excluded)
        or set(expected) != set(excluded)
    ):
        raise ValueError("recovery requires exclusion of all other writers")
    if not isinstance(value.histories, tuple) or tuple(item.host for item in value.histories) != value.inventory.hosts:
        raise ValueError("recovery history requires every replica exactly once")
    for history in value.histories:
        _digest(history.history_receipt_digest)
        _time(history.history_from)
        _time(history.history_through)
        if not history.history_from <= prepared_at or history.history_through != now:
            raise ValueError("recovery requires complete history through current observation")
        if any(
            item != ()
            for item in (
                history.gaps,
                history.matching_ddl_entries,
                history.pending_requests,
                history.active_mutations,
                history.active_writers,
            )
        ):
            raise ValueError("unknown history or active work prevents recovery")


def _time(value: int) -> None:
    if type(value) is not int or value < 0:
        raise ValueError("nonnegative UTC epoch required")


def _digest(value: str) -> None:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError("canonical recovery SHA-256 required")
