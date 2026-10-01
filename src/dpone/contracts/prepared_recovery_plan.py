"""Closed operator scope and bounded native recovery serialization.

These bytes bind review/confirmation to a verified context and two admitted
endpoints. They never authenticate that context, historical exclusion or current
authority, and carry no credentials, executable SQL or dispatch capability.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from dpone.contracts import clickhouse_cluster_publication as c
from dpone.contracts.prepared_recovery import (
    PreparedRecoveryPlan,
    PreparedRecoverySafetyObservation,
    RecoveryReplicaHistory,
    recovery_dispatch_token,
    require_unpublished_safety,
)
from dpone.contracts.publication_authority_binding import PublicationAuthorityBinding, publication_binding_digest
from dpone.contracts.publication_preparation import NativePublicationPreparation
from dpone.contracts.publication_retirement import PublicationFreezeObservation


@dataclass(frozen=True, slots=True)
class PreparedRecoveryOperatorPlan:
    binding: PublicationAuthorityBinding
    endpoint_identity: str
    context_subject: str
    sink_connection_ref: str
    sink_endpoint_identity: str
    recovery: PreparedRecoveryPlan

    def __post_init__(self) -> None:
        for value in (self.endpoint_identity, self.context_subject, self.sink_endpoint_identity):
            _digest(value)
        _identity(self.sink_connection_ref)
        if not isinstance(self.binding, PublicationAuthorityBinding):
            raise ValueError("exact authority binding required")
        _require_plan(self.recovery)
        if publication_binding_digest(self.binding, endpoint_identity=self.endpoint_identity) != (
            self.recovery.preparation.binding_digest
        ):
            raise ValueError("recovery authority binding differs")

    @property
    def payload(self) -> str:
        return c.canonical_json(
            {
                "contract": "dpone.native-recovery-operator-plan.v1",
                "binding": asdict(self.binding),
                "endpoint_identity": self.endpoint_identity,
                "context_subject": self.context_subject,
                "sink_connection_ref": self.sink_connection_ref,
                "sink_endpoint_identity": self.sink_endpoint_identity,
                "recovery": json.loads(self.recovery.payload),
            }
        )

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.payload.encode()).hexdigest()

    def confirms(self, value: object) -> bool:
        return (
            isinstance(value, str)
            and re.fullmatch(r"[0-9a-f]{64}", value) is not None
            and hmac.compare_digest(value, self.digest)
        )


def decode_recovery_operator_plan(raw: bytes) -> PreparedRecoveryOperatorPlan:
    """Decode exact bounded bytes; saved expired observations are historical only."""
    try:
        if not isinstance(raw, bytes) or not 0 < len(raw) <= 1024 * 1024:
            raise ValueError("bounded bytes required")
        document = _object(json.loads(raw.decode("utf-8")))
        if document.pop("contract") != "dpone.native-recovery-operator-plan.v1":
            raise ValueError("unsupported recovery operator version")
        document["binding"] = PublicationAuthorityBinding.from_mapping(document["binding"])
        document["recovery"] = _recovery(document["recovery"])
        result = PreparedRecoveryOperatorPlan(**document)
        if result.payload.encode() != raw:
            raise ValueError("noncanonical recovery operator plan")
        return result
    except (KeyError, TypeError, ValueError, AttributeError, RecursionError, OverflowError, c.ClusterPublicationError):
        raise ValueError("invalid recovery operator plan") from None


def _recovery(value: Any) -> PreparedRecoveryPlan:
    envelope = _object(value)
    if set(envelope) != {"contract", "plan"} or envelope["contract"] != "dpone.native-prepared-recovery.v1":
        raise ValueError("unsupported recovery version")
    plan = _object(envelope["plan"])
    preparation = _object(plan["preparation"])
    preparation["prepared_at"] = datetime.fromisoformat(preparation["prepared_at"])
    for name in ("prepared", "current"):
        versioned = _object(preparation[name])
        record = _object(versioned["record"])
        for key in ("quality_evidence", "quality_reader", "authority_write_id"):
            if record.get(key) is None:
                record.pop(key, None)
        versioned["record"] = c.AuthorityRecord.from_payload(c.canonical_json(record).encode())
        preparation[name] = c.VersionedAuthorityRecord(**versioned)
    plan["preparation"] = NativePublicationPreparation(**preparation)
    observed = _object(plan["safety"])
    inventory = _object(observed["inventory"])
    inventory["replicas"] = tuple(c.ClusterReplica(**_object(item)) for item in _array(inventory["replicas"]))
    observed["inventory"] = c.ClusterInventory(**inventory)
    freeze = _object(observed["freeze"])
    for name in ("expected_writers", "excluded_writers"):
        freeze[name] = tuple(_array(freeze[name]))
    observed["freeze"] = PublicationFreezeObservation(**freeze)
    observed["histories"] = tuple(_history(item) for item in _array(observed["histories"]))
    plan["safety"] = PreparedRecoverySafetyObservation(**observed)
    return PreparedRecoveryPlan(**plan)


def _history(value: Any) -> RecoveryReplicaHistory:
    item = _object(value)
    item["gaps"] = tuple(tuple(_array(gap)) for gap in _array(item["gaps"]))
    for name in ("matching_ddl_entries", "pending_requests", "active_mutations", "active_writers"):
        item[name] = tuple(_array(item[name]))
    return RecoveryReplicaHistory(**item)


def _require_plan(plan: PreparedRecoveryPlan) -> None:
    original = plan.preparation
    record = original.prepared.record
    for current in (original.prepared, original.current):
        if type(current.version) is not int or not 0 < current.version < 2**63 - 1:
            raise ValueError("native revision required")
    if original.prepared != original.current or record.phase is not c.AuthorityPhase.PREPARED:
        raise ValueError("exact native PREPARED origin required")
    c.AuthorityRecord.from_payload(record.payload.encode())
    if (
        record.authority_write_id is None
        or record.staged_rows <= 0
        or any(
            getattr(record, name) is not None
            for name in (
                "ddl_correlation_token",
                "ddl_entry",
                "ddl_query_digest",
                "cleanup_correlation_token",
                "cleanup_entry",
                "cleanup_query_digest",
                "quality_reader",
                "quality_evidence",
            )
        )
    ):
        raise ValueError("unsupported prepared policy or existing intent")
    for value in (record.operation_id, record.fence_token, record.database, record.target, record.candidate):
        _identity(value)
    for value in (record.target_key, record.plan_digest, plan.dispatch_query_digest):
        _digest(value)
    _identity(plan.dispatch_token, limit=512)
    if plan.dispatch_token != recovery_dispatch_token(original):
        raise ValueError("native recovery intent differs")
    if record.candidate == record.target or (record.predecessor and record.predecessor.uuid == record.desired.uuid):
        raise ValueError("isolated candidate required")
    for replica in plan.safety.inventory.replicas:
        for value in (replica.host, replica.address):
            _identity(value, limit=512)
        for ordinal in (replica.native_port, replica.shard_num, replica.replica_num):
            if type(ordinal) is not int or not 0 < ordinal < 65536:
                raise ValueError("invalid replica identity")
        if type(replica.internal_replication) is not bool:
            raise ValueError("invalid replica replication flag")
    if type(plan.safety.preparation_version) is not int:
        raise ValueError("invalid observation revision")
    boundary = max(item.history_through for item in plan.safety.histories)
    require_unpublished_safety(plan.safety, original, now=boundary)


def _digest(value: str) -> None:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError("canonical recovery digest required")


def _identity(value: str, *, limit: int = 128) -> None:
    if not isinstance(value, str) or not 0 < len(value) <= limit or value != value.strip() or not value.isprintable():
        raise ValueError("exact recovery identity required")


def _object(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("recovery object required")
    return dict(value)


def _array(value: Any) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError("recovery array required")
    return value
