"""Bounded, exact decoding of historical retirement provenance.

This checks canonical bytes and policy consistency, not deployment authenticity.
Only the admitted observer and acknowledged SQL history can establish authority.
Decoding expired historical observations never authorizes a fresh mutation.
"""

from __future__ import annotations

import json
from typing import Any

from dpone.contracts import clickhouse_cluster_publication as c
from dpone.contracts.publication_retirement import (
    PublicationFreezeObservation,
    PublicationRetirementObservation,
    PublicationRetirementPlan,
    RetirementReplicaObservation,
    require_retirement_history,
)


def decode_retirement_plan(raw: bytes) -> PublicationRetirementPlan:
    """Reject coercion, unknown fields/versions, duplicate keys and byte drift."""
    if not isinstance(raw, bytes) or not 0 < len(raw) <= 1024 * 1024:
        raise ValueError("invalid retirement provenance size")
    try:
        envelope = _object(json.loads(raw.decode("utf-8")))
        if (
            set(envelope) != {"contract", "observation"}
            or envelope["contract"] != "dpone.publication-retirement-plan.v1"
        ):
            raise ValueError("unsupported retirement provenance")
        observed = _object(envelope["observation"])
        inventory = _object(observed["inventory"])
        inventory["replicas"] = tuple(c.ClusterReplica(**_object(item)) for item in _array(inventory["replicas"]))
        observed["inventory"] = c.ClusterInventory(**inventory)
        observed["replicas"] = tuple(_replica(item) for item in _array(observed["replicas"]))
        freeze = _object(observed["freeze"])
        for name in ("expected_writers", "excluded_writers"):
            freeze[name] = tuple(_array(freeze[name]))
        observed["freeze"] = PublicationFreezeObservation(**freeze)
        result = PublicationRetirementPlan(PublicationRetirementObservation(**observed))
        if result.payload.encode() != raw:
            raise ValueError("noncanonical retirement provenance")
        require_retirement_history(result)
        return result
    except (KeyError, TypeError, ValueError, c.ClusterPublicationError):
        raise ValueError("invalid retirement provenance") from None


def _replica(value: Any) -> RetirementReplicaObservation:
    item = _object(value)
    authority = _object(item["authority"])
    record = _object(authority["record"])
    record["phase"] = c.AuthorityPhase(record["phase"])
    record["desired"] = c.GenerationIdentity(**_object(record["desired"]))
    if record["predecessor"] is not None:
        record["predecessor"] = c.GenerationIdentity(**_object(record["predecessor"]))
    authority["record"] = c.AuthorityRecord(**record)
    item["authority"] = c.VersionedAuthorityRecord(**authority)
    item["original_payload"] = bytes.fromhex(item["original_payload"])
    generation = _object(item["generation"])
    for role in ("target", "candidate"):
        if generation[role] is not None:
            generation[role] = c.GenerationIdentity(**_object(generation[role]))
    item["generation"] = c.ReplicaGeneration(**generation)
    item["history_gaps"] = tuple(tuple(_array(gap)) for gap in _array(item["history_gaps"]))
    for name in ("matching_ddl_entries", "pending_requests", "active_mutations", "active_writers"):
        item[name] = tuple(_array(item[name]))
    return RetirementReplicaObservation(**item)


def _object(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("retirement object required")
    return dict(value)


def _array(value: Any) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError("retirement array required")
    return value
