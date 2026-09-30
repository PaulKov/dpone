"""Canonical envelope and monotonic-transition guards for SQL authority slots."""

import json
from dataclasses import asdict
from typing import Any
from uuid import UUID

from dpone.ports.clickhouse_cluster_publication import contracts as c


def decode_envelope(raw: bytes) -> c.AuthorityRecord:
    """Decode exact canonical UTF-8; reject coercion, unknown schema and size."""
    if not isinstance(raw, bytes) or not 0 < len(raw) <= 1024 * 1024:
        raise ValueError("invalid publication envelope size")
    data = json.loads(raw.decode("utf-8"))
    data["phase"] = c.AuthorityPhase(data["phase"])
    data["desired"] = c.GenerationIdentity(**data["desired"])
    if data.get("predecessor") is not None:
        data["predecessor"] = c.GenerationIdentity(**data["predecessor"])
    record = c.AuthorityRecord(**data)
    if record.payload.encode() != raw:
        raise ValueError("noncanonical publication envelope")
    if record.schema_version not in {c.SCHEMA_VERSION, c.QUALITY_SCHEMA_VERSION}:
        raise ValueError("unsupported publication envelope")
    if (record.schema_version == c.QUALITY_SCHEMA_VERSION) != (record.quality_evidence is not None):
        raise ValueError("publication quality schema differs")
    record.desired.validate()
    if record.predecessor is not None:
        record.predecessor.validate()
    for value in (record.dispatch_epoch, record.staged_rows):
        if type(value) is not int or not 0 <= value < 2**63 - 1:
            raise ValueError("invalid publication count/epoch")
    if not isinstance(record.operation_id, str) or not 0 < len(record.operation_id) <= 128:
        raise ValueError("invalid publication operation identity")
    if record.authority_write_id is not None and UUID(hex=record.authority_write_id).hex != record.authority_write_id:
        raise ValueError("invalid publication write identity")
    return record


def require_transition(current: c.VersionedAuthorityRecord | None, desired: c.AuthorityRecord) -> bool:
    """Validate monotonic native mutation and return whether it is a DDL intent.

    Same-phase quality-reader/capsule changes are allowed only after publication;
    they never mint a dispatch permit. Governance still authenticates evidence.
    Legacy adoption is not represented by native create.
    """
    decode_envelope(desired.payload.encode())
    if current is None:
        if desired.phase is not c.AuthorityPhase.PREPARED or desired.dispatch_epoch != 0:
            raise ValueError("native publication must start PREPARED")
        _empty_intents(desired)
        return False
    before = current.record
    decode_envelope(before.payload.encode())
    if type(current.version) is not int or not 0 < current.version < 2**63 - 1:
        raise ValueError("invalid publication revision")
    if desired.target_key != before.target_key:
        raise ValueError("publication target changed")
    if before.phase is c.AuthorityPhase.RETIRED_UNPUBLISHED:
        _require_fresh_after_retirement(current, desired)
        return False
    if before.phase is c.AuthorityPhase.COMPLETED and desired.phase is c.AuthorityPhase.PREPARED:
        if (
            before.operation_id == desired.operation_id
            or before.quality_reader is not None
            or desired.dispatch_epoch != before.dispatch_epoch + 1
            or (desired.database, desired.target) != (before.database, before.target)
        ):
            raise ValueError("invalid next publication operation")
        _empty_intents(desired)
        return False
    pair = (before.phase, desired.phase)
    allowed: dict[tuple[c.AuthorityPhase, c.AuthorityPhase], set[str]] = {
        (c.AuthorityPhase.PREPARED, c.AuthorityPhase.DISPATCHING): {
            "ddl_correlation_token",
            "ddl_query_digest",
            "dispatch_epoch",
        },
        (c.AuthorityPhase.DISPATCHING, c.AuthorityPhase.COMMITTED): {"ddl_entry"},
        (c.AuthorityPhase.COMMITTED, c.AuthorityPhase.CLEANUP_DISPATCHING): {
            "cleanup_correlation_token",
            "cleanup_query_digest",
            "dispatch_epoch",
        },
        (c.AuthorityPhase.COMMITTED, c.AuthorityPhase.COMPLETED): set(),
        (c.AuthorityPhase.CLEANUP_DISPATCHING, c.AuthorityPhase.COMPLETED): {"cleanup_entry"},
        (c.AuthorityPhase.COMMITTED, c.AuthorityPhase.COMMITTED): {"quality_reader", "quality_evidence"},
        (c.AuthorityPhase.COMPLETED, c.AuthorityPhase.COMPLETED): {"quality_reader", "quality_evidence"},
    }
    if pair not in allowed:
        raise ValueError("invalid publication phase transition")
    mutable = allowed[pair] | {"phase", "authority_write_id"}
    if _without(before, mutable) != _without(desired, mutable):
        raise ValueError("publication immutable envelope changed")
    if desired.phase is c.AuthorityPhase.COMMITTED and not desired.ddl_entry:
        raise ValueError("committed publication requires original DDL entry")
    dispatch = desired.phase in {c.AuthorityPhase.DISPATCHING, c.AuthorityPhase.CLEANUP_DISPATCHING}
    if dispatch:
        token, digest = (
            (desired.ddl_correlation_token, desired.ddl_query_digest)
            if desired.phase is c.AuthorityPhase.DISPATCHING
            else (desired.cleanup_correlation_token, desired.cleanup_query_digest)
        )
        if not token or not digest or desired.dispatch_epoch != before.dispatch_epoch + 1:
            raise ValueError("publication dispatch intent is incomplete")
    return dispatch


def _require_fresh_after_retirement(current: c.VersionedAuthorityRecord, desired: c.AuthorityRecord) -> None:
    """New preparation against the unchanged predecessor, not the old candidate.

    This envelope check neither authenticates retirement provenance nor grants
    ownership. The SQL adapter must bind immutable history and win exact CAS.
    A different UUID sharing the old replication path is not a fresh generation.
    """
    before = current.record
    prior_generations = (before.desired,) + ((before.predecessor,) if before.predecessor else ())
    if (
        current.version != 1
        or before.dispatch_epoch != 0
        or before.quality_evidence is not None
        or desired.phase is not c.AuthorityPhase.PREPARED
        or desired.operation_id == before.operation_id
        or not desired.fence_token
        or desired.fence_token == before.fence_token
        or not desired.candidate
        or desired.candidate in {before.candidate, before.target}
        or desired.dispatch_epoch != before.dispatch_epoch + 1
        or (desired.database, desired.target, desired.inventory_digest)
        != (before.database, before.target, before.inventory_digest)
        or desired.predecessor != before.predecessor
        or any(
            desired.desired.uuid == prior.uuid
            or (desired.desired.keeper_name, desired.desired.keeper_path) == (prior.keeper_name, prior.keeper_path)
            for prior in prior_generations
        )
    ):
        raise ValueError("invalid fresh publication after retirement")
    _empty_intents(before)
    _empty_intents(desired)


def _without(record: c.AuthorityRecord, names: set[str]) -> dict[str, Any]:
    return {key: value for key, value in asdict(record).items() if key not in names}


def _empty_intents(record: c.AuthorityRecord) -> None:
    if any(
        (
            record.ddl_correlation_token,
            record.ddl_query_digest,
            record.ddl_entry,
            record.cleanup_correlation_token,
            record.cleanup_query_digest,
            record.cleanup_entry,
            record.quality_reader,
        )
    ):
        raise ValueError("PREPARED publication contains prior dispatch/reader state")
