"""Validate native operation origin against the selected SQL authority history.

This is not a proof of absent ClickHouse DDL or of valid quality evidence. It
establishes which native preparation the current operation descends from. The
caller owns a single transaction containing both current and origin reads.
"""

from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

from dpone.ports.clickhouse_cluster_publication import contracts as c
from dpone.ports.mssql_publication import NativePublicationPreparation
from dpone.runtime.quality_replay_contracts import contracts as quality_contracts
from dpone.runtime.state.mssql_publication_envelope import require_transition

ReceiptDecoder = Callable[[list[tuple[Any, ...]], str], tuple[bool, c.VersionedAuthorityRecord]]


def decode_native_preparation(
    rows: list[tuple[Any, ...]],
    *,
    current: c.VersionedAuthorityRecord,
    binding_digest: str,
    decode_receipt: ReceiptDecoder,
) -> NativePublicationPreparation:
    """Bind earliest operation event, predecessor and current immutable identity."""
    if len(rows) != 1 or len(rows[0]) != 27:
        raise ValueError("ambiguous native preparation receipt")
    row = rows[0]
    prepared = decode_receipt([row[:13]], current.record.target_key)[1]
    if prepared.record.phase is not c.AuthorityPhase.PREPARED or row[8] != "native":
        raise ValueError("operation did not originate as native PREPARED")
    if prepared.version == 1:
        if any(value is not None for value in row[13:26]):
            raise ValueError("initial preparation has a predecessor event")
        predecessor = None
    else:
        predecessor = decode_receipt([row[13:26]], current.record.target_key)[1]
        if predecessor.version != prepared.version - 1:
            raise ValueError("preparation predecessor revision differs")
    require_transition(predecessor, prepared.record)
    _require_same_operation(prepared, current)
    created = row[26]
    if not isinstance(created, datetime) or created.tzinfo is not None:
        raise ValueError("expected SQL UTC datetime2 preparation timestamp")
    return NativePublicationPreparation(binding_digest, prepared, current, created.replace(tzinfo=UTC))


def _require_same_operation(prepared: c.VersionedAuthorityRecord, current: c.VersionedAuthorityRecord) -> None:
    before, after = prepared.record, current.record
    mutable = {
        "phase",
        "authority_write_id",
        "dispatch_epoch",
        "ddl_correlation_token",
        "ddl_query_digest",
        "ddl_entry",
        "cleanup_correlation_token",
        "cleanup_query_digest",
        "cleanup_entry",
        "quality_reader",
        "quality_evidence",
    }
    identities = [{key: value for key, value in asdict(item).items() if key not in mutable} for item in (before, after)]
    if identities[0] != identities[1] or current.version < prepared.version:
        raise ValueError("current publication preparation identity differs")
    cores = [
        quality_contracts.QualityReplayCapsule.parse(item.quality_evidence).core_digest
        if item.quality_evidence is not None
        else None
        for item in (before, after)
    ]
    if cores[0] != cores[1]:
        raise ValueError("current publication quality identity differs")
    if after.phase is c.AuthorityPhase.PREPARED:
        if current != prepared:
            raise ValueError("current preparation was replaced")
        return
    if current.version == prepared.version or after.phase is c.AuthorityPhase.RETIRED_UNPUBLISHED:
        raise ValueError("invalid current publication phase")
    cleanup = bool(after.cleanup_correlation_token or after.cleanup_query_digest or after.cleanup_entry)
    minimum_transitions = {
        c.AuthorityPhase.DISPATCHING: 1,
        c.AuthorityPhase.COMMITTED: 2,
        c.AuthorityPhase.CLEANUP_DISPATCHING: 3,
        c.AuthorityPhase.COMPLETED: 4 if cleanup else 3,
    }[after.phase]
    if current.version - prepared.version < minimum_transitions:
        raise ValueError("current revision skips native publication transitions")
    expected_epoch = before.dispatch_epoch + (2 if cleanup else 1)
    if after.dispatch_epoch != expected_epoch or not after.ddl_correlation_token or not after.ddl_query_digest:
        raise ValueError("current publication dispatch intent differs")
    if after.phase is c.AuthorityPhase.DISPATCHING:
        if after.ddl_entry or cleanup or after.quality_reader:
            raise ValueError("dispatching publication contains later state")
        return
    if not after.ddl_entry:
        raise ValueError("published operation lacks its original DDL entry")
    if cleanup:
        if (
            after.phase not in {c.AuthorityPhase.CLEANUP_DISPATCHING, c.AuthorityPhase.COMPLETED}
            or not after.cleanup_correlation_token
            or not after.cleanup_query_digest
            or (after.phase is c.AuthorityPhase.COMPLETED) != bool(after.cleanup_entry)
        ):
            raise ValueError("current publication cleanup intent differs")
    elif after.phase is c.AuthorityPhase.CLEANUP_DISPATCHING:
        raise ValueError("cleanup publication lacks dispatch intent")
