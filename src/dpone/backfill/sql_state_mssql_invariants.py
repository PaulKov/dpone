"""Immutable-coordinate and monotonic-revision rules for MSSQL backfill state."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

from dpone.backfill.state import (
    CHUNK_STATUS_FAILED,
    CHUNK_STATUS_RUNNING,
    CHUNK_STATUS_SUCCESS,
    BackfillChunkRecord,
    BackfillLedger,
    BackfillPublicationRecord,
)

_UTC = timezone.utc  # noqa: UP017 - mypy target may be older than datetime.UTC.


def require_unique_chunk_shape(ledger: BackfillLedger) -> None:
    """Reject empty, duplicate, or non-positive initial chunk identities."""

    indexes = [record.index for record in ledger.chunks]
    if not indexes or any(index < 1 for index in indexes) or len(indexes) != len(set(indexes)):
        raise ValueError("backfill campaign chunks must have unique positive indexes")


def require_campaign_identity(current: BackfillLedger, incoming: BackfillLedger) -> None:
    """Keep one run key bound to one immutable plan and chunk layout."""

    if campaign_identity(current) != campaign_identity(incoming):
        raise ValueError("backfill campaign immutable identity changed")


def campaign_identity(ledger: BackfillLedger) -> tuple[Any, ...]:
    """Return the immutable identity projection of a campaign ledger."""

    return (
        ledger.run_key,
        ledger.dataset,
        ledger.inner_mode,
        ledger.plan_hash,
        ledger.config_hash,
        ledger.chunk_config,
    )


def require_current(current: BackfillLedger | None, run_key: str) -> BackfillLedger:
    """Return the current ledger or raise a contextual missing-campaign error."""

    if current is None:
        raise KeyError(f"backfill campaign {run_key!r} does not exist")
    return current


def require_chunk_coordinates(current: BackfillChunkRecord, incoming: BackfillChunkRecord) -> None:
    """Reject mutation of one chunk's partition or idempotency identity."""

    current.require_same_coordinates(incoming)


def replace_chunk(ledger: BackfillLedger, changed: BackfillChunkRecord) -> None:
    """Replace exactly one existing in-memory chunk revision."""

    for position, current in enumerate(ledger.chunks):
        if current.index == changed.index:
            ledger.chunks[position] = changed
            return
    raise KeyError(f"backfill ledger {ledger.run_key!r} has no chunk {changed.index}")


def merge_publication(
    current: BackfillPublicationRecord | None,
    incoming: BackfillPublicationRecord | None,
) -> BackfillPublicationRecord | None:
    """Merge publication evidence without coordinate or phase regression."""

    if incoming is None:
        return deepcopy(current)
    if current is None:
        return deepcopy(incoming)
    coordinates = lambda value: (  # noqa: E731 - compact immutable projection.
        value.mode,
        value.target_table,
        value.shadow_table,
        value.backup_table,
    )
    if coordinates(current) != coordinates(incoming):
        raise ValueError("backfill publication immutable coordinates changed")
    digest_enrichment = current.object_contract_sha256 is None and incoming.object_contract_sha256 is not None
    if current.object_contract_sha256 is not None and current.object_contract_sha256 != incoming.object_contract_sha256:
        raise ValueError("backfill publication object contract changed")
    current_generation = (
        current.predecessor_object_id,
        current.predecessor_create_token,
        current.predecessor_receipt_id,
        current.shadow_object_id,
        current.shadow_create_token,
    )
    incoming_generation = (
        incoming.predecessor_object_id,
        incoming.predecessor_create_token,
        incoming.predecessor_receipt_id,
        incoming.shadow_object_id,
        incoming.shadow_create_token,
    )
    full_legacy_binding = current_generation == (None, None, None, None, None)
    token_enrichment = (
        current.predecessor_object_id == incoming.predecessor_object_id
        and current.predecessor_receipt_id == incoming.predecessor_receipt_id
        and current.shadow_object_id == incoming.shadow_object_id
        and current.predecessor_create_token is None
        and current.shadow_create_token is None
    )
    incoming_generation_complete = all(
        value is not None
        for value in (
            incoming.predecessor_object_id,
            incoming.predecessor_create_token,
            incoming.shadow_object_id,
            incoming.shadow_create_token,
        )
    )
    legacy_binding = incoming_generation_complete and (full_legacy_binding or token_enrichment)
    if current_generation != incoming_generation and not legacy_binding:
        raise ValueError("backfill publication immutable generation changed")
    rank = {"planned": 0, "prepared": 1, "published": 2}
    current_rank = rank.get(current.phase)
    incoming_rank = rank.get(incoming.phase)
    if current_rank is None or incoming_rank is None:
        raise ValueError("backfill publication phase is unsupported")
    if incoming_rank < current_rank:
        return deepcopy(current)
    if incoming_rank == current_rank and incoming.to_jsonable() != current.to_jsonable():
        enriched = deepcopy(current)
        if legacy_binding:
            for field in (
                "predecessor_object_id",
                "predecessor_create_token",
                "predecessor_receipt_id",
                "shadow_object_id",
                "shadow_create_token",
            ):
                setattr(enriched, field, getattr(incoming, field))
        if digest_enrichment:
            enriched.object_contract_sha256 = incoming.object_contract_sha256
        if enriched.to_jsonable() == incoming.to_jsonable():
            return deepcopy(incoming)
        raise ValueError("backfill publication revision conflicts with current phase")
    return deepcopy(incoming)


def merge_xmin_handoff(current: Any | None, incoming: Any | None) -> Any | None:
    """Merge PostgreSQL XMin handoff evidence monotonically."""

    if incoming is None:
        return deepcopy(current)
    if current is None:
        return deepcopy(incoming)
    immutable_fields = (
        "contract_version",
        "handoff_id",
        "anchor_xmin",
        "snapshot_token",
        "state_key_sha256",
        "source_authority_sha256",
        "plan_hash",
        "seed_load_id",
    )
    if tuple(getattr(current, field) for field in immutable_fields) != tuple(
        getattr(incoming, field) for field in immutable_fields
    ):
        raise ValueError("postgres XMin handoff immutable identity changed")
    if current.publication_receipt_id not in (None, incoming.publication_receipt_id):
        raise ValueError("postgres XMin handoff publication authority changed")
    rank = {"anchored": 0, "committing": 1, "committed": 2}
    if rank[incoming.status] < rank[current.status]:
        return deepcopy(current)
    if incoming.status == current.status and incoming.to_jsonable() != current.to_jsonable():
        enriched = deepcopy(current)
        if current.publication_receipt_id is None and bool(incoming.publication_receipt_id):
            enriched.publication_receipt_id = incoming.publication_receipt_id
            if enriched.to_jsonable() == incoming.to_jsonable():
                return deepcopy(incoming)
        raise ValueError("postgres XMin handoff revision conflicts with current status")
    return deepcopy(incoming)


def merge_portable_scope_contract(
    current: dict[str, Any] | None,
    incoming: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Allow one legacy contract bind, then keep that authority immutable."""

    if incoming is None:
        return deepcopy(current)
    if current is None:
        return deepcopy(incoming)
    if current != incoming:
        raise ValueError("backfill portable scope campaign contract changed")
    return deepcopy(current)


def future_utc_iso(value: datetime) -> str:
    """Return a future UTC lease timestamp or reject invalid input."""

    instant = aware_utc(value)
    if instant <= datetime.now(_UTC):
        raise ValueError("backfill lease expiry must be in the future")
    return instant.isoformat()


def aware_utc(value: datetime) -> datetime:
    """Normalize an aware timestamp to UTC."""

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("backfill lease time must be timezone-aware")
    return value.astimezone(_UTC)


def expired_iso(value: str | None, *, now: datetime | None = None) -> bool:
    """Return whether an optional lease timestamp is absent or expired."""

    if not value:
        return True
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(_UTC) <= (now or datetime.now(_UTC)).astimezone(_UTC)


def iso_later(candidate: str, current: str | None) -> bool:
    """Return whether a proposed lease expiry advances the current one."""

    if not current:
        return True
    return datetime.fromisoformat(candidate) > datetime.fromisoformat(current.replace("Z", "+00:00"))


__all__ = [
    "CHUNK_STATUS_FAILED",
    "CHUNK_STATUS_RUNNING",
    "CHUNK_STATUS_SUCCESS",
    "BackfillChunkRecord",
    "BackfillLedger",
    "BackfillPublicationRecord",
    "aware_utc",
    "expired_iso",
    "future_utc_iso",
    "iso_later",
    "merge_portable_scope_contract",
    "merge_publication",
    "merge_xmin_handoff",
    "replace_chunk",
    "require_campaign_identity",
    "require_chunk_coordinates",
    "require_current",
    "require_unique_chunk_shape",
]
