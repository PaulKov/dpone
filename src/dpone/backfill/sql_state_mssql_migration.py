"""One-time migration helpers for legacy inline MSSQL chunk journals."""

from __future__ import annotations

from dpone.backfill.state import BackfillChunkRecord, BackfillLedger


class LegacyInlineBackfillLedger(BackfillLedger):
    """In-memory marker for a campaign row that still owns inline chunks."""


def campaign_projection_from_payload(payload: dict[str, object]) -> BackfillLedger:
    """Hydrate a compact or explicitly marked legacy campaign projection."""

    projection_type = LegacyInlineBackfillLedger if "chunks" in payload else BackfillLedger
    return projection_type.from_dict(payload)


def overlay_loaded_chunks(
    campaign: BackfillLedger,
    records: tuple[BackfillChunkRecord, ...],
) -> BackfillLedger:
    """Overlay point reads without discarding a legacy inline fallback."""

    if not has_inline_chunks(campaign):
        campaign.chunks = list(records)
        return campaign
    positions = {record.index: position for position, record in enumerate(campaign.chunks)}
    for record in records:
        position = positions.get(record.index)
        if position is None:
            raise ValueError(f"backfill SQL chunk {record.index} is absent from the legacy campaign shape")
        campaign.chunks[position].require_same_coordinates(record)
        campaign.chunks[position] = record
    return campaign


def chunks_for_compact_append(
    current: BackfillLedger | None,
    candidate: BackfillLedger,
    changed: tuple[BackfillChunkRecord, ...],
) -> tuple[BackfillChunkRecord, ...]:
    """Atomically materialize the complete inline fallback on its first compact write."""

    if current is None or not has_inline_chunks(current):
        return changed
    current_indexes = {record.index for record in current.chunks}
    candidate_indexes = {record.index for record in candidate.chunks}
    if not current_indexes or candidate_indexes != current_indexes:
        raise RuntimeError("mssql_backfill_state.legacy_chunk_materialization_incomplete")
    return tuple(candidate.chunks)


def has_inline_chunks(ledger: BackfillLedger) -> bool:
    """Return whether the latest campaign row is the legacy chunk authority."""

    return isinstance(ledger, LegacyInlineBackfillLedger)


__all__ = [
    "campaign_projection_from_payload",
    "chunks_for_compact_append",
    "has_inline_chunks",
    "overlay_loaded_chunks",
]
