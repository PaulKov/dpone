"""Deterministic backfill chunk plan generation.

The planner is pure: the same spec always yields the same chunk list and the
same idempotency keys, which makes plans reviewable, diffable, and safe to
resume after partial failure.
"""

from __future__ import annotations

import hashlib
from datetime import date, datetime, timedelta
from uuid import UUID

from dpone.backfill.chunk_scope import build_backfill_chunk_scope
from dpone.backfill.models import (
    BackfillChunk,
    BackfillChunkSpec,
    BackfillStep,
    parse_date_boundary,
    parse_step,
    parse_timestamp_boundary,
)
from dpone.contracts.portable_relation_scope import (
    PortableLiteral,
    PortableRangeBound,
    PortableRangeScope,
    portable_scope_sha256,
)


def backfill_run_key(
    *,
    dataset: str,
    spec: BackfillChunkSpec,
    inner_mode: str,
) -> str:
    """Stable identity of one backfill campaign.

    The key is derived from the dataset and the full chunk configuration so a
    re-invocation with identical parameters resumes the same ledger, while any
    boundary/step change starts a fresh campaign.
    """

    material = "|".join((dataset, inner_mode, *spec.fingerprint_parts()))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def plan_chunks(spec: BackfillChunkSpec, *, run_key: str) -> tuple[BackfillChunk, ...]:
    """Build the deterministic, ordered chunk list for a spec."""

    if spec.kind == "uuid":
        chunks = _uuid_chunks(spec, run_key=run_key)
    else:
        step = parse_step(spec.step, kind=spec.kind)
        if spec.kind == "integer":
            chunks = _integer_chunks(spec, step=step, run_key=run_key)
        elif spec.kind == "date":
            chunks = _temporal_chunks(
                spec,
                step=step,
                run_key=run_key,
                cursor=parse_date_boundary(spec.start),
                end_exclusive=parse_date_boundary(spec.end) + timedelta(days=1),
                render=lambda value: value.isoformat(),
            )
        else:
            chunks = _temporal_chunks(
                spec,
                step=step,
                run_key=run_key,
                cursor=parse_timestamp_boundary(spec.start),
                end_exclusive=parse_timestamp_boundary(spec.end),
                render=lambda value: value.isoformat(sep=" "),
            )
    if len(chunks) > spec.max_chunks:
        raise ValueError(
            f"backfill would produce {len(chunks)} chunks, above max_chunks={spec.max_chunks}; "
            "increase backfill.max_chunks or use a larger step"
        )
    return chunks


def _uuid_chunks(spec: BackfillChunkSpec, *, run_key: str) -> tuple[BackfillChunk, ...]:
    """Partition the complete UUID space without source discovery or scans."""

    buckets = spec.buckets
    if buckets is None:
        raise ValueError("backfill.chunk.kind=uuid requires buckets")
    cardinality = 1 << 128
    chunks: list[BackfillChunk] = []
    for offset in range(buckets):
        lower = UUID(int=(offset * cardinality) // buckets)
        is_last = offset == buckets - 1
        upper = UUID(int=cardinality - 1 if is_last else ((offset + 1) * cardinality) // buckets)
        scope = PortableRangeScope(
            column=spec.column,
            lower=PortableRangeBound(PortableLiteral("uuid", lower), inclusive=True),
            upper=PortableRangeBound(PortableLiteral("uuid", upper), inclusive=is_last),
        )
        scope_sha256 = portable_scope_sha256(scope).hex()
        index = offset + 1
        chunks.append(
            BackfillChunk(
                index=index,
                start=str(lower),
                end=str(upper),
                portable_scope=scope,
                idempotency_key=f"{run_key}:{index}:scope:{scope_sha256}",
            )
        )
    return tuple(chunks)


def _integer_chunks(spec: BackfillChunkSpec, *, step: BackfillStep, run_key: str) -> tuple[BackfillChunk, ...]:
    cursor = int(spec.start)
    end = int(spec.end)
    chunks: list[BackfillChunk] = []
    index = 1
    while cursor <= end:
        chunk_end = min(cursor + step.count - 1, end)
        chunks.append(_chunk(spec, run_key, index, str(cursor), str(chunk_end)))
        cursor = chunk_end + 1
        index += 1
    return tuple(chunks)


def _temporal_chunks(
    spec: BackfillChunkSpec,
    *,
    step: BackfillStep,
    run_key: str,
    cursor: date | datetime,
    end_exclusive: date | datetime,
    render,
) -> tuple[BackfillChunk, ...]:
    chunks: list[BackfillChunk] = []
    index = 1
    while cursor < end_exclusive:
        window_end = _advance(cursor, step)
        if window_end > end_exclusive:
            window_end = end_exclusive
        start_text = render(cursor)
        end_text = render(window_end)
        chunks.append(_chunk(spec, run_key, index, start_text, end_text))
        cursor = window_end
        index += 1
    return tuple(chunks)


def _advance(cursor: date | datetime, step: BackfillStep) -> date | datetime:
    unit = step.unit or "d"
    if unit == "h":
        return cursor + timedelta(hours=step.count)
    if unit == "d":
        return cursor + timedelta(days=step.count)
    if unit == "w":
        return cursor + timedelta(weeks=step.count)
    if unit == "mo":
        return _add_months(cursor, step.count)
    return _add_months(cursor, step.count * 12)


def _add_months(cursor: date | datetime, months: int) -> date | datetime:
    month_index = cursor.month - 1 + months
    year = cursor.year + month_index // 12
    month = month_index % 12 + 1
    day = min(cursor.day, _days_in_month(year, month))
    return cursor.replace(year=year, month=month, day=day)


def _days_in_month(year: int, month: int) -> int:
    if month == 12:
        return 31
    first_next = date(year + (month // 12), month % 12 + 1, 1)
    return (first_next - timedelta(days=1)).day


def _chunk(
    spec: BackfillChunkSpec,
    run_key: str,
    index: int,
    start: str,
    end: str,
) -> BackfillChunk:
    scope = build_backfill_chunk_scope(spec, start=start, end=end)
    scope_sha256 = portable_scope_sha256(scope).hex()
    return BackfillChunk(
        index=index,
        start=start,
        end=end,
        portable_scope=scope,
        idempotency_key=f"{run_key}:{index}:scope:{scope_sha256}",
    )


__all__ = ["backfill_run_key", "plan_chunks"]
