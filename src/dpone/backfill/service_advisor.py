"""Backfill command-service advisor assembly helpers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from dpone.backfill.advisor import BackfillPerformanceAdvisor
from dpone.backfill.evidence import collect_advisor_evidence


def build_backfill_advisor_payload(
    *,
    chunks_total: int,
    chunk_rows: Iterable[Mapping[str, Any]],
    profile: str,
    current_step: str,
    current_parallel_workers: int,
    lease_ttl_minutes: int,
    evidence_paths: tuple[str | Path, ...],
) -> dict[str, Any]:
    """Build advisor output from ledger rows plus optional external evidence."""

    evidence_bundle = collect_advisor_evidence(chunks=chunk_rows, evidence_paths=evidence_paths)
    payload = BackfillPerformanceAdvisor().advise(
        chunks_total=chunks_total,
        historical_rows_per_second=None,
        optimize_for=profile,
        current_step=current_step,
        current_parallel_workers=current_parallel_workers,
        lease_ttl_minutes=lease_ttl_minutes,
        chunk_evidence=evidence_bundle.observations,
    )
    payload["evidence_sources"] = list(evidence_bundle.sources)
    return payload


__all__ = ["build_backfill_advisor_payload"]
