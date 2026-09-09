"""Backfill performance evidence normalization.

The advisor consumes connector-neutral observations, not connector objects.
This module converts the evidence shapes dpone already produces (chunk ledgers,
load-step audit details, and matrix reports) into that narrow contract.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class BackfillEvidenceBundle:
    """Normalized evidence passed to the backfill performance advisor."""

    observations: tuple[dict[str, Any], ...]
    sources: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "dpone.backfill.evidence.v1",
            "sources": list(self.sources),
            "observations": list(self.observations),
        }


class BackfillEvidenceCollector:
    """Collect normalized observations from durable runtime evidence."""

    def collect(
        self,
        *,
        chunks: Iterable[Mapping[str, Any]] = (),
        load_steps: Iterable[Mapping[str, Any]] = (),
        matrix_report: Mapping[str, Any] | None = None,
    ) -> BackfillEvidenceBundle:
        observations: list[dict[str, Any]] = []
        observations.extend(_chunk_observation(chunk) for chunk in chunks)
        observations.extend(_load_step_observation(step) for step in load_steps)
        if matrix_report:
            observations.append(_matrix_observation(matrix_report))
        sources = tuple(dict.fromkeys(str(item["source"]) for item in observations))
        return BackfillEvidenceBundle(observations=tuple(observations), sources=sources)


def collect_advisor_evidence(
    *,
    chunks: Iterable[Mapping[str, Any]],
    evidence_paths: Iterable[str | Path] = (),
) -> BackfillEvidenceBundle:
    """Collect advisor evidence from the ledger plus optional JSON artifacts."""

    extra_chunks: list[Mapping[str, Any]] = []
    load_steps: list[Mapping[str, Any]] = []
    matrix_reports: list[Mapping[str, Any]] = []
    for path in evidence_paths:
        document = _read_json(Path(path))
        extra_chunks.extend(_as_mappings(document.get("chunks")))
        load_steps.extend(_as_mappings(document.get("load_steps") or document.get("steps")))
        matrix_report = document.get("matrix_report") or document.get("certification_report")
        if isinstance(matrix_report, Mapping):
            matrix_reports.append(matrix_report)
        elif "total_cases" in document or "case_count" in document:
            matrix_reports.append(document)
    observations = (
        BackfillEvidenceCollector()
        .collect(
            chunks=tuple(chunks) + tuple(extra_chunks),
            load_steps=load_steps,
        )
        .observations
    )
    if matrix_reports:
        matrix_observations = BackfillEvidenceCollector().collect(matrix_report=matrix_reports[-1]).observations
        observations = (*observations, *matrix_observations)
    sources = tuple(dict.fromkeys(str(item["source"]) for item in observations))
    return BackfillEvidenceBundle(observations=observations, sources=sources)


def _chunk_observation(chunk: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "source": "chunk_ledger",
        "index": chunk.get("index"),
        "status": chunk.get("status"),
        "rows_loaded": chunk.get("rows_loaded"),
        "rows_extracted": chunk.get("rows_extracted"),
        "started_at": chunk.get("started_at"),
        "finished_at": chunk.get("finished_at"),
        "attempts": chunk.get("attempts"),
        "error": chunk.get("error"),
    }


def _load_step_observation(step: Mapping[str, Any]) -> dict[str, Any]:
    details = _details(step.get("details_json"))
    raw_throughput = details.get("throughput")
    throughput: Mapping[str, Any] = raw_throughput if isinstance(raw_throughput, Mapping) else {}
    return {
        "source": "load_steps",
        "stage": step.get("step_id") or step.get("phase") or step.get("kind"),
        "status": step.get("status"),
        "duration_seconds": throughput.get("duration_seconds") or details.get("duration_seconds"),
        "rows_per_second": throughput.get("rows_per_second"),
        "row_count": throughput.get("row_count") or details.get("row_count") or details.get("rows"),
        "bytes_per_second": throughput.get("bytes_per_second"),
    }


def _matrix_observation(report: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "source": "matrix_report",
        "status": "success" if bool(report.get("passed")) else "failed",
        "case_count": report.get("total_cases") or report.get("case_count"),
        "failed_case_count": report.get("failed_cases") or report.get("failed_case_count") or 0,
        "blockers": list(report.get("blockers") or ()),
    }


def _details(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, Mapping) else {}


def _read_json(path: Path) -> Mapping[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return {"load_steps": data}
    if isinstance(data, Mapping):
        return data
    return {}


def _as_mappings(value: Any) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list):
        return tuple()
    return tuple(item for item in value if isinstance(item, Mapping))


__all__ = ["BackfillEvidenceBundle", "BackfillEvidenceCollector", "collect_advisor_evidence"]
