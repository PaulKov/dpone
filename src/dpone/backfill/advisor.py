"""Conservative performance advice for backfill campaigns."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class BackfillPerformanceAdvisor:
    """Recommend a safe execution profile without mutating the manifest."""

    max_parallel_cap: int = 4

    def advise(
        self,
        *,
        chunks_total: int,
        historical_rows_per_second: int | None,
        optimize_for: str = "balanced",
        current_step: str | None = None,
        current_parallel_workers: int | None = None,
        lease_ttl_minutes: int | None = None,
        chunk_evidence: Iterable[Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        raw_profile = (optimize_for or "balanced").strip().lower()
        profile, profile_warnings = _normalize_profile(raw_profile)
        evidence = _summarize_chunk_evidence(chunk_evidence)
        observed_rps = evidence.get("rows_per_second") or historical_rows_per_second
        recommended_step = current_step
        if evidence["failed_matrix_reports"]:
            parallel = 1
            risk = "high"
            recommendation = "resolve_vendor_certification_failures"
        elif evidence["failed_chunks"]:
            parallel = 1
            risk = "high"
            recommendation = "stabilize_failed_chunks"
        elif _sink_finalize_bottleneck(evidence):
            parallel = 1
            risk = "medium"
            recommendation = "tune_sink_finalize_before_parallelism"
        elif _many_empty_chunks(evidence) and current_step:
            parallel = min(current_parallel_workers or 1, self.max_parallel_cap)
            risk = "low"
            recommended_step = _scale_step(str(current_step), 2)
            recommendation = "increase_window_to_reduce_empty_chunks"
        elif profile == "worker_safety" or chunks_total <= 2:
            parallel = 1
            risk = "low"
            recommendation = "keep_current_shape"
        elif profile == "speed":
            parallel = min(self.max_parallel_cap, max(2, chunks_total // 50))
            risk = "medium"
            recommendation = "increase_parallelism"
            if _can_increase_window(chunks_total=chunks_total, evidence=evidence, current_step=current_step):
                parallel = max(parallel, self.max_parallel_cap)
                recommended_step = _scale_step(str(current_step), 2)
                recommendation = "increase_window_and_parallelism"
        elif profile == "source_safety":
            parallel = 1
            risk = "low"
            recommendation = "throttle_source"
        else:
            parallel = min(2, self.max_parallel_cap) if chunks_total > 12 else 1
            risk = "low" if parallel == 1 else "medium"
            recommendation = "balanced_parallelism" if parallel > 1 else "keep_current_shape"

        warnings: list[str] = list(profile_warnings)
        if evidence["failed_matrix_reports"]:
            warnings.append("vendor backfill certification failed; fix matrix blockers before tuning throughput")
        if evidence["lease_expired_chunks"]:
            warnings.append("lease_expired")
        if evidence["failed_chunks"]:
            warnings.append("failed chunks exist; retry failed chunks before increasing throughput")
        if _sink_finalize_bottleneck(evidence):
            warnings.append("sink finalize is slower than source read")
        if _many_empty_chunks(evidence):
            warnings.append("many empty chunks observed; use a larger chunk window or narrower backfill bounds")
        if parallel > 1:
            warnings.append("source pressure may increase; keep Airflow pools and DB limits aligned")
        if observed_rps is None:
            warnings.append("historical throughput unavailable; recommendation uses chunk-count heuristics")
        lease = int(lease_ttl_minutes or 60)
        recommended_lease = min(360, max(lease, lease * 2)) if evidence["lease_expired_chunks"] else lease

        return {
            "schema_version": "dpone.backfill.advisor.v1",
            "optimize_for": profile,
            "recommendation": recommendation,
            "recommended_max_parallel_chunks": parallel,
            "recommended_step": recommended_step,
            "recommended_lease_ttl_minutes": recommended_lease,
            "recommended_overrides": _recommended_overrides(
                current_step=current_step,
                recommended_step=recommended_step,
                current_parallel_workers=current_parallel_workers,
                recommended_parallel_workers=parallel,
                current_lease_ttl_minutes=lease,
                recommended_lease_ttl_minutes=recommended_lease,
            ),
            "current_parallel_workers": current_parallel_workers,
            "risk": risk,
            "warnings": warnings,
            "evidence": evidence,
            "rationale": (
                f"{profile} profile over {chunks_total} chunks"
                + (f" with historical throughput {observed_rps} rows/sec" if observed_rps else "")
            ),
        }


def _summarize_chunk_evidence(chunk_evidence: Iterable[Mapping[str, Any]] | None) -> dict[str, Any]:
    evidence = tuple(chunk_evidence or ())
    rows_per_seconds: list[int] = []
    stage_rate_samples: dict[str, list[float]] = {}
    failed = 0
    lease_expired = 0
    empty = 0
    chunk_count = 0
    success_count = 0
    matrix_count = 0
    failed_matrix_count = 0
    matrix_cases = 0
    failed_matrix_cases = 0
    matrix_blockers: list[str] = []
    for item in evidence:
        status = str(item.get("status") or "")
        source = str(item.get("source") or "chunk_ledger")
        if source == "load_steps":
            stage = str(item.get("stage") or "")
            stage_rps = _positive_float(item.get("rows_per_second"))
            if stage and stage_rps is not None:
                stage_rate_samples.setdefault(stage, []).append(stage_rps)
            continue
        if source == "matrix_report":
            matrix_count += 1
            matrix_case_count = _nonnegative_int(item.get("case_count"))
            matrix_failed_cases = _nonnegative_int(item.get("failed_case_count"))
            matrix_cases += matrix_case_count
            failed_matrix_cases += matrix_failed_cases
            if status == "failed" or matrix_failed_cases > 0:
                failed_matrix_count += 1
                matrix_blockers.extend(str(blocker) for blocker in _list(item.get("blockers")))
            continue
        chunk_count += 1
        if status == "failed":
            failed += 1
        if status == "success":
            success_count += 1
        if str(item.get("error") or "") == "lease_expired":
            lease_expired += 1
        if status == "success" and _row_count(item) == 0:
            empty += 1
        rps = _chunk_rows_per_second(item)
        if rps is not None:
            rows_per_seconds.append(rps)
    stage_rates = {stage: min(samples) for stage, samples in stage_rate_samples.items()}
    bottleneck_stage = _bottleneck_stage(stage_rates)
    return {
        "observations": len(evidence),
        "chunks_observed": chunk_count,
        "successful_chunks": success_count,
        "failed_chunks": failed,
        "lease_expired_chunks": lease_expired,
        "empty_chunks": empty,
        "empty_chunk_ratio": round(empty / chunk_count, 4) if chunk_count else 0.0,
        "rows_per_second": int(sum(rows_per_seconds) / len(rows_per_seconds)) if rows_per_seconds else None,
        "stage_rows_per_second": stage_rates,
        "bottleneck_stage": bottleneck_stage,
        "matrix_reports_observed": matrix_count,
        "failed_matrix_reports": failed_matrix_count,
        "matrix_cases": matrix_cases,
        "failed_matrix_cases": failed_matrix_cases,
        "matrix_blockers": matrix_blockers[:10],
    }


def _chunk_rows_per_second(item: Mapping[str, Any]) -> int | None:
    started = _parse_dt(item.get("started_at"))
    finished = _parse_dt(item.get("finished_at"))
    rows = _row_count(item)
    if not started or not finished or rows <= 0:
        return None
    seconds = (finished - started).total_seconds()
    return int(rows / seconds) if seconds > 0 else None


def _row_count(item: Mapping[str, Any]) -> int:
    return int(item.get("rows_loaded") or item.get("rows_extracted") or item.get("row_count") or 0)


def _positive_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _nonnegative_int(value: Any) -> int:
    if value is None or isinstance(value, bool):
        return 0
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return max(0, parsed)


def _list(value: Any) -> tuple[Any, ...]:
    if isinstance(value, (list, tuple)):
        return tuple(value)
    return tuple()


def _bottleneck_stage(stage_rates: Mapping[str, float]) -> str | None:
    if not stage_rates:
        return None
    return min(stage_rates, key=lambda key: stage_rates[key])


def _sink_finalize_bottleneck(evidence: Mapping[str, Any]) -> bool:
    rates = evidence.get("stage_rows_per_second")
    if not isinstance(rates, Mapping):
        return False
    sink_rate = _positive_float(rates.get("sink_finalize"))
    source_rate = _positive_float(rates.get("source_read"))
    return bool(sink_rate is not None and source_rate is not None and source_rate >= sink_rate * 3)


def _many_empty_chunks(evidence: Mapping[str, Any]) -> bool:
    return float(evidence.get("empty_chunk_ratio") or 0.0) >= 0.5 and int(evidence.get("empty_chunks") or 0) >= 2


def _normalize_profile(profile: str) -> tuple[str, tuple[str, ...]]:
    allowed = {"balanced", "speed", "source_safety", "worker_safety"}
    if profile in allowed:
        return profile, ()
    return "balanced", (f"unknown optimize_for={profile}; using balanced",)


def _recommended_overrides(
    *,
    current_step: str | None,
    recommended_step: str | None,
    current_parallel_workers: int | None,
    recommended_parallel_workers: int,
    current_lease_ttl_minutes: int,
    recommended_lease_ttl_minutes: int,
) -> dict[str, Any]:
    overrides: dict[str, Any] = {}
    if current_step and recommended_step and current_step != recommended_step:
        overrides["chunk.step"] = recommended_step
    if current_parallel_workers is not None and current_parallel_workers != recommended_parallel_workers:
        overrides["parallel_workers"] = recommended_parallel_workers
    if current_lease_ttl_minutes != recommended_lease_ttl_minutes:
        overrides["lease_ttl_minutes"] = recommended_lease_ttl_minutes
    return overrides


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _can_increase_window(*, chunks_total: int, evidence: Mapping[str, Any], current_step: str | None) -> bool:
    return bool(
        current_step
        and chunks_total >= 20
        and evidence["successful_chunks"] >= 2
        and not evidence["failed_chunks"]
        and (evidence.get("rows_per_second") or 0) >= 10_000
    )


def _scale_step(step: str, factor: int) -> str:
    match = re.fullmatch(r"(?P<count>\d+)(?P<unit>h|d|w|mo|y)?", step.strip())
    if not match:
        return step
    unit = match.group("unit") or ""
    return f"{int(match.group('count')) * factor}{unit}"


__all__ = ["BackfillPerformanceAdvisor"]
