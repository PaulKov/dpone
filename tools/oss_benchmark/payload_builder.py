"""Build the base benchmark evidence payload."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from tools.oss_benchmark.config import BENCHMARK_DATE, IGNORED_DIRS, SOURCE_SUFFIXES, TEST_DIR_NAMES
from tools.oss_benchmark.contexts import ReleaseContext, release_context_to_jsonable
from tools.oss_benchmark.maintainability import (
    compute_architecture_risk,
    compute_coverage_confidence,
    compute_industrial_maintainability_index,
    detect_ci_evidence,
)
from tools.oss_benchmark.models import ProjectMetrics
from tools.oss_benchmark.schema import normalize_benchmark_payload_v2, with_project_identity
from tools.oss_benchmark.state import RunContext, fresh_metric_groups, run_context_to_jsonable


def build_benchmark_payload(
    metrics: tuple[ProjectMetrics, ...],
    *,
    generated_at: str,
    run_context: RunContext,
    release_context: ReleaseContext,
) -> dict[str, Any]:
    """Build a v2 base payload before merge/enrichment/rendering steps."""

    payload = {
        "schema_version": 2,
        "benchmark_date": BENCHMARK_DATE,
        "generated_at": generated_at,
        "run_context": run_context_to_jsonable(run_context),
        "release_context": release_context_to_jsonable(release_context),
        "methodology": {
            "scope": "Static source-code maintainability proxies for comparable OSS data-integration repositories.",
            "ignored_dirs": sorted(IGNORED_DIRS),
            "source_suffixes": sorted(SOURCE_SUFFIXES),
            "test_exclusion": sorted(TEST_DIR_NAMES),
            "note": "Scores are static quality indicators, not runtime performance or product feature parity claims.",
        },
        "closed_core_notes": [
            {
                "name": "Fivetran",
                "reason": "closed-core / not code-comparable managed ELT platform; public repositories are SDK/provider surfaces.",
                "url": "https://github.com/fivetran",
            },
            {
                "name": "Informatica",
                "reason": "closed-core / not code-comparable PowerCenter and IDMC products.",
                "url": "https://www.informatica.com/download.html",
            },
        ],
        "projects": [project_to_jsonable(metric, generated_at=generated_at) for metric in metrics],
    }
    return normalize_benchmark_payload_v2(payload)


def project_to_jsonable(metric: ProjectMetrics, *, generated_at: str) -> dict[str, Any]:
    """Convert collected metrics into JSON-compatible project evidence."""

    payload = asdict(metric)
    payload["spec"]["path"] = str(metric.spec.path)
    payload["metric_groups"] = fresh_metric_groups(generated_at)
    payload["ci_evidence"] = detect_ci_evidence(metric.spec.path)
    payload["coverage_confidence"] = compute_coverage_confidence(payload)
    payload["architecture_risk"] = compute_architecture_risk(payload)
    payload["industrial_maintainability"] = compute_industrial_maintainability_index(payload)
    return with_project_identity(payload)
