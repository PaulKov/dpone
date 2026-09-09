"""Evidence-backed public claims for the benchmark trust product."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tools.oss_benchmark.config import ROOT
from tools.oss_benchmark.state import project_freshness_status


@dataclass(frozen=True)
class ClaimSpec:
    claim_id: str
    title: str
    project_id: str
    claim_type: str
    evidence_refs: tuple[str, ...]
    gate_impact: str


CLAIMS: tuple[ClaimSpec, ...] = (
    ClaimSpec(
        claim_id="release_quality_gate",
        title="dpone release benchmark gates are passing",
        project_id="dpone",
        claim_type="release-readiness",
        evidence_refs=(
            "json:release_context.release_tag",
            "json:quality_gates.status",
        ),
        gate_impact="blocker",
    ),
    ClaimSpec(
        claim_id="module_size_budget",
        title="dpone production modules stay within the 400 SLOC release gate",
        project_id="dpone",
        claim_type="architecture-quality",
        evidence_refs=(
            "json:projects[dpone].loc_without_tests.max_sloc",
            "json:quality_budgets.status",
        ),
        gate_impact="blocker",
    ),
    ClaimSpec(
        claim_id="nested_lineage_runtime_contract",
        title="dpone nested lineage keeps root and parent identity evidence visible",
        project_id="dpone",
        claim_type="runtime-certification",
        evidence_refs=(
            "json:runtime_certification_v2.scenarios[nested-lineage].status",
            "artifact:docs/nested-normalization.md",
            "artifact:tests/test_nested_normalization_contracts.py",
        ),
        gate_impact="blocker",
    ),
    ClaimSpec(
        claim_id="public_auditability",
        title="dpone benchmark claims are backed by raw evidence and provenance artifacts",
        project_id="dpone",
        claim_type="auditability",
        evidence_refs=(
            "json:evidence_trust.overall_confidence_score",
            "json:source_verification.status",
            "artifact:docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json",
        ),
        gate_impact="warning",
    ),
)


def build_claims_ledger(payload: dict[str, Any], *, root: Path = ROOT) -> dict[str, Any]:
    """Resolve public claims against the merged benchmark payload."""

    claims = [_resolve_claim(spec, payload, root=root) for spec in CLAIMS]
    summary = _status_summary(claims)
    return {
        "schema_version": 1,
        "policy": "Sales claims must resolve to JSON evidence, generated artifacts, or public sources.",
        "summary": summary,
        "claims": claims,
    }


def _resolve_claim(spec: ClaimSpec, payload: dict[str, Any], *, root: Path) -> dict[str, Any]:
    refs = [_resolve_ref(ref, payload, root=root) for ref in spec.evidence_refs]
    missing = [ref["ref"] for ref in refs if ref["status"] == "missing"]
    freshness = _claim_freshness(spec.project_id, payload, refs)
    status = _claim_status(missing, freshness)
    return {
        "claim_id": spec.claim_id,
        "title": spec.title,
        "project_id": spec.project_id,
        "claim_type": spec.claim_type,
        "status": status,
        "confidence": _confidence(status),
        "freshness": freshness,
        "gate_impact": spec.gate_impact,
        "evidence_refs": list(spec.evidence_refs),
        "resolved_refs": refs,
        "missing_refs": missing,
    }


def _resolve_ref(ref: str, payload: dict[str, Any], *, root: Path) -> dict[str, Any]:
    prefix, _, value = ref.partition(":")
    if prefix == "json":
        found = _json_ref_exists(payload, value)
    elif prefix == "artifact":
        found = (root / value).exists()
    elif prefix == "public":
        found = value.startswith(("https://", "http://"))
    else:
        found = False
    return {"ref": ref, "status": "resolved" if found else "missing"}


def _json_ref_exists(payload: dict[str, Any], path: str) -> bool:
    current: Any = payload
    for segment in path.split("."):
        key, selector = _split_selector(segment)
        if not isinstance(current, dict) or key not in current:
            return False
        current = current[key]
        if selector is not None:
            current = _select_item(current, selector)
            if current is None:
                return False
    return current is not None


def _split_selector(segment: str) -> tuple[str, str | None]:
    if "[" not in segment or not segment.endswith("]"):
        return segment, None
    key, selector = segment[:-1].split("[", 1)
    return key, selector


def _select_item(value: Any, selector: str) -> Any:
    if isinstance(value, list):
        for item in value:
            if _matches_selector(item, selector):
                return item
    if isinstance(value, dict):
        return value.get(selector)
    return None


def _matches_selector(item: Any, selector: str) -> bool:
    if not isinstance(item, dict):
        return False
    ids = {
        item.get("project_id"),
        item.get("scenario_id"),
        item.get("claim_id"),
        (item.get("spec") or {}).get("slug") if isinstance(item.get("spec"), dict) else None,
    }
    return selector in ids


def _claim_freshness(project_id: str, payload: dict[str, Any], refs: list[dict[str, Any]]) -> str:
    if any(ref["status"] == "missing" for ref in refs):
        return "unavailable"
    project = _find_project(payload, project_id)
    if not project:
        return "fresh"
    return project_freshness_status(project)


def _find_project(payload: dict[str, Any], project_id: str) -> dict[str, Any]:
    for project in payload.get("projects") or []:
        slug = project.get("project_id") or (project.get("spec") or {}).get("slug")
        if slug == project_id:
            return project
    return {}


def _claim_status(missing: list[str], freshness: str) -> str:
    if missing:
        return "unverified"
    if freshness == "fresh":
        return "verified"
    return "stale"


def _confidence(status: str) -> int:
    return {"verified": 95, "stale": 65}.get(status, 0)


def _status_summary(claims: list[dict[str, Any]]) -> dict[str, int]:
    summary = {"verified": 0, "stale": 0, "unverified": 0}
    for claim in claims:
        status = str(claim.get("status") or "unverified")
        summary[status] = summary.get(status, 0) + 1
    return summary
