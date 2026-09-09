"""Operational reliability posture scoring for benchmarked data tools."""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ReliabilityTool:
    slug: str
    name: str
    code_comparable: bool
    comparator_note: str


@dataclass(frozen=True)
class ReliabilityDimension:
    slug: str
    label: str
    weight: int
    value: str


@dataclass(frozen=True)
class ReliabilitySignal:
    level: str
    evidence: str
    sources: tuple[str, ...]


LEVEL_POINTS = {"measured": 3.0, "documented": 2.0, "partial": 1.0, "not_detected": 0.0}
DIMENSIONS = (
    ReliabilityDimension("retry_resume", "Retry / resume", 3, "recover failed loads without manual rebuilds"),
    ReliabilityDimension("checkpoint_safety", "Checkpoint safety", 3, "avoid advancing state before durable success"),
    ReliabilityDimension("idempotency", "Idempotency", 3, "avoid duplicates and lost rows on retry"),
    ReliabilityDimension("fault_injection", "Fault injection", 2, "prove recovery at known failure stages"),
    ReliabilityDimension("data_reconciliation", "Data reconciliation", 3, "verify source and target correctness"),
    ReliabilityDimension("cdc_recovery", "CDC recovery", 3, "resume incremental streams safely"),
    ReliabilityDimension("observability_evidence", "Observability evidence", 2, "debug and audit runtime behavior"),
    ReliabilityDimension("performance_slo", "Performance SLO", 2, "guard throughput, lag, and failure rate"),
    ReliabilityDimension("schema_drift", "Schema drift", 2, "survive source shape changes"),
    ReliabilityDimension("release_evidence", "Release evidence", 2, "ship with auditable certification packs"),
)
PUBLIC_TOOLS = (
    ReliabilityTool("airbyte", "Airbyte", True, "public-doc posture and OSS source"),
    ReliabilityTool("dlt", "dlt", True, "public-doc posture and OSS source"),
    ReliabilityTool("pentaho-kettle", "Pentaho Kettle", True, "public-doc posture and OSS source"),
    ReliabilityTool("apache-hop", "Apache Hop", True, "public-doc posture and OSS source"),
    ReliabilityTool("sling", "Sling", True, "public-doc posture and OSS source"),
    ReliabilityTool("fivetran", "Fivetran", False, "managed closed-core public-doc posture"),
    ReliabilityTool("informatica", "Informatica", False, "managed closed-core public-doc posture"),
)
PUBLIC_SOURCES = {
    "airbyte": ("https://docs.airbyte.com/platform/understanding-airbyte/cdc",),
    "dlt": ("https://dlthub.com/docs/general-usage/schema-evolution",),
    "pentaho-kettle": (
        "https://docs.pentaho.com/pdia-data-integration/pipeline-designer/working-with-transformations",
    ),
    "apache-hop": ("https://hop.apache.org/",),
    "sling": ("https://github.com/slingdata-io/sling-cli", "https://docs.slingdata.io/"),
    "fivetran": ("https://fivetran.com/docs/core-concepts/features",),
    "informatica": ("https://www.informatica.com/products/data-governance/cloud-data-governance-and-catalog.html",),
}
PUBLIC_LEVELS = {
    "airbyte": {
        "retry_resume": "documented",
        "checkpoint_safety": "documented",
        "idempotency": "partial",
        "fault_injection": "partial",
        "data_reconciliation": "partial",
        "cdc_recovery": "documented",
        "observability_evidence": "documented",
        "performance_slo": "partial",
        "schema_drift": "documented",
        "release_evidence": "partial",
    },
    "dlt": {
        "retry_resume": "documented",
        "checkpoint_safety": "documented",
        "idempotency": "partial",
        "schema_drift": "documented",
        "observability_evidence": "partial",
    },
    "pentaho-kettle": {
        "retry_resume": "partial",
        "checkpoint_safety": "partial",
        "data_reconciliation": "partial",
        "observability_evidence": "partial",
        "schema_drift": "partial",
    },
    "apache-hop": {
        "retry_resume": "partial",
        "checkpoint_safety": "partial",
        "data_reconciliation": "partial",
        "observability_evidence": "partial",
        "schema_drift": "partial",
        "release_evidence": "partial",
    },
    "sling": {
        "retry_resume": "partial",
        "checkpoint_safety": "partial",
        "idempotency": "partial",
        "data_reconciliation": "partial",
        "cdc_recovery": "partial",
        "observability_evidence": "partial",
        "schema_drift": "partial",
    },
    "fivetran": {
        "retry_resume": "documented",
        "checkpoint_safety": "documented",
        "idempotency": "documented",
        "data_reconciliation": "documented",
        "cdc_recovery": "documented",
        "observability_evidence": "documented",
        "performance_slo": "documented",
        "schema_drift": "documented",
        "release_evidence": "partial",
    },
    "informatica": {
        "retry_resume": "documented",
        "checkpoint_safety": "documented",
        "idempotency": "documented",
        "data_reconciliation": "documented",
        "cdc_recovery": "documented",
        "observability_evidence": "documented",
        "performance_slo": "documented",
        "schema_drift": "documented",
        "release_evidence": "documented",
    },
}


def build_operational_reliability_matrix(projects: Iterable[dict[str, Any]]) -> dict[str, Any]:
    project_list = [project for project in projects if (project.get("spec") or {}).get("slug") == "dpone"]
    tools = [_tool_from_project(project) for project in project_list]
    entries: list[dict[str, Any]] = []
    summary: dict[str, dict[str, Any]] = {}
    for tool, project in zip(tools, project_list, strict=False):
        project_entries = [_entry(tool, dimension, _detect_dpone(project, dimension)) for dimension in DIMENSIONS]
        entries.extend(project_entries)
        summary[tool.slug] = _summarize(tool, project_entries)
    for tool in PUBLIC_TOOLS:
        public_entries = [_entry(tool, dimension, _public_signal(tool.slug, dimension)) for dimension in DIMENSIONS]
        entries.extend(public_entries)
        summary[tool.slug] = _summarize(tool, public_entries, evidence_mode="public-doc")
        tools.append(tool)
    return {
        "schema_version": 1,
        "methodology": (
            "Operational reliability compares measured dpone runtime evidence with public reliability posture for "
            "external tools. Measured means JSON proof artifacts were found; documented means public or local docs."
        ),
        "levels": LEVEL_POINTS,
        "tools": [tool.__dict__ for tool in tools],
        "dimensions": [dimension.__dict__ for dimension in DIMENSIONS],
        "entries": entries,
        "summary": summary,
    }


def _tool_from_project(project: dict[str, Any]) -> ReliabilityTool:
    spec = project.get("spec") or {}
    return ReliabilityTool(
        str(spec.get("slug", "dpone")), str(spec.get("name", "dpone")), True, "measured local evidence"
    )


def _entry(tool: ReliabilityTool, dimension: ReliabilityDimension, signal: ReliabilitySignal) -> dict[str, Any]:
    return {
        "tool": tool.slug,
        "dimension": dimension.slug,
        "level": signal.level,
        "points": LEVEL_POINTS.get(signal.level, 0.0),
        "evidence": signal.evidence,
        "sources": list(signal.sources),
    }


def _detect_dpone(project: dict[str, Any], dimension: ReliabilityDimension) -> ReliabilitySignal:
    root = Path(str((project.get("spec") or {}).get("path", ".")))
    resume = _resume_evidence(root)
    measured = {
        "retry_resume": bool(resume),
        "checkpoint_safety": any(item.get("safe_to_resume") for item in resume),
        "idempotency": any(_zero_duplicate_retry(item) for item in resume),
        "fault_injection": any("fault" in source or "failure_stage" in item for source, item in _resume_pairs(root)),
        "data_reconciliation": _passed_json(root, "**/live_state_reconciliation.json")
        or any(_row_counts_match(item) for item in resume),
        "cdc_recovery": _has_state_check(root, "cdc_offsets"),
        "performance_slo": _passed_json(root, "**/benchmark_slo_gate.json"),
        "release_evidence": _passed_json(root, "**/release_evidence_pack.json")
        or _json_exists(root, "**/evidence_chain_index.json"),
    }
    if measured.get(dimension.slug):
        return ReliabilitySignal(
            "measured", f"{dimension.label} has measured dpone proof.", _sources(root, dimension.slug)
        )
    documented_sources = _doc_sources(root, dimension.slug)
    if documented_sources:
        return ReliabilitySignal("documented", f"{dimension.label} is documented for dpone.", documented_sources)
    return ReliabilitySignal("not_detected", f"{dimension.label} evidence was not detected.", ())


def _public_signal(slug: str, dimension: ReliabilityDimension) -> ReliabilitySignal:
    level = PUBLIC_LEVELS.get(slug, {}).get(dimension.slug, "not_detected")
    evidence = (
        f"{dimension.label} is {level} from public posture."
        if level != "not_detected"
        else f"{dimension.label} was not detected in public posture."
    )
    return ReliabilitySignal(level, evidence, PUBLIC_SOURCES.get(slug, ()))


def _summarize(
    tool: ReliabilityTool, entries: list[dict[str, Any]], *, evidence_mode: str | None = None
) -> dict[str, Any]:
    weighted = sum(float(entry["points"]) * _weight(entry["dimension"]) for entry in entries)
    maximum = sum(3 * _weight(entry["dimension"]) for entry in entries)
    score = round((weighted / maximum) * 100) if maximum else 0
    measured_count = sum(1 for entry in entries if entry.get("level") == "measured")
    mode = evidence_mode or ("measured" if measured_count else "documented")
    return {
        "name": tool.name,
        "score": score,
        "band": "leader" if score >= 85 else "strong" if score >= 70 else "watch" if score >= 45 else "limited",
        "evidence_mode": mode,
        "code_comparable": tool.code_comparable,
        "comparator_note": tool.comparator_note,
        "measured_controls": measured_count,
        "documented_controls": sum(1 for entry in entries if entry.get("level") == "documented"),
        "notable_gaps": [entry["dimension"] for entry in entries if entry.get("level") == "not_detected"][:4],
    }


def _resume_pairs(root: Path) -> tuple[tuple[str, dict[str, Any]], ...]:
    paths = sorted((root / "test_artifacts").glob("**/native_transfer_resume*.json"))[:40]
    return tuple((str(path.relative_to(root)), payload) for path in paths if (payload := _json_payload(path)))


def _resume_evidence(root: Path) -> tuple[dict[str, Any], ...]:
    return tuple(item for _, item in _resume_pairs(root) if item.get("passed") is True)


def _zero_duplicate_retry(payload: dict[str, Any]) -> bool:
    request = payload.get("request") or {}
    return _row_counts_match(payload) and int(request.get("duplicate_rows_after_retry", 1)) == 0


def _row_counts_match(payload: dict[str, Any]) -> bool:
    request = payload.get("request") or {}
    return request.get("expected_rows") == request.get("actual_rows_after_retry")


def _passed_json(root: Path, pattern: str) -> bool:
    return any((_json_payload(path) or {}).get("passed") is True for path in (root / "test_artifacts").glob(pattern))


def _has_state_check(root: Path, check: str) -> bool:
    payloads = [_json_payload(path) for path in (root / "test_artifacts").glob("**/live_state_reconciliation.json")]
    return any(check in (payload or {}).get("state_checks", []) for payload in payloads)


def _json_exists(root: Path, pattern: str) -> bool:
    return any((root / "test_artifacts").glob(pattern))


def _json_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _sources(root: Path, slug: str) -> tuple[str, ...]:
    patterns = {
        "retry_resume": ("**/native_transfer_resume*.json",),
        "checkpoint_safety": ("**/native_transfer_resume*.json",),
        "idempotency": ("**/native_transfer_resume*.json",),
        "fault_injection": ("**/native_transfer_resume*.json",),
        "data_reconciliation": ("**/live_state_reconciliation.json", "**/native_transfer_resume*.json"),
        "cdc_recovery": ("**/live_state_reconciliation.json",),
        "performance_slo": ("**/benchmark_slo_gate.json",),
        "release_evidence": ("**/release_evidence_pack.json", "**/evidence_chain_index.json"),
    }.get(slug, ())
    return tuple(
        _relative(root, path) for pattern in patterns for path in sorted((root / "test_artifacts").glob(pattern))[:4]
    )


def _doc_sources(root: Path, slug: str) -> tuple[str, ...]:
    docs = {
        "retry_resume": ("docs/orchestration.md", "docs/cdc.md"),
        "checkpoint_safety": ("docs/orchestration.md", "docs/route-certification-pack.md"),
        "idempotency": ("docs/cdc-runtime-orchestrator.md",),
        "fault_injection": ("docs/live-certification.md",),
        "data_reconciliation": ("docs/live-certification.md", "docs/route-certification-pack.md"),
        "cdc_recovery": ("docs/release-evidence.md", "docs/cdc.md"),
        "observability_evidence": ("docs/release-evidence.md", "docs/observability.md"),
        "performance_slo": ("docs/live-certification.md",),
        "schema_drift": ("docs/physical-ddl-apply.md", "docs/schema-evolution.md"),
        "release_evidence": ("docs/route-certification-pack.md", "docs/certification-suite.md"),
    }.get(slug, ())
    return tuple(path for path in docs if (root / path).exists())


def _relative(root: Path, path: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def _weight(slug: str) -> int:
    for dimension in DIMENSIONS:
        if dimension.slug == slug:
            return dimension.weight
    return 1
