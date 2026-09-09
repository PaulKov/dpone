"""Governance and compliance posture scoring for benchmarked data tools."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class GovernanceTool:
    slug: str
    name: str
    code_comparable: bool
    comparator_note: str


@dataclass(frozen=True)
class GovernanceDimension:
    slug: str
    label: str
    weight: int
    value: str


@dataclass(frozen=True)
class GovernanceSignal:
    level: str
    evidence: str
    sources: tuple[str, ...]


LEVEL_POINTS = {
    "strong": 3.0,
    "managed": 2.5,
    "documented": 2.0,
    "partial": 1.0,
    "external": 1.0,
    "opaque": 0.5,
    "not_detected": 0.0,
}
DIMENSIONS = (
    GovernanceDimension("auditability", "Auditability", 3, "run records and operational audit trails"),
    GovernanceDimension("lineage_catalog", "Lineage / catalog", 3, "lineage capture and catalog interoperability"),
    GovernanceDimension("data_contracts", "Data contracts", 3, "schema and payload contract enforcement"),
    GovernanceDimension("schema_governance", "Schema governance", 3, "drift, DDL and compatibility policy"),
    GovernanceDimension("policy_gates", "Policy gates", 3, "automated go/no-go controls"),
    GovernanceDimension("evidence_chain", "Evidence chain", 3, "tamper-evident release or run evidence"),
    GovernanceDimension("access_secrets", "Access and secrets", 2, "credential and least-privilege posture"),
    GovernanceDimension("release_certification", "Release certification", 3, "publishable certification pack"),
    GovernanceDimension("compliance_runbooks", "Compliance runbooks", 2, "self-service governance operations"),
    GovernanceDimension("data_quality_reconciliation", "Data quality reconciliation", 3, "quality and state checks"),
)
PUBLIC_TOOLS = (
    GovernanceTool("airbyte", "Airbyte", True, "OSS/public-doc governance posture"),
    GovernanceTool("dlt", "dlt", True, "OSS/public-doc governance posture"),
    GovernanceTool("pentaho-kettle", "Pentaho Kettle", True, "legacy OSS/public-doc governance posture"),
    GovernanceTool("apache-hop", "Apache Hop", True, "OSS/public-doc governance posture"),
    GovernanceTool("sling", "Sling", True, "OSS/public-doc governance posture"),
    GovernanceTool("fivetran", "Fivetran", False, "managed governance posture; source controls are closed-core"),
    GovernanceTool("informatica", "Informatica", False, "managed governance posture; source controls are closed-core"),
)
PUBLIC_SOURCES = {
    "airbyte": (
        "https://docs.airbyte.com/platform/using-airbyte/schema-change-management",
        "https://docs.airbyte.com/platform/understanding-airbyte/cdc",
    ),
    "dlt": (
        "https://dlthub.com/docs/general-usage/schema-contracts",
        "https://dlthub.com/docs/general-usage/schema-evolution",
    ),
    "pentaho-kettle": ("https://docs.pentaho.com/pdia-data-integration/",),
    "apache-hop": (
        "https://hop.apache.org/manual/latest/metadata-types/index.html",
        "https://hop.apache.org/manual/latest/pipeline/transforms/index.html",
    ),
    "sling": ("https://github.com/slingdata-io/sling-cli", "https://docs.slingdata.io/"),
    "fivetran": (
        "https://fivetran.com/docs/core-concepts/features",
        "https://fivetran.com/docs/logs",
    ),
    "informatica": ("https://www.informatica.com/products/data-governance/cloud-data-governance-and-catalog.html",),
}
PUBLIC_LEVELS = {
    "airbyte": {
        "auditability": "documented",
        "lineage_catalog": "partial",
        "data_contracts": "partial",
        "schema_governance": "documented",
        "policy_gates": "partial",
        "compliance_runbooks": "documented",
        "data_quality_reconciliation": "partial",
    },
    "dlt": {
        "auditability": "partial",
        "lineage_catalog": "partial",
        "data_contracts": "documented",
        "schema_governance": "documented",
        "policy_gates": "partial",
        "compliance_runbooks": "documented",
        "data_quality_reconciliation": "documented",
    },
    "pentaho-kettle": {
        "auditability": "partial",
        "lineage_catalog": "partial",
        "schema_governance": "partial",
        "compliance_runbooks": "documented",
        "data_quality_reconciliation": "partial",
    },
    "apache-hop": {
        "auditability": "partial",
        "lineage_catalog": "documented",
        "schema_governance": "partial",
        "policy_gates": "partial",
        "compliance_runbooks": "documented",
        "data_quality_reconciliation": "partial",
    },
    "sling": {
        "auditability": "partial",
        "data_contracts": "partial",
        "schema_governance": "partial",
        "access_secrets": "partial",
        "compliance_runbooks": "documented",
        "data_quality_reconciliation": "partial",
    },
    "fivetran": {
        "auditability": "managed",
        "lineage_catalog": "managed",
        "data_contracts": "managed",
        "schema_governance": "managed",
        "policy_gates": "managed",
        "evidence_chain": "opaque",
        "access_secrets": "managed",
        "release_certification": "opaque",
        "compliance_runbooks": "documented",
        "data_quality_reconciliation": "managed",
    },
    "informatica": {
        "auditability": "managed",
        "lineage_catalog": "managed",
        "data_contracts": "managed",
        "schema_governance": "managed",
        "policy_gates": "managed",
        "evidence_chain": "managed",
        "access_secrets": "managed",
        "release_certification": "managed",
        "compliance_runbooks": "documented",
        "data_quality_reconciliation": "managed",
    },
}


def build_governance_compliance_matrix(projects: Iterable[dict[str, Any]]) -> dict[str, Any]:
    project_list = [project for project in projects if (project.get("spec") or {}).get("slug") == "dpone"]
    tools = [_project_tool(project) for project in project_list]
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
            "Governance and compliance scores are static posture proxies. Repo-evidence controls are measured from "
            "dpone docs, commands and artifacts; external tools are scored only from public documentation."
        ),
        "levels": LEVEL_POINTS,
        "tools": [tool.__dict__ for tool in tools],
        "dimensions": [dimension.__dict__ for dimension in DIMENSIONS],
        "entries": entries,
        "summary": summary,
    }


def _project_tool(project: dict[str, Any]) -> GovernanceTool:
    spec = project.get("spec") or {}
    return GovernanceTool(
        str(spec.get("slug", "dpone")), str(spec.get("name", "dpone")), True, "repo-evidence governance posture"
    )


def _entry(tool: GovernanceTool, dimension: GovernanceDimension, signal: GovernanceSignal) -> dict[str, Any]:
    return {
        "tool": tool.slug,
        "dimension": dimension.slug,
        "level": signal.level,
        "points": LEVEL_POINTS.get(signal.level, 0.0),
        "evidence": signal.evidence,
        "sources": list(signal.sources),
    }


def _detect_dpone(project: dict[str, Any], dimension: GovernanceDimension) -> GovernanceSignal:
    root = Path(str((project.get("spec") or {}).get("path", ".")))
    sources = _dpone_sources(root, dimension.slug)
    if sources:
        return GovernanceSignal("strong", f"{dimension.label} is backed by repo evidence.", sources)
    return GovernanceSignal("not_detected", f"{dimension.label} governance evidence was not detected.", ())


def _public_signal(slug: str, dimension: GovernanceDimension) -> GovernanceSignal:
    level = PUBLIC_LEVELS.get(slug, {}).get(dimension.slug, "not_detected")
    evidence = (
        f"{dimension.label} is {level} from public governance posture."
        if level != "not_detected"
        else f"{dimension.label} was not detected in public governance posture."
    )
    return GovernanceSignal(level, evidence, PUBLIC_SOURCES.get(slug, ()))


def _summarize(
    tool: GovernanceTool, entries: list[dict[str, Any]], *, evidence_mode: str | None = None
) -> dict[str, Any]:
    weighted = sum(float(entry["points"]) * _weight(entry["dimension"]) for entry in entries)
    maximum = sum(3 * _weight(entry["dimension"]) for entry in entries)
    score = round((weighted / maximum) * 100) if maximum else 0
    return {
        "name": tool.name,
        "score": score,
        "band": "leader" if score >= 85 else "strong" if score >= 70 else "governed" if score >= 50 else "limited",
        "evidence_mode": evidence_mode or "repo-evidence",
        "code_comparable": tool.code_comparable,
        "comparator_note": tool.comparator_note,
        "strong_controls": sum(1 for entry in entries if entry.get("level") == "strong"),
        "managed_controls": sum(1 for entry in entries if entry.get("level") == "managed"),
        "external_controls": sum(1 for entry in entries if entry.get("level") in {"documented", "external"}),
        "notable_gaps": [entry["dimension"] for entry in entries if entry.get("level") in {"opaque", "not_detected"}][
            :4
        ],
    }


def _dpone_sources(root: Path, slug: str) -> tuple[str, ...]:
    candidates = {
        "auditability": (
            "docs/unified-run-evidence.md",
            "docs/release-evidence.md",
            "src/dpone/runtime/state/load_audit.py",
        ),
        "lineage_catalog": ("docs/lineage.md", "docs/load-lineage.md", "src/dpone/runtime/lineage/audit.py"),
        "data_contracts": (
            "docs/schema-contracts.md",
            "docs/data-contract-runtime.md",
            "src/dpone/services/readiness.py",
        ),
        "schema_governance": ("docs/schema-evolution.md", "src/dpone/runtime/schema_evolution.py"),
        "policy_gates": ("src/dpone/services/ops/command_handlers_release.py", "src/dpone/ops/cdc/policy.py"),
        "evidence_chain": ("docs/release-evidence.md", "test_artifacts/**/evidence_chain_index.json"),
        "access_secrets": ("docs/supply-chain.md", "SECURITY.md", ".github/workflows/secret-scan.yml"),
        "release_certification": (
            "docs/certification-suite.md",
            "docs/route-certification-pack.md",
            "test_artifacts/**/release_evidence_pack.json",
        ),
        "compliance_runbooks": (
            "docs/live-certification.md",
            "docs/connector-certification.md",
            "docs/release-evidence.md",
        ),
        "data_quality_reconciliation": (
            "docs/quality-tooling.md",
            "docs/route-certification-pack.md",
            "test_artifacts/**/live_state_reconciliation.json",
        ),
    }.get(slug, ())
    return tuple(_existing_sources(root, candidates))


def _existing_sources(root: Path, candidates: tuple[str, ...]) -> list[str]:
    sources: list[str] = []
    for candidate in candidates:
        if "**" in candidate:
            sources.extend(path.relative_to(root).as_posix() for path in sorted(root.glob(candidate))[:3])
        elif (root / candidate).exists():
            sources.append(candidate)
    return sources


def _weight(slug: str) -> int:
    for dimension in DIMENSIONS:
        if dimension.slug == slug:
            return dimension.weight
    return 1
