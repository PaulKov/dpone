"""TCO and operability posture scoring for benchmarked data tools."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class OperabilityTool:
    slug: str
    name: str
    code_comparable: bool
    comparator_note: str


@dataclass(frozen=True)
class OperabilityDimension:
    slug: str
    label: str
    weight: int
    value: str


@dataclass(frozen=True)
class OperabilitySignal:
    level: str
    evidence: str
    sources: tuple[str, ...]


LEVEL_POINTS = {"strong": 3.0, "managed": 2.5, "moderate": 2.0, "partial": 1.0, "opaque": 0.5, "not_detected": 0.0}
DIMENSIONS = (
    OperabilityDimension("deployment_footprint", "Deployment footprint", 3, "how much platform must be installed"),
    OperabilityDimension("infra_prerequisites", "Infrastructure prerequisites", 3, "required runtime services"),
    OperabilityDimension("configuration_surface", "Configuration surface", 2, "operator-facing config complexity"),
    OperabilityDimension("secrets_operations", "Secrets operations", 2, "safe secret handling and rotation path"),
    OperabilityDimension("self_service_docs", "Self-service docs", 3, "runbooks and user-facing automation docs"),
    OperabilityDimension("ci_cd_automation", "CI/CD automation", 2, "repeatable release and validation flows"),
    OperabilityDimension("observability_ops", "Observability operations", 2, "built-in diagnostics for operators"),
    OperabilityDimension("upgrade_rollback", "Upgrade / rollback posture", 2, "safe change management"),
    OperabilityDimension("operator_toil", "Operator toil", 3, "manual work per production route"),
    OperabilityDimension("lock_in_transparency", "Vendor lock-in transparency", 2, "license and portability clarity"),
)
PUBLIC_TOOLS = (
    OperabilityTool("airbyte", "Airbyte", True, "OSS/self-managed platform posture"),
    OperabilityTool("dlt", "dlt", True, "Python library posture"),
    OperabilityTool("pentaho-kettle", "Pentaho Kettle", True, "legacy OSS platform posture"),
    OperabilityTool("apache-hop", "Apache Hop", True, "OSS platform posture"),
    OperabilityTool("sling", "Sling", True, "CLI-first OSS posture"),
    OperabilityTool("fivetran", "Fivetran", False, "managed platform trade-off"),
    OperabilityTool("informatica", "Informatica", False, "managed enterprise platform trade-off"),
)
PUBLIC_SOURCES = {
    "airbyte": ("https://docs.airbyte.com/platform/deploying-airbyte",),
    "dlt": ("https://dlthub.com/docs/intro", "https://dlthub.com/docs/walkthroughs/deploy-a-pipeline"),
    "pentaho-kettle": ("https://docs.pentaho.com/install",),
    "apache-hop": ("https://hop.apache.org/manual/latest/installation-configuration.html",),
    "sling": ("https://github.com/slingdata-io/sling-cli", "https://docs.slingdata.io/"),
    "fivetran": (
        "https://fivetran.com/docs/getting-started",
        "https://fivetran.com/docs/core-concepts/deployment-models",
    ),
    "informatica": ("https://www.informatica.com/products/cloud-data-integration.html",),
}
PUBLIC_LEVELS = {
    "airbyte": {
        "deployment_footprint": "partial",
        "infra_prerequisites": "partial",
        "configuration_surface": "moderate",
        "secrets_operations": "moderate",
        "self_service_docs": "strong",
        "ci_cd_automation": "moderate",
        "observability_ops": "moderate",
        "upgrade_rollback": "moderate",
        "operator_toil": "partial",
        "lock_in_transparency": "strong",
    },
    "dlt": {
        "deployment_footprint": "strong",
        "infra_prerequisites": "strong",
        "configuration_surface": "moderate",
        "secrets_operations": "partial",
        "self_service_docs": "strong",
        "ci_cd_automation": "moderate",
        "observability_ops": "partial",
        "upgrade_rollback": "moderate",
        "operator_toil": "moderate",
        "lock_in_transparency": "strong",
    },
    "pentaho-kettle": {
        "deployment_footprint": "partial",
        "infra_prerequisites": "partial",
        "configuration_surface": "partial",
        "self_service_docs": "moderate",
        "observability_ops": "partial",
        "upgrade_rollback": "partial",
        "operator_toil": "partial",
        "lock_in_transparency": "moderate",
    },
    "apache-hop": {
        "deployment_footprint": "moderate",
        "infra_prerequisites": "moderate",
        "configuration_surface": "moderate",
        "self_service_docs": "strong",
        "observability_ops": "moderate",
        "upgrade_rollback": "partial",
        "operator_toil": "moderate",
        "lock_in_transparency": "strong",
    },
    "sling": {
        "deployment_footprint": "strong",
        "infra_prerequisites": "strong",
        "configuration_surface": "moderate",
        "secrets_operations": "partial",
        "self_service_docs": "strong",
        "ci_cd_automation": "moderate",
        "observability_ops": "partial",
        "upgrade_rollback": "moderate",
        "operator_toil": "moderate",
        "lock_in_transparency": "strong",
    },
    "fivetran": {
        "deployment_footprint": "managed",
        "infra_prerequisites": "managed",
        "configuration_surface": "managed",
        "secrets_operations": "managed",
        "self_service_docs": "strong",
        "ci_cd_automation": "managed",
        "observability_ops": "managed",
        "upgrade_rollback": "managed",
        "operator_toil": "managed",
        "lock_in_transparency": "opaque",
    },
    "informatica": {
        "deployment_footprint": "moderate",
        "infra_prerequisites": "moderate",
        "configuration_surface": "partial",
        "secrets_operations": "moderate",
        "self_service_docs": "strong",
        "ci_cd_automation": "moderate",
        "observability_ops": "managed",
        "upgrade_rollback": "moderate",
        "operator_toil": "partial",
        "lock_in_transparency": "opaque",
    },
}


def build_operability_tco_matrix(projects: Iterable[dict[str, Any]]) -> dict[str, Any]:
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
            "TCO and operability are static proxies for operational surface area, not commercial pricing claims. "
            "Higher scores mean lower self-managed complexity, better self-service UX, and clearer portability."
        ),
        "levels": LEVEL_POINTS,
        "tools": [tool.__dict__ for tool in tools],
        "dimensions": [dimension.__dict__ for dimension in DIMENSIONS],
        "entries": entries,
        "summary": summary,
    }


def _project_tool(project: dict[str, Any]) -> OperabilityTool:
    spec = project.get("spec") or {}
    return OperabilityTool(
        str(spec.get("slug", "dpone")), str(spec.get("name", "dpone")), True, "repo-evidence posture"
    )


def _entry(tool: OperabilityTool, dimension: OperabilityDimension, signal: OperabilitySignal) -> dict[str, Any]:
    return {
        "tool": tool.slug,
        "dimension": dimension.slug,
        "level": signal.level,
        "points": LEVEL_POINTS.get(signal.level, 0.0),
        "evidence": signal.evidence,
        "sources": list(signal.sources),
    }


def _detect_dpone(project: dict[str, Any], dimension: OperabilityDimension) -> OperabilitySignal:
    root = Path(str((project.get("spec") or {}).get("path", ".")))
    sources = _dpone_sources(root, dimension.slug)
    if sources:
        return OperabilitySignal("strong", f"{dimension.label} has repo evidence for low operator drag.", sources)
    return OperabilitySignal("not_detected", f"{dimension.label} evidence was not detected.", ())


def _public_signal(slug: str, dimension: OperabilityDimension) -> OperabilitySignal:
    level = PUBLIC_LEVELS.get(slug, {}).get(dimension.slug, "not_detected")
    evidence = (
        f"{dimension.label} is {level} from public operability posture."
        if level != "not_detected"
        else f"{dimension.label} was not detected in public operability posture."
    )
    return OperabilitySignal(level, evidence, PUBLIC_SOURCES.get(slug, ()))


def _summarize(
    tool: OperabilityTool, entries: list[dict[str, Any]], *, evidence_mode: str | None = None
) -> dict[str, Any]:
    weighted = sum(float(entry["points"]) * _weight(entry["dimension"]) for entry in entries)
    maximum = sum(3 * _weight(entry["dimension"]) for entry in entries)
    score = round((weighted / maximum) * 100) if maximum else 0
    strong = sum(1 for entry in entries if entry.get("level") == "strong")
    return {
        "name": tool.name,
        "score": score,
        "band": "excellent" if score >= 85 else "strong" if score >= 70 else "watch" if score >= 45 else "drag",
        "evidence_mode": evidence_mode or "repo-evidence",
        "code_comparable": tool.code_comparable,
        "comparator_note": tool.comparator_note,
        "strong_controls": strong,
        "managed_controls": sum(1 for entry in entries if entry.get("level") == "managed"),
        "drag_controls": sum(1 for entry in entries if entry.get("level") in {"partial", "opaque", "not_detected"}),
        "notable_drags": [entry["dimension"] for entry in entries if entry.get("level") in {"partial", "opaque"}][:4],
    }


def _dpone_sources(root: Path, slug: str) -> tuple[str, ...]:
    candidates = {
        "deployment_footprint": ("src/dpone/ops/deploy_profiles.py", "docs/ci-cd.md", "pyproject.toml"),
        "infra_prerequisites": ("pyproject.toml", "docs/ci-cd.md"),
        "configuration_surface": ("docs/ops-cli.md", "docs/connectors.md", "docs/connector-sdk.md"),
        "secrets_operations": ("docs/supply-chain.md", "SECURITY.md", ".github/workflows/secret-scan.yml"),
        "self_service_docs": ("docs/ops-cli.md", "docs/ci-cd.md", "docs/route-certification-pack.md"),
        "ci_cd_automation": (".github/workflows/ci.yml", ".github/workflows/release.yml"),
        "observability_ops": ("docs/observability.md", "docs/release-evidence.md"),
        "upgrade_rollback": ("docs/ci-cd.md", ".github/workflows/release.yml"),
        "operator_toil": ("docs/ops-cli.md", "src/dpone/ops/__init__.py", "src/dpone/services/ops/__init__.py"),
        "lock_in_transparency": ("pyproject.toml", "LICENSE"),
    }.get(slug, ())
    return tuple(path for path in candidates if (root / path).exists())


def _weight(slug: str) -> int:
    for dimension in DIMENSIONS:
        if dimension.slug == slug:
            return dimension.weight
    return 1
