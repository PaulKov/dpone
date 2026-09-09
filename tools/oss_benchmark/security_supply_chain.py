"""Security and supply-chain posture scoring for benchmarked tools."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SecurityTool:
    slug: str
    name: str
    code_comparable: bool
    comparator_note: str


@dataclass(frozen=True)
class SecurityDimension:
    slug: str
    label: str
    weight: int
    value: str


@dataclass(frozen=True)
class SecuritySignal:
    status: str
    evidence: str
    sources: tuple[str, ...]


STATUS_POINTS = {"present": 3.0, "partial": 1.5, "not_detected": 0.0, "unavailable": 0.0}
DIMENSIONS = (
    SecurityDimension("license_declared", "License declared", 2, "clear legal reuse boundary"),
    SecurityDimension("secret_scanning", "Secret scanning", 3, "credential leakage prevention"),
    SecurityDimension("sast_codeql", "CodeQL / SAST", 3, "static vulnerability discovery"),
    SecurityDimension("ossf_scorecard", "OSSF Scorecard", 2, "ecosystem supply-chain signal"),
    SecurityDimension("dependency_updates", "Dependency update automation", 2, "known-CVE response loop"),
    SecurityDimension("lockfile_reproducibility", "Lockfile reproducibility", 2, "repeatable dependency graph"),
    SecurityDimension("sbom_inventory", "SBOM inventory", 2, "auditable component inventory"),
    SecurityDimension("security_policy", "Security policy", 2, "vulnerability reporting UX"),
    SecurityDimension(
        "least_privilege_permissions", "Least-privilege CI permissions", 2, "workflow blast-radius control"
    ),
    SecurityDimension("release_provenance", "Release provenance", 2, "artifact origin and attestation"),
)
CLOSED_CORE_TOOLS = (
    SecurityTool("fivetran", "Fivetran", False, "closed-core posture note; source controls cannot be scanned"),
    SecurityTool("informatica", "Informatica", False, "closed-core posture note; source controls cannot be scanned"),
)
CLOSED_CORE_SOURCES = {
    "fivetran": (
        "https://fivetran.com/docs/security-and-privacy/security",
        "https://fivetran.com/docs/core-concepts/syncoverview/data-credential-encryption",
    ),
    "informatica": (
        "https://www.informatica.com/trust-center.html",
        "https://trust.informatica.com/security.html",
    ),
}


def build_security_supply_chain_matrix(projects: Iterable[dict[str, Any]]) -> dict[str, Any]:
    project_list = [project for project in projects if project.get("spec")]
    tools = [_project_tool(project) for project in project_list]
    entries: list[dict[str, Any]] = []
    summary: dict[str, dict[str, Any]] = {}
    for tool, project in zip(tools, project_list, strict=False):
        project_entries = [_entry(tool, dimension, _detect(project, dimension)) for dimension in DIMENSIONS]
        entries.extend(project_entries)
        summary[tool.slug] = _summarize(tool, project_entries)
    for tool in CLOSED_CORE_TOOLS:
        closed_entries = [_entry(tool, dimension, _closed_core_signal(tool.slug)) for dimension in DIMENSIONS]
        entries.extend(closed_entries)
        summary[tool.slug] = _closed_core_summary(tool)
        tools.append(tool)
    return {
        "schema_version": 1,
        "methodology": (
            "Repository-local security and supply-chain controls are detected from source files, GitHub workflows, "
            "lockfiles, policies and docs. Closed-core vendors are listed as posture notes, not code-scored."
        ),
        "status_points": STATUS_POINTS,
        "tools": [tool.__dict__ for tool in tools],
        "dimensions": [dimension.__dict__ for dimension in DIMENSIONS],
        "entries": entries,
        "summary": summary,
    }


def _project_tool(project: dict[str, Any]) -> SecurityTool:
    spec = project.get("spec") or {}
    return SecurityTool(
        str(spec.get("slug", "unknown")),
        str(spec.get("name", spec.get("slug", "unknown"))),
        not bool(project.get("unavailable")),
        str(spec.get("kind", "code-comparable repository")),
    )


def _entry(tool: SecurityTool, dimension: SecurityDimension, signal: SecuritySignal) -> dict[str, Any]:
    return {
        "tool": tool.slug,
        "dimension": dimension.slug,
        "status": signal.status,
        "points": STATUS_POINTS.get(signal.status),
        "evidence": signal.evidence,
        "sources": list(signal.sources),
    }


def _detect(project: dict[str, Any], dimension: SecurityDimension) -> SecuritySignal:
    spec = project.get("spec") or {}
    root = Path(str(spec.get("path", "")))
    if project.get("unavailable") or not root.exists():
        return SecuritySignal("unavailable", "Repository evidence is unavailable for this refresh.", ())
    detectors = {
        "license_declared": _detect_license,
        "secret_scanning": _detect_secret_scanning,
        "sast_codeql": _detect_sast,
        "ossf_scorecard": _detect_scorecard,
        "dependency_updates": _detect_dependency_updates,
        "lockfile_reproducibility": _detect_lockfile,
        "sbom_inventory": _detect_sbom,
        "security_policy": _detect_security_policy,
        "least_privilege_permissions": _detect_ci_permissions,
        "release_provenance": _detect_release_provenance,
    }
    return detectors[dimension.slug](root)


def _detect_license(root: Path) -> SecuritySignal:
    files = _existing(root, ("LICENSE", "LICENSE.md", "COPYING", "NOTICE", "pyproject.toml", "package.json", "pom.xml"))
    return _signal(bool(files), "License metadata found.", "License metadata was not detected.", root, files)


def _detect_secret_scanning(root: Path) -> SecuritySignal:
    return _workflow_signal(
        root, ("gitleaks", "trufflehog", "detect-secrets", "secret-scan"), "Secret scanning workflow detected."
    )


def _detect_sast(root: Path) -> SecuritySignal:
    strong = _workflow_signal(
        root,
        ("github/codeql-action/init", "github/codeql-action/analyze"),
        "CodeQL workflow detected.",
    )
    if strong.status == "present":
        return strong
    return _workflow_signal(root, ("bandit", "semgrep", "snyk code"), "SAST-like workflow detected.", partial=True)


def _detect_scorecard(root: Path) -> SecuritySignal:
    return _workflow_signal(root, ("ossf/scorecard-action", "scorecard-action"), "OSSF Scorecard workflow detected.")


def _detect_dependency_updates(root: Path) -> SecuritySignal:
    files = _existing(
        root,
        (
            ".github/dependabot.yml",
            ".github/dependabot.yaml",
            "renovate.json",
            ".renovaterc",
            ".github/renovate.json",
        ),
    )
    return _signal(
        bool(files),
        "Dependency update automation detected.",
        "Dependency update automation was not detected.",
        root,
        files,
    )


def _detect_lockfile(root: Path) -> SecuritySignal:
    files = _existing(
        root,
        (
            "uv.lock",
            "poetry.lock",
            "requirements.lock",
            "package-lock.json",
            "pnpm-lock.yaml",
            "yarn.lock",
            "gradle.lockfile",
            "Cargo.lock",
        ),
    )
    if files:
        return _signal(True, "Dependency lockfile detected.", "", root, files)
    build_files = _existing(root, ("pom.xml", "build.gradle", "build.gradle.kts", "requirements.txt"))
    if build_files:
        return _signal(
            True, "Dependency manifest detected without a dedicated lockfile.", "", root, build_files, "partial"
        )
    return _signal(False, "", "Dependency reproducibility evidence was not detected.", root, ())


def _detect_sbom(root: Path) -> SecuritySignal:
    files = _existing(
        root, ("sbom.json", "sbom.spdx.json", "bom.json", "docs/supply-chain.md", "docs/developer-supply-chain.md")
    )
    text_hits = _text_hits(root, files, ("sbom", "cyclonedx", "spdx"))
    return _signal(
        bool(text_hits),
        "SBOM or component-inventory evidence detected.",
        "SBOM evidence was not detected.",
        root,
        text_hits,
    )


def _detect_security_policy(root: Path) -> SecuritySignal:
    files = _existing(root, ("SECURITY.md", ".github/SECURITY.md", "docs/security.md", "docs/security-policy.md"))
    return _signal(
        bool(files), "Security reporting policy detected.", "Security reporting policy was not detected.", root, files
    )


def _detect_ci_permissions(root: Path) -> SecuritySignal:
    return _workflow_signal(
        root, ("permissions:", "read-all", "contents: read"), "Explicit GitHub Actions permissions detected."
    )


def _detect_release_provenance(root: Path) -> SecuritySignal:
    return _workflow_signal(
        root,
        ("provenance", "attestation", "slsa", "sigstore", "id-token: write", "trusted publishing"),
        "Release provenance or OIDC publishing evidence detected.",
    )


def _workflow_signal(root: Path, tokens: tuple[str, ...], evidence: str, *, partial: bool = False) -> SecuritySignal:
    hits = [path for path, text in _workflow_texts(root) if any(token in text for token in tokens)]
    status = "partial" if partial and hits else "present"
    return _signal(bool(hits), evidence, "Workflow evidence was not detected.", root, hits, status)


def _workflow_texts(root: Path) -> tuple[tuple[Path, str], ...]:
    workflow_dir = root / ".github" / "workflows"
    files = sorted(workflow_dir.glob("*.yml")) + sorted(workflow_dir.glob("*.yaml"))
    return tuple((path, _read_lower(path)) for path in files[:40])


def _existing(root: Path, relative_paths: tuple[str, ...]) -> tuple[Path, ...]:
    return tuple(root / name for name in relative_paths if (root / name).exists())


def _text_hits(root: Path, files: tuple[Path, ...], tokens: tuple[str, ...]) -> tuple[Path, ...]:
    return tuple(path for path in files if any(token in _read_lower(path) for token in tokens))


def _signal(
    matched: bool,
    yes: str,
    no: str,
    root: Path,
    sources: Iterable[Path],
    status: str = "present",
) -> SecuritySignal:
    if not matched:
        return SecuritySignal("not_detected", no, ())
    return SecuritySignal(status, yes, tuple(_relative(root, path) for path in sources))


def _relative(root: Path, path: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def _read_lower(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore").lower()
    except OSError:
        return ""


def _closed_core_signal(slug: str) -> SecuritySignal:
    return SecuritySignal(
        "unavailable",
        "Closed-core product posture is documented publicly, but source supply-chain controls cannot be scanned.",
        CLOSED_CORE_SOURCES[slug],
    )


def _summarize(tool: SecurityTool, entries: list[dict[str, Any]]) -> dict[str, Any]:
    weighted = sum(float(entry.get("points") or 0) * _dimension_weight(entry["dimension"]) for entry in entries)
    maximum = sum(3 * _dimension_weight(entry["dimension"]) for entry in entries)
    score = round((weighted / maximum) * 100) if maximum else 0
    return {
        "name": tool.name,
        "score": score,
        "band": "excellent" if score >= 85 else "strong" if score >= 70 else "watch" if score >= 45 else "risk",
        "code_comparable": tool.code_comparable,
        "comparator_note": tool.comparator_note,
        "present_controls": sum(1 for entry in entries if entry.get("status") == "present"),
        "partial_controls": sum(1 for entry in entries if entry.get("status") == "partial"),
        "missing_controls": sum(1 for entry in entries if entry.get("status") == "not_detected"),
        "notable_gaps": [entry["dimension"] for entry in entries if entry.get("status") == "not_detected"][:4],
    }


def _closed_core_summary(tool: SecurityTool) -> dict[str, Any]:
    return {
        "name": tool.name,
        "score": None,
        "band": "not code-scored",
        "code_comparable": False,
        "comparator_note": tool.comparator_note,
        "present_controls": None,
        "partial_controls": None,
        "missing_controls": None,
        "notable_gaps": ["source supply-chain controls are unavailable"],
    }


def _dimension_weight(slug: str) -> int:
    for dimension in DIMENSIONS:
        if dimension.slug == slug:
            return dimension.weight
    return 1
