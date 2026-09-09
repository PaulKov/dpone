"""Evidence confidence, provenance and reproducibility helpers."""

from __future__ import annotations

import hashlib
import json
import platform
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from tools.oss_benchmark.config import PROVENANCE_PATH, ROOT
from tools.oss_benchmark.evidence_trust_scoring import confidence_band, confidence_for_project
from tools.oss_benchmark.evidence_trust_sources import build_metric_provenance, mode_counts
from tools.oss_benchmark.payload_utils import as_int, get_value, project_slug


def build_evidence_trust_summary(payload: dict[str, Any]) -> dict[str, Any]:
    """Build the public confidence summary from merged benchmark evidence."""

    projects = list(payload.get("projects") or [])
    project_scores = {project_slug(project): confidence_for_project(project, payload) for project in projects}
    provenance = build_metric_provenance(payload)
    provenance_modes = mode_counts(provenance)
    scores = [as_int(item.get("confidence_score")) for item in project_scores.values()]
    overall = int(round(sum(scores) / len(scores))) if scores else 0
    closed_notes = len(payload.get("closed_core_notes") or [])
    if closed_notes:
        overall = max(0, overall - min(6, closed_notes * 2))
    return {
        "schema_version": 1,
        "overall_confidence_score": min(100, overall),
        "overall_band": confidence_band(overall),
        "provenance_path": str(PROVENANCE_PATH),
        "cross_check": run_external_loc_cross_check(_find_cross_check_project(projects)),
        "projects": project_scores,
        "mode_counts": provenance_modes,
        "metric_provenance": provenance,
    }


def build_provenance_export(
    payload: dict[str, Any],
    *,
    artifact_paths: list[Path] | tuple[Path, ...],
    root: Path = ROOT,
) -> dict[str, Any]:
    """Build the reproducibility ledger after generated artifacts exist."""

    trust = payload.get("evidence_trust") or build_evidence_trust_summary(payload)
    run_context = payload.get("run_context") or {}
    return {
        "schema_version": 1,
        "generated_at": payload.get("generated_at") or run_context.get("generated_at"),
        "run_context": run_context,
        "confidence": trust,
        "metric_provenance": trust.get("metric_provenance") or build_metric_provenance(payload),
        "reproducibility": {
            "python_version": sys.version.split()[0],
            "platform": platform.platform(),
            "git_sha": run_context.get("git_sha"),
            "branch": run_context.get("branch"),
            "analyzer_schema_versions": _analyzer_schema_versions(payload),
            "artifact_checksums": checksum_artifacts(artifact_paths, root=root),
            "third_party_cross_check": trust.get("cross_check") or {},
        },
    }


def write_provenance_export(
    payload: dict[str, Any],
    *,
    artifact_paths: list[Path] | tuple[Path, ...],
    path: Path = ROOT / PROVENANCE_PATH,
) -> dict[str, Any]:
    """Write the provenance ledger and return the payload for tests/CI logs."""

    export = build_provenance_export(payload, artifact_paths=artifact_paths, root=ROOT)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(export, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return export


def checksum_artifacts(paths: list[Path] | tuple[Path, ...], *, root: Path = ROOT) -> list[dict[str, Any]]:
    """Return SHA-256 checksums for existing generated benchmark artifacts."""

    checksums: list[dict[str, Any]] = []
    for raw_path in paths:
        path = raw_path if raw_path.is_absolute() else root / raw_path
        if not path.exists() or path.is_dir():
            continue
        data = path.read_bytes()
        checksums.append(
            {
                "path": _relative_path(path, root),
                "sha256": hashlib.sha256(data).hexdigest(),
                "bytes": len(data),
            }
        )
    return sorted(checksums, key=lambda item: item["path"])


def run_external_loc_cross_check(project: dict[str, Any] | None, *, root: Path = ROOT) -> dict[str, Any]:
    """Run an optional third-party LOC/SLOC cross-check when a tool exists."""

    tool = shutil.which("tokei") or shutil.which("cloc")
    if not tool:
        return {"status": "unavailable", "tool": None, "reason": "no external LOC tool found"}
    if not project:
        return {"status": "unavailable", "tool": Path(tool).name, "reason": "no project selected for cross-check"}
    path = Path(str(get_value(project, "spec", "path", default=root)))
    path = path if path.is_absolute() else root / path
    if not path.exists():
        return {"status": "unavailable", "tool": Path(tool).name, "reason": f"{path} does not exist"}
    if Path(tool).name == "tokei":
        return _run_tokei(tool, path, project)
    return _run_cloc(tool, path, project)


def _find_cross_check_project(projects: list[dict[str, Any]]) -> dict[str, Any] | None:
    for project in projects:
        if project_slug(project) == "dpone":
            return project
    return projects[0] if projects else None


def _analyzer_schema_versions(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "benchmark_payload": 1,
        "evidence_trust": (payload.get("evidence_trust") or {}).get("schema_version", 1),
        "independent_validation": (payload.get("independent_validation") or {}).get("schema_version", 1),
        "public_evidence_integrity": (payload.get("public_evidence_integrity") or {}).get("schema_version", 1),
        "source_verification": (payload.get("source_verification") or {}).get("schema_version", 1),
        "benchmark_release_readiness": (payload.get("benchmark_release_readiness") or {}).get("schema_version", 1),
        "metric_provenance": "v1",
        "artifact_checksums": "sha256-v1",
    }


def _relative_path(path: Path, root: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.as_posix()


def _run_tokei(tool: str, path: Path, project: dict[str, Any]) -> dict[str, Any]:
    try:
        result = subprocess.run(
            [tool, "--output", "json", str(path)],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        data = json.loads(result.stdout)
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError) as exc:
        return {"status": "unavailable", "tool": "tokei", "reason": str(exc)}
    total = data.get("Total") or {}
    return _cross_check_result("tokei", total.get("code"), project)


def _run_cloc(tool: str, path: Path, project: dict[str, Any]) -> dict[str, Any]:
    try:
        result = subprocess.run(
            [tool, "--json", str(path)],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        data = json.loads(result.stdout)
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError) as exc:
        return {"status": "unavailable", "tool": "cloc", "reason": str(exc)}
    return _cross_check_result("cloc", (data.get("SUM") or {}).get("code"), project)


def _cross_check_result(tool: str, external_sloc: Any, project: dict[str, Any]) -> dict[str, Any]:
    benchmark_sloc = as_int(get_value(project, "loc_with_tests", "total_sloc"))
    external = as_int(external_sloc)
    delta = external - benchmark_sloc if external and benchmark_sloc else None
    return {
        "status": "fresh" if external else "unavailable",
        "tool": tool,
        "project": project_slug(project),
        "external_sloc": external or None,
        "benchmark_sloc": benchmark_sloc or None,
        "delta": delta,
    }
