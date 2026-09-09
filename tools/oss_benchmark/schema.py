"""Benchmark evidence schema normalization."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

BENCHMARK_SCHEMA_VERSION = 2


def normalize_benchmark_payload_v2(payload: dict[str, Any]) -> dict[str, Any]:
    """Return a v2-compatible payload while retaining v1 project `spec` data."""

    normalized = deepcopy(payload)
    normalized["schema_version"] = BENCHMARK_SCHEMA_VERSION
    release_version = _release_version(normalized)
    normalized["projects"] = [
        with_project_identity(project, release_version=release_version) for project in normalized.get("projects", [])
    ]
    return normalized


def with_project_identity(project: dict[str, Any], *, release_version: str | None = None) -> dict[str, Any]:
    """Add stable project identity fields for audit and downstream BI use."""

    updated = deepcopy(project)
    spec = updated.get("spec") if isinstance(updated.get("spec"), dict) else {}
    slug = str(updated.get("project_id") or spec.get("slug") or spec.get("name") or "")
    name = str(updated.get("display_name") or spec.get("name") or slug)
    repo_url = str(updated.get("repo_url") or spec.get("repo_url") or "")
    revision = str(updated.get("revision") or spec.get("commit") or spec.get("branch") or "")
    updated["project_id"] = slug
    updated["display_name"] = name
    updated["repo_url"] = repo_url
    updated["revision"] = revision
    updated["release_version"] = release_version if slug == "dpone" else updated.get("release_version")
    return updated


def project_id(project: dict[str, Any]) -> str:
    """Return stable project id from v2 identity or v1 spec."""

    if project.get("project_id"):
        return str(project["project_id"])
    spec = project.get("spec") if isinstance(project.get("spec"), dict) else {}
    return str(spec.get("slug") or spec.get("name") or "")


def _release_version(payload: dict[str, Any]) -> str | None:
    context = payload.get("release_context")
    if isinstance(context, dict) and context.get("dpone_version"):
        return str(context["dpone_version"])
    return None
