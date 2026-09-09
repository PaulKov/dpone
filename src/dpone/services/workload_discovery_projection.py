"""Application projection for domain-first workload discovery changes."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.manifest.project_discovery import (
    ProjectDiscoveryIssue,
    ProjectDiscoveryProjectionError,
    ProjectDiscoveryService,
    ProjectDiscoverySnapshot,
)
from dpone.manifest.project_discovery_models import empty_discovery_snapshot
from dpone.manifest.project_root import inspect_project_root, verify_project_root
from dpone.manifest.workload_index_io import load_workload_index
from dpone.services.workload_index_contract import (
    WorkloadIndexChangeImpact,
    compare_workload_indexes,
    validate_workload_change_impact,
    validate_workload_index,
    workload_index_from_snapshot,
)


class WorkloadDiscoveryFailed(ProjectDiscoveryProjectionError):
    """Domain-first discovery failed before a workload index could be emitted."""

    def __init__(self, issues: tuple[ProjectDiscoveryIssue, ...]) -> None:
        super().__init__("Project discovery failed; workload index was not emitted.")
        self.issues = issues


def build_workload_index(root: str | Path) -> dict[str, Any]:
    """Build one validated ephemeral workload index from canonical discovery."""

    snapshot = ProjectDiscoveryService(root).discover()
    _require_domain_first(snapshot)
    if not snapshot.ok:
        raise WorkloadDiscoveryFailed(snapshot.issues)
    payload = workload_index_from_snapshot(snapshot)
    return payload


def build_change_impact_report(
    root: str | Path,
    *,
    baseline_index: Mapping[str, Any] | None = None,
    current_index: Mapping[str, Any] | None = None,
    baseline_content_sha256: str | None = None,
    current_content_sha256: str | None = None,
) -> dict[str, Any]:
    """Compare one prior index with a frozen candidate or current discovery."""

    current_snapshot = None
    if current_index is None:
        current_snapshot = ProjectDiscoveryService(root).discover()
        _require_domain_first(current_snapshot)
    has_baseline = baseline_index is not None
    baseline = baseline_index
    if baseline is None:
        assert current_snapshot is not None
        baseline = workload_index_from_snapshot(empty_discovery_snapshot(current_snapshot.layout))
    validate_workload_index(baseline)
    if current_index is not None:
        validate_workload_index(current_index)
        impact = compare_workload_indexes(baseline, current_index)
        current_fingerprint = current_index["project_fingerprint"]
        current_source = "candidate"
        discovery_status = "not_run"
        issues: list[dict[str, str | None]] = []
    else:
        assert current_snapshot is not None
        impact = (
            compare_workload_indexes(baseline, current_snapshot)
            if current_snapshot.ok
            else WorkloadIndexChangeImpact(added=(), modified=(), removed=())
        )
        current_fingerprint = current_snapshot.project_fingerprint
        current_source = "discovery"
        discovery_status = "passed" if current_snapshot.ok else "failed"
        issues = [
            {
                "code": issue.code,
                "message": issue.message,
                "path": issue.path,
            }
            for issue in current_snapshot.issues
        ]
    baseline_fingerprint = baseline.get("project_fingerprint")
    report = {
        "schema": "dpone.workload-change-impact.v1",
        "baseline_fingerprint": (
            baseline_fingerprint if has_baseline and isinstance(baseline_fingerprint, str) else None
        ),
        "current_fingerprint": current_fingerprint,
        "baseline_content_sha256": baseline_content_sha256,
        "current_content_sha256": current_content_sha256,
        "current_source": current_source,
        "validation_status": "passed",
        "discovery_status": discovery_status,
        "issues": issues,
        **impact.to_dict(),
    }
    validate_workload_change_impact(report)
    return report


def build_change_impact_report_from_files(
    root: str | Path,
    *,
    baseline_path: str | Path,
    current_path: str | Path | None = None,
) -> dict[str, Any]:
    """Load confined index snapshots and compare the exact approved bytes."""

    root_identity = inspect_project_root(root)
    assert root_identity is not None
    baseline = load_workload_index(root_identity, baseline_path, label="Baseline")
    current_loaded = (
        load_workload_index(root_identity, current_path, label="Current candidate")
        if current_path is not None
        else None
    )
    report = build_change_impact_report(
        root_identity.path,
        baseline_index=baseline.payload,
        current_index=current_loaded.payload if current_loaded is not None else None,
        baseline_content_sha256=baseline.content_sha256,
        current_content_sha256=current_loaded.content_sha256 if current_loaded is not None else None,
    )
    verify_project_root(root_identity)
    return report


def _require_domain_first(snapshot: ProjectDiscoverySnapshot) -> None:
    if snapshot.layout.is_domain_first:
        return
    raise WorkloadDiscoveryFailed(
        (
            ProjectDiscoveryIssue(
                code="DPONE_DOMAIN_LAYOUT_REQUIRED",
                message="Workload discovery projections require a domain-first project.",
                path="dpone.yaml",
            ),
        )
    )


__all__ = [
    "WorkloadDiscoveryFailed",
    "build_change_impact_report",
    "build_change_impact_report_from_files",
    "build_workload_index",
]
