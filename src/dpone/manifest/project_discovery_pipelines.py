"""Discover and compile pipeline directories during domain-first project scans."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from dpone.contracts.project_discovery import MAX_PROJECT_DISCOVERY_BYTES, MAX_PROJECT_WORKLOADS
from dpone.manifest.project_discovery_files import (
    checked_inputs as _checked_inputs,
)
from dpone.manifest.project_discovery_files import (
    confined_size,
    relative_label,
)
from dpone.manifest.project_discovery_files import (
    digest_matches as _digest_matches,
)
from dpone.manifest.project_discovery_issues import discovery_issue as _issue
from dpone.manifest.project_discovery_issues import state_changed_issue as _state_changed_issue
from dpone.manifest.project_discovery_models import DiscoveredWorkload, ProjectDiscoveryIssue
from dpone.manifest.project_discovery_namespace import NamespaceObservation, path_kind
from dpone.manifest.project_discovery_reader import ProjectDiscoveryReader


def discover_domain_pipelines(
    *,
    root: Path,
    reader: ProjectDiscoveryReader,
    domain: str,
    domain_dir: Path,
    layout_root: str,
    owner_team: str | None,
    ownership_fingerprint: str,
    observe_bounded: Callable[..., NamespaceObservation | None],
    scan_budget_exceeded: Callable[[NamespaceObservation], bool],
    pipeline_entries: int,
    seen_ids: dict[str, str],
    workloads: list[DiscoveredWorkload],
    issues: list[ProjectDiscoveryIssue],
    consumed: dict[str, str],
    total_bytes: int,
) -> tuple[NamespaceObservation | None, int, int, bool]:
    """Scan one domain ``pipelines/`` tree.

    Returns ``(namespace, pipeline_entries, total_bytes, stop_scan)``.
    """

    pipelines_dir = domain_dir / "pipelines"
    pipeline_namespace = observe_bounded(
        pipelines_dir,
        limit=MAX_PROJECT_WORKLOADS - pipeline_entries,
    )
    if pipeline_namespace is None:
        issues.append(
            _issue(
                "DPONE_DISCOVERY_PATH_INVALID",
                "Domain pipelines path could not be listed safely.",
                f"{layout_root}/{domain}/pipelines",
                domain=domain,
            )
        )
        return None, pipeline_entries, total_bytes, False
    if scan_budget_exceeded(pipeline_namespace):
        return pipeline_namespace, pipeline_entries, total_bytes, True
    if pipeline_namespace.kind == "missing":
        return pipeline_namespace, pipeline_entries, total_bytes, False
    if pipeline_namespace.kind != "directory":
        issues.append(
            _issue(
                "DPONE_DISCOVERY_PATH_INVALID",
                "Domain pipelines path must be a real directory.",
                f"{layout_root}/{domain}/pipelines",
                domain=domain,
            )
        )
        return pipeline_namespace, pipeline_entries, total_bytes, False
    remaining_entries = MAX_PROJECT_WORKLOADS - pipeline_entries
    pipeline_children = pipeline_namespace.children
    if len(pipeline_children) > remaining_entries:
        issues.append(
            _issue(
                "DPONE_DISCOVERY_LIMIT_EXCEEDED",
                "Pipeline discovery budget was exceeded.",
                layout_root,
            )
        )
        return pipeline_namespace, pipeline_entries, total_bytes, True
    pipeline_entries += len(pipeline_children)
    pipeline_kinds = {child: path_kind(child) for child in pipeline_children}
    for child in pipeline_children:
        if pipeline_kinds[child] == "symlink":
            issues.append(
                _issue(
                    "DPONE_DISCOVERY_PATH_INVALID",
                    "Pipeline discovery does not follow symbolic links.",
                    relative_label(root, child),
                    domain=domain,
                )
            )
        elif pipeline_kinds[child] != "directory":
            issues.append(
                _issue(
                    "DPONE_DISCOVERY_PATH_INVALID",
                    "Every visible pipelines entry must be a pipeline directory.",
                    relative_label(root, child),
                    domain=domain,
                )
            )
    for pipeline_dir in (child for child in pipeline_children if pipeline_kinds[child] == "directory"):
        workload, issue = reader.compile_pipeline(
            domain=domain,
            owner=owner_team,
            ownership_fingerprint=ownership_fingerprint,
            pipeline_dir=pipeline_dir,
        )
        if issue is not None:
            issues.append(issue)
            continue
        assert workload is not None
        prior_domain = seen_ids.get(workload.pipeline_id)
        if prior_domain is not None:
            issues.append(
                _issue(
                    "DPONE_PIPELINE_ID_DUPLICATE",
                    "Pipeline id must be unique across the project.",
                    workload.checked_source.source_label,
                    pipeline_id=workload.pipeline_id,
                    domain=workload.domain,
                )
            )
            continue
        seen_ids[workload.pipeline_id] = workload.domain
        workloads.append(workload)
        checked = workload.checked_source
        for path, digest in _checked_inputs(checked):
            input_bytes = confined_size(root, path)
            if input_bytes is None or not _digest_matches(root, path, digest):
                issues.append(
                    _state_changed_issue(
                        path,
                        pipeline_id=workload.pipeline_id,
                        domain=workload.domain,
                    )
                )
                break
            existing = consumed.get(path)
            if existing is not None and existing != digest:
                issues.append(
                    _issue(
                        "DPONE_SELECTION_STATE_CHANGED",
                        "The same discovery input was observed with different content.",
                        path,
                        pipeline_id=workload.pipeline_id,
                        domain=workload.domain,
                    )
                )
                continue
            consumed[path] = digest
            total_bytes += input_bytes
        if total_bytes > MAX_PROJECT_DISCOVERY_BYTES:
            issues.append(_issue("DPONE_DISCOVERY_LIMIT_EXCEEDED", "Project discovery byte budget was exceeded."))
            return pipeline_namespace, pipeline_entries, total_bytes, True
    return pipeline_namespace, pipeline_entries, total_bytes, False


__all__ = ["discover_domain_pipelines"]
