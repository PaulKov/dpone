"""Integrate colocated and legacy domain DAG discovery into one project scan."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING

from dpone.manifest.domain_dag_loader import load_domain_dag_file
from dpone.manifest.legacy_catalog_adapter import (
    adapt_legacy_domain_catalogs,
    merge_discovered_domain_dags,
    validate_domain_dag_pipeline_refs,
)
from dpone.manifest.project_discovery_files import confined_size, relative_label
from dpone.manifest.project_discovery_issues import discovery_issue as _issue
from dpone.manifest.project_discovery_issues import state_changed_issue as _state_changed_issue
from dpone.manifest.project_discovery_models import DiscoveredWorkload, ProjectDiscoveryIssue
from dpone.manifest.project_discovery_namespace import NamespaceObservation, path_kind

if TYPE_CHECKING:
    from dpone.manifest.domain_dag_models import DiscoveredDomainDag

MAX_DOMAIN_DAGS = 500
ALLOWED_DOMAIN_ENTRIES = frozenset({"ownership.yaml", "pipelines", "dags", "airflow"})


def unexpected_domain_entries(children: tuple[Path, ...]) -> tuple[Path, ...]:
    """Return visible domain children outside the frozen domain-first allowlist."""

    return tuple(child for child in children if child.name not in ALLOWED_DOMAIN_ENTRIES)


def scan_domain_dags(
    *,
    root: Path,
    domain: str,
    domain_dir: Path,
    layout_root: str,
    observe_bounded: Callable[[Path], NamespaceObservation | None],
    scan_budget_exceeded: Callable[[NamespaceObservation], bool],
) -> tuple[list[DiscoveredDomainDag], list[ProjectDiscoveryIssue], dict[str, str], NamespaceObservation | None]:
    """Discover colocated domain DAG files under one domain directory."""

    dags: list[DiscoveredDomainDag] = []
    issues: list[ProjectDiscoveryIssue] = []
    consumed: dict[str, str] = {}
    dags_dir = domain_dir / "dags"
    kind = path_kind(dags_dir)
    if kind == "missing":
        return dags, issues, consumed, None
    if kind != "directory":
        issues.append(
            _issue(
                "DPONE_DISCOVERY_PATH_INVALID",
                "Domain dags path must be a real directory.",
                f"{layout_root}/{domain}/dags",
                domain=domain,
            )
        )
        return dags, issues, consumed, None
    namespace = observe_bounded(dags_dir)
    if namespace is None:
        issues.append(
            _issue(
                "DPONE_DISCOVERY_PATH_INVALID",
                "Domain dags path could not be listed safely.",
                f"{layout_root}/{domain}/dags",
                domain=domain,
            )
        )
        return dags, issues, consumed, None
    if scan_budget_exceeded(namespace):
        return dags, issues, consumed, namespace
    if len(namespace.children) > MAX_DOMAIN_DAGS:
        issues.append(
            _issue(
                "DPONE_DISCOVERY_LIMIT_EXCEEDED",
                "Domain DAG discovery budget was exceeded.",
                f"{layout_root}/{domain}/dags",
                domain=domain,
            )
        )
        return dags, issues, consumed, namespace
    for child in namespace.children:
        if child.name.startswith("."):
            continue
        if not child.name.endswith((".yaml", ".yml")):
            issues.append(
                _issue(
                    "DPONE_DISCOVERY_PATH_INVALID",
                    "Domain dags directory contains an unsupported visible entry.",
                    relative_label(root, child),
                    domain=domain,
                )
            )
            continue
        relative = f"{layout_root}/{domain}/dags/{child.name}"
        discovered, load_issues = load_domain_dag_file(root, relative, expected_domain=domain)
        for item in load_issues:
            issues.append(
                _issue(
                    item.code,
                    item.message,
                    item.path or relative,
                    domain=item.domain or domain,
                )
            )
        if discovered is None:
            continue
        dags.append(discovered)
        consumed[discovered.path] = discovered.source_sha256
    return dags, issues, consumed, namespace


def discover_domain_dags_for_domain(
    *,
    root: Path,
    domain: str,
    domain_dir: Path,
    layout_root: str,
    observe_bounded: Callable[..., NamespaceObservation | None],
    scan_budget_exceeded: Callable[[NamespaceObservation], bool],
    consumed: dict[str, str],
    domain_dags: list[DiscoveredDomainDag],
    issues: list[ProjectDiscoveryIssue],
) -> tuple[NamespaceObservation | None, int]:
    """Scan one domain ``dags/`` tree and merge digests into the discovery budget."""

    scanned_dags, dag_issues, dag_consumed, dags_namespace = scan_domain_dags(
        root=root,
        domain=domain,
        domain_dir=domain_dir,
        layout_root=layout_root,
        observe_bounded=lambda path: observe_bounded(path, limit=MAX_DOMAIN_DAGS),
        scan_budget_exceeded=scan_budget_exceeded,
    )
    issues.extend(dag_issues)
    added_bytes = 0
    for path, digest in dag_consumed.items():
        existing = consumed.get(path)
        if existing is not None and existing != digest:
            issues.append(
                _issue(
                    "DPONE_SELECTION_STATE_CHANGED",
                    "The same discovery input was observed with different content.",
                    path,
                    domain=domain,
                )
            )
            continue
        consumed[path] = digest
        input_bytes = confined_size(root, path)
        if input_bytes is None:
            issues.append(_state_changed_issue(path, domain=domain))
            continue
        added_bytes += input_bytes
    for item in scanned_dags:
        prior = next((existing for existing in domain_dags if existing.dag_id == item.dag_id), None)
        if prior is not None:
            issues.append(
                _issue(
                    "DPONE_DOMAIN_DAG_ID_DUPLICATE",
                    "Domain DAG id must be unique across the project.",
                    item.path,
                    domain=domain,
                )
            )
            continue
        domain_dags.append(item)
    return dags_namespace, added_bytes


def finalize_domain_dags(
    *,
    root: Path,
    dual_read_legacy_catalogs: bool,
    workloads: list[DiscoveredWorkload],
    domain_dags: list[DiscoveredDomainDag],
    issues: list[ProjectDiscoveryIssue],
    warnings: list[ProjectDiscoveryIssue],
    consumed: dict[str, str],
) -> list[DiscoveredDomainDag]:
    """Apply optional legacy dual-read and pipeline-reference validation."""

    merged = list(domain_dags)
    if dual_read_legacy_catalogs:
        legacy_dags, legacy_issues, legacy_consumed = adapt_legacy_domain_catalogs(root)
        issues.extend(legacy_issues)
        for path, digest in legacy_consumed.items():
            existing = consumed.get(path)
            if existing is not None and existing != digest:
                issues.append(
                    _issue(
                        "DPONE_SELECTION_STATE_CHANGED",
                        "The same discovery input was observed with different content.",
                        path,
                    )
                )
                continue
            consumed[path] = digest
        merged, merge_blockers, merge_warnings = merge_discovered_domain_dags(merged, legacy_dags)
        issues.extend(merge_blockers)
        warnings.extend(merge_warnings)
    pipeline_domains: Mapping[str, str] = {item.pipeline_id: item.domain for item in workloads}
    issues.extend(validate_domain_dag_pipeline_refs(merged, pipeline_domains=pipeline_domains))
    return merged


def revalidate_discovery_pins(
    *,
    root: Path,
    consumed: Mapping[str, str],
    namespaces: list[NamespaceObservation],
    digest_matches: Callable[[Path, str, str], bool],
    namespace_unchanged: Callable[[Path, NamespaceObservation], bool],
) -> list[ProjectDiscoveryIssue]:
    """Fail closed when pinned discovery inputs or namespaces changed mid-scan."""

    issues: list[ProjectDiscoveryIssue] = []
    for path, digest in consumed.items():
        if not digest_matches(root, path, digest):
            issues.append(_state_changed_issue(path))
            break
    for namespace in namespaces:
        if not namespace_unchanged(root, namespace):
            issues.append(_state_changed_issue(namespace.label))
            break
    return issues


__all__ = [
    "ALLOWED_DOMAIN_ENTRIES",
    "MAX_DOMAIN_DAGS",
    "discover_domain_dags_for_domain",
    "finalize_domain_dags",
    "revalidate_discovery_pins",
    "scan_domain_dags",
    "unexpected_domain_entries",
]
