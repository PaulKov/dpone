"""Bounded exact-depth scan for one domain-first project layout."""

from __future__ import annotations

from pathlib import Path

from dpone.contracts.project_discovery import (
    MAX_PROJECT_DISCOVERY_BYTES,
    MAX_PROJECT_DISCOVERY_ENTRIES,
    MAX_PROJECT_DOMAINS,
)
from dpone.manifest.domain_dag_models import DiscoveredDomainDag
from dpone.manifest.project_config import ProjectConfigSnapshot, ProjectLayout
from dpone.manifest.project_discovery_domain_dags import (
    discover_domain_dags_for_domain,
    finalize_domain_dags,
    revalidate_discovery_pins,
    unexpected_domain_entries,
)
from dpone.manifest.project_discovery_files import confined_size, relative_label
from dpone.manifest.project_discovery_files import (
    digest_matches as _digest_matches,
)
from dpone.manifest.project_discovery_issues import (
    discovery_issue as _issue,
)
from dpone.manifest.project_discovery_issues import (
    parse_domain as _parse_domain,
)
from dpone.manifest.project_discovery_issues import (
    state_changed_issue as _state_changed_issue,
)
from dpone.manifest.project_discovery_models import (
    DiscoveredWorkload,
    ProjectDiscoveryIssue,
    ProjectDiscoverySnapshot,
    build_discovery_snapshot,
    empty_discovery_snapshot,
)
from dpone.manifest.project_discovery_namespace import (
    NamespaceObservation,
    namespace_unchanged,
    observe_namespace,
    path_contains_symlink,
    path_kind,
)
from dpone.manifest.project_discovery_pipelines import discover_domain_pipelines
from dpone.manifest.project_discovery_reader import ProjectDiscoveryReader
from dpone.manifest.project_layout_authority import conflicting_authoring_layout


class ProjectDiscoveryScanner:
    """Produce one revalidated filesystem snapshot for a resolved layout."""

    def __init__(
        self,
        root: Path,
        reader: ProjectDiscoveryReader,
    ) -> None:
        self._root = root
        self._reader = reader

    def scan(
        self,
        resolved: ProjectLayout,
        *,
        config_snapshot: ProjectConfigSnapshot | None,
    ) -> ProjectDiscoverySnapshot:
        workloads: list[DiscoveredWorkload] = []
        domain_dags: list[DiscoveredDomainDag] = []
        issues: list[ProjectDiscoveryIssue] = []
        warnings: list[ProjectDiscoveryIssue] = []
        consumed: dict[str, str] = {}
        configured_root = self._root.joinpath(*Path(resolved.root).parts)
        conflicting_layout = conflicting_authoring_layout(
            self._root,
            expected_mode=resolved.mode,
            domain_first_root=resolved.root,
        )
        configured_root_kind = path_kind(configured_root)
        if conflicting_layout is not None and not (
            resolved.is_domain_first and conflicting_layout == "unsafe" and configured_root_kind in {"symlink", "other"}
        ):
            issues.append(
                _issue(
                    "DPONE_PROJECT_LAYOUT_MIGRATION_REQUIRED",
                    "Existing authoring authority does not match the configured project layout.",
                    resolved.root,
                )
            )
            return build_discovery_snapshot(resolved, workloads, issues, consumed)
        if not resolved.is_domain_first:
            return empty_discovery_snapshot(resolved)
        namespaces: list[NamespaceObservation] = []
        total_bytes = 0
        pipeline_entries = 0
        scanned_entries = 0

        def observe_bounded(path: Path, *, limit: int) -> NamespaceObservation | None:
            nonlocal scanned_entries
            observed = observe_namespace(
                self._root,
                path,
                limit=limit,
                scan_limit=max(0, MAX_PROJECT_DISCOVERY_ENTRIES - scanned_entries),
            )
            if observed is not None:
                scanned_entries += observed.scanned_entries
            return observed

        def scan_budget_exceeded(observed: NamespaceObservation) -> bool:
            if not observed.scan_exceeded:
                return False
            issues.append(
                _issue(
                    "DPONE_DISCOVERY_LIMIT_EXCEEDED",
                    "Project discovery entry budget was exceeded.",
                    observed.label,
                )
            )
            return True

        def finish_scan() -> ProjectDiscoverySnapshot:
            """Finalize dual-read/pipeline checks even on budget early returns."""

            finalized = finalize_domain_dags(
                root=self._root,
                dual_read_legacy_catalogs=resolved.dual_read_legacy_catalogs,
                workloads=workloads,
                domain_dags=domain_dags,
                issues=issues,
                warnings=warnings,
                consumed=consumed,
            )
            issues.extend(
                revalidate_discovery_pins(
                    root=self._root,
                    consumed=consumed,
                    namespaces=namespaces,
                    digest_matches=_digest_matches,
                    namespace_unchanged=namespace_unchanged,
                )
            )
            return build_discovery_snapshot(
                resolved, workloads, issues, consumed, domain_dags=finalized, warnings=warnings
            )

        if config_snapshot is not None:
            consumed["dpone.yaml"] = config_snapshot.sha256
            config_bytes = confined_size(self._root, "dpone.yaml")
            if config_bytes is None:
                issues.append(_state_changed_issue("dpone.yaml"))
                return finish_scan()
            total_bytes += config_bytes
        if path_contains_symlink(self._root, resolved.root):
            issues.append(
                _issue("DPONE_LAYOUT_ROOT_INVALID", "Domain-first root must be a real directory.", resolved.root)
            )
            return finish_scan()
        root_namespace = observe_bounded(configured_root, limit=MAX_PROJECT_DOMAINS)
        if root_namespace is None:
            issues.append(
                _issue(
                    "DPONE_DISCOVERY_PATH_INVALID",
                    "Domain-first root could not be listed safely.",
                    resolved.root,
                )
            )
            return finish_scan()
        namespaces.append(root_namespace)
        if scan_budget_exceeded(root_namespace):
            return finish_scan()
        if root_namespace.kind == "missing":
            if any(not _digest_matches(self._root, path, digest) for path, digest in consumed.items()):
                issues.append(_state_changed_issue("dpone.yaml"))
            if not namespace_unchanged(self._root, root_namespace):
                issues.append(_state_changed_issue(resolved.root))
            return finish_scan()
        if root_namespace.kind != "directory":
            issues.append(
                _issue("DPONE_LAYOUT_ROOT_INVALID", "Domain-first root must be a real directory.", resolved.root)
            )
            return finish_scan()
        domain_children = root_namespace.children
        domain_kinds = {child: path_kind(child) for child in domain_children}
        unsafe_domains = tuple(child for child in domain_children if domain_kinds[child] == "symlink")
        for child in unsafe_domains:
            issues.append(
                _issue(
                    "DPONE_DISCOVERY_PATH_INVALID",
                    "Domain discovery does not follow symbolic links.",
                    f"{resolved.root}/{child.name}",
                    domain=child.name,
                )
            )
        if len(domain_children) > MAX_PROJECT_DOMAINS:
            issues.append(
                _issue("DPONE_DISCOVERY_LIMIT_EXCEEDED", "Domain discovery budget was exceeded.", resolved.root)
            )
            return finish_scan()
        invalid_domain_entries = tuple(
            child for child in domain_children if domain_kinds[child] not in {"directory", "symlink"}
        )
        for child in invalid_domain_entries:
            issues.append(
                _issue(
                    "DPONE_DISCOVERY_PATH_INVALID",
                    "Every visible domain-root entry must be a domain directory.",
                    f"{resolved.root}/{child.name}",
                    domain=child.name,
                )
            )
        domain_dirs = tuple(child for child in domain_children if domain_kinds[child] == "directory")
        seen_ids: dict[str, str] = {}
        for domain_dir in domain_dirs:
            domain, domain_issue = _parse_domain(domain_dir.name, resolved.root)
            if domain_issue is not None:
                issues.append(domain_issue)
                continue
            domain_namespace = observe_bounded(domain_dir, limit=4)
            if domain_namespace is None or domain_namespace.kind != "directory":
                issues.append(
                    _issue(
                        "DPONE_DISCOVERY_PATH_INVALID",
                        "Domain directory could not be listed safely.",
                        f"{resolved.root}/{domain}",
                        domain=domain,
                    )
                )
                continue
            namespaces.append(domain_namespace)
            if scan_budget_exceeded(domain_namespace):
                return finish_scan()
            for child in unexpected_domain_entries(domain_namespace.children):
                issues.append(
                    _issue(
                        "DPONE_DISCOVERY_PATH_INVALID",
                        "Domain directory contains an unsupported visible entry.",
                        relative_label(self._root, child),
                        domain=domain,
                    )
                )
            ownership, ownership_issue = self._reader.load_ownership(
                domain,
                layout_root=resolved.root,
            )
            if ownership_issue is not None:
                issues.append(ownership_issue)
                continue
            assert ownership is not None
            ownership_bytes = confined_size(self._root, ownership.path)
            if ownership_bytes is None:
                issues.append(
                    _issue(
                        "DPONE_SELECTION_STATE_CHANGED",
                        "Domain ownership changed while discovery was running.",
                        ownership.path,
                        domain=domain,
                    )
                )
                continue
            consumed[ownership.path] = ownership.source_sha256
            total_bytes += ownership_bytes
            if total_bytes > MAX_PROJECT_DISCOVERY_BYTES:
                issues.append(_issue("DPONE_DISCOVERY_LIMIT_EXCEEDED", "Project discovery byte budget was exceeded."))
                return finish_scan()
            dags_namespace, dag_bytes = discover_domain_dags_for_domain(
                root=self._root,
                domain=domain,
                domain_dir=domain_dir,
                layout_root=resolved.root,
                observe_bounded=observe_bounded,
                scan_budget_exceeded=scan_budget_exceeded,
                consumed=consumed,
                domain_dags=domain_dags,
                issues=issues,
            )
            total_bytes += dag_bytes
            if dags_namespace is not None:
                namespaces.append(dags_namespace)
            if total_bytes > MAX_PROJECT_DISCOVERY_BYTES:
                issues.append(_issue("DPONE_DISCOVERY_LIMIT_EXCEEDED", "Project discovery byte budget was exceeded."))
                return finish_scan()
            pipeline_namespace, pipeline_entries, total_bytes, stop_scan = discover_domain_pipelines(
                root=self._root,
                reader=self._reader,
                domain=domain,
                domain_dir=domain_dir,
                layout_root=resolved.root,
                owner_team=ownership.owner_team,
                ownership_fingerprint=ownership.fingerprint,
                observe_bounded=observe_bounded,
                scan_budget_exceeded=scan_budget_exceeded,
                pipeline_entries=pipeline_entries,
                seen_ids=seen_ids,
                workloads=workloads,
                issues=issues,
                consumed=consumed,
                total_bytes=total_bytes,
            )
            if pipeline_namespace is not None:
                namespaces.append(pipeline_namespace)
            if stop_scan:
                return finish_scan()
        return finish_scan()


__all__ = ["ProjectDiscoveryScanner"]
