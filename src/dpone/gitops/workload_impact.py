from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path

from dpone.gitops.airflow_runner_contract import parse_runner_embed_paths
from dpone.gitops.path_policy import GitOpsPathValidationError, safe_relative_path
from dpone.gitops.workload_catalog_models import (
    GitOpsAffectedWorkload,
    GitOpsAffectedWorkloadReport,
    GitOpsWorkloadCatalogIssue,
    GitOpsWorkloadCatalogReport,
    GitOpsWorkloadDefinition,
    GitOpsWorkloadImpactReason,
    issue,
)
from dpone.gitops.workload_dependencies import WorkloadDependencyError, WorkloadDependencyResolver


class AffectedWorkloadResolver:
    """Map changed repo paths to workload-level impact using resolved catalog metadata.

    Impact includes the dependency closure that is embedded into compact packs:
    manifest, sql_file, authoring fragments/recipes/profiles/components, and
    ``runner.embed_assets``. SQL-only edits must affect the owning workload.
    """

    def __init__(
        self,
        *,
        repo_root: str | Path | None = None,
        dependency_resolver: WorkloadDependencyResolver | None = None,
    ) -> None:
        self._repo_root = Path(repo_root).resolve() if repo_root is not None else None
        self._dependency_resolver = dependency_resolver or WorkloadDependencyResolver()

    def resolve(
        self,
        catalog: GitOpsWorkloadCatalogReport,
        *,
        changed_files: Sequence[object],
    ) -> GitOpsAffectedWorkloadReport:
        normalized, blockers = _normalize_changed_files(changed_files)
        warnings: list[GitOpsWorkloadCatalogIssue] = []
        if blockers:
            return GitOpsAffectedWorkloadReport(
                changed_files=normalized, affected_workloads=(), blockers=tuple(blockers)
            )
        reasons_by_id: dict[str, list[GitOpsWorkloadImpactReason]] = defaultdict(list)
        manifest_to_ids: dict[str, set[str]] = defaultdict(set)
        dependency_paths = self._dependency_index(catalog, warnings=warnings)
        for workload in catalog.workloads:
            manifest_to_ids[workload.manifest].add(workload.workload_id)
        for changed in normalized:
            if changed == catalog.workload_set:
                for workload in catalog.workloads:
                    reasons_by_id[workload.workload_id].append(
                        _reason(changed, catalog.workload_set, "workload_set_changed")
                    )
                continue
            for workload in catalog.workloads:
                if changed == workload.manifest:
                    reasons_by_id[workload.workload_id].append(_reason(changed, workload.manifest, "manifest_changed"))
                if changed == workload.catalog_path:
                    for related_id in manifest_to_ids[workload.manifest]:
                        reasons_by_id[related_id].append(_reason(changed, workload.catalog_path, "catalog_changed"))
                for dep_path, dep_kind in dependency_paths.get(workload.workload_id, ()):
                    if changed == dep_path:
                        reasons_by_id[workload.workload_id].append(_reason(changed, dep_path, f"{dep_kind}_changed"))
        affected = tuple(
            GitOpsAffectedWorkload(
                workload_id=workload.workload_id,
                manifest=workload.manifest,
                reasons=tuple(_dedupe_reasons(reasons_by_id[workload.workload_id])),
            )
            for workload in catalog.workloads
            if workload.workload_id in reasons_by_id
        )
        return GitOpsAffectedWorkloadReport(
            changed_files=normalized,
            affected_workloads=affected,
            warnings=tuple(warnings),
        )

    def _dependency_index(
        self,
        catalog: GitOpsWorkloadCatalogReport,
        *,
        warnings: list[GitOpsWorkloadCatalogIssue],
    ) -> dict[str, tuple[tuple[str, str], ...]]:
        if self._repo_root is None:
            return {}
        index: dict[str, tuple[tuple[str, str], ...]] = {}
        for workload in catalog.workloads:
            paths: list[tuple[str, str]] = []
            try:
                for dependency in self._dependency_resolver.resolve(
                    repo_root=self._repo_root,
                    manifest=workload.manifest,
                ):
                    if dependency.kind == "manifest":
                        continue
                    paths.append((dependency.path, dependency.kind))
            except WorkloadDependencyError as exc:
                warnings.append(
                    issue(
                        code="workload_dependency_resolve_failed",
                        message=f"Could not resolve embed dependency closure: {exc}",
                        path=workload.manifest,
                    )
                )
            for embed_path in _embed_asset_paths(workload):
                paths.append((embed_path, "embed_asset"))
            # Stable unique order.
            seen: set[tuple[str, str]] = set()
            unique: list[tuple[str, str]] = []
            for item in paths:
                if item in seen:
                    continue
                seen.add(item)
                unique.append(item)
            index[workload.workload_id] = tuple(unique)
        return index


def _embed_asset_paths(workload: GitOpsWorkloadDefinition) -> tuple[str, ...]:
    airflow = workload.effective_config.get("airflow")
    if not isinstance(airflow, Mapping):
        return ()
    try:
        return parse_runner_embed_paths(airflow)
    except Exception:  # noqa: BLE001 - keep impact resolvable; pack build validates embeds
        return ()


def _normalize_changed_files(
    changed_files: Sequence[object],
) -> tuple[tuple[str, ...], list[GitOpsWorkloadCatalogIssue]]:
    normalized: list[str] = []
    blockers: list[GitOpsWorkloadCatalogIssue] = []
    for raw in changed_files:
        try:
            path = safe_relative_path(raw, source="--changed-files").as_posix()
        except GitOpsPathValidationError as exc:
            blockers.append(
                issue(code="invalid_changed_path", message=str(exc), path=str(raw or ""), source="--changed-files")
            )
            continue
        if path not in normalized:
            normalized.append(path)
    return tuple(normalized), blockers


def _reason(changed: str, matched: str, reason: str) -> GitOpsWorkloadImpactReason:
    return GitOpsWorkloadImpactReason(changed_path=changed, matched_path=matched, reason=reason)


def _dedupe_reasons(reasons: list[GitOpsWorkloadImpactReason]) -> tuple[GitOpsWorkloadImpactReason, ...]:
    seen: set[tuple[str, str, str]] = set()
    deduped: list[GitOpsWorkloadImpactReason] = []
    for reason in reasons:
        key = (reason.changed_path, reason.matched_path, reason.reason)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(reason)
    return tuple(deduped)


__all__ = ["AffectedWorkloadResolver"]
