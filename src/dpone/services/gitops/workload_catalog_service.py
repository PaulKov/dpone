from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from dpone.gitops.workload_catalog import WorkloadCatalogResolver
from dpone.gitops.workload_catalog_models import (
    GitOpsWorkloadCatalogIssue,
    GitOpsWorkloadCatalogReport,
    GitOpsWorkloadDefinition,
    issue,
)
from dpone.ports.filesystem import FileSystem
from dpone.services.gitops.views import GitOpsView, build_gitops_meta


class _GitOpsSettings(Protocol):
    repo_root: Path


class GitOpsWorkloadCatalogContext(Protocol):
    settings: _GitOpsSettings
    fs: FileSystem


@dataclass(frozen=True, slots=True)
class GitOpsWorkloadExplainReport:
    workload_set: str
    env: str
    workload: GitOpsWorkloadDefinition | None
    warnings: tuple[GitOpsWorkloadCatalogIssue, ...] = ()
    blockers: tuple[GitOpsWorkloadCatalogIssue, ...] = ()
    kind: str = "gitops.workload_explain"
    schema_version: str = "1"
    producer: str = "dpone gitops workloads explain"

    @property
    def passed(self) -> bool:
        return not self.blockers

    def to_jsonable(self) -> dict[str, object]:
        return {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "producer": self.producer,
            "workload_set": self.workload_set,
            "env": self.env,
            "workload": self.workload.to_jsonable() if self.workload is not None else None,
            "warnings": [item.to_jsonable() for item in self.warnings],
            "blockers": [item.to_jsonable() for item in self.blockers],
        }


class GitOpsWorkloadCatalogService:
    """CLI-facing service for workload catalog list/explain commands."""

    def __init__(self, *, ctx: GitOpsWorkloadCatalogContext) -> None:
        self._ctx = ctx

    def list_view(self, args: object) -> GitOpsView:
        report = self._resolve(args)
        return GitOpsView(meta=build_gitops_meta(report.kind, path=report.workload_set), report=report)

    def explain_view(self, args: object) -> GitOpsView:
        catalog = self._resolve(args)
        workload_id = str(getattr(args, "workload_id", ""))
        try:
            workload = catalog.by_id(workload_id)
            blockers: tuple[GitOpsWorkloadCatalogIssue, ...] = catalog.blockers
        except KeyError:
            workload = None
            blockers = (
                *catalog.blockers,
                issue(code="workload_not_found", message="GitOps workload id was not found", path=workload_id),
            )
        report = GitOpsWorkloadExplainReport(
            workload_set=catalog.workload_set,
            env=catalog.env,
            workload=workload,
            warnings=catalog.warnings,
            blockers=blockers,
        )
        return GitOpsView(meta=build_gitops_meta(report.kind, path=workload_id), report=report)

    def _resolve(self, args: object) -> GitOpsWorkloadCatalogReport:
        return WorkloadCatalogResolver(repo_root=self._ctx.settings.repo_root).resolve(
            getattr(args, "workload_set"), env=str(getattr(args, "env", "dev"))
        )


__all__ = ["GitOpsWorkloadCatalogContext", "GitOpsWorkloadCatalogService"]
