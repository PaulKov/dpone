from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

import yaml

from dpone.gitops.changed_files import resolve_changed_files
from dpone.gitops.workload_catalog import WorkloadCatalogResolver
from dpone.gitops.workload_impact import AffectedWorkloadResolver
from dpone.ports.filesystem import FileSystem


class _GitOpsSettings(Protocol):
    repo_root: Path


class GitOpsGitLabChildPipelineContext(Protocol):
    settings: _GitOpsSettings
    fs: FileSystem


class GitOpsGitLabChildPipelineService:
    """Render a dynamic GitLab child pipeline for affected dpone workloads."""

    def __init__(self, *, ctx: GitOpsGitLabChildPipelineContext) -> None:
        self._ctx = ctx

    def render(self, args: object) -> str:
        env = str(getattr(args, "env", "dev"))
        workload_set = getattr(args, "workload_set")
        catalog = WorkloadCatalogResolver(repo_root=self._ctx.settings.repo_root).resolve(workload_set, env=env)
        selected_files, _warnings, blockers = resolve_changed_files(
            fs=self._ctx.fs,
            repo_root=self._ctx.settings.repo_root,
            args=args,
        )
        if blockers:
            raise ValueError(blockers[0].message)
        impact = AffectedWorkloadResolver(repo_root=self._ctx.settings.repo_root).resolve(
            catalog, changed_files=selected_files
        )
        pipeline: dict[str, Any] = {
            "stages": ["validate"],
        }
        for workload in impact.affected_workloads:
            job_name = f"validate:dpone-gitops-{workload.workload_id.replace('_', '-')}"
            pipeline[job_name] = {
                "stage": "validate",
                "script": [
                    (
                        "dpone gitops airflow reconcile "
                        f"--workload-set {catalog.workload_set} --env {env} "
                        f"--changed-files {workload.manifest} --output-dir .dpone/gitops"
                    )
                ],
                "artifacts": {"when": "always", "expire_in": "7 days", "paths": [".dpone/gitops/"]},
            }
        rendered = yaml.safe_dump(pipeline, sort_keys=False, allow_unicode=True)
        output = getattr(args, "output", None)
        if output:
            self._ctx.fs.write_text(self._ctx.settings.repo_root / str(output), rendered, encoding="utf-8")
        return rendered


__all__ = ["GitOpsGitLabChildPipelineContext", "GitOpsGitLabChildPipelineService"]
