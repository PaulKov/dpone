"""CLI-facing service for compact pack → release-set promotion."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from dpone.contracts.dbt_compact_release import COMPACT_OUTPUT_INVALID
from dpone.gitops.paths import compact_report_output_is_safe
from dpone.readiness.airflow_compact_pack_release import (
    CompactPackReleaseReport,
    materialize_compact_pack_release,
)
from dpone.services.gitops.views import GitOpsView, build_gitops_meta


class _GitOpsSettings(Protocol):
    repo_root: Path


class GitOpsAirflowCompactPackReleaseContext(Protocol):
    settings: _GitOpsSettings


class GitOpsAirflowCompactPackReleaseService:
    """Promote reconcile packs into an immutable release-set under cache-root."""

    def __init__(self, *, ctx: GitOpsAirflowCompactPackReleaseContext) -> None:
        self._ctx = ctx

    def materialize_view(self, args: object) -> GitOpsView:
        repo_root = self._ctx.settings.repo_root
        pack_root = _resolve_path(repo_root, str(getattr(args, "pack_root")))
        cache_root = _resolve_path(repo_root, str(getattr(args, "cache_root")))
        dag_ids = tuple(str(item) for item in (getattr(args, "dag_id", None) or ()))
        if not compact_report_output_is_safe(
            repo_root, getattr(args, "output", None), pack_root=pack_root, cache_root=cache_root
        ):
            report = CompactPackReleaseReport(
                release_id="",
                release_dir="",
                dag_ids=(),
                workload_ids=(),
                pack_fingerprints={},
                connection_projection_mode="",
                xcom_sidecar_image="",
                blockers=(
                    f"{COMPACT_OUTPUT_INVALID}: output must be a confined report file outside input and cache trees",
                ),
            )
        else:
            report = materialize_compact_pack_release(
                pack_root=pack_root,
                cache_root=cache_root,
                xcom_sidecar_image=str(getattr(args, "xcom_sidecar_image") or ""),
                dag_ids=dag_ids or None,
                provenance={
                    "repo_root": repo_root.as_posix(),
                },
            )
        return GitOpsView(
            meta=build_gitops_meta(
                report.kind,
                path=report.release_dir or None,
                options={
                    "pack_root": pack_root.as_posix(),
                    "cache_root": cache_root.as_posix(),
                    "dag_ids": list(dag_ids) or None,
                },
            ),
            report=report,
        )


def _resolve_path(repo_root: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else (repo_root / path)


__all__ = [
    "CompactPackReleaseReport",
    "GitOpsAirflowCompactPackReleaseService",
]
