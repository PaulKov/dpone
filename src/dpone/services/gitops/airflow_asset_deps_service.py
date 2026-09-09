"""Service for `dpone gitops airflow deps`."""

from __future__ import annotations

from pathlib import Path

from dpone.gitops.airflow_asset_deps_build import AirflowAssetDepsReport, build_airflow_asset_deps_report


class AirflowAssetDepsService:
    def __init__(self, *, repo_root: str | Path) -> None:
        self._repo_root = Path(repo_root).resolve(strict=False)

    def build(self, *, workload_set: str, env: str = "dev") -> AirflowAssetDepsReport:
        return build_airflow_asset_deps_report(
            repo_root=self._repo_root,
            workload_set=workload_set,
            env=env,
        )


__all__ = ["AirflowAssetDepsReport", "AirflowAssetDepsService"]
