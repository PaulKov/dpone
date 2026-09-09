"""Stage deployment-owned connection registries for dbt pack builds."""

from __future__ import annotations

from pathlib import Path

from dpone.gitops.airflow_mssql_registry_snapshot import (
    ResolvedMssqlAssetRegistry,
)
from dpone.gitops.airflow_mssql_registry_snapshot import (
    resolve_mssql_asset_registry as _resolve,
)
from dpone.gitops.airflow_mssql_registry_staging import stage_connection_registries as _stage


def stage_project_connection_registries(project_root: Path, build_root: Path) -> tuple[Path, ...]:
    """Copy env-bound connection registries from the dbt project into pack build root."""

    return _stage(project_root, build_root)


def resolve_project_mssql_asset_registry(build_root: Path, *, env: str) -> ResolvedMssqlAssetRegistry:
    """Resolve one immutable MSSQL registry snapshot for a staged dbt build root."""

    return _resolve(build_root, env=env)


__all__ = ["resolve_project_mssql_asset_registry", "stage_project_connection_registries"]
