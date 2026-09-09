"""Narrow dbt-facing adapter over the canonical GitOps Airflow pack builder."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from dpone.gitops.airflow_asset_uri import ResolvedMssqlAssetRegistry
from dpone.gitops.airflow_compact_pack import AirflowCompactPackBuilder
from dpone.gitops.airflow_compact_pack_outlets import OutletBinding
from dpone.gitops.workload_catalog_models import GitOpsWorkloadDefinition


class AirflowPackReportPort(Protocol):
    """Minimal generated-pack view consumed by dbt release compilation."""

    @property
    def passed(self) -> bool: ...

    def to_jsonable(self) -> dict[str, Any]: ...


class AirflowPackBuilderPort(Protocol):
    """Build one deterministic transfer pack from an opaque workload."""

    def build(
        self,
        *,
        workload: object,
        output_path: str,
        repo_root: str | Path | None = None,
        mode: str = "plan",
        runner_policy: str | None = None,
        include_live_gates: bool = False,
        env: str = "dev",
        mssql_registry: ResolvedMssqlAssetRegistry | None = None,
        outlet_binding: OutletBinding = "logical",
    ) -> AirflowPackReportPort: ...


class DbtAirflowPackBuilder:
    """Adapt dbt's opaque workload boundary to the canonical GitOps builder.

    dbt release packs use ``outlet_binding="logical"`` so MSSQL Asset identity
    stays environment-neutral (``asset_ref``) inside immutable release bytes.
    """

    def __init__(self, builder: AirflowCompactPackBuilder | None = None) -> None:
        self._builder = builder or AirflowCompactPackBuilder()

    def build(
        self,
        *,
        workload: object,
        output_path: str,
        repo_root: str | Path | None = None,
        mode: str = "plan",
        runner_policy: str | None = None,
        include_live_gates: bool = False,
        env: str = "dev",
        mssql_registry: ResolvedMssqlAssetRegistry | None = None,
        outlet_binding: OutletBinding = "logical",
    ) -> AirflowPackReportPort:
        if not isinstance(workload, GitOpsWorkloadDefinition):
            raise TypeError("workload must be a GitOpsWorkloadDefinition")
        return self._builder.build(
            workload=workload,
            output_path=output_path,
            repo_root=repo_root,
            mode=mode,
            runner_policy=runner_policy,
            include_live_gates=include_live_gates,
            env=env,
            mssql_registry=mssql_registry,
            outlet_binding=outlet_binding,
        )


__all__ = [
    "AirflowPackBuilderPort",
    "AirflowPackReportPort",
    "DbtAirflowPackBuilder",
]
