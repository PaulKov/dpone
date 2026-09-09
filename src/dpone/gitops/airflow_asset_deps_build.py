"""Build plane orchestration for ``dpone gitops airflow deps`` reports."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from dpone.gitops.airflow_asset_cross_dag import cross_dag_recommendations
from dpone.gitops.airflow_asset_dag_index import node_dag_index
from dpone.gitops.airflow_asset_deps_report import render_asset_deps_markdown
from dpone.gitops.airflow_asset_graph import AssetGraphReport, build_asset_graph
from dpone.gitops.airflow_dag_spec_build_models import GitOpsAirflowDagSpecBuildReport
from dpone.gitops.airflow_dag_spec_builder import AirflowDagSpecBuilder
from dpone.gitops.workload_catalog import WorkloadCatalogResolver
from dpone.gitops.workload_catalog_models import GitOpsWorkloadCatalogIssue
from dpone.manifest.loader import ManifestLoaderRouter


@dataclass(frozen=True, slots=True)
class AirflowAssetDepsReport:
    workload_set: str
    env: str
    asset_graph: AssetGraphReport
    dag_spec_report: GitOpsAirflowDagSpecBuildReport
    markdown: str
    warnings: tuple[GitOpsWorkloadCatalogIssue, ...]

    @property
    def passed(self) -> bool:
        return self.dag_spec_report.passed


def build_airflow_asset_deps_report(
    *,
    repo_root: str | Path,
    workload_set: str,
    env: str = "dev",
) -> AirflowAssetDepsReport:
    from dpone.gitops.airflow_asset_uri import resolve_mssql_asset_registry

    root = Path(repo_root).resolve(strict=False)
    catalog = WorkloadCatalogResolver(repo_root=root).resolve(workload_set, env=env)
    loader = ManifestLoaderRouter()
    builder = AirflowDagSpecBuilder(repo_root=root, manifest_loader=loader)
    mssql_registry = resolve_mssql_asset_registry(root, env=env)
    asset_graph = build_asset_graph(
        catalog.workloads,
        repo_root=root,
        manifest_loader=loader,
        env=env,
        mssql_registry=mssql_registry,
    )
    dag_spec_report = builder.build(workload_set=workload_set, env=env, mssql_registry=mssql_registry)
    node_dags = node_dag_index(dag_spec_report.specs)
    cross_dag = cross_dag_recommendations(
        asset_graph=asset_graph,
        specs=dag_spec_report.specs,
        catalog=catalog,
    )
    markdown = render_asset_deps_markdown(
        workload_set=workload_set,
        env=env,
        asset_graph=asset_graph,
        specs=dag_spec_report.specs,
        node_dags=node_dags,
        cross_dag=cross_dag,
    )
    warnings = (*catalog.warnings, *asset_graph.warnings, *dag_spec_report.warnings)
    return AirflowAssetDepsReport(
        workload_set=workload_set,
        env=env,
        asset_graph=asset_graph,
        dag_spec_report=dag_spec_report,
        markdown=markdown,
        warnings=warnings,
    )


__all__ = ["AirflowAssetDepsReport", "build_airflow_asset_deps_report"]
