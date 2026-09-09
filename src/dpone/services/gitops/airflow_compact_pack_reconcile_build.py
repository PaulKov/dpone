"""Shared MSSQL registry snapshot pack/DAG build for reconcile."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from dpone.gitops.airflow_asset_uri import ResolvedMssqlAssetRegistry
from dpone.gitops.airflow_compact_pack import (
    AirflowCompactPackBuildError,
    GitOpsAirflowCompactPackReport,
)
from dpone.gitops.airflow_dag_spec_build_models import GitOpsAirflowDagSpecBuildReport
from dpone.gitops.airflow_dag_spec_builder import AirflowDagSpecBuilder, AirflowDagSpecBuildError
from dpone.gitops.airflow_reconcile_policy import RECONCILE_SOURCE, build_failure_issue
from dpone.gitops.workload_catalog_models import GitOpsWorkloadCatalogIssue, GitOpsWorkloadCatalogReport, issue


@dataclass(frozen=True, slots=True)
class ReconcileArtifactBuild:
    pack_reports: tuple[GitOpsAirflowCompactPackReport, ...]
    pack_warnings: tuple[GitOpsWorkloadCatalogIssue, ...]
    dag_spec_report: GitOpsAirflowDagSpecBuildReport | None
    blockers: tuple[GitOpsWorkloadCatalogIssue, ...]


def build_reconcile_artifacts(
    *,
    catalog: GitOpsWorkloadCatalogReport,
    selected_ids: tuple[str, ...],
    mssql_registry: ResolvedMssqlAssetRegistry,
    dag_spec_builder: AirflowDagSpecBuilder,
    build_pack: Callable[..., GitOpsAirflowCompactPackReport],
    repo_root: Path,
) -> ReconcileArtifactBuild:
    pack_reports: list[GitOpsAirflowCompactPackReport] = []
    pack_warnings: list[GitOpsWorkloadCatalogIssue] = []
    blockers: list[GitOpsWorkloadCatalogIssue] = []
    for workload_id in selected_ids:
        workload = catalog.by_id(workload_id)
        try:
            report = build_pack(workload_id=workload_id, mssql_registry=mssql_registry)
        except AirflowCompactPackBuildError as exc:
            blockers.append(build_failure_issue(workload_id=workload_id, manifest_path=workload.manifest, exc=exc))
            continue
        pack_warnings.extend(report.warnings)
        if report.blockers:
            blockers.extend(report.blockers)
            continue
        pack_reports.append(report)
    try:
        dag_spec_report = dag_spec_builder.build(
            workload_set=catalog.workload_set,
            env=catalog.env,
            mssql_registry=mssql_registry,
        )
    except AirflowDagSpecBuildError as exc:
        blockers.append(build_failure_issue(workload_id="<dag-specs>", manifest_path=catalog.workload_set, exc=exc))
        return ReconcileArtifactBuild(
            pack_reports=tuple(pack_reports),
            pack_warnings=tuple(pack_warnings),
            dag_spec_report=None,
            blockers=tuple(blockers),
        )
    blockers.extend(dag_spec_report.blockers)
    drift = mssql_registry.verify_unchanged(repo_root)
    if drift is not None:
        blockers.append(
            issue(
                code=drift.code,
                message=drift.message,
                path=drift.path or catalog.env,
                source=RECONCILE_SOURCE,
            )
        )
    return ReconcileArtifactBuild(
        pack_reports=tuple(pack_reports),
        pack_warnings=tuple(pack_warnings),
        dag_spec_report=dag_spec_report,
        blockers=tuple(blockers),
    )


__all__ = ["ReconcileArtifactBuild", "build_reconcile_artifacts"]
