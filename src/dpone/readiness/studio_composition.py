"""Composition root for the local read-only Studio application service."""

from __future__ import annotations

from pathlib import Path

from dpone.readiness.airflow_authoring_check_service import AirflowAuthoringCheckService
from dpone.readiness.airflow_explain_service import AirflowExplainService
from dpone.readiness.capability_discovery_composition import build_capability_discovery_service
from dpone.readiness.doctor import DoctorService
from dpone.readiness.managed import (
    ConnectorScaffoldService,
    ExecutionPlanService,
    PerformanceAdvisor,
    QualityService,
    StateInspectorService,
)
from dpone.readiness.studio_api import StudioApplicationService
from dpone.readiness.studio_authoring import StudioAuthoringService
from dpone.readiness.studio_http_models import StudioAuditLog
from dpone.readiness.studio_legacy_operations import StudioLegacyOperations
from dpone.readiness.studio_pipeline_summary import StudioPipelineCatalog
from dpone.readiness.studio_planning import StudioManifestPlanningService
from dpone.readiness.studio_process_evidence import StudioProcessEvidencePolicy
from dpone.readiness.studio_project_paths import StudioProjectPathPolicy
from dpone.readiness.studio_route_validation import StudioRouteValidator
from dpone.readiness.studio_ui_assets import (
    StudioUiAssetsStatus,
    probe_studio_ui_assets,
)


def build_studio_api_service(
    *,
    root: Path,
    audit: StudioAuditLog | None = None,
    artifact_roots: tuple[str, ...] = (".dpone/runs", "test_artifacts"),
    ui_assets: StudioUiAssetsStatus | None = None,
) -> StudioApplicationService:
    """Construct Studio services once; adapters receive the finished facade."""

    project_root = root.resolve(strict=True)

    def capability_snapshot():
        return build_capability_discovery_service(root=project_root).snapshot()

    authoring_check = AirflowAuthoringCheckService(root=project_root)
    explain = AirflowExplainService(
        root=project_root,
        authoring_check=authoring_check,
    )
    pipelines = StudioPipelineCatalog(
        root=project_root,
        authoring_check=authoring_check,
        explain=explain,
        capabilities=capability_snapshot,
    )
    audit_log = audit or StudioAuditLog()
    doctor = DoctorService().run
    paths = StudioProjectPathPolicy(project_root)
    routes = StudioRouteValidator(capability_snapshot)
    evidence = StudioProcessEvidencePolicy(project_root)
    planning = StudioManifestPlanningService(
        root=project_root,
        planner=ExecutionPlanService(),
        paths=paths,
        routes=routes,
        evidence=evidence,
    )
    authoring = StudioAuthoringService(
        root=project_root,
        routes=routes,
        authoring_check=authoring_check,
        scaffold=ConnectorScaffoldService(),
        planning=planning,
        paths=paths,
        evidence=evidence,
    )
    legacy = StudioLegacyOperations(
        root=project_root,
        capabilities=capability_snapshot,
        authoring=authoring,
        audit=audit_log,
        doctor=doctor,
        artifact_roots=artifact_roots,
        state=StateInspectorService(),
        perf=PerformanceAdvisor(),
    )
    return StudioApplicationService(
        capability_snapshot_factory=capability_snapshot,
        pipelines=pipelines,
        explain=explain,
        authoring=authoring,
        legacy=legacy,
        quality=QualityService(),
        ui_assets=ui_assets or probe_studio_ui_assets(),
    )


__all__ = ["build_studio_api_service"]
