"""Strict-DI self-service Airflow authoring and preview application."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from dpone.manifest.project_authoring_authority import (
    DomainId,
    DomainIdError,
    ProjectConfigError,
    ProjectRootError,
    ProjectRootIdentity,
    conflicting_authoring_layout,
    ensure_project_root,
    inspect_project_root,
    load_project_layout,
    project_airflow_decision,
    project_root_candidate,
    verify_project_root,
)
from dpone.ports.project_authoring_lock import AuthoringLockFactory, ProjectAuthoringLockError
from dpone.readiness.airflow_authoring_check_service import AirflowAuthoringCheckService
from dpone.readiness.airflow_dag_scaffold import AirflowDagScaffoldService
from dpone.readiness.airflow_domain_scaffold import AirflowDomainScaffoldService
from dpone.readiness.airflow_explain_service import AirflowExplainService
from dpone.readiness.airflow_pipeline_init_validation import parse_pipeline_id
from dpone.readiness.airflow_pipeline_scaffold import AirflowPipelineScaffolder
from dpone.readiness.airflow_pipeline_source_reader import resolve_pipeline_path
from dpone.readiness.airflow_preview_service import AirflowPreviewService
from dpone.readiness.airflow_project_scaffold import (
    AirflowProjectScaffoldService,
    project_config_failure,
    project_layout_migration_failure,
)
from dpone.readiness.airflow_scaffold_apply import ScaffoldApplyPlan
from dpone.readiness.airflow_scaffold_result import scaffold_apply_failure_result
from dpone.readiness.airflow_self_service_cache import (
    cache_recovery_apply_result,
    cache_recovery_plan_result,
    cache_retention_apply_result,
    cache_retention_plan_result,
)
from dpone.readiness.airflow_self_service_models import Change, SelfServiceResult, dpone_error
from dpone.readiness.capability_discovery_composition import build_capability_discovery_service
from dpone.readiness.capability_discovery_service import CapabilityDiscoveryError
from dpone.readiness.error_contract import error_docs_url, manual_fix

if TYPE_CHECKING:
    from dpone.manifest.authoring import AuthoringCompiler
    from dpone.readiness.airflow_live_preflight import AirflowLivePreflightRunner


class AirflowSelfServiceService:
    """Build beginner-safe authoring sources and non-runnable Airflow previews."""

    def __init__(
        self,
        *,
        root: str | Path = ".",
        live_preflight_runner: AirflowLivePreflightRunner | None = None,
        authoring_compiler: AuthoringCompiler | None = None,
        authoring_lock: AuthoringLockFactory,
    ) -> None:
        try:
            self._root = project_root_candidate(root)
            self._root_identity = inspect_project_root(self._root, allow_missing=True)
        except ProjectRootError as exc:
            raise ProjectAuthoringLockError(str(exc)) from exc
        self._authoring_lock = authoring_lock
        self._authoring_check = AirflowAuthoringCheckService(
            root=self._root,
            live_preflight_runner=live_preflight_runner,
            authoring_compiler=authoring_compiler,
        )

    def init_project(self, *, airflow: bool, layout: str | None = None) -> SelfServiceResult:
        try:
            with self._authoring_lock(self._root):
                root_identity = self._authoring_root_identity()
                return AirflowProjectScaffoldService(
                    self._root,
                    root_identity=root_identity,
                ).init_project(airflow=airflow, layout=layout)
        except ProjectAuthoringLockError as exc:
            return _project_lock_failure(exc, stage="init_project")
        except OSError as exc:
            scaffold_failure = _scaffold_apply_failure(exc, stage="init_project")
            if scaffold_failure is not None:
                return scaffold_failure
            return project_config_failure(stage="init_project", reason="project_root_invalid")

    def init_domain(
        self,
        *,
        domain: str,
        owner_team: str,
        owner_contact: str,
        approver_team: str,
    ) -> SelfServiceResult:
        try:
            with self._authoring_lock(self._root):
                root_identity = self._authoring_root_identity()
                return AirflowDomainScaffoldService(
                    self._root,
                    root_identity=root_identity,
                    project_config_failure=project_config_failure,
                    project_layout_migration_failure=project_layout_migration_failure,
                ).init_domain(
                    domain=domain,
                    owner_team=owner_team,
                    owner_contact=owner_contact,
                    approver_team=approver_team,
                )
        except ProjectAuthoringLockError as exc:
            return _project_lock_failure(exc, stage="init_domain")
        except OSError as exc:
            scaffold_failure = _scaffold_apply_failure(exc, stage="init_domain")
            if scaffold_failure is not None:
                return scaffold_failure
            return project_config_failure(stage="init_domain", reason="project_root_invalid")

    def init_dag(
        self,
        *,
        dag_id: str,
        domain: str,
        schedule: str | None = None,
        pipelines: tuple[str, ...] = (),
        description: str | None = None,
    ) -> SelfServiceResult:
        try:
            with self._authoring_lock(self._root):
                root_identity = self._authoring_root_identity()
                return AirflowDagScaffoldService(
                    self._root,
                    root_identity=root_identity,
                ).init_dag(
                    dag_id=dag_id,
                    domain=domain,
                    schedule=schedule,
                    pipelines=pipelines,
                    description=description,
                )
        except ProjectAuthoringLockError as exc:
            return _project_lock_failure(exc, stage="init_dag")
        except OSError as exc:
            scaffold_failure = _scaffold_apply_failure(exc, stage="init_dag")
            if scaffold_failure is not None:
                return scaffold_failure
            return project_config_failure(stage="init_dag", reason="project_root_invalid")

    def init_pipeline(
        self,
        *,
        pipeline_id: str,
        recipe: str | None = None,
        route: str | None = None,
        airflow: bool | None,
        authoring_mode: str = "flow",
        profile: str | None = None,
        answers: str | Path | None = None,
        domain: str | None = None,
        from_locator: str | None = None,
        to_locator: str | None = None,
        unique_key: str | None = None,
    ) -> SelfServiceResult:
        if recipe is not None and route is not None:
            return SelfServiceResult(
                passed=False,
                errors=(
                    dpone_error(
                        "DPONE_RECIPE_ROUTE_CONFLICT",
                        "--recipe and --route are mutually exclusive.",
                        stage="init_pipeline",
                        fixes=[
                            manual_fix(
                                "choose_recipe_or_route",
                                command="dpone init pipeline --help",
                            )
                        ],
                        docs_url=error_docs_url("DPONE_RECIPE_ROUTE_CONFLICT"),
                    ),
                ),
                exit_code=2,
            )
        requested_recipe = recipe or "mssql-to-clickhouse-incremental"
        try:
            selected_recipe = (
                build_capability_discovery_service(root=self._root).resolve_beginner_recipe(route)
                if route is not None
                else requested_recipe
            )
        except CapabilityDiscoveryError as exc:
            return SelfServiceResult(
                passed=False,
                errors=(
                    dpone_error(
                        exc.code,
                        str(exc),
                        stage="init_pipeline",
                        entity={"kind": "route", "id": route or requested_recipe},
                    ),
                ),
                exit_code=2,
            )
        canonical_id = parse_pipeline_id(
            pipeline_id,
            recipe=selected_recipe,
            route=route,
            airflow=airflow,
            authoring_mode=authoring_mode,
            profile=profile,
            answers=answers,
        )
        if isinstance(canonical_id, SelfServiceResult):
            return canonical_id
        normalized_domain: str | None = None
        if domain is not None:
            try:
                normalized_domain = str(DomainId.parse(domain))
            except DomainIdError as exc:
                return SelfServiceResult(
                    passed=False,
                    errors=(
                        dpone_error(
                            "DPONE_DOMAIN_ID_INVALID",
                            str(exc),
                            stage="init_pipeline",
                            entity={"kind": "pipeline", "id": str(canonical_id)},
                            fixes=[
                                manual_fix(
                                    "use_canonical_domain_id",
                                    command="dpone init pipeline --help",
                                )
                            ],
                            docs_url=error_docs_url("DPONE_DOMAIN_ID_INVALID"),
                            extra={"suggested_id": exc.suggested_id},
                        ),
                    ),
                    exit_code=2,
                )
        try:
            with self._authoring_lock(self._root):
                root_identity = self._authoring_root_identity()
                layout, config_snapshot = load_project_layout(self._root)
                conflicting_layout = conflicting_authoring_layout(
                    self._root,
                    expected_mode=layout.mode,
                    domain_first_root=layout.root,
                )
                if conflicting_layout is not None:
                    return project_layout_migration_failure(
                        stage="init_pipeline",
                        detected_layout=conflicting_layout,
                        requested_layout=layout.mode,
                    )
                airflow_decision = project_airflow_decision(config_snapshot, explicit=airflow)
                return AirflowPipelineScaffolder(
                    self._root,
                    recipe_resolver=self._resolve_scaffold_recipe,
                    root_identity=root_identity,
                ).scaffold(
                    pipeline_id=canonical_id,
                    recipe=selected_recipe,
                    airflow=airflow_decision.enabled,
                    authoring_mode=authoring_mode,
                    profile=profile,
                    answers=Path(answers) if answers is not None else None,
                    airflow_source=airflow_decision.source,
                    layout=layout,
                    project_config_sha256=(config_snapshot.sha256 if config_snapshot is not None else None),
                    domain=normalized_domain,
                    from_locator=from_locator,
                    to_locator=to_locator,
                    unique_key=unique_key,
                )
        except ProjectAuthoringLockError as exc:
            return _project_lock_failure(exc, stage="init_pipeline")
        except ProjectConfigError as exc:
            return project_config_failure(stage="init_pipeline", reason=exc.reason)
        except OSError as exc:
            scaffold_failure = _scaffold_apply_failure(exc, stage="init_pipeline")
            if scaffold_failure is not None:
                return scaffold_failure
            return project_config_failure(stage="init_pipeline", reason="project_root_invalid")

    def _resolve_scaffold_recipe(self, recipe_ref: str) -> str:
        if not self._root.exists():
            return recipe_ref
        return build_capability_discovery_service(root=self._root).resolve_scaffold_recipe(recipe_ref)

    def _authoring_root_identity(self) -> ProjectRootIdentity:
        if self._root_identity is None:
            self._root_identity = ensure_project_root(self._root)
        else:
            verify_project_root(self._root_identity)
        return self._root_identity

    def check(self, target: str | Path, *, mode: str = "static", environment: str = "dev") -> SelfServiceResult:
        return self._authoring_check.inspect(target, mode=mode, environment=environment).result

    def preview(self, pipeline_ref: str) -> SelfServiceResult:
        return AirflowPreviewService(root=self._root, authoring_check=self._authoring_check).preview(pipeline_ref)

    def explain(self, pipeline_ref: str) -> SelfServiceResult:
        return AirflowExplainService(root=self._root, authoring_check=self._authoring_check).explain(pipeline_ref)

    def cache_retention_plan(
        self,
        *,
        cache_root: str | Path,
        environment: str,
        protected_deployment_ids: tuple[str, ...] = (),
        evidence_files: tuple[str | Path, ...] = (),
    ) -> SelfServiceResult:
        return cache_retention_plan_result(
            cache_root=cache_root,
            environment=environment,
            protected_deployment_ids=protected_deployment_ids,
            evidence_files=evidence_files,
        )

    def cache_retention_apply(
        self,
        *,
        cache_root: str | Path,
        environment: str,
        confirm_delete: bool,
        promoted_by: str,
        allowed_promoters: tuple[str, ...],
        expected_plan_sha256: str | None = None,
        review_id: str | None = None,
        loader_ack_file: str | Path | None = None,
        evidence_version: str = "v1",
        protected_deployment_ids: tuple[str, ...] = (),
        evidence_files: tuple[str | Path, ...] = (),
    ) -> SelfServiceResult:
        return cache_retention_apply_result(
            cache_root=cache_root,
            environment=environment,
            confirm_delete=confirm_delete,
            promoted_by=promoted_by,
            allowed_promoters=allowed_promoters,
            expected_plan_sha256=expected_plan_sha256,
            review_id=review_id,
            loader_ack_file=loader_ack_file,
            evidence_version=evidence_version,
            protected_deployment_ids=protected_deployment_ids,
            evidence_files=evidence_files,
        )

    def cache_recovery_plan(self, *, cache_root: str | Path, environment: str) -> SelfServiceResult:
        return cache_recovery_plan_result(cache_root=cache_root, environment=environment)

    def cache_recovery_apply(
        self,
        *,
        cache_root: str | Path,
        environment: str,
        deployment_id: str,
        confirm_repair: bool,
        promoted_by: str,
        expected_current_deployment_id: str | None,
        allowed_promoters: tuple[str, ...] = (),
    ) -> SelfServiceResult:
        return cache_recovery_apply_result(
            cache_root=cache_root,
            environment=environment,
            deployment_id=deployment_id,
            confirm_repair=confirm_repair,
            promoted_by=promoted_by,
            expected_current_deployment_id=expected_current_deployment_id,
            allowed_promoters=allowed_promoters,
        )

    def _resolve_pipeline_path(self, target: str | Path) -> Path:
        return resolve_pipeline_path(self._root, target)


def _project_lock_failure(error: ProjectAuthoringLockError, *, stage: str) -> SelfServiceResult:
    return SelfServiceResult(
        passed=False,
        errors=(
            dpone_error(
                error.code,
                str(error),
                stage=stage,
                docs_url=error_docs_url(error.code),
            ),
        ),
        exit_code=4,
    )


def _scaffold_apply_failure(error: OSError, *, stage: str) -> SelfServiceResult | None:
    receipt = getattr(error, "scaffold_receipt", None)
    if not isinstance(receipt, ScaffoldApplyPlan):
        return None
    return scaffold_apply_failure_result(receipt, stage=stage)


__all__ = ["AirflowSelfServiceService", "AuthoringLockFactory", "Change", "SelfServiceResult"]
