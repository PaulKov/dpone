"""Runtime composition root for shell-free dbt pack execution."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.adapters.dbt_runtime import (
    DBT_DEV_EVIDENCE_ROOT_ENV,
    DBT_DEV_EVIDENCE_SET_ENV,
    CampaignDbtExecutionEvidenceWriter,
    DistributionDbtToolchainInspector,
    LocalDbtExecutionEvidenceWriter,
    LocalDbtRunResultsReader,
    OfficialDbtRunResultsValidator,
    RuntimeDbtProfileRenderer,
    SemanticRefreshDbtRuntimeAuthority,
    SubprocessDbtCommandRunner,
    TemporaryDbtProfileStore,
    build_hvac_kubernetes_vault_kv_v2_reader,
    materialize_semantic_refresh_project,
)
from dpone.contracts.dbt_runtime import (
    AIRFLOW_RUN_IDENTITY_ENV,
    DBT_RUNTIME_WIRE_V1,
    DbtExecutionPack,
    DbtPublishingError,
    airflow_attempt_from_environment,
    dbt_execution_interval_from_environment,
    parse_airflow_run_identity_json,
    prove_mutation_closure,
)
from dpone.ports.composition_dbt import (
    COMPOSITION_SUPERVISOR_B64_ENV,
    CompositionNativeDbtExecutor,
)
from dpone.runtime.composition_dbt_bootstrap import (
    RuntimeDbtManifestSchemaValidator,
    build_dbt_runtime_preflight,
)
from dpone.runtime.credentials.runtime_context import (
    RuntimeConnectionContextLoader,
)
from dpone.runtime.dbt_execution_pack_reader import execution_pack_payload
from dpone.runtime.dbt_execution_service import (
    DEFAULT_DBT_RUN_OUTPUT_ROOT,
    DbtExecutionOutcome,
    DbtExecutionService,
)
from dpone.runtime.dbt_semantic_refresh_execution import (
    SemanticRefreshDbtCommandRunner,
    SemanticRefreshDbtExecutionVariables,
    SemanticRefreshDbtRuntimePreflight,
    SemanticRefreshDbtScopeMapLoaderPort,
    load_semantic_refresh_scope_map,
)
from dpone.runtime.dbt_semantic_refresh_run_authority import (
    SemanticRefreshDbtImmutableProofRecheckPort,
    SemanticRefreshDbtRunAdmissionPort,
    SemanticRefreshDbtStaticProjectionIdentity,
    validate_static_projection_identity,
)
from dpone.runtime.dbt_workspace_attempt_bootstrap import (
    workspace_attempt_dependencies as _workspace_attempt_dependencies,
)

MAX_DBT_EXECUTION_PACK_BYTES = 1024 * 1024


def execute_dbt_pack(
    relative_pack_path: str,
    *,
    environ: dict[str, str] | None = None,
    runtime_root: Path | None = None,
    run_output_root: Path = DEFAULT_DBT_RUN_OUTPUT_ROOT,
    profiles_tmpfs_root: Path = Path("/dev/shm/dpone"),
    composition_executor: CompositionNativeDbtExecutor | None = None,
) -> DbtExecutionOutcome:
    """Compose and execute one scheduler-verified pack without a shell.

    ``composition_executor`` is the supervised parent root. When the runtime
    carries a pinned composition supervisor capability the pack must execute
    through it; native-v2 workspace admission and generic subprocess dispatch are
    never an alternative path for an authenticated v3 deployment.
    """

    environment = os.environ if environ is None else environ
    root = (runtime_root or Path.cwd()).absolute()
    output_root = Path(run_output_root).absolute()
    pack = DbtExecutionPack.from_mapping(
        execution_pack_payload(
            root,
            relative_pack_path,
            max_bytes=MAX_DBT_EXECUTION_PACK_BYTES,
        )
    )
    return _execute_loaded_pack(
        pack,
        environment=environment,
        runtime_root=root,
        run_output_root=output_root,
        profiles_tmpfs_root=profiles_tmpfs_root,
        composition_executor=composition_executor,
    )


def _execute_loaded_pack(
    pack: DbtExecutionPack,
    *,
    environment: dict[str, str] | os._Environ[str],
    runtime_root: Path,
    run_output_root: Path,
    profiles_tmpfs_root: Path,
    command_runner: Any | None = None,
    preflight: Any | None = None,
    interval: Any | None = None,
    composition_executor: CompositionNativeDbtExecutor | None = None,
) -> DbtExecutionOutcome:
    """Execute one already-authenticated pack through the shared pinned engine."""

    raw_identity = str(environment.get(AIRFLOW_RUN_IDENTITY_ENV) or "")
    if not raw_identity:
        raise DbtPublishingError(
            "DPONE_DBT_EXECUTION_FAILED",
            "Airflow run identity is required",
        )
    try:
        run_identity = parse_airflow_run_identity_json(raw_identity)
    except ValueError as exc:
        raise DbtPublishingError(
            "DPONE_DBT_EXECUTION_FAILED",
            "Airflow run identity is invalid",
        ) from exc
    attempt = airflow_attempt_from_environment(environment, run_identity)
    supervisor = environment.get(COMPOSITION_SUPERVISOR_B64_ENV)
    if supervisor:
        if composition_executor is None or command_runner is not None or preflight is not None:
            raise DbtPublishingError(
                "DPONE_DBT_COMPOSITION_EXECUTOR_UNAVAILABLE",
                "supervised composition execution requires the parent worker root",
            )
        return composition_executor.execute_native_pack(
            pack=pack,
            run_identity=run_identity,
            airflow_attempt=attempt,
            runtime_root=runtime_root,
            interval=interval or dbt_execution_interval_from_environment(environment),
            supervisor_transport=supervisor,
        )
    context = RuntimeConnectionContextLoader(
        vault_reader_factory=build_hvac_kubernetes_vault_kv_v2_reader,
    ).load(environment)
    if context is None:
        raise DbtPublishingError(
            "DPONE_DBT_PROFILE_INVALID",
            "Pinned runtime connection context is required",
        )
    local_evidence_writer = LocalDbtExecutionEvidenceWriter(run_output_root / f"dbt-{pack.workflow_id}-execution.json")
    runner = command_runner or SubprocessDbtCommandRunner()
    artifact_reader = LocalDbtRunResultsReader()
    workspace_attempt_factory, workspace_attempt_admission = _workspace_attempt_dependencies(
        pack,
        environment=environment,
        run_identity=run_identity,
        resolver=context.resolver,
    )
    service = DbtExecutionService(
        command_runner=runner,
        toolchain_inspector=DistributionDbtToolchainInspector(),
        profile_renderer=RuntimeDbtProfileRenderer(context.resolver),
        profile_store=TemporaryDbtProfileStore(profiles_tmpfs_root),
        run_results_reader=artifact_reader,
        run_results_validator=OfficialDbtRunResultsValidator(),
        preflight=preflight or build_dbt_runtime_preflight(runner, artifact_reader=artifact_reader),
        evidence_writer=CampaignDbtExecutionEvidenceWriter(
            local_evidence_writer,
            evidence_root=environment.get(DBT_DEV_EVIDENCE_ROOT_ENV),
            evidence_set_id=environment.get(DBT_DEV_EVIDENCE_SET_ENV),
        ),
        workspace_attempt_factory=workspace_attempt_factory,
        workspace_attempt_admission=workspace_attempt_admission,
    )
    return service.execute(
        pack,
        runtime_root=runtime_root,
        run_output_root=run_output_root,
        run_identity=run_identity,
        airflow_attempt=attempt,
        interval=interval or dbt_execution_interval_from_environment(environment),
    )


def execute_semantic_refresh_dbt_pack(
    *,
    dbt_execution_pack: Mapping[str, object],
    project_config_overlay: Mapping[str, object],
    profile_sha256: str,
    topology_sha256: str,
    plan_bundle: Mapping[str, object],
    projection_identity: Mapping[str, object],
    workflow_execution_id: str,
    package_source_root: Path,
    run_admission: SemanticRefreshDbtRunAdmissionPort,
    scope_map_loader: SemanticRefreshDbtScopeMapLoaderPort,
    immutable_proof_rechecker: SemanticRefreshDbtImmutableProofRecheckPort,
    environ: dict[str, str] | None = None,
    runtime_root: Path | None = None,
    run_output_root: Path = DEFAULT_DBT_RUN_OUTPUT_ROOT,
    profiles_tmpfs_root: Path = Path("/dev/shm/dpone"),
) -> DbtExecutionOutcome:
    """Admit the actual DagRun, then execute V2 with protected package/scope authority."""

    static_identity = _semantic_refresh_projection_identity(
        projection_identity,
        plan_bundle=plan_bundle,
        topology_sha256=topology_sha256,
    )
    admitted = run_admission.admit(
        plan_bundle=plan_bundle,
        workflow_execution_id=workflow_execution_id,
        projection_identity=static_identity,
    )
    if admitted.workflow_execution_id != workflow_execution_id or (
        plan_bundle.get("plan_bundle_sha256") != admitted.plan_bundle_sha256
        or plan_bundle.get("pre_release_bundle_sha256") != admitted.pre_release_bundle_sha256
        or plan_bundle.get("package_artifacts_sha256") != admitted.package_artifacts_sha256
    ):
        raise DbtPublishingError(
            "DPONE_DBT_V2_RUN_AUTHORITY_INVALID",
            "protected run admission differs from the actual DagRun or deployment plan",
        )
    authority = SemanticRefreshDbtRuntimeAuthority(
        workflow_execution_id=admitted.workflow_execution_id,
        workflow_execution_binding_sha256=admitted.workflow_execution_binding_sha256,
        profile_sha256=profile_sha256,
        topology_sha256=topology_sha256,
        package_artifacts_sha256=admitted.package_artifacts_sha256,
        package_source_root=package_source_root,
    )
    pack = DbtExecutionPack.from_mapping(dbt_execution_pack)
    pack.require_wire_contract(DBT_RUNTIME_WIRE_V1)
    if pack.selection_lock.selected_graph_unique_ids != pack.selection_lock.publish_model_unique_ids:
        raise DbtPublishingError(
            "DPONE_DBT_V2_GRAPH_UNVERIFIED",
            "semantic-refresh execution pack is not an exact operation closure",
        )
    if admitted.model_unique_ids != pack.selection_lock.publish_model_unique_ids:
        raise DbtPublishingError(
            "DPONE_DBT_V2_RUN_AUTHORITY_INVALID",
            "protected run admission differs from the exact dbt model closure",
        )
    scope_map = load_semantic_refresh_scope_map(
        scope_map_loader,
        workflow_execution_binding_sha256=admitted.workflow_execution_binding_sha256,
        model_unique_ids=pack.selection_lock.publish_model_unique_ids,
    )
    pack_bytes = (
        json.dumps(
            pack.to_dict(),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    root = (runtime_root or Path.cwd()).absolute()
    environment = os.environ if environ is None else environ
    with materialize_semantic_refresh_project(
        runtime_root=root,
        project_subdir=pack.project_subdir,
        execution_pack_bytes=pack_bytes,
        project_config_overlay=project_config_overlay,
        authority=authority,
    ) as materialized_root:
        base_interval = dbt_execution_interval_from_environment(environment)
        variables = SemanticRefreshDbtExecutionVariables(
            start=base_interval.start,
            end=base_interval.end,
            scope_map=scope_map,
            statement_timeout_seconds=pack.adapter_runtime.query_timeout_seconds,
        )
        delegate = SubprocessDbtCommandRunner()
        runner = SemanticRefreshDbtCommandRunner(
            delegate,
            expected_vars_json=variables.dbt_vars_json(),
        )
        artifact_reader = LocalDbtRunResultsReader()
        preflight = SemanticRefreshDbtRuntimePreflight(
            command_runner=runner,
            artifact_reader=artifact_reader,
            manifest_validator=RuntimeDbtManifestSchemaValidator(),
            mutation_prover=prove_mutation_closure,
            immutable_proof_rechecker=immutable_proof_rechecker,
            plan_bundle=plan_bundle,
            projection_identity=static_identity,
            error_factory=lambda code, message: DbtPublishingError(code, message),
        )
        return _execute_loaded_pack(
            pack,
            environment=environment,
            runtime_root=materialized_root,
            run_output_root=run_output_root,
            profiles_tmpfs_root=profiles_tmpfs_root,
            command_runner=runner,
            preflight=preflight,
            interval=variables,
        )


def _semantic_refresh_projection_identity(
    value: Mapping[str, object],
    *,
    plan_bundle: Mapping[str, object],
    topology_sha256: str,
) -> SemanticRefreshDbtStaticProjectionIdentity:
    try:
        identity = SemanticRefreshDbtStaticProjectionIdentity.from_mapping(value)
        validate_static_projection_identity(
            identity,
            plan_bundle=plan_bundle,
            topology_sha256=topology_sha256,
        )
        return identity
    except (TypeError, ValueError) as exc:
        raise DbtPublishingError(
            "DPONE_DBT_V2_RUN_AUTHORITY_INVALID",
            "semantic-refresh static projection authority is invalid",
        ) from exc


__all__ = [
    "MAX_DBT_EXECUTION_PACK_BYTES",
    "execute_dbt_pack",
    "execute_semantic_refresh_dbt_pack",
]
