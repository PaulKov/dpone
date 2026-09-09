from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest
from dpone_airflow_pack.pack_identity import compute_pack_fingerprint, verify_pack_fingerprint

from dpone.contracts.dbt_contract_validation import DbtPublishingError, canonical_fingerprint
from dpone.contracts.dbt_invocation import DbtInvocationContext
from dpone.contracts.dbt_publishing import (
    DbtExecutionPack,
    DbtProfileSpec,
    DbtSelectionLock,
    DbtSqlServerAdapterPolicy,
    DbtSqlServerRuntimePolicy,
)
from dpone.contracts.dbt_semantic_refresh_activation import (
    SemanticRefreshActivationAuthorityReceipt,
    SemanticRefreshActivationAuthoritySet,
)
from dpone.contracts.dbt_semantic_refresh_certification import (
    SemanticRefreshCertificationDecision,
    SemanticRefreshCertificationRequest,
)
from dpone.contracts.dbt_semantic_refresh_lifecycle import (
    SemanticRefreshLifecycleReport,
)
from dpone.contracts.dbt_semantic_refresh_plan_codec import (
    semantic_refresh_plan_bundle_from_mapping,
)
from dpone.contracts.dbt_semantic_refresh_plan_compiler import (
    SemanticRefreshPostDeploymentPlanCompiler,
)
from dpone.contracts.dbt_semantic_refresh_plan_contracts import (
    SemanticRefreshArtifactAuthority,
    SemanticRefreshDeploymentAuthoritySubject,
    SemanticRefreshDeploymentModelAuthority,
    SemanticRefreshPlanBundle,
    SemanticRefreshPreReleaseModelInput,
    SemanticRefreshPreReleaseProofBundle,
    SemanticRefreshReleaseDeploymentAuthority,
    SemanticRefreshResourcePolicy,
    SemanticRefreshWritableColumn,
    SemanticRefreshWritableRole,
)
from dpone.contracts.dbt_semantic_refresh_recovery_authority import (
    SemanticRefreshCompleteScopeReplayAuthority,
    SemanticRefreshFailedPrecommitReplacementAuthority,
)
from dpone.contracts.dbt_semantic_refresh_recovery_head import (
    SemanticRefreshRecoveryTargetHead,
)
from dpone.contracts.dbt_semantic_refresh_run_contracts import (
    SemanticRefreshRunAdmissionAuthority,
    SemanticRefreshRunAdmissionCompiler,
)
from dpone.contracts.dbt_sqlserver_graph_policy import (
    DBT_SQLSERVER_GRAPH_POLICY_ID,
    DBT_SQLSERVER_GRAPH_POLICY_SHA256,
)
from dpone.contracts.dbt_toolchain import DBT_SQLSERVER_1_12_CERTIFIED
from dpone.contracts.semantic_refresh_baseline_receipt import (
    BaselineAssuranceKind,
    SemanticRefreshBaselineAdoptionReceipt,
    semantic_refresh_target_predecessor_generation_id,
)
from dpone.contracts.semantic_refresh_core import SemanticRefreshContractError
from dpone.contracts.semantic_refresh_effective_key_identity import EffectiveKeyTemplateColumn
from dpone.contracts.semantic_refresh_failure_summary import (
    FailedModelOutcome,
    SemanticRefreshFailedWorkflowSummary,
)
from dpone.contracts.semantic_refresh_lifecycle_policy import SemanticRefreshSqlServerLifecyclePolicy
from dpone.contracts.semantic_refresh_model_proof import SemanticRefreshModelDefinitionProof
from dpone.contracts.semantic_refresh_mutation_closure import SemanticRefreshMutationClosure
from dpone.contracts.semantic_refresh_plan_refs import ReplacementActionBinding
from dpone.contracts.semantic_refresh_read_dependency import SemanticRefreshReadDependencyProof
from dpone.contracts.semantic_refresh_route_certification import (
    LiveCertificationStatus,
    SemanticRefreshRouteLiveCertificationReceipt,
)
from dpone.contracts.semantic_refresh_route_coordinates import (
    SemanticRefreshRouteCapabilityCoordinates,
)
from dpone.contracts.semantic_refresh_runtime_assurance import (
    RuntimeAssuranceKind,
    RuntimeAssuranceSubjectType,
    SemanticRefreshRuntimeAssuranceReceipt,
    SemanticRefreshRuntimeAssuranceSubject,
)
from dpone.contracts.semantic_refresh_types import (
    DATE_DOMAIN_MAX,
    DATE_DOMAIN_MIN,
    DATETIME_DOMAIN_MAX,
    DATETIME_DOMAIN_MIN,
    ClosureStatus,
    ReplacementAction,
    SqlServerModelOutcome,
    WorkflowMode,
)
from dpone.contracts.semantic_refresh_workflow_summary import (
    SemanticRefreshDurableModelPublication,
    SemanticRefreshDurableWorkflowSummary,
)
from dpone.ports.semantic_refresh_production_activation import (
    SemanticRefreshProductionActivationUnavailableError,
)
from dpone.readiness.dbt_semantic_refresh_airflow_pack import (
    SemanticRefreshTemplateProofAuthority,
    semantic_refresh_activated_pack,
    semantic_refresh_template_pack,
)
from dpone.services.dbt_semantic_refresh_activation import (
    SemanticRefreshDeploymentAuthorityService,
    load_exact_deployment_authority_receipt,
)

MODEL_ID = "model.project.events"
DIGESTS = tuple("sha256:" + character * 64 for character in "123456789abcdef")


def _pre_release(event_time_type: str = "date") -> SemanticRefreshPreReleaseProofBundle:
    definition = SemanticRefreshModelDefinitionProof.build(
        status=ClosureStatus.PROVEN,
        model_unique_id=MODEL_ID,
        manifest_sha256=DIGESTS[0],
        raw_code_sha256=DIGESTS[1],
        compiled_sql_sha256=DIGESTS[2],
        macro_closure_sha256=DIGESTS[3],
        toolchain_sha256=DBT_SQLSERVER_1_12_CERTIFIED.sha256,
        resolved_relation_dependency_digest=DIGESTS[5],
        resolved_module_dependency_digest=DIGESTS[6],
        catalog_observation_digest=DIGESTS[7],
        parser_runtime_policy_digest=DIGESTS[8],
        target_independence_policy_digest=DIGESTS[9],
    )
    dependency = SemanticRefreshReadDependencyProof.build(
        status=ClosureStatus.PROVEN,
        model_unique_id=MODEL_ID,
        database_name="DWH",
        compiled_sql_sha256=DIGESTS[2],
        catalog_snapshot_sha256=DIGESTS[7],
        normalized_definitions_sha256=DIGESTS[6],
        policy_sha256=DIGESTS[8],
        dependency_edges=(),
        base_relation_object_ids=(101,),
        max_depth=4,
        max_nodes=20,
        max_edges=40,
        max_definition_bytes=100_000,
    )
    mutation = SemanticRefreshMutationClosure.build(
        status=ClosureStatus.PROVEN,
        selectors=("fqn:project.events",),
        selected_node_ids=(MODEL_ID,),
        selected_mutating_node_ids=(MODEL_ID,),
        selected_read_only_node_ids=(),
        unclassified_mutating_node_ids=(),
    )
    lifecycle = SemanticRefreshSqlServerLifecyclePolicy.build(
        python_version="3.12.12",
        runtime_image_digest=DIGESTS[0],
        pyodbc_version="5.2.0",
        odbc_driver="ODBC Driver 18 for SQL Server",
        sqlserver_version="16.0.1000.6",
        compatibility_level=160,
        macro_closure_sha256=DIGESTS[3],
        adapter_policy_digest=DIGESTS[4],
        project_policy_digest=DIGESTS[5],
        profile_policy_digest=DIGESTS[6],
        invocation_policy_digest=DIGESTS[7],
        package_artifacts_digest=DIGESTS[8],
        materialization_closure_digest=DIGESTS[9],
        dispatch_closure_digest=DIGESTS[10],
        driver_digest=DIGESTS[11],
    )
    event_target = "Date" if event_time_type == "date" else "DateTime64(6,'UTC')"
    event_min = DATE_DOMAIN_MIN if event_time_type == "date" else DATETIME_DOMAIN_MIN
    event_max = DATE_DOMAIN_MAX if event_time_type == "date" else DATETIME_DOMAIN_MAX
    model = SemanticRefreshPreReleaseModelInput(
        MODEL_ID,
        DIGESTS[12],
        "event_date",
        (
            EffectiveKeyTemplateColumn("event_date", event_time_type, event_target, event_min, event_max),
            EffectiveKeyTemplateColumn("event_id", "bigint", "Int64"),
        ),
        (
            SemanticRefreshWritableColumn(
                "event_date",
                event_time_type,
                event_target,
                False,
                SemanticRefreshWritableRole.EFFECTIVE_KEY_EVENT_TIME,
            ),
            SemanticRefreshWritableColumn(
                "event_id",
                "bigint",
                "Int64",
                False,
                SemanticRefreshWritableRole.EFFECTIVE_KEY,
            ),
            SemanticRefreshWritableColumn(
                "payload",
                "nvarchar(200)",
                "String",
                True,
                SemanticRefreshWritableRole.MUTABLE_VALUE,
            ),
        ),
        definition,
        dependency,
    )
    certification_request = SemanticRefreshCertificationRequest.build(
        certification_coordinate_sha256=DIGESTS[12],
        manifest_sha256=DIGESTS[0],
        profile_sha256=DIGESTS[5],
        toolchain_sha256=DBT_SQLSERVER_1_12_CERTIFIED.sha256,
        verification_time="2026-08-08T00:00:00Z",
    )
    certification_decision = SemanticRefreshCertificationDecision(
        certification_request.request_sha256,
        "CERTIFIED",
        _route_receipt().route_certification_receipt_sha256,
        "2026-08-01T00:00:00Z",
        "2026-09-01T00:00:00Z",
    )
    return SemanticRefreshPreReleaseProofBundle.build(
        workflow_name="daily_events",
        environment="prod",
        manifest_sha256=DIGESTS[0],
        profile_sha256=DIGESTS[5],
        toolchain_sha256=DBT_SQLSERVER_1_12_CERTIFIED.sha256,
        certification_request=certification_request,
        certification_decision=certification_decision,
        platform_policy_digest=DIGESTS[5],
        resource_policy=SemanticRefreshResourcePolicy(
            max_source_scope_rows=1_000_000,
            max_target_scope_rows=1_000_000,
            max_before_image_rows=1_000_000,
            max_before_image_bytes=1_000_000_000,
            max_after_image_rows=1_000_000,
            max_after_image_bytes=1_000_000_000,
            max_transaction_log_bytes=2_000_000_000,
            min_mssql_log_free_bytes=4_000_000_000,
            max_mssql_scope_lock_seconds=300,
            max_mssql_version_store_bytes=1_000_000_000,
            max_scope_image_total_bytes=2_000_000_000,
            max_dbt_temp_rows=1_000_000,
            max_dbt_temp_bytes=2_000_000_000,
            max_statement_seconds=600,
            max_clickhouse_staging_bytes=2_000_000_000,
            max_clickhouse_shadow_bytes=4_000_000_000,
            max_clickhouse_retained_backup_bytes=4_000_000_000,
            max_clickhouse_total_transient_bytes=10_000_000_000,
        ),
        mutation_closure=mutation,
        lifecycle_policy=lifecycle,
        lifecycle_report=SemanticRefreshLifecycleReport.build(
            status="PROVEN",
            lifecycle_policy_sha256=lifecycle.sqlserver_lifecycle_policy_sha256,
            lifecycle_authority_sha256=DIGESTS[10],
            certification_coordinate_sha256=certification_request.certification_coordinate_sha256,
            issues=(),
        ),
        models=(model,),
    )


def _authority() -> SemanticRefreshReleaseDeploymentAuthority:
    return SemanticRefreshReleaseDeploymentAuthority(
        release_id=DIGESTS[0],
        deployment_id=DIGESTS[1],
        binding_set_ref="bindings/prod.json",
        connection_registry_ref="connections/prod.json",
        credential_runtime_ref="vault/prod/dbt",
        authority_receipt_sha256=DIGESTS[2],
    )


def _deployment_model() -> SemanticRefreshDeploymentModelAuthority:
    baseline = _baseline_receipt()
    return SemanticRefreshDeploymentModelAuthority(
        model_unique_id=MODEL_ID,
        target_predecessor_generation_id=semantic_refresh_target_predecessor_generation_id(baseline),
        owner_generation=1,
        target_resource_id="DWH.mart.events",
        mssql_connection_authority_id="mssql-prod",
        mssql_target_authority_id="mssql://mssql-prod/DWH/mart.events",
        mssql_control_database="DWH",
        mssql_control_schema="dpone_control",
        mssql_image_schema="dpone_scope_images",
        scope_image_namespace_policy_sha256=DIGESTS[10],
        clickhouse_cluster_authority_id="clickhouse-prod",
        clickhouse_target_authority_id="clickhouse://clickhouse-prod/analytics/mart.events",
        publication_database="analytics",
        publication_target_table="mart.events",
        baseline_receipt=baseline,
        artifact_authority=SemanticRefreshArtifactAuthority(
            provider="s3",
            provider_profile="s3_create_only_versioned_kms_object_lock_v1",
            endpoint_authority_id="https://s3.eu-central-1.amazonaws.com",
            bucket_or_container_authority_id="semantic-refresh-prod",
            kms_key_authority_id=("arn:aws:kms:eu-central-1:123456789012:key/12345678-1234-1234-1234-123456789012"),
            capability_evidence_sha256=DIGESTS[2],
            writer_scope="semantic-refresh/prod/events",
            artifact_prefix="semantic-refresh/prod/events",
            encryption_policy_sha256=DIGESTS[3],
            retention_policy_id="semantic-refresh-30d",
            retention_policy_sha256=DIGESTS[4],
            retention_days=30,
            retention_issued_at="2026-08-08T00:00:00Z",
            retention_until="2026-09-07T00:00:00Z",
            max_artifact_bytes=2_000_000_000,
        ),
    )


@pytest.mark.parametrize(
    ("endpoint_authority_id", "kms_key_authority_id", "expected_message"),
    (
        (
            "artifact-endpoint-prod",
            "arn:aws:kms:eu-central-1:123456789012:key/12345678-1234-1234-1234-123456789012",
            "normalized HTTPS S3 endpoint",
        ),
        (
            "https://s3.eu-central-1.amazonaws.com",
            "kms-semantic-refresh-prod",
            "full AWS KMS key ARN",
        ),
    ),
)
def test_production_artifact_authority_requires_physical_endpoint_and_kms_key(
    endpoint_authority_id: str,
    kms_key_authority_id: str,
    expected_message: str,
) -> None:
    with pytest.raises(SemanticRefreshContractError, match=expected_message):
        SemanticRefreshArtifactAuthority(
            provider="s3",
            provider_profile="s3_create_only_versioned_kms_object_lock_v1",
            endpoint_authority_id=endpoint_authority_id,
            bucket_or_container_authority_id="semantic-refresh-prod",
            kms_key_authority_id=kms_key_authority_id,
            capability_evidence_sha256=DIGESTS[2],
            writer_scope="semantic-refresh/prod/events",
            artifact_prefix="semantic-refresh/prod/events",
            encryption_policy_sha256=DIGESTS[3],
            retention_policy_id="semantic-refresh-30d",
            retention_policy_sha256=DIGESTS[4],
            retention_days=30,
            retention_issued_at="2026-08-08T00:00:00Z",
            retention_until="2026-09-07T00:00:00Z",
            max_artifact_bytes=2_000_000_000,
        )


def test_artifact_authority_requires_full_retention_from_immutable_issuance() -> None:
    with pytest.raises(SemanticRefreshContractError, match="cover retention_days"):
        replace(
            _deployment_model().artifact_authority,
            retention_issued_at="2026-08-09T00:00:00Z",
        )


def _baseline_receipt() -> SemanticRefreshBaselineAdoptionReceipt:
    return SemanticRefreshBaselineAdoptionReceipt.build(
        model_unique_id=MODEL_ID,
        baseline_kind=BaselineAssuranceKind.ADOPTED_COMPLETE_RELATION_CONFORMANT,
        release_id=DIGESTS[0],
        deployment_id=DIGESTS[1],
        source_snapshot_sha256=DIGESTS[2],
        source_relation_id="source.analytics.events",
        source_generation=3,
        mssql_relation_id="DWH.mart.events",
        mssql_generation=4,
        clickhouse_relation_id="analytics.mart.events",
        clickhouse_generation=8,
        clickhouse_target_uuid="0198f11c-6956-74f2-984b-4cfcb1653b87",
        mssql_connection_authority_id="mssql-prod",
        mssql_target_authority_id="mssql://mssql-prod/DWH/mart.events",
        clickhouse_cluster_authority_id="clickhouse-prod",
        clickhouse_target_authority_id="clickhouse://clickhouse-prod/analytics/mart.events",
        source_schema_sha256=DIGESTS[3],
        mssql_schema_sha256=DIGESTS[4],
        clickhouse_schema_sha256=DIGESTS[5],
        source_key_sha256=DIGESTS[3],
        mssql_key_sha256=DIGESTS[4],
        clickhouse_key_sha256=DIGESTS[5],
        source_physical_sha256=DIGESTS[3],
        mssql_physical_sha256=DIGESTS[4],
        clickhouse_physical_sha256=DIGESTS[5],
        coverage_start="2026-08-01T00:00:00Z",
        coverage_end="2026-08-08T00:00:00Z",
        source_coverage_sha256=DIGESTS[3],
        mssql_coverage_sha256=DIGESTS[4],
        clickhouse_coverage_sha256=DIGESTS[5],
        source_assurance_sha256=DIGESTS[3],
        mssql_assurance_sha256=DIGESTS[4],
        clickhouse_assurance_sha256=DIGESTS[5],
        utc_assurance_sha256=DIGESTS[6],
        writer_assurance_sha256=DIGESTS[7],
        ddl_assurance_sha256=DIGESTS[8],
        certified_codec_mapping_sha256=DIGESTS[9],
        historical_clickhouse_internal_multiset_evidence_sha256=DIGESTS[10],
        adopted_at="2026-08-08T00:00:00Z",
    )


def _route_receipt() -> SemanticRefreshRouteLiveCertificationReceipt:
    return SemanticRefreshRouteLiveCertificationReceipt.build(
        coordinates=SemanticRefreshRouteCapabilityCoordinates(
            source_connector="mssql",
            source_connector_version="16.0.1000.6",
            sink_connector="clickhouse",
            sink_connector_version="25.7",
            load_strategy="semantic_refresh_v2",
            environment="prod",
            runtime_image_digest=DIGESTS[0],
            toolchain_sha256=DBT_SQLSERVER_1_12_CERTIFIED.sha256,
            capability_policy_sha256=DIGESTS[5],
            source_capability_sha256=DIGESTS[6],
            sink_capability_sha256=DIGESTS[7],
            route_policy_sha256=DIGESTS[8],
        ),
        status=LiveCertificationStatus.PASS,
        tested_at="2026-08-01T00:00:00Z",
        effective_from="2026-08-01T00:00:00Z",
        expires_at="2026-09-01T00:00:00Z",
        live_environment_sha256=DIGESTS[9],
        live_evidence_sha256=DIGESTS[10],
        failure_matrix_sha256=DIGESTS[11],
        certification_policy_sha256=DIGESTS[12],
        issuer_authority="dpone-release-certifier",
        issuer_attestation_sha256=DIGESTS[13],
        issuer_signature_sha256=DIGESTS[14],
    )


def _assurance_subject(
    kind: RuntimeAssuranceKind,
    pre_release: SemanticRefreshPreReleaseProofBundle,
    deployment: SemanticRefreshDeploymentModelAuthority,
) -> SemanticRefreshRuntimeAssuranceSubject:
    model = pre_release.models[0]
    return SemanticRefreshRuntimeAssuranceSubject(
        assurance_kind=kind,
        subject_type=(
            RuntimeAssuranceSubjectType.COLUMN
            if kind is RuntimeAssuranceKind.UTC_SEMANTICS
            else RuntimeAssuranceSubjectType.TARGET
        ),
        release_id=DIGESTS[0],
        deployment_id=DIGESTS[1],
        environment="prod",
        database="DWH",
        model_unique_id=MODEL_ID,
        mssql_target_authority_id=deployment.mssql_target_authority_id,
        model_definition_proof_sha256=model.model_definition_proof.model_definition_proof_sha256,
        effective_key_template_sha256=model.effective_key_template_sha256,
        writable_schema_sha256=model.writable_schema_sha256,
        sqlserver_lifecycle_policy_sha256=(pre_release.lifecycle_policy.sqlserver_lifecycle_policy_sha256),
        route_certification_receipt_sha256=_route_receipt().route_certification_receipt_sha256,
        mssql_control_database=deployment.mssql_control_database,
        mssql_control_schema=deployment.mssql_control_schema,
        mssql_image_schema=deployment.mssql_image_schema,
        scope_image_namespace_policy_sha256=deployment.scope_image_namespace_policy_sha256,
        column_name=model.event_time_column if kind is RuntimeAssuranceKind.UTC_SEMANTICS else None,
    )


def _runtime_assurances(
    pre_release: SemanticRefreshPreReleaseProofBundle | None = None,
    deployment: SemanticRefreshDeploymentModelAuthority | None = None,
) -> tuple[SemanticRefreshRuntimeAssuranceReceipt, ...]:
    pre_release = pre_release or _pre_release()
    deployment = deployment or _deployment_model()
    values = []
    kinds = [RuntimeAssuranceKind.DDL_FREEZE]
    if any(item.utc_assurance_required for item in pre_release.models[0].effective_key_templates):
        kinds.append(RuntimeAssuranceKind.UTC_SEMANTICS)
    kinds.append(RuntimeAssuranceKind.WRITER_EXCLUSIVITY)
    for index, kind in enumerate(kinds):
        kwargs: dict[str, object] = {
            "subject": _assurance_subject(kind, pre_release, deployment),
            "producer_version": "1.0.0",
            "transformation_version": "1.0.0",
            "acl_policy_version": "1.0.0",
            "runtime_assurance_policy_sha256": DIGESTS[5],
            "approver_authority": "dpone-control-plane",
            "approver_attestation_sha256": DIGESTS[6],
            "approver_signature_sha256": DIGESTS[7],
            "effective_from": "2026-08-01T00:00:00Z",
            "expires_at": "2026-09-01T00:00:00Z",
            "evidence_sha256": DIGESTS[8 + index],
        }
        if kind is RuntimeAssuranceKind.WRITER_EXCLUSIVITY:
            kwargs.update(
                engine_acl_proof_sha256=DIGESTS[9],
                platform_allowlist_sha256=DIGESTS[10],
                external_job_inventory_sha256=DIGESTS[11],
                organizational_control_sha256=DIGESTS[12],
            )
        elif kind is RuntimeAssuranceKind.DDL_FREEZE:
            kwargs["ddl_epoch"] = 7
        values.append(SemanticRefreshRuntimeAssuranceReceipt.build(**kwargs))
    return tuple(values)


class _AssuranceVerifier:
    def verify_route(self, *_args: object, **_kwargs: object) -> bool:
        return True

    def verify_runtime(self, *_args: object, **_kwargs: object) -> bool:
        return True


class _RecoveryVerifier:
    def verify(self, _authority: object) -> bool:
        return True


class _TemplateVerifier:
    def verify(self, _subject: object) -> bool:
        return True


class _AllowLocalActivation:
    def authorize(self, **_: object) -> None:
        return None


def _compile_kwargs(
    pre_release: SemanticRefreshPreReleaseProofBundle | None = None,
    deployment: SemanticRefreshDeploymentModelAuthority | None = None,
) -> dict[str, object]:
    pre_release = pre_release or _pre_release()
    deployment = deployment or _deployment_model()
    return {
        "pre_release": pre_release,
        "authority": _authority(),
        "workflow_id": "workflow-1",
        "scope_start": "2026-08-07T00:00:00Z",
        "scope_end": "2026-08-08T00:00:00Z",
        "deployment_models": (deployment,),
        "route_certification": _route_receipt(),
        "runtime_assurances": _runtime_assurances(pre_release, deployment),
        "verification_time": datetime(2026, 8, 8, tzinfo=UTC),
    }


def test_pre_release_bundle_cannot_claim_final_plan_identities() -> None:
    payload = _pre_release().to_dict()

    assert "release_id" not in payload
    assert "deployment_id" not in payload
    assert "operation_plans" not in payload
    assert "workflow_execution_binding" not in payload


def test_post_deployment_builder_requires_protected_exact_authority() -> None:
    compiler = SemanticRefreshPostDeploymentPlanCompiler(
        type("Verifier", (), {"verify": lambda *_: False})(),
        _AssuranceVerifier(),
    )

    with pytest.raises(SemanticRefreshContractError, match="not protected and exact"):
        compiler.compile(**_compile_kwargs())


def test_post_deployment_builder_emits_canonical_mssql_authority_handoff() -> None:
    verifier = _SubjectVerifier()
    compiler = SemanticRefreshPostDeploymentPlanCompiler(verifier, _AssuranceVerifier())

    bundle = compiler.compile(**_compile_kwargs())

    operation = bundle.operation_plans[0]
    assert operation.release_id == DIGESTS[0]
    assert operation.deployment_id == DIGESTS[1]
    assert not hasattr(bundle, "workflow_execution_binding")
    assert bundle.targets[0].baseline_receipt_sha256 == _baseline_receipt().baseline_adoption_receipt_sha256
    assert bundle.targets[0].baseline_receipt == _baseline_receipt()
    assert bundle.targets[0].to_dict()["baseline_receipt"]["baseline_kind"] == ("adopted_complete_relation_conformant")
    assert bundle.targets[0].baseline_status == "COMPLETE"
    assert bundle.targets[0].clickhouse_target_uuid == "0198f11c-6956-74f2-984b-4cfcb1653b87"
    assert bundle.targets[0].target_predecessor_generation == 8
    assert bundle.targets[0].target_predecessor_generation_id == operation.target_predecessor_generation_id
    assert bundle.targets[0].target_predecessor_operation_id == _baseline_receipt().baseline_adoption_receipt_sha256
    assert (
        bundle.targets[0].target_head_authority_receipt_sha256 == _baseline_receipt().baseline_adoption_receipt_sha256
    )
    assert bundle.targets[0].target_head_terminal_receipt_sha256 is None
    assert bundle.targets[0].clickhouse_target_authority_id == ("clickhouse://clickhouse-prod/analytics/mart.events")
    assert bundle.targets[0].mssql_target_authority_id == "mssql://mssql-prod/DWH/mart.events"
    assert bundle.targets[0].mssql_control_database == "DWH"
    assert bundle.targets[0].mssql_control_schema == "dpone_control"
    assert bundle.targets[0].mssql_image_schema == "dpone_scope_images"
    assert bundle.targets[0].scope_image_namespace_policy_sha256 == DIGESTS[10]
    assert bundle.targets[0].strategy_template_sha256.startswith("sha256:")
    assert "strategy_authority_sha256" not in bundle.targets[0].to_dict()
    assert bundle.targets[0].writable_columns[0].name == "event_date"
    assert bundle.targets[0].resource_policy_sha256 == _pre_release().resource_policy_digest
    assert bundle.targets[0].model_definition_proof_sha256 == operation.model_definition_proof_sha256
    assert bundle.targets[0].route_certification_receipt_sha256 == (_route_receipt().route_certification_receipt_sha256)
    assert bundle.targets[0].artifact_authority.provider == "s3"
    assert bundle.targets[0].publication_scope_id == "2026-08-07T00:00:00Z/2026-08-08T00:00:00Z"
    assert bundle.package_artifacts_sha256 == _pre_release().lifecycle_policy.package_artifacts_digest
    assert bundle.run_guard_closure.resource_guard_ids == ("DWH.mart.events",)
    assert bundle.run_guard_closure.workflow_guard_resource_id.startswith("workflow://sha256:")
    assert bundle.plan_bundle_sha256.startswith("sha256:")
    assert verifier.subject is not None
    assert verifier.subject.models[0].baseline_receipt == _baseline_receipt()
    assert verifier.subject.authority == _authority()

    with pytest.raises(SemanticRefreshContractError, match="plan bundle digest differs"):
        replace(bundle, package_artifacts_sha256=DIGESTS[11])


def test_complete_scope_replay_derives_revision_and_predecessor_from_protected_summary() -> None:
    predecessor = _plan_bundle()
    recovery = SemanticRefreshCompleteScopeReplayAuthority.build(
        predecessor_plan=predecessor,
        predecessor_summary=_complete_summary(predecessor),
        predecessor_workflow_execution_id="daily/complete-1",
        target_heads=(_replay_head(predecessor),),
    )
    compiler = SemanticRefreshPostDeploymentPlanCompiler(
        _SubjectVerifier(),
        _AssuranceVerifier(),
        _RecoveryVerifier(),
    )

    bundle = compiler.compile(**_compile_kwargs(), recovery_authority=recovery)
    operation = bundle.operation_plans[0]

    assert operation.operation_kind is WorkflowMode.COMPLETE_SCOPE_REPLAY
    assert operation.scope_revision == 2
    assert operation.scope_predecessor_operation_id == predecessor.operation_plans[0].operation_id
    assert operation.target_predecessor_generation_id == DIGESTS[14]
    assert operation.replaces_failed_operation_id is None
    assert bundle.targets[0].target_predecessor_generation == 9
    assert bundle.targets[0].clickhouse_target_uuid == "0198f11c-6956-74f2-984b-4cfcb1653b88"
    assert (
        bundle.targets[0].target_head_authority_receipt_sha256 == recovery.target_heads[0].head_authority_receipt_sha256
    )
    assert bundle.targets[0].target_head_terminal_receipt_sha256 == recovery.target_heads[0].terminal_receipt_sha256
    assert bundle.workflow_plan.workflow_mode is WorkflowMode.COMPLETE_SCOPE_REPLAY
    assert bundle.workflow_plan.replacement_action_ids == ()
    assert bundle.workflow_replacement_plan is None

    run = SemanticRefreshRunAdmissionCompiler(type("Verifier", (), {"verify": lambda *_: True})()).compile(
        plan_bundle=bundle,
        authority=SemanticRefreshRunAdmissionAuthority("daily/replay-1", bundle.plan_bundle_sha256, DIGESTS[11]),
    )
    binding = run.workflow_execution_binding
    assert binding.workflow_mode is WorkflowMode.COMPLETE_SCOPE_REPLAY
    assert binding.workflow_execution_id == "daily/replay-1"
    assert binding.workflow_replacement_plan_sha256 is None
    assert binding.recovery_plan_digest is None

    with pytest.raises(SemanticRefreshContractError, match="replay authority receipt differs"):
        replace(recovery, authority_receipt_sha256=DIGESTS[12])


def test_failed_precommit_replacement_derives_lineage_plan_and_run_binding() -> None:
    predecessor = _plan_bundle()
    summary = _failed_summary(predecessor, SqlServerModelOutcome.COMMITTED_WITH_IMAGES)
    recovery = SemanticRefreshFailedPrecommitReplacementAuthority.build(
        predecessor_plan=predecessor,
        predecessor_summary=summary,
        predecessor_workflow_execution_id="daily/failed-1",
        replacement_actions=(
            ReplacementActionBinding(
                MODEL_ID,
                SqlServerModelOutcome.COMMITTED_WITH_IMAGES,
                ReplacementAction.RESTORE_THEN_REBUILD,
            ),
        ),
    )
    compiler = SemanticRefreshPostDeploymentPlanCompiler(
        _SubjectVerifier(),
        _AssuranceVerifier(),
        _RecoveryVerifier(),
    )

    bundle = compiler.compile(**_compile_kwargs(), recovery_authority=recovery)
    operation = bundle.operation_plans[0]
    replacement = bundle.workflow_replacement_plan

    assert operation.operation_kind is WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT
    assert operation.replaces_failed_operation_id == predecessor.operation_plans[0].operation_id
    assert operation.replacement_reason == "failed_precommit_replacement"
    assert operation.replacement_ordinal == 1
    assert bundle.workflow_plan.replacement_action_ids == (MODEL_ID,)
    assert replacement is not None
    assert replacement.predecessor_workflow_summary_sha256 == summary.terminal_summary_sha256
    assert replacement.replacement_actions == recovery.replacement_actions

    with pytest.raises(SemanticRefreshContractError, match="replacement authority receipt differs"):
        replace(recovery, authority_receipt_sha256=DIGESTS[12])

    run = SemanticRefreshRunAdmissionCompiler(type("Verifier", (), {"verify": lambda *_: True})()).compile(
        plan_bundle=bundle,
        authority=SemanticRefreshRunAdmissionAuthority("daily/recovery-1", bundle.plan_bundle_sha256, DIGESTS[11]),
    )
    binding = run.workflow_execution_binding
    assert binding.workflow_mode is WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT
    assert binding.workflow_replacement_plan_sha256 == replacement.workflow_replacement_plan_sha256
    assert binding.recovery_plan_digest == replacement.recovery_plan_digest


def test_recovery_compile_rejects_absent_protected_verifier() -> None:
    predecessor = _plan_bundle()
    recovery = SemanticRefreshCompleteScopeReplayAuthority.build(
        predecessor_plan=predecessor,
        predecessor_summary=_complete_summary(predecessor),
        predecessor_workflow_execution_id="daily/complete-1",
        target_heads=(_replay_head(predecessor),),
    )

    with pytest.raises(SemanticRefreshContractError, match="recovery authority is not protected"):
        SemanticRefreshPostDeploymentPlanCompiler(_SubjectVerifier(), _AssuranceVerifier()).compile(
            **_compile_kwargs(),
            recovery_authority=recovery,
        )


def test_recovery_authority_accepts_protected_interleaved_target_head() -> None:
    predecessor = _plan_bundle()
    current_owner = DIGESTS[13]
    current_receipt = DIGESTS[12]
    authority = SemanticRefreshCompleteScopeReplayAuthority.build(
        predecessor_plan=predecessor,
        predecessor_summary=_complete_summary(predecessor),
        predecessor_workflow_execution_id="daily/complete-1",
        target_heads=(
            SemanticRefreshRecoveryTargetHead.build(
                model_unique_id=MODEL_ID,
                clickhouse_target_authority_id=predecessor.targets[0].clickhouse_target_authority_id,
                target_generation=10,
                target_generation_id=DIGESTS[14],
                target_uuid="0198f11c-6956-74f2-984b-4cfcb1653b88",
                owner_operation_id=current_owner,
                terminal_receipt_sha256=current_receipt,
            ),
        ),
    )

    assert authority.target_heads[0].target_generation == 10
    assert authority.target_heads[0].owner_operation_id == current_owner
    assert authority.target_heads[0].terminal_receipt_sha256 == current_receipt


def test_recovery_authority_rejects_tampered_head_and_logical_workflow_identity() -> None:
    predecessor = _plan_bundle()

    with pytest.raises(SemanticRefreshContractError, match="head authority receipt differs"):
        replace(
            _replay_head(predecessor),
            target_uuid="0198f11c-6956-74f2-984b-4cfcb1653b89",
        )

    summary = _failed_summary(predecessor, SqlServerModelOutcome.ROLLED_BACK)
    with pytest.raises(SemanticRefreshContractError, match="workflow execution identity differs"):
        SemanticRefreshFailedPrecommitReplacementAuthority.build(
            predecessor_plan=predecessor,
            predecessor_summary=summary,
            predecessor_workflow_execution_id="daily_events",
            replacement_actions=(
                ReplacementActionBinding(
                    MODEL_ID,
                    SqlServerModelOutcome.ROLLED_BACK,
                    ReplacementAction.BUILD_FRESH,
                ),
            ),
        )


def test_datetime_plan_requires_and_binds_exact_utc_assurance() -> None:
    pre_release = _pre_release("datetime2(6)")
    assurances = _runtime_assurances(pre_release)
    bundle = SemanticRefreshPostDeploymentPlanCompiler(
        type("Verifier", (), {"verify": lambda *_: True})(),
        _AssuranceVerifier(),
    ).compile(**_compile_kwargs(pre_release))
    utc = next(item for item in assurances if item.subject.assurance_kind is RuntimeAssuranceKind.UTC_SEMANTICS)

    assert bundle.operation_plans[0].effective_key_columns[0].utc_assurance_sha256 == (
        utc.runtime_assurance_receipt_sha256
    )
    assert bundle.targets[0].utc_semantics_assurance_receipt_sha256 == (utc.runtime_assurance_receipt_sha256)

    without_utc = tuple(
        item for item in assurances if item.subject.assurance_kind is not RuntimeAssuranceKind.UTC_SEMANTICS
    )
    with pytest.raises(SemanticRefreshContractError, match="assurance closure differs"):
        SemanticRefreshPostDeploymentPlanCompiler(
            type("Verifier", (), {"verify": lambda *_: True})(),
            _AssuranceVerifier(),
        ).compile(**{**_compile_kwargs(pre_release), "runtime_assurances": without_utc})


def test_post_deployment_builder_rejects_baseline_for_another_target() -> None:
    invalid = replace(
        _deployment_model(),
        target_resource_id="DWH.mart.other_events",
        mssql_target_authority_id="mssql://mssql-prod/DWH/mart.other_events",
    )
    compiler = SemanticRefreshPostDeploymentPlanCompiler(
        type("Verifier", (), {"verify": lambda *_: True})(),
        _AssuranceVerifier(),
    )

    with pytest.raises(SemanticRefreshContractError, match="baseline receipt differs"):
        compiler.compile(**{**_compile_kwargs(), "deployment_models": (invalid,)})


def test_run_binding_requires_protected_logical_workflow_execution_id() -> None:
    plan = _plan_bundle()
    authority = SemanticRefreshRunAdmissionAuthority(
        "DAG__analytics__daily__refresh/run-2026-08-08",
        plan.plan_bundle_sha256,
        DIGESTS[11],
    )
    compiler = SemanticRefreshRunAdmissionCompiler(type("Verifier", (), {"verify": lambda *_: False})())

    with pytest.raises(SemanticRefreshContractError, match="not protected and exact"):
        compiler.compile(plan_bundle=plan, authority=authority)


def test_new_logical_dagrun_produces_a_distinct_execution_binding() -> None:
    plan = _plan_bundle()
    compiler = SemanticRefreshRunAdmissionCompiler(type("Verifier", (), {"verify": lambda *_: True})())
    first = compiler.compile(
        plan_bundle=plan,
        authority=SemanticRefreshRunAdmissionAuthority("daily/run-1", plan.plan_bundle_sha256, DIGESTS[11]),
    )
    second = compiler.compile(
        plan_bundle=plan,
        authority=SemanticRefreshRunAdmissionAuthority("daily/run-2", plan.plan_bundle_sha256, DIGESTS[12]),
    )

    assert first.workflow_execution_binding.workflow_execution_id == "daily/run-1"
    assert second.workflow_execution_binding.workflow_execution_id == "daily/run-2"
    assert (
        first.workflow_execution_binding.workflow_execution_binding_sha256
        != second.workflow_execution_binding.workflow_execution_binding_sha256
    )
    assert not hasattr(first, "attempt_binding")


def test_plan_bundle_codec_reconstructs_exact_closed_authority() -> None:
    plan = _plan_bundle()

    decoded = semantic_refresh_plan_bundle_from_mapping(plan.to_dict())

    assert decoded == plan
    assert decoded.to_dict() == plan.to_dict()


@pytest.mark.parametrize(
    ("path", "value"),
    (
        (("unknown",), True),
        (("plan_bundle_sha256",), DIGESTS[0]),
        (("release_deployment_authority", "release_id"), DIGESTS[3]),
        (("targets", 0, "resource_policy", "max_source_scope_rows"), 0),
        (("targets", 0, "artifact_authority", "unknown"), "caller"),
        (("run_guard_closure", "resource_guard_ids"), ["caller-guard"]),
    ),
)
def test_plan_bundle_codec_rejects_unknown_or_tampered_nested_authority(
    path: tuple[str | int, ...],
    value: object,
) -> None:
    payload = _plan_bundle().to_dict()
    cursor: object = payload
    for segment in path[:-1]:
        cursor = cursor[segment]  # type: ignore[index]
    cursor[path[-1]] = value  # type: ignore[index]

    with pytest.raises((SemanticRefreshContractError, DbtPublishingError)):
        semantic_refresh_plan_bundle_from_mapping(payload)


def test_protected_activation_materializes_only_run_bound_semantic_pack() -> None:
    plan = _plan_bundle()
    run = _run_bundle(plan)
    template = _template_pack()

    activated = semantic_refresh_activated_pack(
        template_pack=template,
        plan_bundle=plan,
        run_execution=run,
        authority_receipt=_activation_receipt(plan),
    )

    payload = activated.to_dict()
    assert verify_pack_fingerprint(payload) == payload["pack_fingerprint"]
    assert payload["activation"] == "PROTECTED_RUN_AUTHORITY_BOUND"
    assert payload["executable"] is True
    assert payload["semantic_refresh"]["mode"] == "semantic_refresh_v2_activated"
    assert payload["semantic_refresh"]["workflow_execution_id"] == "daily/run-1"
    assert payload["semantic_refresh"]["package_artifacts_sha256"] == plan.package_artifacts_sha256
    assert payload["semantic_refresh"]["pre_release_bundle_sha256"] == plan.pre_release_bundle_sha256
    assert payload["semantic_refresh"]["task_projection"]["schema"] == ("dpone.semantic-refresh-task-projection.v1")
    assert payload["semantic_refresh"]["activation_authority"]["baseline_receipts"] == [
        [MODEL_ID, _baseline_receipt().baseline_adoption_receipt_sha256]
    ]
    assert "provider_execution" not in payload
    assert "kpo_kwargs" not in payload
    assert "mode: replace" not in str(payload)


def test_template_or_mismatched_protected_receipt_cannot_activate() -> None:
    plan = _plan_bundle()
    run = _run_bundle(plan)
    template = _template_pack()
    tampered = {**template, "executable": True}

    with pytest.raises(ValueError, match="fingerprint"):
        semantic_refresh_activated_pack(
            template_pack=tampered,
            plan_bundle=plan,
            run_execution=run,
            authority_receipt=_activation_receipt(plan),
        )

    other_release = _template_pack()
    other_semantic = dict(other_release["semantic_refresh"])
    other_semantic["pre_release_bundle_sha256"] = DIGESTS[12]
    other_release = {**other_release, "semantic_refresh": other_semantic}
    other_release["pack_fingerprint"] = compute_pack_fingerprint(other_release)
    with pytest.raises(ValueError, match="template/plan closure"):
        semantic_refresh_activated_pack(
            template_pack=other_release,
            plan_bundle=plan,
            run_execution=run,
            authority_receipt=_activation_receipt(plan),
        )


def test_activation_service_rejects_template_outside_protected_release() -> None:
    plan = _plan_bundle()
    store = _RecordingAuthorityStore()
    verifier = type("RejectTemplate", (), {"verify": lambda *_: False})()

    with pytest.raises(ValueError, match="not protected by the exact release"):
        SemanticRefreshDeploymentAuthorityService(
            store,
            verifier,
            _AllowLocalActivation(),
        ).persist_deployment_authorities(
            template_pack=_template_pack(),
            plan_bundle=plan,
            route_certification=_route_receipt(),
            runtime_assurances=_runtime_assurances(),
            persisted_at="2026-08-08T00:00:00Z",
        )
    assert store.authority is None


def test_deployment_service_two_argument_api_defaults_to_preview_block() -> None:
    plan = _plan_bundle()
    store = _RecordingAuthorityStore()
    service = SemanticRefreshDeploymentAuthorityService(store, _TemplateVerifier())

    with pytest.raises(
        SemanticRefreshProductionActivationUnavailableError,
        match="DPONE_SEMANTIC_REFRESH_PRODUCTION_ACTIVATION_UNAVAILABLE",
    ):
        service.persist_deployment_authorities(
            template_pack=_template_pack(),
            plan_bundle=plan,
            route_certification=_route_receipt(),
            runtime_assurances=_runtime_assurances(),
            persisted_at="2026-08-08T00:00:00Z",
        )

    assert store.authority is None


def test_deployment_service_persists_full_receipts_without_future_run() -> None:
    plan = _plan_bundle()
    store = _RecordingAuthorityStore()

    receipt = SemanticRefreshDeploymentAuthorityService(
        store,
        _TemplateVerifier(),
        _AllowLocalActivation(),
    ).persist_deployment_authorities(
        template_pack=_template_pack(),
        plan_bundle=plan,
        route_certification=_route_receipt(),
        runtime_assurances=_runtime_assurances(),
        persisted_at="2026-08-08T00:00:00Z",
    )

    assert receipt.plan_bundle_sha256 == plan.plan_bundle_sha256
    assert not hasattr(receipt, "workflow_execution_id")
    assert store.authority is not None
    assert store.authority.baselines == (_baseline_receipt(),)
    assert store.authority.route_certification == _route_receipt()
    assert store.authority.runtime_assurances == _runtime_assurances()


def test_deployment_authority_plan_freezes_persisted_at_for_ack_loss_replay() -> None:
    plan = _plan_bundle()
    store = _RecordingAuthorityStore()
    service = SemanticRefreshDeploymentAuthorityService(
        store,
        _TemplateVerifier(),
        _AllowLocalActivation(),
    )
    authority = service.plan_deployment_authorities(
        template_pack=_template_pack(),
        plan_bundle=plan,
        route_certification=_route_receipt(),
        runtime_assurances=_runtime_assurances(),
        persisted_at="2026-08-08T00:00:00Z",
    )

    first = service.persist_planned_deployment_authorities(
        template_pack=_template_pack(),
        plan_bundle=plan,
        authority=authority,
    )
    replay = service.persist_planned_deployment_authorities(
        template_pack=_template_pack(),
        plan_bundle=plan,
        authority=authority,
    )

    assert first == replay
    assert authority.persisted_at == "2026-08-08T00:00:00Z"
    assert store.authorities == [authority, authority]

    with pytest.raises(ValueError, match="full protected authority differs"):
        service.persist_planned_deployment_authorities(
            template_pack=_template_pack(),
            plan_bundle=plan,
            authority=replace(authority, plan_bundle_sha256=DIGESTS[14]),
        )
    assert store.authorities == [authority, authority]


def test_activation_authority_rejects_timestamp_precision_that_mssql_cannot_replay() -> None:
    plan = _plan_bundle()
    authority = SemanticRefreshDeploymentAuthorityService(
        _RecordingAuthorityStore(),
        _TemplateVerifier(),
        _AllowLocalActivation(),
    ).plan_deployment_authorities(
        template_pack=_template_pack(),
        plan_bundle=plan,
        route_certification=_route_receipt(),
        runtime_assurances=_runtime_assurances(),
        persisted_at="2026-08-08T00:00:00.123456Z",
    )
    assert authority.persisted_at == "2026-08-08T00:00:00.123456Z"

    for invalid in (
        "2026-08-08T00:00:00.1234567Z",
        "2026-08-08T00:00:00,1234567Z",
        "2026-08-08T00:00:00,123456Z",
        "20260808T000000Z",
        "2026-W32-6T00:00:00Z",
        "2026-08-08 00:00:00Z",
        "2026-08-08T00Z",
    ):
        with pytest.raises(SemanticRefreshContractError, match=r"datetime2\(6\)"):
            replace(authority, persisted_at=invalid)

    receipt = _activation_receipt(plan)
    with pytest.raises(SemanticRefreshContractError, match=r"datetime2\(6\)"):
        SemanticRefreshActivationAuthorityReceipt.build(
            release_id=receipt.release_id,
            deployment_id=receipt.deployment_id,
            plan_bundle_sha256=receipt.plan_bundle_sha256,
            authority_store_ref=receipt.authority_store_ref,
            baseline_receipts=receipt.baseline_receipts,
            route_certification_receipt_sha256=(receipt.route_certification_receipt_sha256),
            runtime_assurance_receipts=receipt.runtime_assurance_receipts,
            persisted_at="2026-08-08T00:00:00.9999999Z",
        )


def test_worker_resolves_exact_deployment_receipt_without_static_run_identity() -> None:
    plan = _plan_bundle()
    loader = _DeploymentAuthorityLoader(_activation_receipt(plan))

    receipt = load_exact_deployment_authority_receipt(loader, plan_bundle=plan)

    authority = plan.release_deployment_authority
    assert loader.calls == [(authority.release_id, authority.deployment_id, plan.plan_bundle_sha256)]
    assert receipt == _activation_receipt(plan)

    mismatched = SemanticRefreshActivationAuthorityReceipt.build(
        release_id=receipt.release_id,
        deployment_id=receipt.deployment_id,
        plan_bundle_sha256=DIGESTS[14],
        authority_store_ref=receipt.authority_store_ref,
        baseline_receipts=receipt.baseline_receipts,
        route_certification_receipt_sha256=(receipt.route_certification_receipt_sha256),
        runtime_assurance_receipts=receipt.runtime_assurance_receipts,
        persisted_at=receipt.persisted_at,
    )
    with pytest.raises(ValueError, match="differs from the exact plan"):
        load_exact_deployment_authority_receipt(
            _DeploymentAuthorityLoader(mismatched),
            plan_bundle=plan,
        )


def test_activation_service_rejects_absent_or_conflicting_store_ack() -> None:
    plan = _plan_bundle()
    kwargs = {
        "template_pack": _template_pack(),
        "plan_bundle": plan,
        "route_certification": _route_receipt(),
        "runtime_assurances": _runtime_assurances(),
        "persisted_at": "2026-08-08T00:00:00Z",
    }

    with pytest.raises(ValueError, match="acknowledgement differs"):
        SemanticRefreshDeploymentAuthorityService(
            _AbsentAuthorityStore(),
            _TemplateVerifier(),
            _AllowLocalActivation(),
        ).persist_deployment_authorities(**kwargs)
    with pytest.raises(ValueError, match="acknowledgement differs"):
        SemanticRefreshDeploymentAuthorityService(
            _ConflictingAuthorityStore(),
            _TemplateVerifier(),
            _AllowLocalActivation(),
        ).persist_deployment_authorities(**kwargs)

    untouched_store = _RecordingAuthorityStore()
    with pytest.raises(ValueError, match="fingerprint"):
        SemanticRefreshDeploymentAuthorityService(
            untouched_store,
            _TemplateVerifier(),
            _AllowLocalActivation(),
        ).persist_deployment_authorities(**{**kwargs, "template_pack": {**_template_pack(), "executable": True}})
    assert untouched_store.authority is None

    mismatched = SemanticRefreshActivationAuthorityReceipt.build(
        release_id=plan.release_deployment_authority.release_id,
        deployment_id=plan.release_deployment_authority.deployment_id,
        plan_bundle_sha256=plan.plan_bundle_sha256,
        authority_store_ref="mssql-control://prod/dpone_control",
        baseline_receipts=((MODEL_ID, DIGESTS[10]),),
        route_certification_receipt_sha256=_route_receipt().route_certification_receipt_sha256,
        runtime_assurance_receipts=(
            (MODEL_ID, "ddl_freeze", DIGESTS[3]),
            (MODEL_ID, "writer_exclusivity", DIGESTS[7]),
        ),
        persisted_at="2026-08-08T00:00:00Z",
    )
    with pytest.raises(ValueError, match="authority differs"):
        semantic_refresh_activated_pack(
            template_pack=_template_pack(),
            plan_bundle=plan,
            run_execution=_run_bundle(plan),
            authority_receipt=mismatched,
        )


def _complete_summary(plan: SemanticRefreshPlanBundle) -> SemanticRefreshDurableWorkflowSummary:
    operation = plan.operation_plans[0]
    binding_sha256 = DIGESTS[2]
    return SemanticRefreshDurableWorkflowSummary.build(
        workflow_execution_id="daily/complete-1",
        workflow_plan_sha256=plan.workflow_plan.workflow_plan_sha256,
        workflow_execution_binding_sha256=binding_sha256,
        expected_operation_ids=(operation.operation_id,),
        publications=(
            SemanticRefreshDurableModelPublication(
                operation_id=operation.operation_id,
                operation_plan_sha256=operation.operation_plan_sha256,
                workflow_execution_binding_sha256=binding_sha256,
                attempt_binding_sha256=DIGESTS[3],
                artifact_manifest_sha256=DIGESTS[4],
                clickhouse_terminal_receipt_sha256=DIGESTS[5],
                terminal_receipt_sha256=DIGESTS[6],
                target_generation=9,
                scope_revision=operation.scope_revision,
            ),
        ),
    )


def _failed_summary(
    plan: SemanticRefreshPlanBundle,
    outcome: SqlServerModelOutcome,
) -> SemanticRefreshFailedWorkflowSummary:
    operation = plan.operation_plans[0]
    return SemanticRefreshFailedWorkflowSummary.build(
        workflow_id="daily/failed-1",
        workflow_plan_sha256=plan.workflow_plan.workflow_plan_sha256,
        workflow_execution_binding_sha256=DIGESTS[2],
        expected_operation_ids=(operation.operation_id,),
        models=(FailedModelOutcome(operation.operation_id, DIGESTS[3], outcome, DIGESTS[4]),),
    )


def _replay_head(plan: SemanticRefreshPlanBundle) -> SemanticRefreshRecoveryTargetHead:
    operation = plan.operation_plans[0]
    return SemanticRefreshRecoveryTargetHead.build(
        model_unique_id=operation.model_unique_id,
        clickhouse_target_authority_id=plan.targets[0].clickhouse_target_authority_id,
        target_generation=9,
        target_generation_id=DIGESTS[14],
        target_uuid="0198f11c-6956-74f2-984b-4cfcb1653b88",
        owner_operation_id=operation.operation_id,
        terminal_receipt_sha256=_complete_summary(plan).publications[0].terminal_receipt_sha256,
    )


def _plan_bundle() -> SemanticRefreshPlanBundle:
    compiler = SemanticRefreshPostDeploymentPlanCompiler(
        type("Verifier", (), {"verify": lambda *_: True})(),
        _AssuranceVerifier(),
    )
    return compiler.compile(**_compile_kwargs())


def _run_bundle(plan: SemanticRefreshPlanBundle):
    return SemanticRefreshRunAdmissionCompiler(type("Verifier", (), {"verify": lambda *_: True})()).compile(
        plan_bundle=plan,
        authority=SemanticRefreshRunAdmissionAuthority("daily/run-1", plan.plan_bundle_sha256, DIGESTS[11]),
    )


def _activation_receipt(plan: SemanticRefreshPlanBundle) -> SemanticRefreshActivationAuthorityReceipt:
    return SemanticRefreshActivationAuthorityReceipt.build(
        release_id=plan.release_deployment_authority.release_id,
        deployment_id=plan.release_deployment_authority.deployment_id,
        plan_bundle_sha256=plan.plan_bundle_sha256,
        authority_store_ref="mssql-control://prod/dpone_control",
        baseline_receipts=((MODEL_ID, _baseline_receipt().baseline_adoption_receipt_sha256),),
        route_certification_receipt_sha256=_route_receipt().route_certification_receipt_sha256,
        runtime_assurance_receipts=tuple(
            sorted(
                (
                    item.subject.model_unique_id,
                    item.subject.assurance_kind.value,
                    item.runtime_assurance_receipt_sha256,
                )
                for item in _runtime_assurances()
            )
        ),
        persisted_at="2026-08-08T00:00:00Z",
    )


def _template_pack() -> dict[str, object]:
    topology_unsigned: dict[str, object] = {
        "activation": "POST_DEPLOYMENT_AUTHORITY_REQUIRED",
        "dag_id": "DAG__analytics__daily_events",
        "dag_policy": {
            "catchup": False,
            "max_active_runs": 1,
            "max_active_tasks": 4,
            "owner": "data-platform",
            "schedule": "0 2 * * *",
            "start_date": "2026-01-01",
            "tags": ["dbt", "dpone", "semantic-refresh-v2"],
            "timezone": "UTC",
        },
        "dependencies": {MODEL_ID: []},
        "logical_output_asset_uris": {MODEL_ID: "clickhouse://analytics/mart.events"},
        "model_unique_ids": [MODEL_ID],
        "profile_sha256": DIGESTS[5],
        "project_config_overlay": {},
        "schema": "dpone.dbt-semantic-refresh-topology-template.v1",
        "workflow_name": "daily_events",
    }
    topology = {**topology_unsigned, "topology_sha256": canonical_fingerprint(topology_unsigned)}
    return semantic_refresh_template_pack(
        workflow_id="daily_events",
        execution_pack=_execution_pack(),
        runtime_payload_ids=("dbt_project", "dbt_manifest", "dbt_selection_daily_events"),
        topology=topology,
        proof_authority=SemanticRefreshTemplateProofAuthority.from_pre_release(_pre_release()),
    )


def _execution_pack() -> DbtExecutionPack:
    invocation = DbtInvocationContext.canonical()
    lock = DbtSelectionLock.build(
        manifest_sha256=DIGESTS[0],
        toolchain_sha256=DBT_SQLSERVER_1_12_CERTIFIED.sha256,
        invocation_context_sha256=invocation.invocation_context_sha256,
        graph_contract_sha256=DIGESTS[1],
        graph_policy_id=DBT_SQLSERVER_GRAPH_POLICY_ID,
        graph_policy_sha256=DBT_SQLSERVER_GRAPH_POLICY_SHA256,
        selectors=("fqn:project.events",),
        selected_graph_unique_ids=(MODEL_ID,),
        expected_run_result_unique_ids=(MODEL_ID,),
        publish_model_unique_ids=(MODEL_ID,),
    )
    return DbtExecutionPack.build(
        workflow_id="daily_events",
        project_bundle_sha256=DIGESTS[0],
        project_subdir="dbt-project",
        target_path="target",
        profile=DbtProfileSpec(
            profile_name="dpone_runtime",
            target_name="runtime",
            connection_ref="mssql_prod",
            adapter_type="sqlserver",
            database="DWH",
            schema="mart",
            threads=4,
        ),
        selection_lock=lock,
        invocation_context=invocation,
        adapter_runtime=DbtSqlServerRuntimePolicy.for_process_timeout(900),
        adapter_policy=DbtSqlServerAdapterPolicy.canonical(),
        dbt_warning_policy="fail",
        timeout_seconds=900,
    )


class _SubjectVerifier:
    subject: SemanticRefreshDeploymentAuthoritySubject | None = None

    def verify(self, subject: SemanticRefreshDeploymentAuthoritySubject) -> bool:
        self.subject = subject
        return True


class _DeploymentAuthorityLoader:
    def __init__(self, receipt: SemanticRefreshActivationAuthorityReceipt) -> None:
        self.receipt = receipt
        self.calls: list[tuple[str, str, str]] = []

    def load_exact(
        self,
        *,
        release_id: str,
        deployment_id: str,
        plan_bundle_sha256: str,
    ) -> SemanticRefreshActivationAuthorityReceipt:
        self.calls.append((release_id, deployment_id, plan_bundle_sha256))
        return self.receipt


class _RecordingAuthorityStore:
    def __init__(self) -> None:
        self.authority: SemanticRefreshActivationAuthoritySet | None = None
        self.authorities: list[SemanticRefreshActivationAuthoritySet] = []

    def persist_exact(
        self, authority: SemanticRefreshActivationAuthoritySet
    ) -> SemanticRefreshActivationAuthorityReceipt:
        self.authority = authority
        self.authorities.append(authority)
        return _stored_receipt(authority)


class _AbsentAuthorityStore:
    def persist_exact(self, _authority: SemanticRefreshActivationAuthoritySet) -> None:
        return None


class _ConflictingAuthorityStore:
    def persist_exact(
        self, authority: SemanticRefreshActivationAuthoritySet
    ) -> SemanticRefreshActivationAuthorityReceipt:
        return SemanticRefreshActivationAuthorityReceipt.build(
            release_id=authority.release_id,
            deployment_id=authority.deployment_id,
            plan_bundle_sha256=authority.plan_bundle_sha256,
            authority_store_ref="mssql-control://prod/dpone_control",
            baseline_receipts=((MODEL_ID, DIGESTS[10]),),
            route_certification_receipt_sha256=(authority.route_certification.route_certification_receipt_sha256),
            runtime_assurance_receipts=authority.runtime_assurance_receipts,
            persisted_at=authority.persisted_at,
        )


def _stored_receipt(
    authority: SemanticRefreshActivationAuthoritySet,
) -> SemanticRefreshActivationAuthorityReceipt:
    return SemanticRefreshActivationAuthorityReceipt.build(
        release_id=authority.release_id,
        deployment_id=authority.deployment_id,
        plan_bundle_sha256=authority.plan_bundle_sha256,
        authority_store_ref="mssql-control://prod/dpone_control",
        baseline_receipts=authority.baseline_receipts,
        route_certification_receipt_sha256=(authority.route_certification.route_certification_receipt_sha256),
        runtime_assurance_receipts=authority.runtime_assurance_receipts,
        persisted_at=authority.persisted_at,
    )
