"""Opt-in local MSSQL -> MinIO/KES -> ClickHouse -> MSSQL proof.

The test uses the production composition root and concrete infrastructure
adapters.  Compiler bootstrap verifiers and the seal-policy signer are bounded
test authorities; their Vault/Kubernetes implementations are exercised by the
separate local Vault profile.  This is local evidence, never production route
certification.
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import fields, replace
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from dpone.adapters.semantic_refresh_artifact_s3_resolver import (
    S3ArtifactStorePolicy,
    S3CreateOnlyArtifactStoreResolver,
)
from dpone.adapters.semantic_refresh_mssql_activation import (
    MssqlSemanticRefreshActivationStore,
)
from dpone.adapters.semantic_refresh_mssql_activation_authority import (
    MssqlSemanticRefreshActivationAuthorityStore,
)
from dpone.adapters.semantic_refresh_mssql_authority import (
    MssqlSemanticRefreshCanonicalAuthorityLoader,
)
from dpone.adapters.semantic_refresh_mssql_schema import (
    MssqlSemanticRefreshSchemaMigration,
)
from dpone.adapters.semantic_refresh_mssql_state import (
    MssqlSemanticRefreshStateAdapter,
)
from dpone.adapters.semantic_refresh_mssql_workflow_publication import (
    MssqlSemanticRefreshWorkflowPublicationReader,
)
from dpone.adapters.semantic_refresh_mssql_workflow_summary import (
    MssqlSemanticRefreshWorkflowSummaryState,
)
from dpone.app.semantic_refresh_composition import (
    build_semantic_refresh_publication_runtime,
)
from dpone.contracts.dbt_semantic_refresh_activation import (
    SemanticRefreshActivationAuthoritySet,
)
from dpone.contracts.dbt_semantic_refresh_plan_compiler import (
    SemanticRefreshPostDeploymentPlanCompiler,
)
from dpone.contracts.dbt_semantic_refresh_plan_contracts import (
    SemanticRefreshArtifactAuthority,
    SemanticRefreshDeploymentAuthoritySubject,
)
from dpone.contracts.dbt_semantic_refresh_run_contracts import (
    SemanticRefreshRunAdmissionAuthority,
    SemanticRefreshRunAdmissionCompiler,
)
from dpone.contracts.semantic_refresh_attempt_binding import (
    SemanticRefreshAttemptBinding,
)
from dpone.contracts.semantic_refresh_baseline_receipt import (
    SemanticRefreshBaselineAdoptionReceipt,
    semantic_refresh_target_predecessor_generation_id,
)
from dpone.contracts.semantic_refresh_workflow_summary import (
    SemanticRefreshDurableModelPublication,
    SemanticRefreshDurableWorkflowSummary,
)
from dpone.ports.semantic_refresh_clickhouse_authority import (
    clickhouse_operation_table_names,
)
from dpone.ports.semantic_refresh_mssql import MssqlBuildReceiptEvidence, MssqlGuardClaim
from dpone.ports.semantic_refresh_mssql_admission_authority import compose_admission
from dpone.ports.semantic_refresh_mssql_worker_admission import (
    mssql_workflow_resource_budget,
)
from dpone.services.semantic_refresh_mssql_activation import (
    SemanticRefreshMssqlActivationService,
)
from dpone.services.semantic_refresh_mssql_authority import (
    MssqlCanonicalAdmissionBundle,
    SemanticRefreshMssqlAuthorityAdmissionService,
    mssql_model_resource_authority_from_plan_target,
)
from tests.test_dbt_semantic_refresh_plan_compiler import (
    _AssuranceVerifier,
    _authority,
    _baseline_receipt,
    _compile_kwargs,
    _deployment_model,
    _pre_release,
    _route_receipt,
    _runtime_assurances,
)
from tests.test_semantic_refresh_clickhouse_local_integration import (
    _LOCAL_CLUSTER_AUTHORITY_ID,
    _LOCAL_KMS_KEY_ARN,
    _ch,
    _clickhouse_client,
    _local_connection_authority,
    _s3_client,
    _table_uuid,
)
from tests.test_semantic_refresh_mssql_live import (
    _connect_factory,
    _drop_named_schema,
    _execute,
    _execute_params,
    _live_activated_pack,
    _live_image_sha256,
    _LocalSealPolicyAuthority,
    _scalar,
    _strategy_relation,
)


class _AllowLocalActivation:
    def authorize(self, **_: object) -> None:
        return None


pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_mssql,
    pytest.mark.integration_clickhouse,
]

_ENABLED = os.getenv("DPONE_RUN_SEMANTIC_REFRESH_E2E_LIVE") == "1"
_CONTROL_SCHEMA = "dpone_sr_e2e"
_DATABASE = "analytics"
_BUCKET = "dpone-semref-v2-live"
_UTC = timezone.utc  # noqa: UP017 - package type-checks against Python 3.10


@pytest.mark.skipif(
    not _ENABLED,
    reason="Set DPONE_RUN_SEMANTIC_REFRESH_E2E_LIVE=1 for the local cross-provider proof",
)
def test_local_cross_provider_semantic_refresh_completes_durable_summary() -> None:
    suffix = uuid.uuid4().hex[:12]
    target_table = f"mart.events_e2e_{suffix}"
    artifact_prefix = f"semantic-refresh/e2e/{suffix}"
    workflow_id = f"daily-events-e2e-{suffix}"
    workflow_execution_id = f"local-e2e/{suffix}"
    now = datetime.now(_UTC).replace(microsecond=0)
    retention_until = (now + timedelta(days=2)).isoformat().replace("+00:00", "Z")
    factory = _connect_factory()
    setup = factory()
    setup.autocommit = True
    created_dwh = False
    operation_id: str | None = None
    try:
        created_dwh = _scalar(setup, "SELECT DB_ID(N'DWH')") is None
        if created_dwh:
            _execute(setup, "EXEC(N'CREATE DATABASE [DWH]');")
        _drop_named_schema(setup, _CONTROL_SCHEMA)
        MssqlSemanticRefreshSchemaMigration(factory, control_schema=_CONTROL_SCHEMA).apply()

        _ch(f"CREATE DATABASE IF NOT EXISTS `{_DATABASE}` ENGINE = Atomic")
        _ch(f"DROP TABLE IF EXISTS `{_DATABASE}`.`{target_table}`")
        _ch(
            f"CREATE TABLE `{_DATABASE}`.`{target_table}` ("
            "event_date Date, event_id Int64, payload Nullable(String)"
            ") ENGINE = MergeTree ORDER BY (event_date, event_id)"
        )
        _ch(f"INSERT INTO `{_DATABASE}`.`{target_table}` VALUES ('2026-08-07', 1, 'before')")
        target_uuid = _table_uuid(_DATABASE, target_table)

        baseline = _local_baseline(target_table=target_table, target_uuid=target_uuid)
        artifact_authority = SemanticRefreshArtifactAuthority(
            provider="s3",
            provider_profile="MINIO_LOCAL_UNVERIFIED",
            endpoint_authority_id=os.getenv(
                "DPONE_IT_MINIO_ENDPOINT",
                "http://127.0.0.1:59290",
            ),
            bucket_or_container_authority_id=os.getenv("DPONE_IT_MINIO_BUCKET", _BUCKET),
            kms_key_authority_id=_LOCAL_KMS_KEY_ARN,
            capability_evidence_sha256=_digest("c"),
            writer_scope=f"local-e2e/{suffix}",
            artifact_prefix=artifact_prefix,
            encryption_policy_sha256=_digest("d"),
            retention_policy_id="local-compliance-2d",
            retention_policy_sha256=_digest("e"),
            retention_days=1,
            retention_issued_at=now.isoformat().replace("+00:00", "Z"),
            retention_until=retention_until,
            max_artifact_bytes=1_000_000,
        )
        deployment = replace(
            _deployment_model(),
            target_predecessor_generation_id=(semantic_refresh_target_predecessor_generation_id(baseline)),
            clickhouse_cluster_authority_id=_LOCAL_CLUSTER_AUTHORITY_ID,
            clickhouse_target_authority_id=(f"clickhouse://{_LOCAL_CLUSTER_AUTHORITY_ID}/{_DATABASE}/{target_table}"),
            publication_database=_DATABASE,
            publication_target_table=target_table,
            baseline_receipt=baseline,
            artifact_authority=artifact_authority,
        )
        pre_release = _pre_release()
        assurances = _runtime_assurances(pre_release, deployment)
        plan = SemanticRefreshPostDeploymentPlanCompiler(
            type("ExactLocalDeploymentVerifier", (), {"verify": lambda *_: True})(),
            _AssuranceVerifier(),
        ).compile(
            **{
                **_compile_kwargs(pre_release, deployment),
                "workflow_id": workflow_id,
                "runtime_assurances": assurances,
            }
        )
        run = SemanticRefreshRunAdmissionCompiler(
            type("ExactLocalRunVerifier", (), {"verify": lambda *_: True})()
        ).compile(
            plan_bundle=plan,
            authority=SemanticRefreshRunAdmissionAuthority(
                workflow_execution_id,
                plan.plan_bundle_sha256,
                _digest("f"),
            ),
        )
        bundle = _admission_bundle(plan, run, suffix=suffix)
        operation = bundle.operation_plans[0]
        operation_id = operation.operation_id

        activation_authorities = MssqlSemanticRefreshActivationAuthorityStore(
            factory,
            authority_store_ref=f"mssql://local/{_CONTROL_SCHEMA}/activation",
            control_schema=_CONTROL_SCHEMA,
        )
        activation_receipt = activation_authorities.persist_exact(
            SemanticRefreshActivationAuthoritySet(
                release_id=plan.release_deployment_authority.release_id,
                deployment_id=plan.release_deployment_authority.deployment_id,
                plan_bundle_sha256=plan.plan_bundle_sha256,
                baselines=(baseline,),
                route_certification=_route_receipt(),
                runtime_assurances=assurances,
                persisted_at=now.isoformat().replace("+00:00", "Z"),
            )
        )
        activation_store = MssqlSemanticRefreshActivationStore(
            factory,
            control_schema=_CONTROL_SCHEMA,
        )
        activation = SemanticRefreshMssqlActivationService(
            activation=activation_store,
            deployment_verifier=type(
                "ExactLocalDeploymentVerifier",
                (),
                {"verify": lambda *_: True},
            )(),
            activation_guard=_AllowLocalActivation(),
        )
        subject = SemanticRefreshDeploymentAuthoritySubject.build(
            _authority(),
            (deployment,),
        )
        activation.activate_deployment(subject=subject, plan_bundle=plan)
        activation.register_run(bundle)
        activation_store.register_activated_pack(
            _live_activated_pack(
                plan=plan,
                activation_authority_receipt_sha256=(activation_receipt.activation_authority_receipt_sha256),
                authority_store_ref=activation_receipt.authority_store_ref,
                workflow_execution_id=bundle.workflow_execution_id,
                workflow_execution_binding_sha256=(bundle.execution_binding.workflow_execution_binding_sha256),
                workflow_plan_sha256=bundle.workflow_plan.workflow_plan_sha256,
                run_execution_bundle_sha256=run.run_execution_bundle_sha256,
            )
        )
        SemanticRefreshMssqlAuthorityAdmissionService(
            authority=MssqlSemanticRefreshCanonicalAuthorityLoader(
                factory,
                control_schema=_CONTROL_SCHEMA,
            ),
            admission=MssqlSemanticRefreshStateAdapter(
                factory,
                control_schema=_CONTROL_SCHEMA,
            ),
        ).admit(run.workflow_execution_binding.workflow_execution_binding_sha256)
        _install_committed_evidence(setup, bundle, control_schema=_CONTROL_SCHEMA)

        artifact_policy = S3ArtifactStorePolicy(
            provider_profile=artifact_authority.provider_profile,
            endpoint_authority_id=artifact_authority.endpoint_authority_id,
            bucket_or_container_authority_id=(artifact_authority.bucket_or_container_authority_id),
            kms_key_authority_id=artifact_authority.kms_key_authority_id,
            capability_evidence_sha256=artifact_authority.capability_evidence_sha256,
            writer_scope=artifact_authority.writer_scope,
            artifact_prefix=artifact_authority.artifact_prefix,
            encryption_policy_sha256=artifact_authority.encryption_policy_sha256,
            retention_policy_id=artifact_authority.retention_policy_id,
            retention_policy_sha256=artifact_authority.retention_policy_sha256,
            retention_days=artifact_authority.retention_days,
            retention_issued_at=artifact_authority.retention_issued_at,
            retention_until=artifact_authority.retention_until,
            max_artifact_bytes=artifact_authority.max_artifact_bytes,
            conditional_create_authorized=True,
            require_object_lock=True,
            object_lock_mode="COMPLIANCE",
        )
        clickhouse_client = _clickhouse_client()
        runtime = build_semantic_refresh_publication_runtime(
            mssql_connection_factory=factory,
            mssql_connection_authority_id="mssql-prod",
            artifact_stores=S3CreateOnlyArtifactStoreResolver(
                client=_s3_client(),
                provider_policy=artifact_policy,
                clock=lambda: now,
            ),
            seal_policy=_LocalSealPolicyAuthority(),
            clickhouse_http_client=clickhouse_client,
            clickhouse_connection_authority=_local_connection_authority(clickhouse_client),
            now=lambda: now,
            control_schema=_CONTROL_SCHEMA,
        )
        binding_sha256 = run.workflow_execution_binding.workflow_execution_binding_sha256
        prepared = runtime.models.prepare(
            workflow_execution_binding_sha256=binding_sha256,
            operation_id=operation.operation_id,
        )
        terminal = runtime.models.commit(
            workflow_execution_binding_sha256=binding_sha256,
            operation_id=operation.operation_id,
        )
        assert prepared.status == "PREPARED"
        assert terminal.status == "COMPLETE"
        assert (
            _ch(
                f"SELECT payload FROM `{_DATABASE}`.`{target_table}` WHERE event_date = '2026-08-07' AND event_id = 1",
                read=True,
            )
            == "after"
        )

        durable = MssqlSemanticRefreshWorkflowPublicationReader(
            factory,
            control_schema=_CONTROL_SCHEMA,
        ).read(
            workflow_execution_id=workflow_execution_id,
            workflow_execution_binding_sha256=binding_sha256,
            expected_operation_ids=(operation.operation_id,),
        )
        publication = durable[0]
        summary = SemanticRefreshDurableWorkflowSummary.build(
            workflow_execution_id=workflow_execution_id,
            workflow_plan_sha256=plan.workflow_plan.workflow_plan_sha256,
            workflow_execution_binding_sha256=binding_sha256,
            expected_operation_ids=(operation.operation_id,),
            publications=(
                SemanticRefreshDurableModelPublication(
                    operation_id=publication.operation_id,
                    operation_plan_sha256=publication.operation_plan_sha256,
                    workflow_execution_binding_sha256=(publication.workflow_execution_binding_sha256),
                    attempt_binding_sha256=publication.attempt_binding_sha256,
                    artifact_manifest_sha256=_required(publication.artifact_manifest_sha256),
                    clickhouse_terminal_receipt_sha256=_required(publication.clickhouse_terminal_receipt_sha256),
                    terminal_receipt_sha256=_required(publication.terminal_receipt_sha256),
                    target_generation=_required_int(publication.target_generation),
                    scope_revision=_required_int(publication.scope_revision),
                ),
            ),
        )
        persisted = MssqlSemanticRefreshWorkflowSummaryState(
            factory,
            control_schema=_CONTROL_SCHEMA,
        ).persist(summary.to_dict())
        assert persisted["status"] == "FULLY_COMPLETE"
        assert (
            _scalar(
                setup,
                f"SELECT COUNT_BIG(*) FROM [{_CONTROL_SCHEMA}].[semantic_refresh_guards] WHERE status = N'HELD'",
            )
            == 0
        )
    finally:
        if operation_id is not None:
            _drop_operation_images(setup, operation_id=operation_id, control_schema=_CONTROL_SCHEMA)
            staging, shadow = clickhouse_operation_table_names(target_table, operation_id)
            for table in (staging, shadow):
                _ch(f"DROP TABLE IF EXISTS `{_DATABASE}`.`{table}`")
        _ch(f"DROP TABLE IF EXISTS `{_DATABASE}`.`{target_table}`")
        _drop_named_schema(setup, _CONTROL_SCHEMA)
        if created_dwh:
            _execute(
                setup,
                "ALTER DATABASE [DWH] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [DWH];",
            )
        setup.close()


def _local_baseline(*, target_table: str, target_uuid: str) -> SemanticRefreshBaselineAdoptionReceipt:
    baseline = _baseline_receipt()
    excluded = {
        "baseline_adoption_receipt_sha256",
        "historical_clickhouse_internal_multiset_conformance",
        "historical_cross_engine_payload_value_equivalence",
        "schema",
        "status",
    }
    values = {item.name: getattr(baseline, item.name) for item in fields(baseline) if item.name not in excluded}
    values.update(
        clickhouse_relation_id=f"{_DATABASE}.{target_table}",
        clickhouse_target_uuid=target_uuid,
        clickhouse_cluster_authority_id=_LOCAL_CLUSTER_AUTHORITY_ID,
        clickhouse_target_authority_id=(f"clickhouse://{_LOCAL_CLUSTER_AUTHORITY_ID}/{_DATABASE}/{target_table}"),
    )
    return SemanticRefreshBaselineAdoptionReceipt.build(**values)


def _admission_bundle(plan: Any, run: Any, *, suffix: str) -> MssqlCanonicalAdmissionBundle:
    operation = plan.operation_plans[0]
    execution = run.workflow_execution_binding
    attempt = SemanticRefreshAttemptBinding.build(
        workflow_execution_id=execution.workflow_execution_id,
        workflow_execution_binding_sha256=execution.workflow_execution_binding_sha256,
        operation_id=operation.operation_id,
        operation_plan_sha256=operation.operation_plan_sha256,
        dag_run_id=execution.workflow_execution_id,
        task_id="dbt-build-and-tests",
        try_number=1,
        pod_uid=str(uuid.uuid4()),
        fencing_epoch=1,
        owner_id=f"local-e2e-owner-{suffix}",
    )
    target = plan.targets[0]
    return MssqlCanonicalAdmissionBundle(
        workflow_execution_id=execution.workflow_execution_id,
        workflow_plan=plan.workflow_plan,
        execution_binding=execution,
        operation_plans=(operation,),
        attempt_bindings=(attempt,),
        workflow_guard=MssqlGuardClaim(f"workflow://local-e2e/{suffix}", 0, 1),
        resource_guards=(MssqlGuardClaim(target.target_resource_id, 0, 1),),
        model_resources=(mssql_model_resource_authority_from_plan_target(target),),
        controller_id="local-e2e-controller",
        owner_id=attempt.owner_id,
        reservation_id=f"local-e2e-reservation-{suffix}",
        resource_budget=mssql_workflow_resource_budget(plan),
        replacement_plan=None,
    )


def _drop_operation_images(setup: Any, *, operation_id: str, control_schema: str) -> None:
    cursor = setup.cursor()
    try:
        row = cursor.execute(
            f"SELECT strategy_authority_json FROM [{control_schema}].[semantic_refresh_journals] "
            "WHERE operation_id = ?",
            operation_id,
        ).fetchone()
    finally:
        cursor.close()
    if row is None:
        return
    strategy = json.loads(str(row[0]))
    before = _strategy_relation(strategy["before_image_relation"])
    after = _strategy_relation(strategy["after_image_relation"])
    _execute(setup, f"DROP TABLE IF EXISTS {before}; DROP TABLE IF EXISTS {after};")


def _install_committed_evidence(
    setup: Any,
    bundle: MssqlCanonicalAdmissionBundle,
    *,
    control_schema: str,
) -> MssqlBuildReceiptEvidence:
    operation = bundle.operation_plans[0]
    attempt = bundle.attempt_bindings[0]
    journal = compose_admission(bundle).journals[0]
    strategy = json.loads(journal.strategy_authority_json)
    before_relation = _strategy_relation(strategy["before_image_relation"])
    after_relation = _strategy_relation(strategy["after_image_relation"])
    _execute(
        setup,
        "IF NOT EXISTS (SELECT 1 FROM [DWH].sys.schemas WHERE name = N'dpone_scope_images') "
        "EXEC [DWH].sys.sp_executesql N'CREATE SCHEMA [dpone_scope_images]';",
    )
    _execute(
        setup,
        f"""
DROP TABLE IF EXISTS {before_relation};
DROP TABLE IF EXISTS {after_relation};
CREATE TABLE {before_relation} (
    event_date date NOT NULL,
    event_id bigint NOT NULL,
    payload nvarchar(200) NULL
);
CREATE TABLE {after_relation} (
    event_date date NOT NULL,
    event_id bigint NOT NULL,
    payload nvarchar(200) NULL
);
INSERT INTO {before_relation} VALUES ('2026-08-07', 1, N'before');
INSERT INTO {after_relation} VALUES ('2026-08-07', 1, N'after');
""".strip(),
    )
    receipt = MssqlBuildReceiptEvidence.build_exact(
        operation_id=operation.operation_id,
        operation_plan_sha256=operation.operation_plan_sha256,
        attempt_binding_sha256=attempt.attempt_binding_sha256,
        fencing_epoch=attempt.fencing_epoch,
        before_image_sha256=_live_image_sha256(setup, before_relation),
        after_image_sha256=_live_image_sha256(setup, after_relation),
        inserted_count=0,
        updated_count=1,
        model_unique_id=operation.model_unique_id,
        strategy_authority_sha256=journal.strategy_authority_sha256,
        before_image_relation=before_relation,
        after_image_relation=after_relation,
    )
    _execute_params(
        setup,
        f"""
INSERT INTO [{control_schema}].[semantic_refresh_receipts] (
    operation_id, operation_plan_sha256, attempt_binding_sha256, fencing_epoch,
    before_image_relation, before_image_sha256, after_image_relation, after_image_sha256,
    updated_count, inserted_count, build_receipt_sha256
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
""".strip(),
        receipt.operation_id,
        receipt.operation_plan_sha256,
        receipt.attempt_binding_sha256,
        receipt.fencing_epoch,
        receipt.before_image_relation,
        receipt.before_image_sha256,
        receipt.after_image_relation,
        receipt.after_image_sha256,
        receipt.updated_count,
        receipt.inserted_count,
        receipt.build_receipt_sha256,
    )
    return receipt


def _required(value: str | None) -> str:
    assert value is not None
    return value


def _required_int(value: int | None) -> int:
    assert value is not None
    return value


def _digest(character: str) -> str:
    return "sha256:" + character * 64
