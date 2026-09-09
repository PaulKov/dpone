from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from dpone.ports.semantic_refresh_mssql import (
    MssqlBuildReceiptEvidence,
    MssqlImageEvidence,
    MssqlOperationEvidence,
    MssqlPrerequisiteAuthorityClaim,
    MssqlRuntimeAssuranceClaim,
    MssqlTransactionDisposition,
)
from dpone.ports.semantic_refresh_mssql_authority import (
    MssqlProtectedArtifactAuthority,
    MssqlProtectedOperationAuthority,
    MssqlProtectedResourcePolicy,
    MssqlProtectedWritableColumn,
)
from dpone.ports.semantic_refresh_seal_policy import (
    SemanticRefreshSealPolicyAuthority,
    SemanticRefreshSealPolicySubject,
    semantic_refresh_artifact_authority_sha256,
)
from dpone.services.semantic_refresh_mssql_failure import SemanticRefreshMssqlOutcomeService
from dpone.services.semantic_refresh_seal_authorization import (
    SemanticRefreshSealAuthorizationIssueError,
    SemanticRefreshSealAuthorizationIssuer,
)


def _digest(char: str) -> str:
    return "sha256:" + char * 64


class _Protected:
    def __init__(self, operation: MssqlProtectedOperationAuthority) -> None:
        self.operation = operation

    def load_operation(self, **_identity: object) -> MssqlProtectedOperationAuthority:
        return self.operation


class _Prerequisites:
    def __init__(self) -> None:
        self.calls = 0

    def require_current(self, _claim: MssqlPrerequisiteAuthorityClaim) -> None:
        self.calls += 1


class _Evidence:
    def __init__(self, value: MssqlOperationEvidence) -> None:
        self.value = value

    def read_operation_evidence(self, **_identity: object) -> MssqlOperationEvidence:
        return self.value


class _Codec:
    serializer_sha256 = _digest("7")

    def parquet_schema_mapping_sha256(self, _columns: object) -> str:
        return _digest("8")


class _Policy:
    def __init__(self) -> None:
        self.subject: SemanticRefreshSealPolicySubject | None = None

    def load(self, subject: SemanticRefreshSealPolicySubject) -> SemanticRefreshSealPolicyAuthority:
        self.subject = subject
        return SemanticRefreshSealPolicyAuthority.build(
            subject=subject,
            clickhouse_input_mapping_sha256=_digest("9"),
            codec_mapping_certification_sha256=_digest("a"),
            seal_policy_sha256=_digest("b"),
            issuer_authority="vault://dpone-kv/semantic-refresh/seal-policies",
            issuer_attestation_sha256=_digest("c"),
            issuer_signature_sha256=_digest("d"),
        )


class _Store:
    def __init__(self) -> None:
        self.receipt = None

    def persist_exact(self, receipt: object) -> None:
        if self.receipt is not None and self.receipt != receipt:
            raise RuntimeError("conflict")
        self.receipt = receipt

    def load(self, **_identity: object) -> object:
        return self.receipt


def test_issuer_binds_committed_images_code_derived_codec_and_vault_policy() -> None:
    operation = _operation()
    evidence = _evidence(operation)
    prerequisites, policy, store = _Prerequisites(), _Policy(), _Store()

    receipt = SemanticRefreshSealAuthorizationIssuer(
        protected_operation=_Protected(operation),
        prerequisites=prerequisites,
        evidence=_Evidence(evidence),
        outcome=SemanticRefreshMssqlOutcomeService(),
        codec=_Codec(),
        policy=policy,
        store=store,  # type: ignore[arg-type]
        now=lambda: datetime(2026, 8, 8, 12, tzinfo=UTC),
    ).issue(
        workflow_execution_binding_sha256=operation.workflow_execution_binding_sha256,
        operation_id=operation.operation_id,
    )

    assert receipt.workflow_id == "daily_events"
    assert receipt.model_build_receipt_sha256 == evidence.receipt.build_receipt_sha256  # type: ignore[union-attr]
    assert receipt.after_image_row_count == 3
    assert receipt.serializer_sha256 == _Codec.serializer_sha256
    assert receipt.artifact_authority_sha256 == semantic_refresh_artifact_authority_sha256(operation.artifact_authority)
    assert policy.subject is not None
    assert policy.subject.release_id == _digest("1")
    assert policy.subject.deployment_id == _digest("2")
    assert prerequisites.calls == 3
    assert store.receipt == receipt


def test_issuer_rejects_committed_evidence_that_differs_from_protected_operation() -> None:
    operation = _operation()
    evidence = _evidence(operation)
    assert evidence.after_image is not None
    evidence = replace(evidence, after_image=replace(evidence.after_image, image_sha256=_digest("f")))

    with pytest.raises(SemanticRefreshSealAuthorizationIssueError, match="not committed|differs"):
        SemanticRefreshSealAuthorizationIssuer(
            protected_operation=_Protected(operation),
            prerequisites=_Prerequisites(),
            evidence=_Evidence(evidence),
            outcome=SemanticRefreshMssqlOutcomeService(),
            codec=_Codec(),
            policy=_Policy(),
            store=_Store(),  # type: ignore[arg-type]
            now=lambda: datetime(2026, 8, 8, 12, tzinfo=UTC),
        ).issue(
            workflow_execution_binding_sha256=operation.workflow_execution_binding_sha256,
            operation_id=operation.operation_id,
        )


def _operation() -> MssqlProtectedOperationAuthority:
    prerequisite = MssqlPrerequisiteAuthorityClaim(
        release_id=_digest("1"),
        deployment_id=_digest("2"),
        model_unique_id="model.analytics.events",
        route_certification_receipt_sha256=_digest("3"),
        runtime_assurances=(
            MssqlRuntimeAssuranceClaim("ddl_freeze", _digest("4"), "{}"),
            MssqlRuntimeAssuranceClaim("utc_semantics", _digest("5"), "{}"),
            MssqlRuntimeAssuranceClaim("writer_exclusivity", _digest("6"), "{}"),
        ),
    )
    return MssqlProtectedOperationAuthority(
        workflow_execution_id="scheduled__2026-08-08",
        workflow_execution_binding_sha256=_digest("0"),
        canonical_authority_sha256=_digest("1"),
        workflow_plan_sha256=_digest("2"),
        operation_id=_digest("3"),
        operation_plan_sha256=_digest("4"),
        attempt_binding_sha256=_digest("5"),
        fencing_epoch=7,
        owner_id="owner-1",
        guard_resource_id="analytics.dbo.events",
        guard_status="HELD",
        journal_status="PREPARING",
        strategy_authority_json="{}",
        strategy_authority_sha256=_digest("6"),
        model_unique_id="model.analytics.events",
        target_resource_id="analytics.dbo.events",
        target_authority_id="clickhouse://cluster-a/analytics/events",
        mssql_connection_authority_id="mssql-primary",
        mssql_target_authority_id="mssql://mssql-primary/analytics/dbo.events",
        clickhouse_cluster_authority_id="cluster-a",
        publication_database="analytics",
        publication_target_table="events",
        publication_scope_id="2026-08-08T00:00:00Z/2026-08-09T00:00:00Z",
        scope_family_id=_digest("7"),
        scope_start="2026-08-08T00:00:00Z",
        scope_end="2026-08-09T00:00:00Z",
        scope_revision=1,
        mutation_closure_sha256=_digest("8"),
        target_predecessor_generation_id=_digest("9"),
        scope_predecessor_operation_id=None,
        predecessor_target_generation=1,
        predecessor_target_uuid="00000000-0000-0000-0000-000000000001",
        predecessor_target_operation_id=_digest("a"),
        predecessor_scope_revision=None,
        predecessor_checkpoint_sha256=None,
        predecessor_checkpoint_operation_id=None,
        predecessor_checkpoint_version=None,
        clickhouse_target_uuid="00000000-0000-0000-0000-000000000001",
        model_definition_proof_sha256=_digest("b"),
        effective_key_template_sha256=_digest("c"),
        effective_key_mapping_sha256=_digest("d"),
        writable_columns=(
            MssqlProtectedWritableColumn("event_id", "bigint", "Int64", False, "EFFECTIVE_KEY"),
            MssqlProtectedWritableColumn(
                "occurred_at",
                "datetime2(6)",
                "DateTime64(6, 'UTC')",
                False,
                "EFFECTIVE_KEY_EVENT_TIME",
            ),
            MssqlProtectedWritableColumn("amount", "decimal(18,2)", "Decimal(18,2)", False, "MUTABLE_VALUE"),
        ),
        writable_schema_sha256=_digest("e"),
        resource_policy=MssqlProtectedResourcePolicy(
            *(100 for _ in range(18)),
            resource_policy_sha256=_digest("f"),
        ),
        route_certification_receipt_sha256=_digest("3"),
        writer_exclusivity_assurance_receipt_sha256=_digest("6"),
        ddl_freeze_assurance_receipt_sha256=_digest("4"),
        utc_semantics_assurance_receipt_sha256=_digest("5"),
        artifact_authority=MssqlProtectedArtifactAuthority(
            provider="s3",
            provider_profile="AWS_PRODUCTION",
            endpoint_authority_id="https://s3.amazonaws.com",
            bucket_or_container_authority_id="dpone-semantic-refresh",
            kms_key_authority_id="arn:aws:kms:eu-central-1:123456789012:key/test",
            capability_evidence_sha256=_digest("0"),
            writer_scope="semantic-refresh-writer",
            artifact_prefix=f"semantic-refresh/{_digest('3')}",
            encryption_policy_sha256=_digest("1"),
            retention_policy_id="semantic-refresh-30d",
            retention_policy_sha256=_digest("2"),
            retention_days=30,
            retention_issued_at="2026-08-09T00:00:00Z",
            retention_until="2026-09-08T00:00:00Z",
            max_artifact_bytes=1_000_000,
        ),
        before_image_relation="analytics.dpone_images.events_before",
        before_image_sha256=_digest("a"),
        after_image_relation="analytics.dpone_images.events_after",
        after_image_sha256=_digest("b"),
        scope_map_authority_receipt_sha256=_digest("c"),
        prerequisite_authority=prerequisite,
        journal_version=3,
        workflow_id="daily_events",
        baseline_receipt_sha256=_digest("d"),
    )


def _evidence(operation: MssqlProtectedOperationAuthority) -> MssqlOperationEvidence:
    receipt = MssqlBuildReceiptEvidence.build_exact(
        operation_id=operation.operation_id,
        operation_plan_sha256=operation.operation_plan_sha256,
        attempt_binding_sha256=operation.attempt_binding_sha256,
        fencing_epoch=operation.fencing_epoch,
        before_image_sha256=operation.before_image_sha256 or "",
        after_image_sha256=operation.after_image_sha256 or "",
        inserted_count=1,
        updated_count=2,
        model_unique_id=operation.model_unique_id,
        strategy_authority_sha256=operation.strategy_authority_sha256,
        before_image_relation=operation.before_image_relation or "",
        after_image_relation=operation.after_image_relation or "",
    )
    common = {
        "operation_id": operation.operation_id,
        "operation_plan_sha256": operation.operation_plan_sha256,
        "attempt_binding_sha256": operation.attempt_binding_sha256,
        "fencing_epoch": operation.fencing_epoch,
        "committed": True,
    }
    return MssqlOperationEvidence(
        operation_id=operation.operation_id,
        operation_plan_sha256=operation.operation_plan_sha256,
        attempt_binding_sha256=operation.attempt_binding_sha256,
        fencing_epoch=operation.fencing_epoch,
        database_available=True,
        controller_proves_not_invoked=False,
        transaction_disposition=MssqlTransactionDisposition.UNKNOWN,
        receipt=receipt,
        before_image=MssqlImageEvidence(
            **common,
            image_role="BEFORE",
            image_sha256=operation.before_image_sha256 or "",
            row_count=2,
        ),
        after_image=MssqlImageEvidence(
            **common,
            image_role="AFTER",
            image_sha256=operation.after_image_sha256 or "",
            row_count=3,
        ),
    )
