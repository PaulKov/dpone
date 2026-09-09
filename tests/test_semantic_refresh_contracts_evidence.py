"""Closed evidence and authority contract tests for semantic refresh V2."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, ValidationError

from dpone.contracts.semantic_refresh_artifact_manifest import (
    SemanticRefreshArtifactChunk,
    SemanticRefreshSealedArtifactManifest,
)
from dpone.contracts.semantic_refresh_authority_bundle import (
    SemanticRefreshCompilerRuntimeAuthorityBundle,
    SemanticRefreshModelAuthority,
)
from dpone.contracts.semantic_refresh_baseline_receipt import (
    BaselineAssuranceKind,
    SemanticRefreshBaselineAdoptionReceipt,
    semantic_refresh_target_predecessor_generation_id,
)
from dpone.contracts.semantic_refresh_route_certification import (
    LiveCertificationStatus,
    SemanticRefreshRouteCapabilityCoordinates,
    SemanticRefreshRouteLiveCertificationReceipt,
)
from dpone.contracts.semantic_refresh_schemas import (
    render_semantic_refresh_schema,
    semantic_refresh_contract_schemas,
)
from dpone.contracts.semantic_refresh_seal_authorization import SemanticRefreshSealAuthorizationReceipt
from dpone.contracts.semantic_refresh_termination_receipt import (
    SemanticRefreshContainerTermination,
    SemanticRefreshTrustedAttemptTerminationReceipt,
)
from dpone.contracts.semantic_refresh_types import WorkflowMode
from dpone.contracts.semantic_refresh_workflow_summary import (
    SemanticRefreshDurableModelPublication,
    SemanticRefreshDurableWorkflowSummary,
)

_A = "sha256:" + "a" * 64
_B = "sha256:" + "b" * 64
_C = "sha256:" + "c" * 64
_OPERATION_ID = "sha256:" + "d" * 64
_TIMESTAMP = "2026-08-07T12:34:56Z"
_WORKFLOW_EXECUTION_ID = "scheduled__2026-08-07T00:00:00+00:00"


def _baseline(
    kind: BaselineAssuranceKind = BaselineAssuranceKind.ADOPTED_COMPLETE_RELATION_CONFORMANT,
    *,
    clickhouse_target_uuid: str = "0198f11c-6956-74f2-984b-4cfcb1653b87",
    clickhouse_target_authority_id: str = "clickhouse://production-primary/analytics/events",
) -> SemanticRefreshBaselineAdoptionReceipt:
    return SemanticRefreshBaselineAdoptionReceipt.build(
        model_unique_id="model.analytics.events",
        baseline_kind=kind,
        release_id=_A,
        deployment_id=_B,
        source_snapshot_sha256=_C,
        source_relation_id="source.analytics.events",
        source_generation=3,
        mssql_relation_id="analytics.dbo.events",
        mssql_generation=4,
        clickhouse_relation_id="analytics.events",
        clickhouse_generation=8,
        clickhouse_target_uuid=clickhouse_target_uuid,
        mssql_connection_authority_id="mssql://production-primary",
        mssql_target_authority_id="mssql://production-primary/analytics/dbo.events",
        clickhouse_cluster_authority_id="clickhouse://production-primary",
        clickhouse_target_authority_id=clickhouse_target_authority_id,
        source_schema_sha256=_A,
        mssql_schema_sha256=_B,
        clickhouse_schema_sha256=_C,
        source_key_sha256=_A,
        mssql_key_sha256=_B,
        clickhouse_key_sha256=_C,
        source_physical_sha256=_A,
        mssql_physical_sha256=_B,
        clickhouse_physical_sha256=_C,
        coverage_start="2026-08-01T00:00:00Z",
        coverage_end="2026-08-08T00:00:00Z",
        source_coverage_sha256=_A,
        mssql_coverage_sha256=_B,
        clickhouse_coverage_sha256=_C,
        source_assurance_sha256=_A,
        mssql_assurance_sha256=_B,
        clickhouse_assurance_sha256=_C,
        utc_assurance_sha256=_A,
        writer_assurance_sha256=_B,
        ddl_assurance_sha256=_C,
        certified_codec_mapping_sha256=_A,
        historical_clickhouse_internal_multiset_evidence_sha256=_B,
        adopted_at=_TIMESTAMP,
    )


def _coordinates() -> SemanticRefreshRouteCapabilityCoordinates:
    return SemanticRefreshRouteCapabilityCoordinates(
        source_connector="mssql",
        source_connector_version="1.0.0",
        sink_connector="clickhouse",
        sink_connector_version="1.0.0",
        load_strategy="semantic_refresh_v2",
        environment="production",
        runtime_image_digest=_A,
        toolchain_sha256=_B,
        capability_policy_sha256=_C,
        source_capability_sha256=_A,
        sink_capability_sha256=_B,
        route_policy_sha256=_C,
    )


def _certification(
    status: LiveCertificationStatus = LiveCertificationStatus.PASS,
) -> SemanticRefreshRouteLiveCertificationReceipt:
    evidence = _A if status is not LiveCertificationStatus.UNVERIFIED else None
    failure = _B if status is not LiveCertificationStatus.UNVERIFIED else None
    return SemanticRefreshRouteLiveCertificationReceipt.build(
        coordinates=_coordinates(),
        status=status,
        tested_at=_TIMESTAMP,
        effective_from=_TIMESTAMP,
        expires_at="2026-09-07T00:00:00Z",
        live_environment_sha256=(_C if status is not LiveCertificationStatus.UNVERIFIED else None),
        live_evidence_sha256=evidence,
        failure_matrix_sha256=failure,
        certification_policy_sha256=_B,
        issuer_authority="dpone-release-certifier",
        issuer_attestation_sha256=_A,
        issuer_signature_sha256=_C,
    )


def _seal_authorization() -> SemanticRefreshSealAuthorizationReceipt:
    return SemanticRefreshSealAuthorizationReceipt.build(
        operation_id=_OPERATION_ID,
        operation_plan_sha256=_A,
        model_unique_id="model.analytics.events",
        workflow_id="daily_events",
        workflow_plan_sha256=_C,
        workflow_execution_id=_WORKFLOW_EXECUTION_ID,
        workflow_execution_binding_sha256=_B,
        attempt_binding_sha256=_C,
        fencing_epoch=7,
        journal_version=11,
        event_time_source_type="datetime2(6)",
        effective_key_template_sha256=_A,
        effective_key_mapping_sha256=_B,
        ordered_writable_schema_sha256=_C,
        serializer_sha256=_C,
        parquet_schema_mapping_sha256=_A,
        clickhouse_input_mapping_sha256=_B,
        codec_mapping_certification_sha256=_C,
        before_image_relation_id="analytics.dpone_images.events_before_20260807",
        before_image_sha256=_A,
        before_image_row_count=4,
        after_image_relation_id="analytics.dpone_images.events_after_20260807",
        after_image_sha256=_B,
        after_image_row_count=5,
        model_build_receipt_sha256=_B,
        baseline_adoption_receipt_sha256=_baseline().baseline_adoption_receipt_sha256,
        route_certification_receipt_sha256=_certification().route_certification_receipt_sha256,
        writer_exclusivity_assurance_receipt_sha256=_A,
        utc_semantics_assurance_receipt_sha256=_B,
        ddl_freeze_assurance_receipt_sha256=_C,
        artifact_authority_sha256=_A,
        seal_policy_sha256=_B,
        created_at="2026-08-07T12:35:00Z",
        issuer_authority="mssql-protected-control/production",
        issuer_attestation_sha256=_B,
        issuer_signature_sha256=_C,
    )


def _manifest() -> SemanticRefreshSealedArtifactManifest:
    return SemanticRefreshSealedArtifactManifest.build(
        seal_authorization=_seal_authorization(),
        artifact_prefix="semantic-refresh/operation-dddd/attempt-cccc",
        provider="s3",
        encryption_policy_sha256=_B,
        retention_policy_sha256=_C,
        chunks=(
            SemanticRefreshArtifactChunk(1, "part-0001.parquet", "v-101", _A, 100, 4),
            SemanticRefreshArtifactChunk(2, "part-0002.parquet", "v-102", _B, 120, 5),
        ),
    )


def _summary() -> SemanticRefreshDurableWorkflowSummary:
    publication = SemanticRefreshDurableModelPublication(
        operation_id=_OPERATION_ID,
        operation_plan_sha256=_A,
        workflow_execution_binding_sha256=_B,
        attempt_binding_sha256=_C,
        artifact_manifest_sha256=_manifest().artifact_manifest_sha256,
        clickhouse_terminal_receipt_sha256=_A,
        terminal_receipt_sha256=_B,
        target_generation=9,
        scope_revision=1,
    )
    return SemanticRefreshDurableWorkflowSummary.build(
        workflow_execution_id=_WORKFLOW_EXECUTION_ID,
        workflow_plan_sha256=_A,
        workflow_execution_binding_sha256=_B,
        expected_operation_ids=(_OPERATION_ID,),
        publications=(publication,),
    )


def _termination() -> SemanticRefreshTrustedAttemptTerminationReceipt:
    return SemanticRefreshTrustedAttemptTerminationReceipt.build(
        workflow_execution_id=_WORKFLOW_EXECUTION_ID,
        workflow_execution_binding_sha256=_B,
        operation_ids=(_OPERATION_ID,),
        attempt_binding_sha256=_C,
        dag_id="semantic_refresh_daily",
        run_id="scheduled__2026-08-07T00:00:00+00:00",
        task_id="semantic_refresh__events",
        map_index=-1,
        try_number=1,
        cluster_id="production-eu-1",
        namespace="airflow",
        pod_name="semantic-refresh-events-abc",
        pod_uid="0198f11c-6956-74f2-984b-4cfcb1653b87",
        pod_resource_version="103421",
        terminal_phase="Failed",
        container_terminations=(
            SemanticRefreshContainerTermination(
                name="base",
                container_id="containerd://abc",
                reason="Error",
                finished_at=_TIMESTAMP,
                exit_code=1,
            ),
        ),
        observed_at="2026-08-07T12:35:00Z",
        observer_authority="kubernetes-controller/prod-eu-1",
        observer_policy_sha256=_A,
        observer_attestation_sha256=_B,
        observer_signature_sha256=_C,
    )


def _authority() -> SemanticRefreshCompilerRuntimeAuthorityBundle:
    model = SemanticRefreshModelAuthority(
        model_unique_id="model.analytics.events",
        baseline_adoption_receipt_sha256=_baseline().baseline_adoption_receipt_sha256,
        model_definition_proof_sha256=_A,
        mutation_closure_sha256=_B,
        sqlserver_lifecycle_policy_sha256=_C,
        read_dependency_proof_sha256=_A,
        operation_id=_OPERATION_ID,
        operation_plan_sha256=_B,
    )
    return SemanticRefreshCompilerRuntimeAuthorityBundle.build(
        release_id=_A,
        deployment_id=_B,
        environment="production",
        workflow_mode=WorkflowMode.NORMAL,
        workflow_execution_id=_WORKFLOW_EXECUTION_ID,
        route_certification_receipt_sha256=_certification().route_certification_receipt_sha256,
        workflow_plan_sha256=_A,
        workflow_execution_binding_sha256=_B,
        compiler_policy_sha256=_C,
        runtime_policy_sha256=_A,
        models=(model,),
    )


def _documents() -> tuple[object, ...]:
    return (_baseline(), _certification(), _manifest(), _summary(), _termination(), _authority())


def test_additive_evidence_contracts_are_closed_digest_bound_and_schema_valid() -> None:
    schemas = semantic_refresh_contract_schemas()
    for document in _documents():
        payload = document.to_dict()
        assert type(document).from_mapping(payload) == document
        assert type(document).from_json(document.canonical_bytes()) == document
        Draft202012Validator(schemas[document.schema]).validate(payload)

        with pytest.raises(ValueError, match="unknown"):
            type(document).from_mapping({**payload, "airflow_success": True})
        tampered = {**payload, document.digest_field: _C}
        with pytest.raises(ValueError, match="differs"):
            type(document).from_mapping(tampered)


def test_route_live_certification_is_exact_expiring_and_fail_closed() -> None:
    receipt = _certification()
    instant = datetime(2026, 8, 8, tzinfo=timezone.utc)  # noqa: UP017
    assert not receipt.authorizes(instant)
    assert receipt.authorizes(
        instant,
        trusted_receipt_sha256=receipt.route_certification_receipt_sha256,
    )
    assert not receipt.authorizes(
        datetime(2026, 10, 8, tzinfo=timezone.utc),  # noqa: UP017
        trusted_receipt_sha256=receipt.route_certification_receipt_sha256,
    )
    assert not _certification(LiveCertificationStatus.FAIL).authorizes(
        datetime(2026, 8, 8, tzinfo=timezone.utc)  # noqa: UP017
    )
    assert not _certification(LiveCertificationStatus.UNVERIFIED).authorizes(
        datetime(2026, 8, 8, tzinfo=timezone.utc)  # noqa: UP017
    )
    with pytest.raises(ValueError, match="live evidence"):
        SemanticRefreshRouteLiveCertificationReceipt.build(
            coordinates=_coordinates(),
            status=LiveCertificationStatus.PASS,
            tested_at=_TIMESTAMP,
            effective_from=_TIMESTAMP,
            expires_at="2026-09-07T00:00:00Z",
            live_environment_sha256=None,
            live_evidence_sha256=None,
            failure_matrix_sha256=None,
            certification_policy_sha256=_B,
            issuer_authority="dpone-release-certifier",
            issuer_attestation_sha256=_A,
            issuer_signature_sha256=_C,
        )
    revoked = SemanticRefreshRouteLiveCertificationReceipt.build_revocation(
        coordinates=_coordinates(),
        revoked_receipt_sha256=receipt.route_certification_receipt_sha256,
        revoked_at="2026-08-08T00:00:00Z",
        revocation_reason="environment drift",
        certification_policy_sha256=_B,
        issuer_authority="dpone-release-certifier",
        issuer_attestation_sha256=_A,
        issuer_signature_sha256=_C,
    )
    assert revoked.status is LiveCertificationStatus.UNVERIFIED
    assert not revoked.authorizes(datetime(2026, 8, 8, tzinfo=timezone.utc))  # noqa: UP017


@pytest.mark.parametrize("kind", list(BaselineAssuranceKind))
def test_baseline_paths_prove_target_multiset_without_cross_engine_overclaim(
    kind: BaselineAssuranceKind,
) -> None:
    receipt = _baseline(kind)
    assert receipt.historical_clickhouse_internal_multiset_conformance == "PROVEN"
    assert receipt.historical_cross_engine_payload_value_equivalence == "NOT_CLAIMED"
    assert receipt.certified_codec_mapping_sha256 == _A


def test_baseline_target_predecessor_identity_binds_exact_clickhouse_uuid() -> None:
    first = _baseline()
    second = _baseline(clickhouse_target_uuid="0298f11c-6956-74f2-984b-4cfcb1653b87")
    assert semantic_refresh_target_predecessor_generation_id(first) != (
        semantic_refresh_target_predecessor_generation_id(second)
    )
    other_authority = _baseline(clickhouse_target_authority_id="clickhouse://production-secondary/analytics/events")
    assert semantic_refresh_target_predecessor_generation_id(first) != (
        semantic_refresh_target_predecessor_generation_id(other_authority)
    )


def test_manifest_summary_and_termination_reject_false_closure() -> None:
    manifest = _manifest()
    with pytest.raises(ValueError, match="contiguous"):
        SemanticRefreshSealedArtifactManifest.build(
            seal_authorization=_seal_authorization(),
            artifact_prefix=manifest.artifact_prefix,
            provider=manifest.provider,
            encryption_policy_sha256=manifest.encryption_policy_sha256,
            retention_policy_sha256=manifest.retention_policy_sha256,
            chunks=(SemanticRefreshArtifactChunk(2, "part-0002.parquet", "v-102", _B, 1, 1),),
        )

    summary = _summary().to_dict()
    summary["expected_operation_ids"] = [_A]
    with pytest.raises(ValueError, match="closure"):
        SemanticRefreshDurableWorkflowSummary.from_mapping(summary)

    termination = _termination().to_dict()
    termination["operation_set_sha256"] = _A
    with pytest.raises(ValueError, match="operation_set_sha256"):
        SemanticRefreshTrustedAttemptTerminationReceipt.from_mapping(termination)


def test_sealed_manifest_preserves_empty_scope_evidence() -> None:
    populated = _manifest()
    empty = SemanticRefreshSealedArtifactManifest.build(
        seal_authorization=_seal_authorization(),
        artifact_prefix="semantic-refresh/operation-dddd/attempt-cccc/empty",
        provider=populated.provider,
        encryption_policy_sha256=populated.encryption_policy_sha256,
        retention_policy_sha256=populated.retention_policy_sha256,
        chunks=(),
    )
    assert (empty.chunks, empty.chunk_count, empty.total_bytes, empty.total_rows) == ((), 0, 0, 0)
    assert empty.artifact_manifest_sha256 == ("sha256:13c61910d54496b9dd62432896c511597adbddcd92b9e4cda135a6c34777b024")
    Draft202012Validator(semantic_refresh_contract_schemas()[empty.schema]).validate(empty.to_dict())


def test_authority_bundle_is_pre_attempt_and_mode_conditional() -> None:
    authority = _authority()
    forbidden = {
        "attempt_binding_sha256",
        "dag_run_id",
        "pod_uid",
        "fencing_epoch",
        "created_at",
        "artifact_manifest_sha256",
        "outcome",
    }
    assert forbidden.isdisjoint(authority.to_dict())
    for field in forbidden:
        with pytest.raises(ValueError, match="unknown"):
            SemanticRefreshCompilerRuntimeAuthorityBundle.from_mapping({**authority.to_dict(), field: "runtime"})

    with pytest.raises(ValueError, match="replacement"):
        SemanticRefreshCompilerRuntimeAuthorityBundle.build(
            release_id=_A,
            deployment_id=_B,
            environment="production",
            workflow_mode=WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT,
            workflow_execution_id=_WORKFLOW_EXECUTION_ID,
            route_certification_receipt_sha256=_B,
            workflow_plan_sha256=_C,
            workflow_execution_binding_sha256=_A,
            compiler_policy_sha256=_B,
            runtime_policy_sha256=_C,
            models=authority.models,
        )


def test_evidence_json_schemas_fail_closed_on_incomplete_or_runtime_claims() -> None:
    schemas = semantic_refresh_contract_schemas()
    baseline = _baseline().to_dict()
    baseline.pop("source_physical_sha256")
    with pytest.raises(ValidationError):
        Draft202012Validator(schemas[_baseline().schema]).validate(baseline)

    certification = _certification().to_dict()
    certification.pop("failure_matrix_sha256")
    with pytest.raises(ValidationError):
        Draft202012Validator(schemas[_certification().schema]).validate(certification)

    summary = {**_summary().to_dict(), "status": "INCOMPLETE"}
    with pytest.raises(ValidationError):
        Draft202012Validator(schemas[_summary().schema]).validate(summary)

    authority = {**_authority().to_dict(), "attempt_binding_sha256": _A}
    with pytest.raises(ValidationError):
        Draft202012Validator(schemas[_authority().schema]).validate(authority)


def test_evidence_schema_bytes_and_golden_vector_are_stable() -> None:
    schemas = semantic_refresh_contract_schemas()
    assert len(schemas) == 22
    for document in _documents():
        checked = Path("docs/schemas/dbt", f"{document.schema}.schema.json")
        assert checked.read_bytes() == render_semantic_refresh_schema(document.schema)

    expected = json.loads(
        Path("tests/fixtures/semantic-refresh-v2/contracts/evidence-golden-v1.json").read_text(encoding="utf-8")
    )
    assert {document.schema: document.to_dict() for document in _documents()} == expected
