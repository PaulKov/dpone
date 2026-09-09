"""Exact canonical sealed-artifact read contract."""

from __future__ import annotations

import pytest

from dpone.adapters.semantic_refresh_artifact_memory import (
    InMemoryCreateOnlyArtifactStore,
    StaticSemanticRefreshArtifactAuthority,
)
from dpone.adapters.semantic_refresh_artifact_reader import (
    SealedArtifactReadError,
    VersionPinnedSealedArtifactReader,
)
from dpone.contracts.semantic_refresh_seal_authorization import (
    SemanticRefreshSealAuthorizationReceipt,
)
from dpone.ports.semantic_refresh_artifact_seal import ArtifactChunk, ArtifactSealPlan
from dpone.services.semantic_refresh_artifact import SealedArtifactService


def _digest(char: str) -> str:
    return "sha256:" + char * 64


def _plan() -> ArtifactSealPlan:
    operation_id = _digest("1")
    return ArtifactSealPlan(
        seal_authorization=_seal_authorization(),
        artifact_prefix=f"semantic-refresh/{operation_id}",
        provider="memory",
        encryption_policy_sha256=_digest("a"),
        retention_policy_sha256=_digest("b"),
        encryption_scope="test",
        retention_until="2026-09-08T00:00:00Z",
        chunks=(ArtifactChunk(1, b"PAR1exactPAR1", 1),),
    )


def _seal_authorization() -> SemanticRefreshSealAuthorizationReceipt:
    return SemanticRefreshSealAuthorizationReceipt.build(
        operation_id=_digest("1"),
        operation_plan_sha256=_digest("2"),
        model_unique_id="model.analytics.events",
        workflow_id=_digest("a"),
        workflow_plan_sha256=_digest("b"),
        workflow_execution_id="scheduled__2026-08-08",
        workflow_execution_binding_sha256=_digest("3"),
        attempt_binding_sha256=_digest("4"),
        fencing_epoch=7,
        journal_version=11,
        event_time_source_type="datetime2(6)",
        effective_key_template_sha256=_digest("7"),
        effective_key_mapping_sha256=_digest("8"),
        ordered_writable_schema_sha256=_digest("9"),
        serializer_sha256=_digest("7"),
        parquet_schema_mapping_sha256=_digest("a"),
        clickhouse_input_mapping_sha256=_digest("b"),
        codec_mapping_certification_sha256=_digest("c"),
        before_image_relation_id="analytics.dpone_images.events_before",
        before_image_sha256=_digest("5"),
        before_image_row_count=1,
        after_image_relation_id="analytics.dpone_images.events_after",
        after_image_sha256=_digest("6"),
        after_image_row_count=1,
        model_build_receipt_sha256=_digest("6"),
        baseline_adoption_receipt_sha256=_digest("7"),
        route_certification_receipt_sha256=_digest("9"),
        writer_exclusivity_assurance_receipt_sha256=_digest("a"),
        utc_semantics_assurance_receipt_sha256=_digest("b"),
        ddl_freeze_assurance_receipt_sha256=_digest("c"),
        artifact_authority_sha256=_digest("d"),
        seal_policy_sha256=_digest("e"),
        created_at="2026-08-08T12:35:00Z",
        issuer_authority="mssql-protected-control/test",
        issuer_attestation_sha256=_digest("f"),
        issuer_signature_sha256=_digest("0"),
    )


def _sealed():
    plan = _plan()
    store = InMemoryCreateOnlyArtifactStore()
    receipt = SealedArtifactService(
        store=store,
        authority=StaticSemanticRefreshArtifactAuthority(plan.authority_inventory()),
    ).seal(plan)
    return plan, store, receipt


def test_reader_authenticates_canonical_manifest_and_exact_versions() -> None:
    plan, store, receipt = _sealed()

    artifact = VersionPinnedSealedArtifactReader(store).read(
        manifest_key=receipt.manifest.key,
        manifest_version=receipt.manifest.version,
        artifact_manifest_sha256=receipt.sealed_manifest.artifact_manifest_sha256,
        operation_id=plan.operation_id,
        operation_plan_sha256=plan.operation_plan_sha256,
        workflow_execution_binding_sha256=plan.workflow_execution_binding_sha256,
        attempt_binding_sha256=plan.attempt_binding_sha256,
        seal_authorization=plan.seal_authorization,
    )

    assert artifact.manifest == receipt.sealed_manifest
    assert artifact.chunk_bytes == (b"PAR1exactPAR1",)


def test_reader_rejects_old_or_competing_manifest_identity() -> None:
    plan, store, receipt = _sealed()

    with pytest.raises(SealedArtifactReadError, match="identity differs"):
        VersionPinnedSealedArtifactReader(store).read(
            manifest_key=receipt.manifest.key,
            manifest_version=receipt.manifest.version,
            artifact_manifest_sha256=_digest("f"),
            operation_id=plan.operation_id,
            operation_plan_sha256=plan.operation_plan_sha256,
            workflow_execution_binding_sha256=plan.workflow_execution_binding_sha256,
            attempt_binding_sha256=plan.attempt_binding_sha256,
            seal_authorization=plan.seal_authorization,
        )


def test_reader_rejects_manifest_under_another_seal_authorization() -> None:
    plan, store, receipt = _sealed()
    competing = SemanticRefreshSealAuthorizationReceipt.build(
        **{
            key: value
            for key, value in plan.seal_authorization.to_dict().items()
            if key
            not in {
                "seal_authorization_receipt_sha256",
                "schema",
                "journal_state",
                "mssql_outcome",
                "effective_key_mapping_sha256",
            }
        },
        effective_key_mapping_sha256=_digest("f"),
    )

    with pytest.raises(SealedArtifactReadError, match="seal authorization"):
        VersionPinnedSealedArtifactReader(store).read(
            manifest_key=receipt.manifest.key,
            manifest_version=receipt.manifest.version,
            artifact_manifest_sha256=receipt.sealed_manifest.artifact_manifest_sha256,
            operation_id=plan.operation_id,
            operation_plan_sha256=plan.operation_plan_sha256,
            workflow_execution_binding_sha256=plan.workflow_execution_binding_sha256,
            attempt_binding_sha256=plan.attempt_binding_sha256,
            seal_authorization=competing,
        )
