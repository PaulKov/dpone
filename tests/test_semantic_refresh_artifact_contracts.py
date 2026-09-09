from __future__ import annotations

from dataclasses import replace

import pytest

from dpone.adapters.semantic_refresh_artifact_memory import (
    InMemoryCreateOnlyArtifactStore,
    StaticSemanticRefreshArtifactAuthority,
)
from dpone.contracts.semantic_refresh_seal_authorization import (
    SemanticRefreshSealAuthorizationReceipt,
)
from dpone.ports.semantic_refresh_artifact_seal import (
    ArtifactChunk,
    ArtifactSealPlan,
)
from dpone.services.semantic_refresh_artifact import (
    ArtifactConflictError,
    SealedArtifactService,
)

_DIGEST_A = "sha256:" + "a" * 64
_DIGEST_B = "sha256:" + "b" * 64


def _seal_authorization(
    *,
    after_image_row_count: int = 3,
    effective_key_mapping_sha256: str = "sha256:" + "1" * 64,
) -> SemanticRefreshSealAuthorizationReceipt:
    return SemanticRefreshSealAuthorizationReceipt.build(
        operation_id=_DIGEST_A,
        operation_plan_sha256=_DIGEST_A,
        model_unique_id="model.analytics.events",
        workflow_id="sha256:" + "8" * 64,
        workflow_plan_sha256="sha256:" + "7" * 64,
        workflow_execution_id="scheduled__2026-08-08",
        workflow_execution_binding_sha256="sha256:" + "9" * 64,
        attempt_binding_sha256=_DIGEST_B,
        fencing_epoch=7,
        journal_version=11,
        event_time_source_type="datetime2(6)",
        effective_key_template_sha256="sha256:" + "0" * 64,
        effective_key_mapping_sha256=effective_key_mapping_sha256,
        ordered_writable_schema_sha256="sha256:" + "c" * 64,
        serializer_sha256="sha256:" + "e" * 64,
        parquet_schema_mapping_sha256="sha256:" + "5" * 64,
        clickhouse_input_mapping_sha256="sha256:" + "6" * 64,
        codec_mapping_certification_sha256="sha256:" + "7" * 64,
        before_image_relation_id="analytics.dpone_images.events_before",
        before_image_sha256="sha256:" + "c" * 64,
        before_image_row_count=2,
        after_image_relation_id="analytics.dpone_images.events_after",
        after_image_sha256="sha256:" + "d" * 64,
        after_image_row_count=after_image_row_count,
        model_build_receipt_sha256="sha256:" + "d" * 64,
        baseline_adoption_receipt_sha256="sha256:" + "b" * 64,
        route_certification_receipt_sha256="sha256:" + "2" * 64,
        writer_exclusivity_assurance_receipt_sha256="sha256:" + "3" * 64,
        utc_semantics_assurance_receipt_sha256="sha256:" + "4" * 64,
        ddl_freeze_assurance_receipt_sha256="sha256:" + "5" * 64,
        artifact_authority_sha256="sha256:" + "6" * 64,
        seal_policy_sha256="sha256:" + "7" * 64,
        created_at="2026-08-08T12:35:00Z",
        issuer_authority="mssql-protected-control/test",
        issuer_attestation_sha256="sha256:" + "8" * 64,
        issuer_signature_sha256="sha256:" + "9" * 64,
    )


def _plan(*, chunks: tuple[ArtifactChunk, ...] | None = None) -> ArtifactSealPlan:
    selected_chunks = (
        chunks
        if chunks is not None
        else (
            ArtifactChunk(ordinal=1, content=b"PAR1chunk-zeroPAR1", row_count=2),
            ArtifactChunk(ordinal=2, content=b"PAR1chunk-onePAR1", row_count=1),
        )
    )
    return ArtifactSealPlan(
        seal_authorization=_seal_authorization(after_image_row_count=sum(chunk.row_count for chunk in selected_chunks)),
        artifact_prefix=f"semantic-refresh/{_DIGEST_A}",
        provider="memory",
        encryption_policy_sha256="sha256:" + "3" * 64,
        retention_policy_sha256="sha256:" + "4" * 64,
        encryption_scope="semantic-refresh-production",
        retention_until="2026-09-08T00:00:00Z",
        chunks=selected_chunks,
    )


def _service(
    store: InMemoryCreateOnlyArtifactStore,
    *,
    authorized_plan: ArtifactSealPlan | None = None,
) -> SealedArtifactService:
    authority = StaticSemanticRefreshArtifactAuthority(
        authorized_inventory=(authorized_plan or _plan()).authority_inventory(),
    )
    return SealedArtifactService(store=store, authority=authority)


def test_seal_creates_version_pinned_chunks_before_manifest() -> None:
    store = InMemoryCreateOnlyArtifactStore()

    receipt = _service(store).seal(_plan())

    assert store.create_events == (
        f"semantic-refresh/{_DIGEST_A}/chunks/000001.parquet",
        f"semantic-refresh/{_DIGEST_A}/chunks/000002.parquet",
        f"semantic-refresh/{_DIGEST_A}/manifest.json",
    )
    assert receipt.status == "SEALED"
    assert receipt.manifest.version == "v1"
    assert [chunk.version for chunk in receipt.chunks] == ["v1", "v1"]
    manifest = receipt.manifest_payload
    assert manifest["schema"] == "dpone.semantic-refresh-sealed-artifact-manifest.v1"
    assert manifest["artifact_format"] == "dpone_parquet_v1"
    assert manifest["seal_authorization_receipt_sha256"] == (
        _plan().seal_authorization.seal_authorization_receipt_sha256
    )
    assert manifest["model_build_receipt_sha256"] == "sha256:" + "d" * 64
    assert [item["provider_version"] for item in manifest["chunks"]] == ["v1", "v1"]


def test_exact_retry_reconciles_create_only_versions_without_replacement() -> None:
    store = InMemoryCreateOnlyArtifactStore()
    service = _service(store)

    first = service.seal(_plan())
    second = service.seal(_plan())

    assert first == second
    assert store.version_count == 3


def test_conflicting_chunk_blocks_manifest_publication() -> None:
    store = InMemoryCreateOnlyArtifactStore()
    service = _service(store)
    service.seal(_plan())
    conflicting = _plan(
        chunks=(
            ArtifactChunk(ordinal=1, content=b"PAR1differentPAR1", row_count=2),
            ArtifactChunk(ordinal=2, content=b"PAR1chunk-onePAR1", row_count=1),
        )
    )

    with pytest.raises(ArtifactConflictError, match="000001.parquet"):
        _service(store, authorized_plan=conflicting).seal(conflicting)

    assert store.version_count == 3


def test_acknowledgement_loss_is_reconciled_by_exact_version_and_bytes() -> None:
    store = InMemoryCreateOnlyArtifactStore(fail_after_create_on_call=1)

    receipt = _service(store).seal(_plan())

    assert receipt.status == "SEALED"
    assert receipt.chunks[0].version == "v1"


def test_authority_mismatch_fails_before_any_store_write() -> None:
    store = InMemoryCreateOnlyArtifactStore()
    plan = _plan()
    conflicting = replace(
        plan,
        seal_authorization=_seal_authorization(effective_key_mapping_sha256="sha256:" + "f" * 64),
    )

    with pytest.raises(ValueError, match="inventory authority"):
        _service(store).seal(conflicting)

    assert store.create_events == ()


def test_artifact_prefix_must_be_scoped_to_exact_operation() -> None:
    with pytest.raises(ValueError, match="operation-scoped"):
        replace(
            _plan(),
            artifact_prefix="semantic-refresh/another-operation",
        )


def test_empty_scope_seals_one_immutable_empty_manifest_without_chunks() -> None:
    store = InMemoryCreateOnlyArtifactStore()
    plan = _plan(chunks=())

    receipt = _service(store, authorized_plan=plan).seal(plan)

    assert store.create_events == (f"semantic-refresh/{_DIGEST_A}/manifest.json",)
    assert receipt.chunks == ()
    assert receipt.manifest_payload["chunks"] == []
    assert receipt.manifest_payload["total_rows"] == 0
    assert receipt.manifest_payload["total_bytes"] == 0


def test_chunk_rows_must_match_authorized_committed_after_image() -> None:
    with pytest.raises(ValueError, match="committed after-image row count"):
        replace(
            _plan(),
            seal_authorization=_seal_authorization(after_image_row_count=99),
        )
