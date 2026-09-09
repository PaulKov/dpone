"""Runtime assurance and seal-authorization contract tests."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, ValidationError

from dpone.contracts.semantic_refresh_artifact_authority_identity import (
    ARTIFACT_AUTHORITY_IDENTITY_SCHEMA,
    semantic_refresh_artifact_authority_preimage,
    semantic_refresh_artifact_authority_sha256,
)
from dpone.contracts.semantic_refresh_artifact_manifest import (
    SemanticRefreshArtifactChunk,
    SemanticRefreshSealedArtifactManifest,
)
from dpone.contracts.semantic_refresh_effective_key_identity import (
    EffectiveKeyTemplateColumn,
    semantic_refresh_effective_key_mapping_sha256,
    semantic_refresh_effective_key_template_sha256,
)
from dpone.contracts.semantic_refresh_runtime_assurance import (
    RuntimeAssuranceKind,
    RuntimeAssuranceStatus,
    RuntimeAssuranceSubjectType,
    SemanticRefreshRuntimeAssuranceReceipt,
    SemanticRefreshRuntimeAssuranceSubject,
)
from dpone.contracts.semantic_refresh_schemas import (
    render_semantic_refresh_schema,
    semantic_refresh_contract_schemas,
)
from dpone.contracts.semantic_refresh_seal_authorization import SemanticRefreshSealAuthorizationReceipt
from dpone.contracts.semantic_refresh_types import DATETIME_DOMAIN_MAX, DATETIME_DOMAIN_MIN, EffectiveKeyColumn

_A = "sha256:" + "a" * 64
_B = "sha256:" + "b" * 64
_C = "sha256:" + "c" * 64
_D = "sha256:" + "d" * 64
_WORKFLOW_EXECUTION_ID = "scheduled__2026-08-07T00:00:00+00:00"


def _artifact_authority() -> dict[str, object]:
    return {
        "provider": "s3",
        "provider_profile": "production-artifacts",
        "endpoint_authority_id": "s3://production-primary",
        "bucket_or_container_authority_id": "s3://production-primary/dpone-artifacts",
        "kms_key_authority_id": "kms://production/artifacts",
        "capability_evidence_sha256": _A,
        "writer_scope": "semantic-refresh/production",
        "artifact_prefix": "semantic-refresh/analytics/events",
        "encryption_policy_sha256": _B,
        "retention_policy_id": "semantic-refresh-evidence",
        "retention_policy_sha256": _C,
        "retention_days": 30,
        "retention_issued_at": "2026-08-08T00:00:00Z",
        "retention_until": "2026-09-07T00:00:00Z",
        "max_artifact_bytes": 1048576,
    }


def _subject(kind: RuntimeAssuranceKind) -> SemanticRefreshRuntimeAssuranceSubject:
    is_column = kind is RuntimeAssuranceKind.UTC_SEMANTICS
    return SemanticRefreshRuntimeAssuranceSubject(
        assurance_kind=kind,
        subject_type=(RuntimeAssuranceSubjectType.COLUMN if is_column else RuntimeAssuranceSubjectType.TARGET),
        release_id=_A,
        deployment_id=_B,
        environment="production",
        database="analytics",
        model_unique_id="model.analytics.events",
        mssql_target_authority_id="mssql://production/analytics/dbo/events",
        model_definition_proof_sha256=_C,
        effective_key_template_sha256=_A,
        writable_schema_sha256=_B,
        sqlserver_lifecycle_policy_sha256=_C,
        route_certification_receipt_sha256=_A,
        mssql_control_database="analytics",
        mssql_control_schema="dpone_control",
        mssql_image_schema="dpone_images",
        scope_image_namespace_policy_sha256=_B,
        column_name="occurred_at" if is_column else None,
    )


def _assurance(kind: RuntimeAssuranceKind) -> SemanticRefreshRuntimeAssuranceReceipt:
    writer = kind is RuntimeAssuranceKind.WRITER_EXCLUSIVITY
    return SemanticRefreshRuntimeAssuranceReceipt.build(
        subject=_subject(kind),
        producer_version="dpone-runtime/0.73.0",
        transformation_version="semantic-refresh-v2",
        acl_policy_version="mssql-writer-policy/2026-08-08",
        effective_from="2026-08-07T00:00:00Z",
        expires_at="2026-09-07T00:00:00Z",
        evidence_sha256=_C,
        runtime_assurance_policy_sha256=_A,
        approver_authority="dpone-platform-control",
        approver_attestation_sha256=_B,
        approver_signature_sha256=_C,
        engine_acl_proof_sha256=_A if writer else None,
        platform_allowlist_sha256=_B if writer else None,
        external_job_inventory_sha256=_C if writer else None,
        organizational_control_sha256=_D if writer else None,
        ddl_epoch=7 if kind is RuntimeAssuranceKind.DDL_FREEZE else None,
    )


def _seal(
    *,
    event_time_source_type: str = "datetime2(6)",
    effective_key_mapping_sha256: str = _B,
) -> SemanticRefreshSealAuthorizationReceipt:
    return SemanticRefreshSealAuthorizationReceipt.build(
        operation_id=_A,
        operation_plan_sha256=_B,
        model_unique_id="model.analytics.events",
        workflow_id="daily_events",
        workflow_plan_sha256=_D,
        workflow_execution_id=_WORKFLOW_EXECUTION_ID,
        workflow_execution_binding_sha256=_A,
        attempt_binding_sha256=_B,
        fencing_epoch=7,
        journal_version=11,
        event_time_source_type=event_time_source_type,
        effective_key_template_sha256=_A,
        effective_key_mapping_sha256=effective_key_mapping_sha256,
        ordered_writable_schema_sha256=_C,
        serializer_sha256=_A,
        parquet_schema_mapping_sha256=_B,
        clickhouse_input_mapping_sha256=_C,
        codec_mapping_certification_sha256=_D,
        before_image_relation_id="analytics.dpone_images.events_before_20260807",
        before_image_sha256=_C,
        before_image_row_count=4,
        after_image_relation_id="analytics.dpone_images.events_after_20260807",
        after_image_sha256=_D,
        after_image_row_count=5,
        model_build_receipt_sha256=_A,
        baseline_adoption_receipt_sha256=_B,
        route_certification_receipt_sha256=_C,
        writer_exclusivity_assurance_receipt_sha256=_assurance(
            RuntimeAssuranceKind.WRITER_EXCLUSIVITY
        ).runtime_assurance_receipt_sha256,
        utc_semantics_assurance_receipt_sha256=(
            _assurance(RuntimeAssuranceKind.UTC_SEMANTICS).runtime_assurance_receipt_sha256
            if event_time_source_type == "datetime2(6)"
            else None
        ),
        ddl_freeze_assurance_receipt_sha256=_assurance(
            RuntimeAssuranceKind.DDL_FREEZE
        ).runtime_assurance_receipt_sha256,
        artifact_authority_sha256=semantic_refresh_artifact_authority_sha256(_artifact_authority()),
        seal_policy_sha256=_A,
        created_at="2026-08-07T12:35:00Z",
        issuer_authority="mssql-protected-control/production",
        issuer_attestation_sha256=_B,
        issuer_signature_sha256=_C,
    )


def _manifest() -> SemanticRefreshSealedArtifactManifest:
    seal = _seal()
    assert seal.workflow_id == "daily_events"
    return SemanticRefreshSealedArtifactManifest.build(
        seal_authorization=seal,
        artifact_prefix="semantic-refresh/operation-aaaa/attempt-bbbb",
        provider="s3",
        encryption_policy_sha256=_C,
        retention_policy_sha256=_D,
        chunks=(SemanticRefreshArtifactChunk(1, "part-0001.parquet", "version-1", _A, 32, 5),),
    )


@pytest.mark.parametrize("kind", list(RuntimeAssuranceKind))
def test_runtime_assurance_is_exact_timed_signed_and_kind_closed(kind: RuntimeAssuranceKind) -> None:
    receipt = _assurance(kind)
    schemas = semantic_refresh_contract_schemas()
    assert receipt.status is RuntimeAssuranceStatus.CERTIFIED
    assert SemanticRefreshRuntimeAssuranceReceipt.from_mapping(receipt.to_dict()) == receipt
    Draft202012Validator(schemas[receipt.schema]).validate(receipt.to_dict())

    instant = datetime(2026, 8, 8, tzinfo=timezone.utc)  # noqa: UP017
    assert receipt.authorizes(
        instant,
        trusted_digest=receipt.runtime_assurance_receipt_sha256,
        expected_subject=receipt.subject,
    )
    assert not receipt.authorizes(instant, trusted_digest=_A, expected_subject=receipt.subject)
    assert not receipt.authorizes(
        instant,
        trusted_digest=receipt.runtime_assurance_receipt_sha256,
        expected_subject=replace(receipt.subject, environment="staging"),
    )
    assert not receipt.authorizes(
        datetime(2026, 10, 8, tzinfo=timezone.utc),  # noqa: UP017
        trusted_digest=receipt.runtime_assurance_receipt_sha256,
        expected_subject=receipt.subject,
    )


def test_writer_assurance_requires_all_positive_organizational_controls() -> None:
    writer = _assurance(RuntimeAssuranceKind.WRITER_EXCLUSIVITY)
    assert writer.engine_acl_proof_status == "PASS"
    assert writer.platform_allowlist_status == "PASS"
    assert writer.external_job_inventory_status == "PASS"
    assert writer.organizational_control_status == "CERTIFIED"

    with pytest.raises(ValueError, match="writer exclusivity"):
        SemanticRefreshRuntimeAssuranceReceipt.build(
            subject=_subject(RuntimeAssuranceKind.WRITER_EXCLUSIVITY),
            producer_version="dpone-runtime/0.73.0",
            transformation_version="semantic-refresh-v2",
            acl_policy_version="mssql-writer-policy/2026-08-08",
            effective_from="2026-08-07T00:00:00Z",
            expires_at="2026-09-07T00:00:00Z",
            evidence_sha256=_C,
            runtime_assurance_policy_sha256=_A,
            approver_authority="dpone-platform-control",
            approver_attestation_sha256=_B,
            approver_signature_sha256=_C,
        )


def test_effective_key_template_identity_is_acyclic_and_explicit_about_utc() -> None:
    template = EffectiveKeyTemplateColumn(
        name="occurred_at",
        source_type="datetime2(6)",
        target_type="DateTime64(6,'UTC')",
        domain_min=DATETIME_DOMAIN_MIN,
        domain_max=DATETIME_DOMAIN_MAX,
    )
    first = EffectiveKeyColumn(
        name=template.name,
        source_type=template.source_type,
        target_type=template.target_type,
        domain_min=template.domain_min,
        domain_max=template.domain_max,
        utc_assurance_sha256=_A,
    )
    second = replace(first, utc_assurance_sha256=_B)
    assert template.utc_assurance_required is True
    assert "utc_assurance_sha256" not in template.to_dict()
    assert semantic_refresh_effective_key_template_sha256((template,)) == (
        semantic_refresh_effective_key_template_sha256((first,))
    )
    assert semantic_refresh_effective_key_template_sha256((first,)) == (
        semantic_refresh_effective_key_template_sha256((second,))
    )
    assert semantic_refresh_effective_key_mapping_sha256((first,)) != (
        semantic_refresh_effective_key_mapping_sha256((second,))
    )
    with pytest.raises(ValueError, match="unknown"):
        EffectiveKeyTemplateColumn.from_mapping({**template.to_dict(), "utc_assurance_sha256": _A})


def test_artifact_authority_identity_has_one_closed_schema_tagged_preimage() -> None:
    authority = _artifact_authority()
    preimage = semantic_refresh_artifact_authority_preimage(authority)
    assert preimage == {"schema": ARTIFACT_AUTHORITY_IDENTITY_SCHEMA, **authority}
    assert semantic_refresh_artifact_authority_sha256(authority) == (
        "sha256:16176e1387aaf3227e626efce889d200cedfd0847f7789f4e6f9c6282dfd53b5"
    )
    with pytest.raises(ValueError, match="unknown"):
        semantic_refresh_artifact_authority_sha256({**authority, "local_digest": _A})


def test_runtime_assurance_subject_and_revocation_fail_closed() -> None:
    with pytest.raises(ValueError, match="column"):
        replace(_subject(RuntimeAssuranceKind.UTC_SEMANTICS), column_name=None)
    with pytest.raises(ValueError, match="target"):
        replace(_subject(RuntimeAssuranceKind.WRITER_EXCLUSIVITY), column_name="occurred_at")

    active = _assurance(RuntimeAssuranceKind.WRITER_EXCLUSIVITY)
    revoked = SemanticRefreshRuntimeAssuranceReceipt.build_revocation(
        subject=active.subject,
        revoked_receipt_sha256=active.runtime_assurance_receipt_sha256,
        revoked_at="2026-08-08T00:00:00Z",
        revocation_reason="writer inventory changed",
        producer_version=active.producer_version,
        transformation_version=active.transformation_version,
        acl_policy_version=active.acl_policy_version,
        runtime_assurance_policy_sha256=active.runtime_assurance_policy_sha256,
        approver_authority=active.approver_authority,
        approver_attestation_sha256=_B,
        approver_signature_sha256=_C,
    )
    assert revoked.status is RuntimeAssuranceStatus.REVOKED
    # A trust resolver that observes the revocation withholds the original digest.
    assert not active.authorizes(
        datetime(2026, 8, 8, tzinfo=timezone.utc),  # noqa: UP017
        trusted_digest=None,
        expected_subject=active.subject,
    )
    assert not revoked.authorizes(
        datetime(2026, 8, 8, tzinfo=timezone.utc),  # noqa: UP017
        trusted_digest=revoked.runtime_assurance_receipt_sha256,
        expected_subject=revoked.subject,
    )
    Draft202012Validator(semantic_refresh_contract_schemas()[revoked.schema]).validate(revoked.to_dict())


def test_seal_authorization_and_manifest_bind_exact_reconciled_authority() -> None:
    seal = _seal()
    assert seal.journal_state == "PREPARING"
    assert seal.mssql_outcome == "COMMITTED_WITH_IMAGES"
    assert SemanticRefreshSealAuthorizationReceipt.from_mapping(seal.to_dict()) == seal
    Draft202012Validator(semantic_refresh_contract_schemas()[seal.schema]).validate(seal.to_dict())

    manifest = _manifest()
    assert manifest.seal_authorization_receipt_sha256 == seal.seal_authorization_receipt_sha256
    assert manifest.model_build_receipt_sha256 == seal.model_build_receipt_sha256
    assert manifest.matches_seal_authorization(seal)
    assert not manifest.matches_seal_authorization(_seal(effective_key_mapping_sha256=_C))
    assert not {"journal_receipt_sha256", "source_receipt_sha256"} & manifest.to_dict().keys()

    stale = {**seal.to_dict(), "mssql_outcome": "ROLLED_BACK"}
    with pytest.raises(ValueError, match="COMMITTED_WITH_IMAGES"):
        SemanticRefreshSealAuthorizationReceipt.from_mapping(stale)

    date_seal = _seal(event_time_source_type="date")
    assert date_seal.utc_semantics_assurance_receipt_sha256 is None
    invalid_date = {
        **date_seal.to_dict(),
        "utc_semantics_assurance_receipt_sha256": _A,
    }
    with pytest.raises(ValueError, match="forbids UTC"):
        SemanticRefreshSealAuthorizationReceipt.from_mapping(invalid_date)


def test_assurance_schemas_reject_unknown_or_ambiguous_authority() -> None:
    schemas = semantic_refresh_contract_schemas()
    runtime = {**_assurance(RuntimeAssuranceKind.UTC_SEMANTICS).to_dict(), "local_check": "PASS"}
    with pytest.raises(ValidationError):
        Draft202012Validator(schemas[_assurance(RuntimeAssuranceKind.UTC_SEMANTICS).schema]).validate(runtime)

    manifest = _manifest().to_dict()
    manifest["source_receipt_sha256"] = manifest.pop("model_build_receipt_sha256")
    with pytest.raises(ValidationError):
        Draft202012Validator(schemas[_manifest().schema]).validate(manifest)


def test_assurance_schema_bytes_and_golden_vectors_are_stable() -> None:
    documents = {
        "writer_exclusivity": _assurance(RuntimeAssuranceKind.WRITER_EXCLUSIVITY),
        "utc_semantics": _assurance(RuntimeAssuranceKind.UTC_SEMANTICS),
        "ddl_freeze": _assurance(RuntimeAssuranceKind.DDL_FREEZE),
        "seal_authorization": _seal(),
        "sealed_manifest": _manifest(),
    }
    schemas = semantic_refresh_contract_schemas()
    assert len(schemas) == 22
    for document in documents.values():
        checked = Path("docs/schemas/dbt", f"{document.schema}.schema.json")
        assert checked.read_bytes() == render_semantic_refresh_schema(document.schema)

    expected = json.loads(
        Path("tests/fixtures/semantic-refresh-v2/contracts/assurance-golden-v1.json").read_text(encoding="utf-8")
    )
    assert {name: document.to_dict() for name, document in documents.items()} == expected
