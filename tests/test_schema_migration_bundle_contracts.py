from __future__ import annotations

import json

from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_migration_bundle import (
    MigrationBundleBuilder,
    MigrationBundleVerifier,
    MigrationEvidenceArtifact,
    MigrationReviewRenderer,
)


def test_bundle_build_with_pack_only_is_ready_and_attested() -> None:
    pack = _pack()
    artifact = _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True)

    bundle = MigrationBundleBuilder().build(artifacts=(artifact,), attest=True)

    assert bundle["schema_version"] == "dpone.schema_migration_bundle.v1"
    assert bundle["status"] == "ready"
    assert bundle["pack_id"] == pack.pack_id
    assert bundle["target"] == {"sink_type": "clickhouse", "table": "analytics.orders"}
    assert bundle["summary"]["strategy"] == "online_safe"
    assert bundle["summary"]["changes_count"] == 1
    assert bundle["artifacts"][0]["sha256"].startswith("sha256:")
    assert bundle["attestation"]["bundle_digest"].startswith("sha256:")

    verified = MigrationBundleVerifier().verify(
        bundle=bundle,
        artifact_bytes={"pack.json": artifact.content},
        require_attestation=True,
    )

    assert verified["schema_version"] == "dpone.schema_migration_bundle_verification.v1"
    assert verified["status"] == "passed"
    assert verified["blockers"] == []


def test_bundle_blocks_mismatched_impact_and_missing_required_approval() -> None:
    pack = _pack()
    impact = {
        "schema_version": "dpone.schema_impact_plan.v1",
        "pack_id": "sha256:" + "0" * 64,
        "impact_plan_id": "sha256:" + "1" * 64,
        "required_approvals": ["compatibility_breaking"],
        "blockers": [],
        "warnings": [],
    }

    bundle = MigrationBundleBuilder().build(
        artifacts=(
            _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
            _artifact("impact_plan", "impact.json", impact, required=False),
        )
    )

    assert bundle["status"] == "blocked"
    assert "migration_bundle.impact_pack_id_mismatch" in bundle["blockers"]
    assert "migration_bundle.approval_required:compatibility_breaking" in bundle["blockers"]
    assert bundle["summary"]["required_approvals"] == ["compatibility_breaking"]


def test_bundle_blocks_invalid_certification_promotion_chain() -> None:
    pack = _pack()
    certificate = {
        "schema_version": "dpone.schema_migration_environment_certification.v1",
        "status": "certified",
        "environment": "stage",
        "pack_id": pack.pack_id,
        "certification_id": "sha256:" + "2" * 64,
    }
    promotion = {
        "schema_version": "dpone.schema_migration_promotion.v1",
        "status": "promoted",
        "pack_id": pack.pack_id,
        "from_environment": "stage",
        "to_environment": "prod",
        "certification_id": "sha256:" + "3" * 64,
        "promotion_id": "sha256:" + "4" * 64,
    }

    bundle = MigrationBundleBuilder().build(
        artifacts=(
            _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
            _artifact("certification", "cert.json", certificate, required=False),
            _artifact("promotion", "promotion.json", promotion, required=False),
        )
    )

    assert bundle["status"] == "blocked"
    assert "migration_bundle.certification_id_mismatch" in bundle["blockers"]
    assert bundle["summary"]["from_environment"] == "stage"
    assert bundle["summary"]["to_environment"] == "prod"


def test_bundle_verify_blocks_digest_mismatch_and_missing_attestation() -> None:
    pack = _pack()
    artifact = _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True)
    bundle = MigrationBundleBuilder().build(artifacts=(artifact,), attest=False)

    verified = MigrationBundleVerifier().verify(
        bundle=bundle,
        artifact_bytes={"pack.json": b'{"changed":true}'},
        require_attestation=True,
    )

    assert verified["status"] == "blocked"
    assert "migration_bundle.attestation_missing" in verified["blockers"]
    assert "migration_bundle.artifact_digest_mismatch:migration_pack" in verified["blockers"]


def test_review_markdown_contains_pr_mr_evidence_sections() -> None:
    pack = _pack()
    impact = {
        "schema_version": "dpone.schema_impact_plan.v1",
        "pack_id": pack.pack_id,
        "impact_plan_id": "sha256:" + "5" * 64,
        "required_approvals": ["compatibility_breaking"],
        "blockers": [],
        "warnings": ["finance.daily_margin reads amount"],
    }
    approval = {
        "pack_id": pack.pack_id,
        "impact_plan_id": impact["impact_plan_id"],
        "approved_by": "finance-data-owner",
        "approved_risks": ["compatibility_breaking"],
    }
    promotion = {
        "schema_version": "dpone.schema_migration_promotion.v1",
        "status": "promoted",
        "pack_id": pack.pack_id,
        "from_environment": "stage",
        "to_environment": "prod",
        "certification_id": "sha256:" + "6" * 64,
        "promotion_id": "sha256:" + "7" * 64,
    }
    bundle = MigrationBundleBuilder().build(
        artifacts=(
            _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
            _artifact("impact_plan", "impact.json", impact, required=False),
            _artifact("promotion", "promotion.json", promotion, required=False),
            _artifact("approval", "approval.yaml", approval, required=False),
        ),
        attest=True,
    )

    review = MigrationReviewRenderer().render_markdown(bundle)

    assert "# Schema Migration Review" in review
    assert pack.pack_id in review
    assert "analytics.orders" in review
    assert "compatibility_breaking" in review
    assert "stage -> prod" in review
    assert "dpone schema migration bundle verify" in review


def _pack() -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders", "order_by": ["id"]},
        actual={"sink_type": "clickhouse", "table": "analytics.orders", "order_by": ["id"]},
        changes=({"change_type": "table_setting", "path": "table_settings.index_granularity"},),
        ddl=("ALTER TABLE analytics.orders MODIFY SETTING index_granularity = 8192",),
        strategy="online_safe",
    )


def _artifact(kind: str, path: str, payload: dict[str, object], *, required: bool) -> MigrationEvidenceArtifact:
    content = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return MigrationEvidenceArtifact.from_bytes(kind=kind, path=path, content=content, required=required)
