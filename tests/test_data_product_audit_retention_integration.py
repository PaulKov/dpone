from __future__ import annotations

import json

from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import MigrationBundlePolicyEvaluator, MigrationBundlePolicyOptions
from dpone.readiness.schema_migration_evidence_registry import MigrationEvidenceRecorder


def test_audit_retention_artifacts_integrate_with_bundle_policy_and_registry() -> None:
    archive_verification = {
        "schema_version": "dpone.data_product_audit_archive_verification.v1",
        "audit_archive_verification_id": "sha256:" + "7" * 64,
        "audit_archive_run_id": "sha256:" + "8" * 64,
        "status": "verified",
        "product_id": "analytics.orders",
        "blockers": [],
        "warnings": [],
    }
    retention_plan = {
        "schema_version": "dpone.data_product_audit_retention_plan.v1",
        "audit_retention_plan_id": "sha256:" + "9" * 64,
        "status": "ready",
        "product_id": "analytics.orders",
        "blockers": [],
        "warnings": [],
    }
    legal_hold = {
        "schema_version": "dpone.data_product_legal_hold.v1",
        "legal_hold_id": "sha256:" + "a" * 64,
        "status": "held",
        "product_id": "analytics.orders",
        "blockers": [],
        "warnings": [],
    }

    pack = _pack().to_dict(command="plan")
    artifacts = (
        _artifact("migration_pack", "pack.json", pack, required=True),
        _artifact("data_product_audit_archive_verification", "archive-verification.json", archive_verification),
        _artifact("data_product_audit_retention_plan", "retention-plan.json", retention_plan),
        _artifact("data_product_legal_hold", "legal-hold.json", legal_hold),
    )
    bundle = MigrationBundleBuilder().build(artifacts=artifacts, attest=True)
    gate = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "verified", "blockers": [], "warnings": []},
        policy=MigrationBundlePolicyOptions.resolve(
            profile="prod_strict",
            policy_payload={
                "required_artifacts": ["migration_pack", "data_product_audit_archive_verification"],
            },
        ),
        artifact_payloads={
            "data_product_audit_archive_verification": archive_verification,
            "data_product_audit_retention_plan": retention_plan,
            "data_product_legal_hold": legal_hold,
        },
    )
    record = MigrationEvidenceRecorder().build_record(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        gate=None,
        trust_verification=None,
        diff=None,
        environment="prod",
        stage="audit_archive_verified",
        data_product_audit_archive_verification=archive_verification,
        data_product_audit_retention_plan=retention_plan,
        data_product_legal_hold=legal_hold,
    )

    assert gate["status"] == "allowed"
    assert (
        bundle["summary"]["data_product_audit_archive_verification_id"]
        == archive_verification["audit_archive_verification_id"]
    )
    assert {
        "data_product_audit_archive_verification",
        "data_product_audit_retention_plan",
        "data_product_legal_hold",
    } <= {ref["kind"] for ref in record["artifact_refs"]}
    assert record["stage"] == "audit_archive_verified"


def _pack() -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders", "order_by": ["id"]},
        actual={"sink_type": "clickhouse", "table": "analytics.orders", "order_by": ["id"]},
        changes=({"change_type": "table_setting", "path": "table_settings.index_granularity"},),
        ddl=("ALTER TABLE analytics.orders MODIFY SETTING index_granularity = 8192",),
        strategy="online_safe",
    )


def _artifact(kind: str, path: str, payload: dict, *, required: bool = False) -> MigrationEvidenceArtifact:
    content = json.dumps(payload, sort_keys=True).encode("utf-8")
    return MigrationEvidenceArtifact.from_bytes(kind=kind, path=path, content=content, required=required)
