from __future__ import annotations

import json

from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
    MigrationBundlePolicyProfileRegistry,
)


def test_profile_registry_resolves_governance_profiles_deterministically() -> None:
    registry = MigrationBundlePolicyProfileRegistry()

    assert registry.names() == ("advisory", "pr_review", "stage_certified", "prod_strict", "regulated")
    assert registry.resolve("pr_review").require_attestation is True
    assert registry.resolve("prod_strict").required_artifacts == (
        "migration_pack",
        "impact_plan",
        "approval",
        "environment_contract",
        "certification",
        "promotion",
    )
    assert registry.resolve("regulated").approval.require_approved_by is True
    assert registry.resolve("regulated").approval.require_not_expired is True


def test_custom_policy_merges_over_profile_defaults() -> None:
    policy = MigrationBundlePolicyOptions.resolve(
        profile="prod_strict",
        policy_payload={
            "schema_version": "dpone.schema_migration_bundle_policy.v1",
            "profile": "prod_strict",
            "required_artifacts": ["migration_pack"],
            "fail_on_warnings": False,
            "approval": {"require_not_expired": True},
        },
        target_environment="stage",
    )

    assert policy.profile == "prod_strict"
    assert policy.required_artifacts == ("migration_pack",)
    assert policy.fail_on_warnings is False
    assert policy.target_environment == "stage"
    assert policy.approval.require_approved_by is False
    assert policy.approval.require_not_expired is True


def test_pr_review_blocks_missing_attestation_and_required_artifacts() -> None:
    pack = _pack()
    artifact = _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True)
    bundle = MigrationBundleBuilder().build(artifacts=(artifact,), attest=False)

    decision = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "blocked", "blockers": ["migration_bundle.attestation_missing"], "warnings": []},
        policy=MigrationBundlePolicyProfileRegistry().resolve("pr_review"),
        artifact_payloads={"migration_pack": artifact.payload},
    )

    assert decision["schema_version"] == "dpone.schema_migration_bundle_gate.v1"
    assert decision["status"] == "blocked"
    assert "migration_bundle_gate.attestation_required" in decision["blockers"]
    assert "migration_bundle_gate.required_artifact_missing:impact_plan" in decision["blockers"]


def test_impact_required_risk_without_approval_blocks() -> None:
    pack = _pack()
    impact = {
        "schema_version": "dpone.schema_impact_plan.v1",
        "pack_id": pack.pack_id,
        "impact_plan_id": "sha256:" + "1" * 64,
        "required_approvals": ["compatibility_breaking"],
        "blockers": [],
        "warnings": [],
    }
    artifacts = (
        _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
        _artifact("impact_plan", "impact.json", impact, required=False),
    )
    bundle = MigrationBundleBuilder().build(artifacts=artifacts, attest=True)

    decision = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=MigrationBundlePolicyProfileRegistry().resolve("pr_review"),
        artifact_payloads={item.kind: item.payload for item in artifacts},
    )

    assert decision["status"] == "blocked"
    assert "migration_bundle_gate.approval_required:compatibility_breaking" in decision["blockers"]


def test_warnings_block_only_when_policy_fails_on_warnings() -> None:
    pack = _pack()
    impact = {
        "schema_version": "dpone.schema_impact_plan.v1",
        "pack_id": pack.pack_id,
        "impact_plan_id": "sha256:" + "2" * 64,
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
    artifacts = (
        _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
        _artifact("impact_plan", "impact.json", impact, required=False),
        _artifact("approval", "approval.json", approval, required=False),
    )
    bundle = MigrationBundleBuilder().build(artifacts=artifacts, attest=True)
    payloads = {item.kind: item.payload for item in artifacts}

    pr_decision = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=MigrationBundlePolicyProfileRegistry().resolve("pr_review"),
        artifact_payloads=payloads,
    )
    strict_decision = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=MigrationBundlePolicyOptions.resolve(
            profile="prod_strict",
            policy_payload={"required_artifacts": ["migration_pack", "impact_plan", "approval"]},
        ),
        artifact_payloads=payloads,
    )

    assert pr_decision["status"] == "warning"
    assert strict_decision["status"] == "blocked"
    assert "migration_bundle_gate.warnings_present" in strict_decision["blockers"]


def test_regulated_profile_blocks_expired_approval_and_missing_actor() -> None:
    pack = _pack()
    impact = {
        "schema_version": "dpone.schema_impact_plan.v1",
        "pack_id": pack.pack_id,
        "impact_plan_id": "sha256:" + "3" * 64,
        "required_approvals": ["shadow_cutover"],
        "blockers": [],
        "warnings": [],
    }
    approval = {
        "pack_id": pack.pack_id,
        "impact_plan_id": impact["impact_plan_id"],
        "approved_risks": ["shadow_cutover"],
        "expires_at": "2026-01-01T00:00:00Z",
    }
    artifacts = (
        _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
        _artifact("impact_plan", "impact.json", impact, required=False),
        _artifact("approval", "approval.json", approval, required=False),
    )
    bundle = MigrationBundleBuilder().build(artifacts=artifacts, attest=True)

    decision = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=MigrationBundlePolicyOptions.resolve(
            profile="regulated",
            policy_payload={"required_artifacts": ["migration_pack", "impact_plan", "approval"]},
        ),
        artifact_payloads={item.kind: item.payload for item in artifacts},
    )

    assert decision["status"] == "blocked"
    assert "migration_bundle_gate.approved_by_required" in decision["blockers"]
    assert "migration_bundle_gate.approval_expired" in decision["blockers"]


def test_promotion_target_environment_mismatch_blocks() -> None:
    pack = _pack()
    promotion = {
        "schema_version": "dpone.schema_migration_promotion.v1",
        "status": "promoted",
        "pack_id": pack.pack_id,
        "from_environment": "dev",
        "to_environment": "stage",
        "certification_id": "sha256:" + "4" * 64,
        "promotion_id": "sha256:" + "5" * 64,
    }
    artifacts = (
        _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
        _artifact("promotion", "promotion.json", promotion, required=False),
    )
    bundle = MigrationBundleBuilder().build(artifacts=artifacts, attest=True)

    decision = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=MigrationBundlePolicyOptions.resolve(
            profile="prod_strict",
            policy_payload={"required_artifacts": ["migration_pack", "promotion"]},
            target_environment="prod",
        ),
        artifact_payloads={item.kind: item.payload for item in artifacts},
    )

    assert decision["status"] == "blocked"
    assert "migration_bundle_gate.target_environment_mismatch:stage:prod" in decision["blockers"]


def test_gate_id_is_stable_for_same_inputs() -> None:
    pack = _pack()
    artifact = _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True)
    bundle = MigrationBundleBuilder().build(artifacts=(artifact,), attest=True)
    evaluator = MigrationBundlePolicyEvaluator()
    policy = MigrationBundlePolicyProfileRegistry().resolve("advisory")

    first = evaluator.evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=policy,
        artifact_payloads={"migration_pack": artifact.payload},
    )
    second = evaluator.evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=policy,
        artifact_payloads={"migration_pack": artifact.payload},
    )

    assert first["gate_id"] == second["gate_id"]
    assert first["status"] == "allowed"


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
