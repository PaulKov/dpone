from __future__ import annotations

import json

from dpone.readiness.data_product_authority import AuthorityGate
from dpone.readiness.data_product_policy import DataProductPolicyEvaluator, DataProductPolicyGate
from dpone.readiness.data_product_policy_waivers import WaiverApprover, WaiverRequestBuilder
from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
)
from dpone.readiness.schema_migration_evidence_registry import MigrationEvidenceRecorder


def test_policy_waiver_and_gate_require_authority_when_enabled() -> None:
    evaluation = DataProductPolicyEvaluator().evaluate(
        manifest=_policy_manifest(),
        evidence={"data_product_error_budget_gate": _artifact("data_product_error_budget_gate", "allowed")},
    )
    request = WaiverRequestBuilder().request(
        evaluation=evaluation,
        rule_id="require_assertion_gate",
        reason="approved migration window",
        expires_at="2026-07-12T00:00:00Z",
        requested_by="alice",
    )
    authority_check = {
        "schema_version": "dpone.data_product_authority_check.v1",
        "authority_check_id": "sha256:" + "5" * 64,
        "status": "allowed",
        "actor": "data-governance",
        "action": "policy_waiver.approve",
        "blockers": [],
        "warnings": [],
    }
    quorum = {
        "schema_version": "dpone.data_product_approval_quorum.v1",
        "approval_quorum_id": "sha256:" + "6" * 64,
        "status": "allowed",
        "approved_actors": ["bob", "data-governance"],
        "satisfied_roles": ["data_owner", "governance_approver"],
        "blockers": [],
        "warnings": [],
    }
    waiver = WaiverApprover().approve(
        request=request,
        actor="data-governance",
        approval={"ticket": "GOV-123", "approved_by": "data-governance"},
        authority_check=authority_check,
        approval_quorum=quorum,
        approved_at="2026-06-28T12:00:00Z",
    )
    authority_gate = AuthorityGate().evaluate(
        authority_check=authority_check,
        approval_quorum=quorum,
        signatures=(
            {
                "schema_version": "dpone.data_product_evidence_signature.v1",
                "evidence_signature_id": "sha256:" + "7" * 64,
                "status": "signed",
                "blockers": [],
                "warnings": [],
            },
        ),
        profile="prod_strict",
    )
    missing_gate = DataProductPolicyGate().evaluate(
        evaluation=evaluation,
        waivers=(waiver,),
        profile="prod_strict",
        observed_at="2026-06-28T12:00:00Z",
    )
    gated = DataProductPolicyGate().evaluate(
        evaluation=evaluation,
        waivers=(waiver,),
        authority_gate=authority_gate,
        profile="prod_strict",
        observed_at="2026-06-28T12:00:00Z",
    )

    assert evaluation["authority_policy"]["enabled"] is True
    assert waiver["authority_evidence"]["authority_check_id"] == authority_check["authority_check_id"]
    assert waiver["authority_evidence"]["approval_quorum_id"] == quorum["approval_quorum_id"]
    assert missing_gate["status"] == "blocked"
    assert "data_product_policy.authority_gate_required" in missing_gate["blockers"]
    assert gated["status"] == "waived"


def test_bundle_policy_and_registry_accept_authority_artifacts() -> None:
    pack = _pack()
    gate = {
        "schema_version": "dpone.data_product_authority_gate.v1",
        "authority_gate_id": "sha256:" + "1" * 64,
        "status": "allowed",
        "profile": "prod_strict",
        "pack_id": pack.pack_id,
        "blockers": [],
        "warnings": [],
    }
    quorum = {
        "schema_version": "dpone.data_product_approval_quorum.v1",
        "approval_quorum_id": "sha256:" + "2" * 64,
        "status": "allowed",
        "pack_id": pack.pack_id,
        "blockers": [],
        "warnings": [],
    }
    signature = {
        "schema_version": "dpone.data_product_evidence_signature.v1",
        "evidence_signature_id": "sha256:" + "3" * 64,
        "status": "signed",
        "pack_id": pack.pack_id,
        "blockers": [],
        "warnings": [],
    }
    artifacts = (
        _bundle_artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
        _bundle_artifact("data_product_authority_gate", "authority-gate.json", gate, required=False),
        _bundle_artifact("data_product_approval_quorum", "approval-quorum.json", quorum, required=False),
        _bundle_artifact("data_product_evidence_signature", "evidence-signature.json", signature, required=False),
    )

    bundle = MigrationBundleBuilder().build(artifacts=artifacts, attest=True)
    decision = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=MigrationBundlePolicyOptions.resolve(
            profile="prod_strict",
            policy_payload={"required_artifacts": ["migration_pack", "data_product_authority_gate"]},
        ),
        artifact_payloads={item.kind: item.payload for item in artifacts},
    )
    record = MigrationEvidenceRecorder().build_record(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        gate=None,
        trust_verification=None,
        diff=None,
        data_product_authority_gate=gate,
        data_product_approval_quorum=quorum,
        data_product_evidence_signature=signature,
        environment="prod",
        stage="authority_gate_passed",
    )

    assert bundle["summary"]["data_product_authority_gate_id"] == gate["authority_gate_id"]
    assert decision["status"] == "allowed"
    assert record["status"] == "ready"
    assert {ref["kind"] for ref in record["artifact_refs"]} >= {
        "data_product_authority_gate",
        "data_product_approval_quorum",
        "data_product_evidence_signature",
    }


def _policy_manifest() -> dict:
    return {
        "sink": {
            "options": {
                "data_product": {
                    "id": "analytics.orders",
                    "owner": "data-platform",
                    "tier": "gold",
                    "criticality": "high",
                    "policy": {
                        "enabled": True,
                        "mode": "gate",
                        "profile": "prod_strict",
                        "packs": [
                            {
                                "id": "production_release_guardrails",
                                "version": "1.0.0",
                                "owner": "data-governance",
                                "rules": [
                                    {
                                        "id": "require_assertion_gate",
                                        "severity": "critical",
                                        "require_artifacts": ["data_product_assertion_gate"],
                                    },
                                    {
                                        "id": "block_fast_burn_release",
                                        "severity": "critical",
                                        "require_status": {"data_product_error_budget_gate": ["allowed", "warning"]},
                                    },
                                ],
                            }
                        ],
                        "waivers": {"enabled": True, "max_duration_days": 14},
                    },
                    "authority": {"enabled": True, "mode": "gate", "profile": "prod_strict"},
                }
            }
        }
    }


def _artifact(kind: str, status: str) -> dict:
    return {
        "schema_version": f"dpone.{kind}.v1",
        "status": status,
        "blockers": [] if status in {"allowed", "warning", "passed"} else [f"{kind}.blocked"],
        "warnings": ["warning"] if status == "warning" else [],
    }


def _pack() -> MigrationPack:
    return MigrationPack(
        pack_id="sha256:" + "a" * 64,
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired_fingerprint="sha256:" + "b" * 64,
        actual_fingerprint="sha256:" + "c" * 64,
        desired={"engine": "MergeTree", "order_by": ["order_id"]},
        actual={"engine": "MergeTree", "order_by": ["order_id"]},
        strategy="direct",
        changes=({"kind": "table_setting", "setting": "index_granularity"},),
        ddl=("ALTER TABLE analytics.orders MODIFY SETTING index_granularity = 8192",),
        warnings=(),
        blockers=(),
    )


def _bundle_artifact(kind: str, path: str, payload: dict, *, required: bool) -> MigrationEvidenceArtifact:
    return MigrationEvidenceArtifact.from_bytes(
        kind=kind,
        path=path,
        content=json.dumps(payload, sort_keys=True).encode("utf-8"),
        required=required,
    )
