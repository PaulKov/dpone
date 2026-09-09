from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

from dpone.readiness.data_product_policy import (
    DataProductPolicyEvaluator,
    DataProductPolicyGate,
)
from dpone.readiness.data_product_policy_waivers import (
    WaiverApprover,
    WaiverRequestBuilder,
)
from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
)
from dpone.readiness.schema_migration_evidence_registry import MigrationEvidenceRecorder


def test_disabled_policy_emits_noop_evaluation() -> None:
    evaluation = DataProductPolicyEvaluator().evaluate(manifest=_manifest(enabled=False), evidence={})

    assert evaluation["schema_version"] == "dpone.data_product_policy_evaluation.v1"
    assert evaluation["status"] == "disabled"
    assert evaluation["product"]["id"] == "analytics.orders"
    assert evaluation["rules"] == []
    assert evaluation["blockers"] == []


def test_policy_evaluation_checks_required_artifacts_and_allowed_statuses() -> None:
    evaluator = DataProductPolicyEvaluator()
    missing = evaluator.evaluate(manifest=_manifest(), evidence={})
    passing = evaluator.evaluate(
        manifest=_manifest(),
        evidence={
            "data_product_assertion_gate": _artifact("data_product_assertion_gate", "allowed"),
            "data_product_error_budget_gate": _artifact("data_product_error_budget_gate", "warning"),
        },
    )
    blocked = evaluator.evaluate(
        manifest=_manifest(),
        evidence={
            "data_product_assertion_gate": _artifact("data_product_assertion_gate", "blocked"),
            "data_product_error_budget_gate": _artifact("data_product_error_budget_gate", "blocked"),
        },
    )

    assert missing["status"] == "blocked"
    assert (
        "data_product_policy.required_artifact_missing:require_assertion_gate:data_product_assertion_gate"
        in missing["blockers"]
    )
    assert passing["status"] == "warning"
    assert {rule["status"] for rule in passing["rules"]} == {"passed", "warning"}
    assert blocked["status"] == "blocked"
    assert (
        "data_product_policy.artifact_status_blocked:block_fast_burn_release:data_product_error_budget_gate"
        in blocked["blockers"]
    )
    assert (
        passing["policy_evaluation_id"]
        == evaluator.evaluate(
            manifest=_manifest(),
            evidence={
                "data_product_assertion_gate": _artifact("data_product_assertion_gate", "allowed"),
                "data_product_error_budget_gate": _artifact("data_product_error_budget_gate", "warning"),
            },
        )["policy_evaluation_id"]
    )


def test_policy_gate_profiles_and_valid_waiver() -> None:
    evaluation = DataProductPolicyEvaluator().evaluate(
        manifest=_manifest(),
        evidence={"data_product_error_budget_gate": _artifact("data_product_error_budget_gate", "allowed")},
    )
    request = WaiverRequestBuilder().request(
        evaluation=evaluation,
        rule_id="require_assertion_gate",
        reason="approved migration window",
        expires_at="2026-07-12T00:00:00Z",
    )
    waiver = WaiverApprover().approve(
        request=request,
        actor="data-governance",
        approval={"ticket": "GOV-123", "approved_by": "data-governance"},
        approved_at="2026-06-28T12:00:00Z",
    )
    strict = DataProductPolicyGate().evaluate(
        evaluation=evaluation,
        waivers=(waiver,),
        profile="prod_strict",
        observed_at="2026-06-28T12:00:00Z",
    )
    advisory = DataProductPolicyGate().evaluate(evaluation=evaluation, waivers=(), profile="advisory")

    assert request["schema_version"] == "dpone.data_product_waiver_request.v1"
    assert request["status"] == "requested"
    assert waiver["schema_version"] == "dpone.data_product_waiver.v1"
    assert waiver["status"] == "approved"
    assert strict["schema_version"] == "dpone.data_product_policy_gate.v1"
    assert strict["status"] == "waived"
    assert strict["waived_rules"] == ["require_assertion_gate"]
    assert advisory["status"] == "warning"
    assert advisory["blockers"] == []


def test_expired_and_malformed_waivers_block() -> None:
    evaluation = DataProductPolicyEvaluator().evaluate(
        manifest=_manifest(),
        evidence={"data_product_error_budget_gate": _artifact("data_product_error_budget_gate", "allowed")},
    )
    request = WaiverRequestBuilder().request(
        evaluation=evaluation,
        rule_id="require_assertion_gate",
        reason="approved migration window",
        expires_at="2026-06-20T00:00:00Z",
    )
    expired = WaiverApprover().approve(
        request=request,
        actor="data-governance",
        approval={"ticket": "GOV-123"},
        approved_at="2026-06-19T12:00:00Z",
    )
    malformed = {**expired, "rule_id": "different_rule"}

    gate = DataProductPolicyGate().evaluate(
        evaluation=evaluation,
        waivers=(expired, malformed),
        profile="prod_strict",
        observed_at="2026-06-28T12:00:00Z",
    )

    assert gate["status"] == "blocked"
    assert "data_product_policy.waiver_expired:require_assertion_gate" in gate["blockers"]
    assert "data_product_policy.uncovered_rule:require_assertion_gate" in gate["blockers"]


def test_waiver_approval_blocks_duration_beyond_policy() -> None:
    evaluation = DataProductPolicyEvaluator().evaluate(
        manifest=_manifest(),
        evidence={"data_product_error_budget_gate": _artifact("data_product_error_budget_gate", "allowed")},
    )
    request = WaiverRequestBuilder().request(
        evaluation=evaluation,
        rule_id="require_assertion_gate",
        reason="approved migration window",
        expires_at="2026-07-20T00:00:00Z",
    )

    waiver = WaiverApprover().approve(
        request=request,
        actor="data-governance",
        approval={"ticket": "GOV-123"},
        approved_at="2026-06-28T12:00:00Z",
    )

    assert waiver["status"] == "blocked"
    assert "data_product_policy.waiver_duration_exceeds_policy" in waiver["blockers"]


def test_policy_report_and_public_json_schemas_validate_artifacts() -> None:
    evaluation = DataProductPolicyEvaluator().evaluate(manifest=_manifest(), evidence={})
    request = WaiverRequestBuilder().request(
        evaluation=evaluation,
        rule_id="require_assertion_gate",
        reason="approved migration window",
        expires_at="2026-07-12T00:00:00Z",
    )
    waiver = WaiverApprover().approve(
        request=request,
        actor="data-governance",
        approval={"ticket": "GOV-123"},
        approved_at="2026-06-28T12:00:00Z",
    )
    gate = DataProductPolicyGate().evaluate(
        evaluation=evaluation,
        waivers=(waiver,),
        profile="prod_strict",
        observed_at="2026-06-28T12:00:00Z",
    )
    report = DataProductPolicyGate().report(gate=gate)

    assert report["schema_version"] == "dpone.data_product_policy_report.v1"
    assert "# Data Product Policy Report" in report["markdown"]

    for name, payload in (
        ("data-product-policy-evaluation.schema.json", evaluation),
        ("data-product-waiver-request.schema.json", request),
        ("data-product-waiver.schema.json", waiver),
        ("data-product-policy-gate.schema.json", gate),
        ("data-product-policy-report.schema.json", report),
    ):
        schema = json.loads((Path("docs/schemas/data-product") / name).read_text(encoding="utf-8"))
        Draft202012Validator(schema).validate(payload)


def test_bundle_policy_and_registry_accept_policy_artifacts() -> None:
    pack = _pack()
    gate = {
        "schema_version": "dpone.data_product_policy_gate.v1",
        "policy_gate_id": "sha256:" + "1" * 64,
        "status": "waived",
        "profile": "prod_strict",
        "product_id": "analytics.orders",
        "pack_id": pack.pack_id,
        "bundle_id": None,
        "blockers": [],
        "warnings": [],
    }
    report = {
        "schema_version": "dpone.data_product_policy_report.v1",
        "policy_report_id": "sha256:" + "2" * 64,
        "status": "waived",
        "blockers": [],
        "warnings": [],
    }
    waiver = {
        "schema_version": "dpone.data_product_waiver.v1",
        "waiver_id": "sha256:" + "3" * 64,
        "status": "approved",
        "pack_id": pack.pack_id,
        "blockers": [],
        "warnings": [],
    }
    artifacts = (
        _bundle_artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
        _bundle_artifact("data_product_policy_gate", "policy-gate.json", gate, required=False),
        _bundle_artifact("data_product_policy_report", "policy-report.json", report, required=False),
        _bundle_artifact("data_product_waiver", "waiver.json", waiver, required=False),
    )

    bundle = MigrationBundleBuilder().build(artifacts=artifacts, attest=True)
    decision = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=MigrationBundlePolicyOptions.resolve(
            profile="prod_strict",
            policy_payload={"required_artifacts": ["migration_pack", "data_product_policy_gate"]},
        ),
        artifact_payloads={item.kind: item.payload for item in artifacts},
    )
    record = MigrationEvidenceRecorder().build_record(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        gate=None,
        trust_verification=None,
        diff=None,
        data_product_policy_gate=gate,
        data_product_policy_report=report,
        data_product_waiver=waiver,
        environment="prod",
        stage="policy_gate_passed",
    )

    assert bundle["status"] == "ready"
    assert bundle["summary"]["data_product_policy_gate_id"] == gate["policy_gate_id"]
    assert decision["status"] == "allowed"
    assert record["status"] == "ready"
    assert {ref["kind"] for ref in record["artifact_refs"]} >= {
        "data_product_policy_gate",
        "data_product_policy_report",
        "data_product_waiver",
    }


def _manifest(*, enabled: bool = True) -> dict:
    return {
        "sink": {
            "options": {
                "data_product": {
                    "id": "analytics.orders",
                    "owner": "data-platform",
                    "tier": "gold",
                    "criticality": "high",
                    "policy": {
                        "enabled": enabled,
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
                        "waivers": {
                            "enabled": True,
                            "max_duration_days": 14,
                            "require_owner": True,
                            "require_approval_evidence": True,
                            "expired_policy": "block",
                        },
                    },
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
    target = MigrationTarget(sink_type="clickhouse", table="analytics.orders")
    return MigrationPack(
        pack_id="sha256:" + "a" * 64,
        target=target,
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
