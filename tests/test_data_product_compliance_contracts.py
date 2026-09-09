from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

from dpone.readiness.data_product_compliance import (
    AuditPackageRenderer,
    ComplianceControlEvaluator,
    ComplianceControlPlanner,
    ComplianceGate,
)


def test_disabled_compliance_emits_noop_plan() -> None:
    plan = ComplianceControlPlanner().plan(manifest=_manifest(enabled=False), evidence={})

    assert plan["schema_version"] == "dpone.data_product_compliance_control_plan.v1"
    assert plan["status"] == "disabled"
    assert plan["product"]["id"] == "analytics.orders"
    assert plan["controls"] == []
    assert plan["blockers"] == []


def test_compliance_plan_binds_controls_and_evidence_refs() -> None:
    evidence = {
        "data_product_policy_gate": _artifact("data_product_policy_gate", "waived"),
        "data_product_authority_gate": _artifact("data_product_authority_gate", "allowed"),
        "data_product_evidence_signature": _artifact("data_product_evidence_signature", "signed"),
        "data_product_release_closeout_gate": _artifact("data_product_release_closeout_gate", "allowed"),
    }

    plan = ComplianceControlPlanner().plan(manifest=_manifest(), evidence=evidence)
    repeated = ComplianceControlPlanner().plan(manifest=_manifest(), evidence=evidence)

    assert plan["status"] == "ready"
    assert plan["product"]["id"] == "analytics.orders"
    assert [control["control_id"] for control in plan["controls"]] == [
        "CC7.2-data-quality-release",
        "CC9.2-data-quality-monitoring",
    ]
    assert {ref["kind"] for ref in plan["evidence_refs"]} >= set(evidence)
    assert plan["compliance_plan_id"] == repeated["compliance_plan_id"]


def test_compliance_evaluation_fails_missing_blocked_and_stale_evidence() -> None:
    plan = ComplianceControlPlanner().plan(
        manifest=_manifest(stale_after_seconds=100),
        evidence={
            "data_product_policy_gate": _artifact(
                "data_product_policy_gate", "blocked", recorded_at="2026-06-01T00:00:00Z"
            ),
            "data_product_authority_gate": _artifact(
                "data_product_authority_gate", "allowed", recorded_at="2026-06-01T00:00:00Z"
            ),
        },
    )

    evaluation = ComplianceControlEvaluator().evaluate(plan=plan, observed_at="2026-06-28T12:00:00Z")

    assert evaluation["schema_version"] == "dpone.data_product_compliance_control_evaluation.v1"
    assert evaluation["status"] == "blocked"
    assert (
        "data_product_compliance.artifact_status_blocked:CC7.2-data-quality-release:data_product_policy_gate"
        in (evaluation["blockers"])
    )
    assert (
        "data_product_compliance.required_artifact_missing:CC7.2-data-quality-release:data_product_evidence_signature"
        in (evaluation["blockers"])
    )
    assert (
        "data_product_compliance.evidence_stale:CC7.2-data-quality-release:data_product_authority_gate"
        in (evaluation["blockers"])
    )
    assert {control["status"] for control in evaluation["controls"]} == {"failed"}


def test_compliance_gate_profiles_and_audit_package_rendering() -> None:
    evidence = {
        "data_product_policy_gate": _artifact("data_product_policy_gate", "waived"),
        "data_product_authority_gate": _artifact("data_product_authority_gate", "allowed"),
        "data_product_evidence_signature": _artifact("data_product_evidence_signature", "signed"),
        "data_product_release_closeout_gate": _artifact("data_product_release_closeout_gate", "allowed"),
        "data_product_assertion_gate": _artifact("data_product_assertion_gate", "allowed"),
        "data_product_slo_gate": _artifact("data_product_slo_gate", "allowed"),
        "data_product_error_budget_gate": _artifact("data_product_error_budget_gate", "allowed"),
    }
    evaluation = ComplianceControlEvaluator().evaluate(
        plan=ComplianceControlPlanner().plan(manifest=_manifest(), evidence=evidence),
        observed_at="2026-06-28T12:00:00Z",
    )
    gate = ComplianceGate().evaluate(evaluation=evaluation, profile="regulated")
    advisory = ComplianceGate().evaluate(
        evaluation={
            **evaluation,
            "controls": [{**evaluation["controls"][0], "status": "failed", "severity": "critical"}],
            "blockers": ["synthetic"],
            "status": "blocked",
        },
        profile="advisory",
    )
    package = AuditPackageRenderer().render(gate=gate)

    assert evaluation["status"] == "passed"
    assert gate["schema_version"] == "dpone.data_product_compliance_gate.v1"
    assert gate["status"] == "allowed"
    assert advisory["status"] == "warning"
    assert advisory["blockers"] == []
    assert package["schema_version"] == "dpone.data_product_audit_package.v1"
    assert package["status"] == "allowed"
    assert "# Data Product Audit Evidence Package" in package["markdown"]
    assert (
        gate["compliance_gate_id"]
        == ComplianceGate().evaluate(evaluation=evaluation, profile="regulated")["compliance_gate_id"]
    )


def test_compliance_public_json_schemas_validate_artifacts() -> None:
    evidence = {
        "data_product_policy_gate": _artifact("data_product_policy_gate", "waived"),
        "data_product_authority_gate": _artifact("data_product_authority_gate", "allowed"),
        "data_product_evidence_signature": _artifact("data_product_evidence_signature", "verified"),
        "data_product_release_closeout_gate": _artifact("data_product_release_closeout_gate", "allowed"),
        "data_product_assertion_gate": _artifact("data_product_assertion_gate", "allowed"),
        "data_product_slo_gate": _artifact("data_product_slo_gate", "allowed"),
        "data_product_error_budget_gate": _artifact("data_product_error_budget_gate", "allowed"),
    }
    plan = ComplianceControlPlanner().plan(manifest=_manifest(), evidence=evidence)
    evaluation = ComplianceControlEvaluator().evaluate(plan=plan)
    gate = ComplianceGate().evaluate(evaluation=evaluation, profile="regulated")
    package = AuditPackageRenderer().render(gate=gate)

    for name, payload in (
        ("data-product-compliance-control-plan.schema.json", plan),
        ("data-product-compliance-control-evaluation.schema.json", evaluation),
        ("data-product-compliance-gate.schema.json", gate),
        ("data-product-audit-package.schema.json", package),
    ):
        schema = json.loads((Path("docs/schemas/data-product") / name).read_text(encoding="utf-8"))
        Draft202012Validator(schema).validate(payload)


def _manifest(*, enabled: bool = True, stale_after_seconds: int = 2_592_000) -> dict:
    return {
        "sink": {
            "options": {
                "data_product": {
                    "id": "analytics.orders",
                    "owner": "data-platform",
                    "tier": "gold",
                    "criticality": "high",
                    "compliance": {
                        "enabled": enabled,
                        "mode": "gate",
                        "profile": "regulated",
                        "stale_evidence_policy": "block",
                        "stale_after_seconds": stale_after_seconds,
                        "frameworks": [
                            {
                                "id": "soc2",
                                "version": "2026.1",
                                "owner": "security-governance",
                                "controls": [
                                    {
                                        "id": "CC7.2-data-quality-release",
                                        "title": "Data product release evidence is reviewed and approved",
                                        "severity": "critical",
                                        "require_artifacts": [
                                            "data_product_policy_gate",
                                            "data_product_authority_gate",
                                            "data_product_evidence_signature",
                                            "data_product_release_closeout_gate",
                                        ],
                                        "allowed_statuses": {
                                            "data_product_policy_gate": ["allowed", "warning", "waived"],
                                            "data_product_authority_gate": ["allowed", "warning"],
                                            "data_product_evidence_signature": ["signed", "verified"],
                                            "data_product_release_closeout_gate": ["allowed", "warning"],
                                        },
                                    },
                                    {
                                        "id": "CC9.2-data-quality-monitoring",
                                        "title": "Data product quality and SLOs are continuously evaluated",
                                        "severity": "high",
                                        "require_artifacts": [
                                            "data_product_assertion_gate",
                                            "data_product_slo_gate",
                                            "data_product_error_budget_gate",
                                        ],
                                    },
                                ],
                            }
                        ],
                    },
                }
            }
        }
    }


def _artifact(kind: str, status: str, *, recorded_at: str = "2026-06-28T12:00:00Z") -> dict:
    return {
        "schema_version": f"dpone.{kind}.v1",
        f"{kind.removeprefix('data_product_')}_id": "sha256:" + kind[:1] * 64,
        "status": status,
        "product_id": "analytics.orders",
        "recorded_at": recorded_at,
        "blockers": [f"{kind}.blocked"] if status == "blocked" else [],
        "warnings": [f"{kind}.warning"] if status == "warning" else [],
    }
