from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

from dpone.readiness.data_product_remediation import (
    DataProductRemediationCloseout,
    DataProductRemediationGate,
    DataProductRemediationPlanner,
)
from dpone.readiness.data_product_remediation_rendering import RemediationRenderer


def test_disabled_remediation_emits_noop_plan_gate_and_runbook() -> None:
    plan = DataProductRemediationPlanner().plan(manifest=_manifest(enabled=False), trust_gate={}, trust_snapshot={})
    gate = DataProductRemediationGate().evaluate(plan=plan, profile="prod_strict")
    runbook = RemediationRenderer().runbook(plan=plan)

    assert plan["schema_version"] == "dpone.data_product_remediation_plan.v1"
    assert plan["status"] == "disabled"
    assert plan["actions"] == []
    assert gate["status"] == "allowed"
    assert runbook["status"] == "rendered"
    assert "No remediation actions are required" in runbook["markdown"]


def test_planner_creates_owner_routed_actions_from_blocked_trust_domains() -> None:
    plan = DataProductRemediationPlanner().plan(
        manifest=_manifest(),
        trust_gate=_trust_gate(status="blocked"),
        trust_snapshot=_trust_snapshot(),
        evidence_payloads=[_assertion_gate(status="blocked")],
    )
    gate = DataProductRemediationGate().evaluate(plan=plan, profile="prod_strict")
    runbook = RemediationRenderer().runbook(plan=plan)

    assert plan["status"] == "ready"
    assert plan["summary"] == {"signals": 1, "actions": 1, "critical_actions": 1}
    assert {action["domain"] for action in plan["actions"]} == {"quality"}
    assert all(action["owner"] == "data-platform" for action in plan["actions"])
    assert any(
        "dpone data product assertions evaluate" in command
        for action in plan["actions"]
        for command in action["commands"]
    )
    assert gate["status"] == "allowed"
    assert "# Data Product Remediation Runbook" in runbook["markdown"]


def test_plan_gate_blocks_unmapped_failure_signal_in_strict_profiles() -> None:
    plan = DataProductRemediationPlanner().plan(
        manifest=_manifest(),
        trust_gate={
            "schema_version": "dpone.data_product_trust_gate.v1",
            "status": "blocked",
            "product_id": "analytics.orders",
            "trust_gate_id": "sha256:trust",
            "blockers": ["external_catalog.unmapped_failure"],
            "warnings": [],
        },
        trust_snapshot={},
    )
    gate = DataProductRemediationGate().evaluate(plan=plan, profile="prod_strict")
    advisory = DataProductRemediationGate().evaluate(plan=plan, profile="advisory")

    assert plan["status"] == "blocked"
    assert "data_product_remediation.unmapped_signal:external_catalog.unmapped_failure" in plan["blockers"]
    assert gate["status"] == "blocked"
    assert advisory["status"] == "warning"
    assert advisory["blockers"] == []


def test_closeout_requires_fresh_expected_evidence() -> None:
    planner = DataProductRemediationPlanner()
    plan = planner.plan(
        manifest=_manifest(),
        trust_gate=_trust_gate(status="blocked"),
        trust_snapshot=_trust_snapshot(),
        evidence_payloads=[_assertion_gate(status="blocked")],
    )

    stale = DataProductRemediationCloseout().evaluate(plan=plan, evidence_payloads=[_assertion_gate(status="allowed")])
    fresh = DataProductRemediationCloseout().evaluate(
        plan=plan,
        evidence_payloads=[_assertion_gate(status="allowed", evidence_id="sha256:assertion-new")],
    )

    assert stale["status"] == "blocked"
    assert "data_product_remediation.closeout_stale_evidence:data_product_assertion_gate" in stale["blockers"]
    assert fresh["status"] == "allowed"
    assert fresh["summary"]["closed_actions"] == len(plan["actions"])


def test_remediation_artifacts_are_deterministic_and_schema_valid() -> None:
    first = DataProductRemediationPlanner().plan(
        manifest=_manifest(),
        trust_gate=_trust_gate(status="blocked"),
        trust_snapshot=_trust_snapshot(),
        evidence_payloads=[_assertion_gate(status="blocked")],
    )
    second = DataProductRemediationPlanner().plan(
        manifest=_manifest(),
        trust_gate=_trust_gate(status="blocked"),
        trust_snapshot=_trust_snapshot(),
        evidence_payloads=[_assertion_gate(status="blocked")],
    )
    gate = DataProductRemediationGate().evaluate(plan=first, profile="prod_strict")
    runbook = RemediationRenderer().runbook(plan=first)
    closeout = DataProductRemediationCloseout().evaluate(
        plan=first,
        evidence_payloads=[_assertion_gate(status="allowed", evidence_id="sha256:assertion-new")],
    )
    report = RemediationRenderer().report(gate=gate, closeout=closeout)

    assert first["remediation_plan_id"] == second["remediation_plan_id"]
    for name, payload in (
        ("data-product-remediation-plan.schema.json", first),
        ("data-product-remediation-gate.schema.json", gate),
        ("data-product-remediation-runbook.schema.json", runbook),
        ("data-product-remediation-closeout.schema.json", closeout),
        ("data-product-remediation-report.schema.json", report),
    ):
        schema = json.loads((Path("docs/schemas/data-product") / name).read_text(encoding="utf-8"))
        Draft202012Validator(schema).validate(payload)


def _manifest(*, enabled: bool = True) -> dict:
    return {
        "sink": {
            "options": {
                "data_product": {
                    "id": "analytics.orders",
                    "owner": "data-platform",
                    "tier": "gold",
                    "criticality": "high",
                    "remediation": {
                        "enabled": enabled,
                        "mode": "gate",
                        "profile": "prod_strict",
                        "stale_evidence_policy": "block",
                        "require_trust_gate": True,
                        "closeout_requires_fresh_evidence": True,
                    },
                }
            }
        }
    }


def _trust_gate(*, status: str) -> dict:
    return {
        "schema_version": "dpone.data_product_trust_gate.v1",
        "status": status,
        "product_id": "analytics.orders",
        "product": {"id": "analytics.orders", "owner": "data-platform", "tier": "gold", "criticality": "high"},
        "trust_gate_id": "sha256:trust",
        "trust_snapshot_id": "sha256:snapshot",
        "blockers": ["data_product_trust.domain_blocked:quality"],
        "warnings": [],
    }


def _trust_snapshot() -> dict:
    return {
        "schema_version": "dpone.data_product_trust_snapshot.v1",
        "status": "blocked",
        "product_id": "analytics.orders",
        "trust_snapshot_id": "sha256:snapshot",
        "domains": {
            "quality": {
                "status": "blocked",
                "evidence_refs": [
                    {
                        "artifact_kind": "data_product_assertion_gate",
                        "status": "blocked",
                        "evidence_id": "sha256:assertion-old",
                        "owner": "data-platform",
                        "blockers": ["data_product_assertions.critical_failed"],
                    }
                ],
                "blockers": ["data_product_assertions.critical_failed"],
                "warnings": [],
            }
        },
        "blockers": ["data_product_trust.domain_blocked:quality"],
        "warnings": [],
    }


def _assertion_gate(*, status: str, evidence_id: str = "sha256:assertion-old") -> dict:
    return {
        "schema_version": "dpone.data_product_assertion_gate.v1",
        "status": status,
        "product_id": "analytics.orders",
        "product": {"id": "analytics.orders", "owner": "data-platform"},
        "assertion_gate_id": evidence_id,
        "blockers": ["data_product_assertions.critical_failed"] if status == "blocked" else [],
        "warnings": [],
    }
