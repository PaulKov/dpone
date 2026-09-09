from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

from dpone.readiness.data_product_error_budget import (
    DataProductErrorBudgetEvaluator,
    DataProductErrorBudgetGate,
    DataProductErrorBudgetPlanner,
)
from dpone.readiness.data_product_incident_lifecycle import (
    IncidentLifecycleReducer,
    IncidentRouterPayloadRenderer,
)
from dpone.readiness.data_product_release_closeout import ReleaseCloseoutGate
from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
)
from dpone.readiness.schema_migration_evidence_registry import MigrationEvidenceRecorder


def test_disabled_error_budget_manifest_emits_noop_plan() -> None:
    plan = DataProductErrorBudgetPlanner().plan(manifest=_manifest(error_budget_enabled=False))

    assert plan["schema_version"] == "dpone.data_product_error_budget_plan.v1"
    assert plan["status"] == "disabled"
    assert plan["product"]["id"] == "analytics.orders"
    assert plan["windows"] == []
    assert plan["blockers"] == []


def test_error_budget_fast_burn_blocks_prod_strict_and_is_stable() -> None:
    plan = DataProductErrorBudgetPlanner().plan(
        manifest=_manifest(),
        slo_evaluation=_slo_evaluation(status="blocked", recorded_at="2026-06-28T11:55:00Z"),
    )

    first = DataProductErrorBudgetEvaluator().evaluate(
        plan=plan,
        history=(
            _slo_evaluation(status="passed", recorded_at="2026-06-28T11:50:00Z"),
            _slo_evaluation(status="blocked", recorded_at="2026-06-28T11:55:00Z"),
        ),
        observed_at="2026-06-28T12:00:00Z",
    )
    second = DataProductErrorBudgetEvaluator().evaluate(
        plan=plan,
        history=(
            _slo_evaluation(status="passed", recorded_at="2026-06-28T11:50:00Z"),
            _slo_evaluation(status="blocked", recorded_at="2026-06-28T11:55:00Z"),
        ),
        observed_at="2026-06-28T12:00:00Z",
    )
    strict_gate = DataProductErrorBudgetGate().evaluate(evaluation=first, profile="prod_strict")
    advisory_gate = DataProductErrorBudgetGate().evaluate(evaluation=first, profile="advisory")

    assert first["schema_version"] == "dpone.data_product_error_budget_evaluation.v1"
    assert first["status"] == "blocked"
    assert first["error_budget_evaluation_id"] == second["error_budget_evaluation_id"]
    assert "data_product_error_budget.fast_burn_exceeded:fast_burn" in first["blockers"]
    assert first["windows"][0]["burn_rate"] > 14.4
    assert strict_gate["schema_version"] == "dpone.data_product_error_budget_gate.v1"
    assert strict_gate["status"] == "blocked"
    assert advisory_gate["status"] == "warning"


def test_error_budget_monthly_budget_remaining_blocks_when_exhausted() -> None:
    plan = DataProductErrorBudgetPlanner().plan(manifest=_manifest())

    evaluation = DataProductErrorBudgetEvaluator().evaluate(
        plan=plan,
        history=tuple(
            _slo_evaluation(status="blocked", recorded_at=f"2026-06-{day:02d}T12:00:00Z") for day in range(1, 11)
        ),
        observed_at="2026-06-28T12:00:00Z",
    )

    assert "data_product_error_budget.budget_remaining_exhausted:monthly" in evaluation["blockers"]
    monthly = next(item for item in evaluation["windows"] if item["name"] == "monthly")
    assert monthly["budget_remaining"] == 0.0


def test_incident_lifecycle_open_ack_resolve_and_route_payloads_are_deterministic() -> None:
    lifecycle = IncidentLifecycleReducer().open(
        slo_evaluation=_slo_evaluation(status="blocked"),
        slo_gate=_slo_gate(status="blocked"),
        budget_gate=_budget_gate(status="blocked"),
    )
    deduped = IncidentLifecycleReducer().open(
        slo_evaluation=_slo_evaluation(status="blocked"),
        slo_gate=_slo_gate(status="blocked"),
        budget_gate=_budget_gate(status="blocked"),
        existing=lifecycle,
    )
    acknowledged = IncidentLifecycleReducer().ack(incident=lifecycle, actor="data-platform")
    resolved = IncidentLifecycleReducer().resolve(incident=acknowledged, evidence=_slo_gate(status="allowed"))
    first_route = IncidentRouterPayloadRenderer().render(incident=resolved, provider="slack")
    second_route = IncidentRouterPayloadRenderer().render(incident=resolved, provider="slack")

    assert lifecycle["schema_version"] == "dpone.data_product_incident_lifecycle.v1"
    assert lifecycle["status"] == "open"
    assert lifecycle["severity"] == "sev1"
    assert len(deduped["events"]) == len(lifecycle["events"])
    assert acknowledged["status"] == "acknowledged"
    assert resolved["status"] == "resolved"
    assert first_route["schema_version"] == "dpone.data_product_incident_route_payload.v1"
    assert first_route["provider"] == "slack"
    assert first_route["route_payload_id"] == second_route["route_payload_id"]
    assert first_route["network_writes"] == []


def test_release_closeout_blocks_unresolved_incident_and_allows_resolved_evidence() -> None:
    open_incident = IncidentLifecycleReducer().open(
        slo_evaluation=_slo_evaluation(status="blocked"),
        slo_gate=_slo_gate(status="blocked"),
        budget_gate=_budget_gate(status="blocked"),
    )
    resolved = IncidentLifecycleReducer().resolve(
        incident=IncidentLifecycleReducer().ack(incident=open_incident, actor="data-platform"),
        evidence=_slo_gate(status="allowed"),
    )

    blocked = ReleaseCloseoutGate().evaluate(
        slo_gate=_slo_gate(status="allowed"),
        budget_gate=_budget_gate(status="allowed"),
        incident=open_incident,
        watch_certificate=_watch_certificate(status="stable"),
        profile="prod_strict",
    )
    allowed = ReleaseCloseoutGate().evaluate(
        slo_gate=_slo_gate(status="allowed"),
        budget_gate=_budget_gate(status="allowed"),
        incident=resolved,
        watch_certificate=_watch_certificate(status="stable"),
        policy_gate={
            "schema_version": "dpone.data_product_policy_gate.v1",
            "policy_gate_id": "sha256:" + "9" * 64,
            "status": "waived",
            "blockers": [],
            "warnings": [],
        },
        profile="prod_strict",
    )
    policy_blocked = ReleaseCloseoutGate().evaluate(
        slo_gate=_slo_gate(status="allowed"),
        budget_gate=_budget_gate(status="allowed"),
        incident=resolved,
        policy_gate={
            "schema_version": "dpone.data_product_policy_gate.v1",
            "policy_gate_id": "sha256:" + "8" * 64,
            "status": "blocked",
            "blockers": ["data_product_policy.uncovered_rule:require_assertion_gate"],
            "warnings": [],
        },
        profile="prod_strict",
    )

    assert blocked["schema_version"] == "dpone.data_product_release_closeout_gate.v1"
    assert blocked["status"] == "blocked"
    assert "data_product_release_closeout.incident_unresolved" in blocked["blockers"]
    assert allowed["status"] == "allowed"
    assert allowed["policy_gate_id"] == "sha256:" + "9" * 64
    assert policy_blocked["status"] == "blocked"
    assert "data_product_release_closeout.policy_gate_blocked" in policy_blocked["blockers"]


def test_bundle_policy_and_registry_accept_reliability_artifacts() -> None:
    pack = _pack()
    error_budget_gate = _budget_gate(status="allowed", pack_id=pack.pack_id)
    incident = {
        **IncidentLifecycleReducer().resolve(
            incident=IncidentLifecycleReducer().ack(
                incident=IncidentLifecycleReducer().open(
                    slo_evaluation=_slo_evaluation(status="blocked"),
                    slo_gate=_slo_gate(status="blocked"),
                    budget_gate=_budget_gate(status="blocked"),
                ),
                actor="data-platform",
            ),
            evidence=_slo_gate(status="allowed"),
        ),
        "pack_id": pack.pack_id,
    }
    closeout = ReleaseCloseoutGate().evaluate(
        slo_gate=_slo_gate(status="allowed", pack_id=pack.pack_id),
        budget_gate=error_budget_gate,
        incident=incident,
        watch_certificate=_watch_certificate(status="stable", pack_id=pack.pack_id),
        profile="prod_strict",
    )
    artifacts = (
        _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
        _artifact("data_product_error_budget_gate", "budget-gate.json", error_budget_gate, required=False),
        _artifact("data_product_incident_lifecycle", "incident-lifecycle.json", incident, required=False),
        _artifact("data_product_release_closeout_gate", "closeout-gate.json", closeout, required=False),
    )

    bundle = MigrationBundleBuilder().build(artifacts=artifacts, attest=True)
    decision = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=MigrationBundlePolicyOptions.resolve(
            profile="prod_strict",
            policy_payload={
                "required_artifacts": [
                    "migration_pack",
                    "data_product_error_budget_gate",
                    "data_product_incident_lifecycle",
                    "data_product_release_closeout_gate",
                ],
            },
        ),
        artifact_payloads={item.kind: item.payload for item in artifacts},
    )
    record = MigrationEvidenceRecorder().build_record(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        gate=None,
        trust_verification=None,
        diff=None,
        data_product_error_budget_gate=error_budget_gate,
        data_product_incident_lifecycle=incident,
        data_product_release_closeout_gate=closeout,
        environment="prod",
        stage="release_closeout_passed",
    )

    assert bundle["summary"]["data_product_error_budget_gate_id"] == error_budget_gate["error_budget_gate_id"]
    assert bundle["summary"]["data_product_incident_lifecycle_id"] == incident["incident_id"]
    assert bundle["summary"]["data_product_release_closeout_gate_id"] == closeout["release_closeout_gate_id"]
    assert decision["status"] == "allowed"
    assert record["status"] == "ready"
    assert {
        "data_product_error_budget_gate",
        "data_product_incident_lifecycle",
        "data_product_release_closeout_gate",
    } <= {item["kind"] for item in record["artifact_refs"]}


def test_public_json_schemas_validate_reliability_artifacts() -> None:
    plan = DataProductErrorBudgetPlanner().plan(manifest=_manifest())
    evaluation = DataProductErrorBudgetEvaluator().evaluate(
        plan=plan,
        history=(_slo_evaluation(status="passed", recorded_at="2026-06-28T12:00:00Z"),),
        observed_at="2026-06-28T12:00:00Z",
    )
    budget_gate = DataProductErrorBudgetGate().evaluate(evaluation=evaluation, profile="prod_strict")
    incident = IncidentLifecycleReducer().open(
        slo_evaluation=_slo_evaluation(status="blocked"),
        slo_gate=_slo_gate(status="blocked"),
        budget_gate=_budget_gate(status="blocked"),
    )
    route = IncidentRouterPayloadRenderer().render(incident=incident, provider="jira")
    closeout = ReleaseCloseoutGate().evaluate(
        slo_gate=_slo_gate(status="allowed"),
        budget_gate=_budget_gate(status="allowed"),
        incident=IncidentLifecycleReducer().resolve(
            incident=IncidentLifecycleReducer().ack(incident=incident, actor="data-platform"),
            evidence=_slo_gate(status="allowed"),
        ),
        profile="prod_strict",
    )

    for name, payload in (
        ("data-product-error-budget-plan.schema.json", plan),
        ("data-product-error-budget-evaluation.schema.json", evaluation),
        ("data-product-error-budget-gate.schema.json", budget_gate),
        ("data-product-incident-lifecycle.schema.json", incident),
        ("data-product-incident-route-payload.schema.json", route),
        ("data-product-release-closeout-gate.schema.json", closeout),
    ):
        schema = json.loads((Path("docs/schemas/data-product") / name).read_text(encoding="utf-8"))
        Draft202012Validator(schema).validate(payload)


def _manifest(*, error_budget_enabled: bool = True) -> dict[str, object]:
    return {
        "sink": {
            "type": "clickhouse",
            "table": {"schema": "analytics", "name": "orders"},
            "options": {
                "schema_contract": {"id": "analytics.orders", "version": "2.0.0"},
                "data_product": {
                    "id": "analytics.orders",
                    "owner": "data-platform",
                    "tier": "gold",
                    "criticality": "high",
                    "slo": {
                        "enabled": True,
                        "mode": "gate",
                        "profile": "prod_strict",
                        "objectives": {
                            "freshness": {"max_lag_seconds": 900},
                            "availability": {"max_failed_runs": 0},
                        },
                        "error_budget": {
                            "enabled": error_budget_enabled,
                            "mode": "gate",
                            "profile": "prod_strict",
                            "objective_window": "30d",
                            "windows": [
                                {"name": "fast_burn", "duration": "1h", "max_burn_rate": 14.4},
                                {"name": "slow_burn", "duration": "6h", "max_burn_rate": 6.0},
                                {"name": "monthly", "duration": "30d", "min_budget_remaining": 0.10},
                            ],
                            "release_policy": {
                                "freeze_on_budget_exhausted": True,
                                "block_risky_migration_on_fast_burn": True,
                            },
                        },
                        "incident_lifecycle": {
                            "enabled": True,
                            "mode": "gate",
                            "require_ack_for_sev1": True,
                            "require_resolution_evidence": True,
                            "routing": {
                                "enabled": True,
                                "mode": "render",
                                "providers": ["slack", "jira", "pagerduty"],
                            },
                        },
                    },
                },
            },
        }
    }


def _slo_evaluation(
    *,
    status: str,
    recorded_at: str = "2026-06-28T12:00:00Z",
    product_id: str = "analytics.orders",
) -> dict[str, object]:
    blockers = ["data_product_slo.freshness_breach"] if status == "blocked" else []
    return {
        "schema_version": "dpone.data_product_slo_evaluation.v1",
        "status": status,
        "product_id": product_id,
        "slo_evaluation_id": "sha256:" + str(abs(hash((status, recorded_at, product_id))))[:16].ljust(64, "0"),
        "recorded_at": recorded_at,
        "checks": [
            {
                "name": "freshness",
                "status": "failed" if status == "blocked" else "passed",
                "details": blockers,
                "metrics": {"lag_seconds": 1800 if status == "blocked" else 60},
            }
        ],
        "blockers": blockers,
        "warnings": [],
    }


def _slo_gate(*, status: str, pack_id: str | None = None) -> dict[str, object]:
    return {
        "schema_version": "dpone.data_product_slo_gate.v1",
        "status": status,
        "slo_gate_id": "sha256:" + "1" * 64,
        "slo_evaluation_id": "sha256:" + "2" * 64,
        "product_id": "analytics.orders",
        "pack_id": pack_id,
        "blockers": ["data_product_slo.freshness_breach"] if status == "blocked" else [],
        "warnings": [],
    }


def _budget_gate(*, status: str, pack_id: str | None = None) -> dict[str, object]:
    return {
        "schema_version": "dpone.data_product_error_budget_gate.v1",
        "status": status,
        "error_budget_gate_id": "sha256:" + "3" * 64,
        "error_budget_evaluation_id": "sha256:" + "4" * 64,
        "product_id": "analytics.orders",
        "pack_id": pack_id,
        "blockers": ["data_product_error_budget.fast_burn_exceeded:fast_burn"] if status == "blocked" else [],
        "warnings": [],
    }


def _watch_certificate(*, status: str, pack_id: str | None = None) -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_migration_watch_certificate.v1",
        "certificate_id": "sha256:" + "5" * 64,
        "status": status,
        "pack_id": pack_id,
        "blockers": [] if status in {"stable", "warning"} else ["schema_migration_watch.blocked"],
        "warnings": [],
    }


def _pack() -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders"},
        actual={"sink_type": "clickhouse", "table": "analytics.orders"},
        changes=({"change_type": "schema_contract_major", "path": "analytics.orders"},),
        strategy="expand_contract",
    )


def _artifact(kind: str, path: str, payload: dict[str, object], *, required: bool) -> MigrationEvidenceArtifact:
    content = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return MigrationEvidenceArtifact.from_bytes(kind=kind, path=path, content=content, required=required)
