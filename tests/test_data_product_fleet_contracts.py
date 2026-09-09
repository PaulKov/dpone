from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

from dpone.readiness.data_product_fleet import (
    FleetReliabilityEvaluator,
    FleetReliabilityGate,
    IncidentRouteDryRunEvaluator,
    ReliabilityExportRenderer,
)
from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
)
from dpone.readiness.schema_migration_evidence_registry import MigrationEvidenceRecorder


def test_disabled_control_tower_emits_noop_evaluation() -> None:
    evaluation = FleetReliabilityEvaluator().evaluate(manifests=(_manifest(enabled=False),), registry_records=())

    assert evaluation["schema_version"] == "dpone.data_product_fleet_evaluation.v1"
    assert evaluation["status"] == "disabled"
    assert evaluation["summary"]["products"] == 1
    assert evaluation["products"][0]["status"] == "unknown"
    assert evaluation["blockers"] == []


def test_fleet_evaluation_groups_products_and_freezes_fast_burn_incident() -> None:
    evaluation = FleetReliabilityEvaluator().evaluate(
        manifests=(
            _manifest(product_id="analytics.orders", owner="data-platform", tier="gold", criticality="critical"),
            _manifest(product_id="finance.margin", owner="finance-analytics", tier="gold", criticality="high"),
        ),
        registry_records=(
            _record(
                product_id="analytics.orders",
                status="ready",
                artifact_refs=(
                    _ref("data_product_slo_gate", "slo_gate_id", "sha256:" + "1" * 64),
                    _ref("data_product_error_budget_gate", "error_budget_gate_id", "sha256:" + "2" * 64),
                    _ref("data_product_release_closeout_gate", "release_closeout_gate_id", "sha256:" + "3" * 64),
                ),
            ),
            _record(
                product_id="finance.margin",
                status="blocked",
                blockers=(
                    "data_product_error_budget.fast_burn_exceeded:fast_burn",
                    "data_product_incident.sev1_open",
                ),
                artifact_refs=(
                    _ref("data_product_error_budget_gate", "error_budget_gate_id", "sha256:" + "4" * 64),
                    _ref("data_product_incident_lifecycle", "incident_id", "sha256:" + "5" * 64),
                ),
            ),
        ),
        observed_at="2026-06-28T12:00:00Z",
    )
    repeated = FleetReliabilityEvaluator().evaluate(
        manifests=(
            _manifest(product_id="analytics.orders", owner="data-platform", tier="gold", criticality="critical"),
            _manifest(product_id="finance.margin", owner="finance-analytics", tier="gold", criticality="high"),
        ),
        registry_records=tuple(reversed(evaluation["source_records"])),
        observed_at="2026-06-28T12:00:00Z",
    )

    assert evaluation["status"] == "frozen"
    assert evaluation["summary"] == {
        "products": 2,
        "healthy": 1,
        "degraded": 0,
        "incident_active": 0,
        "frozen": 1,
        "unknown": 0,
    }
    assert evaluation["by_owner"]["finance-analytics"]["frozen"] == 1
    assert evaluation["by_tier"]["gold"]["products"] == 2
    assert "finance.margin fast_burn exceeded" in evaluation["release_freeze_reasons"]
    assert "finance.margin has open Sev1 incident" in evaluation["release_freeze_reasons"]
    assert evaluation["fleet_evaluation_id"] == repeated["fleet_evaluation_id"]


def test_fleet_gate_profiles_convert_or_block_freeze_signals() -> None:
    evaluation = FleetReliabilityEvaluator().evaluate(
        manifests=(_manifest(product_id="finance.margin", criticality="critical"),),
        registry_records=(
            _record(
                product_id="finance.margin",
                status="blocked",
                blockers=("data_product_error_budget.budget_remaining_exhausted:monthly",),
            ),
        ),
    )

    strict = FleetReliabilityGate().evaluate(evaluation=evaluation, profile="prod_strict")
    advisory = FleetReliabilityGate().evaluate(evaluation=evaluation, profile="advisory")

    assert strict["schema_version"] == "dpone.data_product_fleet_gate.v1"
    assert strict["status"] == "frozen"
    assert "data_product_fleet.release_frozen" in strict["blockers"]
    assert advisory["status"] == "warning"
    assert advisory["blockers"] == []
    assert advisory["warnings"]


def test_fleet_evaluation_freezes_on_policy_gate_blocker() -> None:
    evaluation = FleetReliabilityEvaluator().evaluate(
        manifests=(_manifest(product_id="analytics.orders", criticality="critical"),),
        registry_records=(
            _record(
                product_id="analytics.orders",
                status="blocked",
                blockers=("data_product_policy.uncovered_rule:require_assertion_gate",),
                artifact_refs=(_ref("data_product_policy_gate", "policy_gate_id", "sha256:" + "8" * 64),),
            ),
        ),
    )

    gate = FleetReliabilityGate().evaluate(evaluation=evaluation, profile="prod_strict")

    assert evaluation["status"] == "frozen"
    assert "analytics.orders policy gate blocked" in evaluation["release_freeze_reasons"]
    assert gate["status"] == "frozen"


def test_reliability_export_renderer_outputs_stable_targets() -> None:
    evaluation = FleetReliabilityEvaluator().evaluate(
        manifests=(_manifest(product_id="analytics.orders"),),
        registry_records=(_record(product_id="analytics.orders", status="ready"),),
    )

    prometheus = ReliabilityExportRenderer().render(evaluation=evaluation, target="prometheus")
    otel = ReliabilityExportRenderer().render(evaluation=evaluation, target="opentelemetry")
    lineage = ReliabilityExportRenderer().render(evaluation=evaluation, target="openlineage")
    datahub = ReliabilityExportRenderer().render(evaluation=evaluation, target="datahub")

    assert prometheus["schema_version"] == "dpone.data_product_reliability_export.v1"
    assert "dpone_data_product_health_status" in prometheus["content"]
    assert otel["payload"]["events"][0]["name"] == "dpone.data_product.health"
    assert lineage["payload"]["facets"]["dataProductReliability"]["status"] == "healthy"
    assert datahub["payload"]["entityType"] == "dataProduct"
    assert (
        prometheus["export_id"]
        == ReliabilityExportRenderer().render(evaluation=evaluation, target="prometheus")["export_id"]
    )


def test_incident_route_dry_run_receipt_validates_payload_and_never_writes_network() -> None:
    payload = {
        "schema_version": "dpone.data_product_incident_route_payload.v1",
        "route_payload_id": "sha256:" + "6" * 64,
        "incident_id": "sha256:" + "7" * 64,
        "provider": "slack",
        "payload": {"text": "sev1 data product incident"},
        "network_writes": [],
    }

    receipt = IncidentRouteDryRunEvaluator().evaluate(payload=payload, provider="slack")
    blocked = IncidentRouteDryRunEvaluator().evaluate(payload={**payload, "incident_id": None}, provider="slack")

    assert receipt["schema_version"] == "dpone.data_product_route_delivery_receipt.v1"
    assert receipt["status"] == "dry_run"
    assert receipt["network_writes"] == []
    assert receipt["provider"] == "slack"
    assert (
        receipt["route_delivery_receipt_id"]
        == IncidentRouteDryRunEvaluator().evaluate(payload=payload, provider="slack")["route_delivery_receipt_id"]
    )
    assert blocked["status"] == "blocked"
    assert "data_product_route_delivery.incident_id_missing" in blocked["blockers"]


def test_bundle_policy_and_registry_accept_fleet_artifacts() -> None:
    pack = _pack()
    gate = {
        "schema_version": "dpone.data_product_fleet_gate.v1",
        "fleet_gate_id": "sha256:" + "8" * 64,
        "status": "allowed",
        "blockers": [],
        "warnings": [],
    }
    report = {
        "schema_version": "dpone.data_product_fleet_report.v1",
        "fleet_report_id": "sha256:" + "9" * 64,
        "status": "healthy",
        "blockers": [],
        "warnings": [],
    }
    export = {
        "schema_version": "dpone.data_product_reliability_export.v1",
        "export_id": "sha256:" + "a" * 64,
        "target": "prometheus",
        "status": "rendered",
        "blockers": [],
        "warnings": [],
    }
    receipt = {
        "schema_version": "dpone.data_product_route_delivery_receipt.v1",
        "route_delivery_receipt_id": "sha256:" + "b" * 64,
        "status": "dry_run",
        "blockers": [],
        "warnings": [],
    }
    artifacts = (
        _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
        _artifact("data_product_fleet_gate", "fleet-gate.json", gate, required=False),
        _artifact("data_product_fleet_report", "fleet-report.json", report, required=False),
        _artifact("data_product_reliability_export", "fleet.prom.json", export, required=False),
        _artifact("data_product_route_delivery_receipt", "route-receipt.json", receipt, required=False),
    )

    bundle = MigrationBundleBuilder().build(artifacts=artifacts, attest=True)
    decision = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=MigrationBundlePolicyOptions.resolve(
            profile="prod_strict",
            policy_payload={"required_artifacts": ["migration_pack", "data_product_fleet_gate"]},
        ),
        artifact_payloads={item.kind: item.payload for item in artifacts},
    )
    record = MigrationEvidenceRecorder().build_record(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        gate=None,
        trust_verification=None,
        diff=None,
        data_product_fleet_gate=gate,
        data_product_fleet_report=report,
        data_product_reliability_export=export,
        data_product_route_delivery_receipt=receipt,
        environment="prod",
        stage="fleet_gate_passed",
    )

    assert bundle["summary"]["data_product_fleet_gate_id"] == gate["fleet_gate_id"]
    assert decision["status"] == "allowed"
    assert record["status"] == "ready"
    assert {
        "data_product_fleet_gate",
        "data_product_fleet_report",
        "data_product_reliability_export",
        "data_product_route_delivery_receipt",
    } <= {item["kind"] for item in record["artifact_refs"]}


def test_public_json_schemas_validate_fleet_artifacts() -> None:
    evaluation = FleetReliabilityEvaluator().evaluate(
        manifests=(_manifest(product_id="analytics.orders"),),
        registry_records=(_record(product_id="analytics.orders", status="ready"),),
    )
    gate = FleetReliabilityGate().evaluate(evaluation=evaluation, profile="prod_strict")
    report = ReliabilityExportRenderer().report(evaluation=evaluation)
    export = ReliabilityExportRenderer().render(evaluation=evaluation, target="json")
    receipt = IncidentRouteDryRunEvaluator().evaluate(
        payload={
            "schema_version": "dpone.data_product_incident_route_payload.v1",
            "route_payload_id": "sha256:" + "6" * 64,
            "incident_id": "sha256:" + "7" * 64,
            "provider": "jira",
            "payload": {"fields": {"summary": "incident"}},
            "network_writes": [],
        },
        provider="jira",
    )

    for name, payload in (
        ("data-product-fleet-evaluation.schema.json", evaluation),
        ("data-product-fleet-gate.schema.json", gate),
        ("data-product-fleet-report.schema.json", report),
        ("data-product-reliability-export.schema.json", export),
        ("data-product-route-delivery-receipt.schema.json", receipt),
    ):
        schema = json.loads((Path("docs/schemas/data-product") / name).read_text(encoding="utf-8"))
        Draft202012Validator(schema).validate(payload)


def _manifest(
    *,
    product_id: str = "analytics.orders",
    owner: str = "data-platform",
    tier: str = "gold",
    criticality: str = "high",
    enabled: bool = True,
) -> dict[str, object]:
    return {
        "sink": {
            "type": "clickhouse",
            "table": {"schema": "analytics", "name": product_id.split(".")[-1]},
            "options": {
                "schema_contract": {"id": product_id, "version": "2.0.0"},
                "data_product": {
                    "id": product_id,
                    "owner": owner,
                    "tier": tier,
                    "criticality": criticality,
                    "reliability_control_tower": {
                        "enabled": enabled,
                        "mode": "gate",
                        "profile": "prod_strict",
                        "fleet": {
                            "unknown_product": "warn",
                            "stale_evidence_policy": "block",
                            "stale_after_seconds": 86400,
                            "freeze_on": [
                                "sev1_open",
                                "fast_burn",
                                "budget_exhausted",
                                "release_closeout_blocked",
                            ],
                        },
                        "export": {
                            "enabled": True,
                            "targets": ["prometheus", "opentelemetry", "openlineage", "datahub"],
                        },
                        "routing": {
                            "enabled": True,
                            "mode": "dry_run",
                            "providers": ["slack", "jira", "pagerduty", "webhook"],
                        },
                    },
                },
            },
        }
    }


def _record(
    *,
    product_id: str,
    status: str,
    blockers: tuple[str, ...] = (),
    warnings: tuple[str, ...] = (),
    artifact_refs: tuple[dict[str, object], ...] = (),
    recorded_at: str = "2026-06-28T12:00:00Z",
) -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_migration_evidence_registry_record.v1",
        "record_id": "sha256:" + str(abs(hash((product_id, status, blockers, recorded_at))))[:16].ljust(64, "0"),
        "recorded_at": recorded_at,
        "target": {"sink_type": "clickhouse", "table": product_id},
        "environment": "prod",
        "stage": "slo_gate_passed",
        "status": status,
        "artifact_refs": list(artifact_refs),
        "blockers": list(blockers),
        "warnings": list(warnings),
    }


def _ref(kind: str, id_key: str, value: str) -> dict[str, object]:
    return {"kind": kind, "schema_version": f"dpone.{kind}.v1", "evidence_id": value, id_key: value}


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
