from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

from dpone.readiness.data_product_slo import (
    DataProductSloEvaluator,
    DataProductSloGate,
    DataProductSloPlanner,
)
from dpone.readiness.data_product_slo_incident import IncidentClassifier
from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
)
from dpone.readiness.schema_migration_evidence_registry import MigrationEvidenceRecorder


def test_disabled_slo_manifest_emits_noop_plan() -> None:
    plan = DataProductSloPlanner().plan(manifest=_manifest(enabled=False))

    assert plan["schema_version"] == "dpone.data_product_slo_plan.v1"
    assert plan["status"] == "disabled"
    assert plan["product"]["id"] == "analytics.orders"
    assert plan["objectives"] == {}
    assert plan["blockers"] == []


def test_slo_plan_binds_product_contract_consumers_and_stable_id() -> None:
    manifest = _manifest(enabled=True)
    contract_gate = _contract_gate()
    consumer_gate = _consumer_gate(status="allowed")

    first = DataProductSloPlanner().plan(
        manifest=manifest,
        contract_gate=contract_gate,
        consumer_gate=consumer_gate,
    )
    second = DataProductSloPlanner().plan(
        manifest=manifest,
        contract_gate=contract_gate,
        consumer_gate=consumer_gate,
    )

    assert first["status"] == "ready"
    assert first["product"] == {
        "id": "analytics.orders",
        "owner": "data-platform",
        "tier": "gold",
        "criticality": "high",
    }
    assert first["contract_gate_id"] == contract_gate["contract_gate_id"]
    assert first["consumer_gate_id"] == consumer_gate["consumer_gate_id"]
    assert first["objectives"]["freshness"]["max_lag_seconds"] == 900
    assert first["slo_plan_id"] == second["slo_plan_id"]


def test_slo_evaluation_and_gate_block_critical_consumer_breach() -> None:
    plan = DataProductSloPlanner().plan(
        manifest=_manifest(enabled=True),
        contract_gate=_contract_gate(),
        consumer_gate=_consumer_gate(status="blocked", critical_failed=True),
    )

    evaluation = DataProductSloEvaluator().evaluate(
        plan=plan,
        registry_records=(),
        runtime_artifacts=(_runtime_artifact(),),
    )
    strict_gate = DataProductSloGate().evaluate(evaluation=evaluation, profile="prod_strict")
    advisory_gate = DataProductSloGate().evaluate(evaluation=evaluation, profile="advisory")

    assert evaluation["schema_version"] == "dpone.data_product_slo_evaluation.v1"
    assert evaluation["status"] == "blocked"
    assert "data_product_slo.consumer_critical_failed" in evaluation["blockers"]
    assert strict_gate["schema_version"] == "dpone.data_product_slo_gate.v1"
    assert strict_gate["status"] == "blocked"
    assert advisory_gate["status"] == "warning"
    assert "data_product_slo.consumer_critical_failed" in advisory_gate["warnings"]


def test_slo_evaluation_detects_freshness_volume_latency_quality_and_availability() -> None:
    plan = DataProductSloPlanner().plan(
        manifest=_manifest(enabled=True),
        contract_gate=_contract_gate(),
        consumer_gate=_consumer_gate(status="allowed"),
    )

    evaluation = DataProductSloEvaluator().evaluate(
        plan=plan,
        registry_records=(),
        runtime_artifacts=(
            _runtime_artifact(
                freshness_lag_seconds=1800,
                rows_loaded=0,
                duration_ms=400_000,
                failed_runs=1,
                duplicate_keys=1,
                null_keys=1,
            ),
        ),
    )

    blocker_set = set(evaluation["blockers"])
    assert evaluation["status"] == "blocked"
    assert "data_product_slo.freshness_breach" in blocker_set
    assert "data_product_slo.volume_breach" in blocker_set
    assert "data_product_slo.latency_breach" in blocker_set
    assert "data_product_slo.availability_breach" in blocker_set
    assert "data_product_slo.duplicate_key_breach" in blocker_set
    assert "data_product_slo.null_key_breach" in blocker_set


def test_slo_volume_uses_runtime_row_count_when_rows_loaded_is_absent() -> None:
    plan = DataProductSloPlanner().plan(
        manifest=_manifest(enabled=True),
        contract_gate=_contract_gate(),
        consumer_gate=_consumer_gate(status="allowed"),
    )

    evaluation = DataProductSloEvaluator().evaluate(
        plan=plan,
        registry_records=(),
        runtime_artifacts=(_runtime_artifact(rows_loaded=None, row_count=10),),
    )

    volume = next(check for check in evaluation["checks"] if check["name"] == "volume")
    assert volume["status"] == "passed"
    assert volume["metrics"]["rows"] == 10.0


def test_unknown_consumer_policy_allow_warn_block() -> None:
    plan_allow = DataProductSloPlanner().plan(
        manifest=_manifest(enabled=True, unknown_consumer="allow"),
        contract_gate=_contract_gate(),
        consumer_gate=_consumer_gate(status="warning", unknown_consumers=1),
    )
    plan_warn = DataProductSloPlanner().plan(
        manifest=_manifest(enabled=True, unknown_consumer="warn"),
        contract_gate=_contract_gate(),
        consumer_gate=_consumer_gate(status="warning", unknown_consumers=1),
    )
    plan_block = DataProductSloPlanner().plan(
        manifest=_manifest(enabled=True, unknown_consumer="block"),
        contract_gate=_contract_gate(),
        consumer_gate=_consumer_gate(status="warning", unknown_consumers=1),
    )

    assert (
        DataProductSloEvaluator().evaluate(
            plan=plan_allow, registry_records=(), runtime_artifacts=(_runtime_artifact(),)
        )["warnings"]
        == []
    )
    assert (
        "data_product_slo.unknown_consumers_present"
        in DataProductSloEvaluator().evaluate(
            plan=plan_warn, registry_records=(), runtime_artifacts=(_runtime_artifact(),)
        )["warnings"]
    )
    assert (
        "data_product_slo.unknown_consumers_present"
        in DataProductSloEvaluator().evaluate(
            plan=plan_block, registry_records=(), runtime_artifacts=(_runtime_artifact(),)
        )["blockers"]
    )


def test_incident_classifier_maps_gate_results_to_severity() -> None:
    plan = DataProductSloPlanner().plan(
        manifest=_manifest(enabled=True),
        contract_gate=_contract_gate(),
        consumer_gate=_consumer_gate(status="blocked", critical_failed=True),
    )
    evaluation = DataProductSloEvaluator().evaluate(
        plan=plan,
        registry_records=(),
        runtime_artifacts=(_runtime_artifact(freshness_lag_seconds=1800),),
    )
    gate = DataProductSloGate().evaluate(evaluation=evaluation, profile="prod_strict")

    report = IncidentClassifier().report(evaluation=evaluation, gate=gate)

    assert report["schema_version"] == "dpone.data_product_incident_report.v1"
    assert report["status"] == "open"
    assert report["severity"] == "sev1"
    assert "critical_consumer_failed" in report["signals"]
    assert "# Data Product Incident Report" in report["markdown"]


def test_bundle_policy_and_registry_accept_slo_and_incident_artifacts() -> None:
    pack = _pack()
    slo_gate = {
        "schema_version": "dpone.data_product_slo_gate.v1",
        "slo_gate_id": "sha256:" + "4" * 64,
        "status": "allowed",
        "profile": "prod_strict",
        "product_id": "analytics.orders",
        "pack_id": pack.pack_id,
        "bundle_id": None,
        "blockers": [],
        "warnings": [],
    }
    incident = {
        "schema_version": "dpone.data_product_incident_report.v1",
        "incident_report_id": "sha256:" + "5" * 64,
        "status": "healthy",
        "severity": "none",
        "product_id": "analytics.orders",
        "pack_id": pack.pack_id,
        "blockers": [],
        "warnings": [],
    }
    artifacts = (
        _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
        _artifact("data_product_slo_gate", "slo-gate.json", slo_gate, required=False),
        _artifact("data_product_incident_report", "incident.json", incident, required=False),
    )

    bundle = MigrationBundleBuilder().build(artifacts=artifacts, attest=True)
    slo_gate["bundle_id"] = bundle["bundle_id"]
    decision = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=MigrationBundlePolicyOptions.resolve(
            profile="prod_strict",
            policy_payload={
                "required_artifacts": [
                    "migration_pack",
                    "data_product_slo_gate",
                    "data_product_incident_report",
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
        data_product_slo_gate=slo_gate,
        data_product_incident_report=incident,
        environment="prod",
        stage="slo_gate_passed",
    )

    assert bundle["summary"]["data_product_slo_gate_id"] == slo_gate["slo_gate_id"]
    assert bundle["summary"]["data_product_incident_report_id"] == incident["incident_report_id"]
    assert decision["status"] == "allowed"
    assert record["status"] == "ready"
    assert {"data_product_slo_gate", "data_product_incident_report"} <= {
        item["kind"] for item in record["artifact_refs"]
    }


def test_public_json_schemas_validate_slo_artifacts() -> None:
    plan = DataProductSloPlanner().plan(
        manifest=_manifest(enabled=True),
        contract_gate=_contract_gate(),
        consumer_gate=_consumer_gate(status="allowed"),
    )
    evaluation = DataProductSloEvaluator().evaluate(
        plan=plan,
        registry_records=(),
        runtime_artifacts=(_runtime_artifact(),),
    )
    gate = DataProductSloGate().evaluate(evaluation=evaluation, profile="prod_strict")
    report = IncidentClassifier().report(evaluation=evaluation, gate=gate)

    for name, payload in (
        ("data-product-slo-plan.schema.json", plan),
        ("data-product-slo-evaluation.schema.json", evaluation),
        ("data-product-slo-gate.schema.json", gate),
        ("data-product-incident-report.schema.json", report),
    ):
        schema = json.loads((Path("docs/schemas/data-product") / name).read_text(encoding="utf-8"))
        Draft202012Validator(schema).validate(payload)


def _manifest(*, enabled: bool, unknown_consumer: str = "warn") -> dict[str, object]:
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
                        "enabled": enabled,
                        "mode": "gate",
                        "profile": "prod_strict",
                        "objectives": {
                            "freshness": {"max_lag_seconds": 900},
                            "volume": {"min_rows": 1, "max_relative_delta": 0.001},
                            "latency": {"max_run_duration_ms": 300000},
                            "quality": {
                                "max_duplicate_keys": 0,
                                "max_null_keys": 0,
                                "typed_hash": "warning",
                            },
                            "availability": {"max_failed_runs": 0},
                        },
                        "consumers": {
                            "require_critical_consumer_green": True,
                            "unknown_consumer": unknown_consumer,
                        },
                        "incident": {
                            "enabled": True,
                            "severity_map": {
                                "critical_consumer_failed": "sev1",
                                "freshness_breach": "sev2",
                                "volume_breach": "sev2",
                                "warning_only": "sev3",
                            },
                        },
                    },
                },
            },
        }
    }


def _contract_gate() -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_contract_gate.v1",
        "contract_gate_id": "sha256:" + "1" * 64,
        "contract_id": "analytics.orders",
        "contract_version_id": "sha256:" + "2" * 64,
        "status": "allowed",
        "blockers": [],
        "warnings": [],
    }


def _consumer_gate(*, status: str, critical_failed: bool = False, unknown_consumers: int = 0) -> dict[str, object]:
    blockers = ["schema_contract_consumer.critical_consumer_failed:finance.daily_margin"] if critical_failed else []
    return {
        "schema_version": "dpone.schema_contract_consumer_gate.v1",
        "consumer_gate_id": "sha256:" + "3" * 64,
        "contract_id": "analytics.orders",
        "consumer_matrix_id": "sha256:" + "6" * 64,
        "status": status,
        "summary": {
            "critical_consumers_failed": 1 if critical_failed else 0,
            "unknown_consumers": unknown_consumers,
        },
        "blockers": blockers,
        "warnings": ["schema_contract_consumer.unknown_consumers_present"] if unknown_consumers else [],
    }


def _runtime_artifact(
    *,
    freshness_lag_seconds: int = 60,
    rows_loaded: int | None = 1000,
    row_count: int | None = None,
    duration_ms: int = 120_000,
    failed_runs: int = 0,
    duplicate_keys: int = 0,
    null_keys: int = 0,
) -> dict[str, object]:
    return {
        "schema_version": "dpone.runtime_run.v1",
        "product_id": "analytics.orders",
        "status": "succeeded" if failed_runs == 0 else "failed",
        "freshness_lag_seconds": freshness_lag_seconds,
        "rows_loaded": rows_loaded,
        "row_count": row_count,
        "duration_ms": duration_ms,
        "failed_runs": failed_runs,
        "quality": {
            "duplicate_keys": duplicate_keys,
            "null_keys": null_keys,
            "typed_hash_status": "passed",
        },
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
