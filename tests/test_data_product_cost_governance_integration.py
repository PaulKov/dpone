from __future__ import annotations

import json

from dpone.readiness.data_product_compliance import ComplianceControlEvaluator, ComplianceControlPlanner, ComplianceGate
from dpone.readiness.data_product_cost import (
    CostBudgetEvaluator,
    CostGovernanceGate,
    CostGovernancePlanner,
)
from dpone.readiness.data_product_cost_rendering import CostForecastEvaluator, CostGovernanceRenderer
from dpone.readiness.data_product_governance import GovernanceExportPlanner
from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
)
from dpone.readiness.schema_migration_evidence_registry import MigrationEvidenceRecorder


def test_cost_governance_bundle_registry_compliance_and_governance_integration() -> None:
    pack = _pack()
    plan = CostGovernancePlanner().plan(manifest=_manifest(), bundle=_bundle(pack_id=pack.pack_id))
    evaluation = CostBudgetEvaluator().evaluate(plan=plan, runtime_artifact=_runtime(), registry_records=())
    gate = CostGovernanceGate().evaluate(evaluation=evaluation, profile="prod_strict")
    forecast = CostForecastEvaluator().forecast(evaluation=evaluation, history={"evaluations": [evaluation]})
    report = CostGovernanceRenderer().report(gate=gate, forecast=forecast)

    bundle = MigrationBundleBuilder().build(
        artifacts=(
            _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
            _artifact("data_product_cost_gate", "cost-gate.json", gate),
            _artifact("data_product_cost_forecast", "cost-forecast.json", forecast),
            _artifact("data_product_cost_report", "cost-report.json", report),
        ),
        attest=True,
    )
    decision = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=MigrationBundlePolicyOptions.resolve(
            profile="prod_strict",
            policy_payload={
                "profile": "prod_strict",
                "required_artifacts": ["migration_pack", "data_product_cost_gate"],
            },
        ),
        artifact_payloads={"data_product_cost_gate": gate},
    )
    record = MigrationEvidenceRecorder().build_record(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        gate=None,
        trust_verification=None,
        diff=None,
        data_product_cost_gate=gate,
        data_product_cost_forecast=forecast,
        data_product_cost_report=report,
        environment="prod",
        stage="cost_gate_passed",
    )
    compliance_plan = ComplianceControlPlanner().plan(
        manifest=_compliance_manifest(),
        evidence={"data_product_cost_gate": gate},
    )
    compliance_eval = ComplianceControlEvaluator().evaluate(plan=compliance_plan)
    compliance_gate = ComplianceGate().evaluate(evaluation=compliance_eval, profile="prod_strict")
    governance = GovernanceExportPlanner().plan(
        manifest=_governance_manifest(),
        evidence={
            "data_product_cost_gate": gate,
            "data_product_cost_forecast": forecast,
            "data_product_cost_report": report,
        },
        registry_records=[record],
        targets=("json",),
    )

    assert bundle["summary"]["data_product_cost_gate_id"] == gate["cost_gate_id"]
    assert decision["status"] == "allowed"
    assert "migration_bundle.unknown_artifact_kind:data_product_cost_gate" not in decision["warnings"]
    assert record["stage"] == "cost_gate_passed"
    assert {"data_product_cost_gate", "data_product_cost_forecast", "data_product_cost_report"} <= {
        item["kind"] for item in record["artifact_refs"]
    }
    assert compliance_gate["status"] == "allowed"
    assert {"data_product_cost_gate", "data_product_cost_forecast", "data_product_cost_report"} <= {
        item["kind"] for item in governance["evidence_refs"]
    }


def _artifact(kind: str, path: str, payload: dict, *, required: bool = False) -> MigrationEvidenceArtifact:
    return MigrationEvidenceArtifact.from_bytes(
        kind=kind,
        path=path,
        content=json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8"),
        required=required,
    )


def _pack() -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"columns": ["amount", "customer_id"]},
        actual={"columns": ["amount", "customer_id"]},
        strategy="additive",
        changes=(),
        blockers=(),
        warnings=(),
        ddl=(),
    )


def _manifest() -> dict:
    from tests.test_data_product_cost_governance_contracts import _manifest

    return _manifest()


def _bundle(*, pack_id: str) -> dict:
    return {"bundle_id": "sha256:bundle", "pack_id": pack_id, "summary": {}}


def _runtime() -> dict:
    from tests.test_data_product_cost_governance_contracts import _safe_runtime_artifact

    return _safe_runtime_artifact()


def _governance_manifest() -> dict:
    manifest = _manifest()
    product = manifest["sink"]["options"]["data_product"]
    product["governance_export"] = {
        "enabled": True,
        "mode": "gate",
        "profile": "prod_strict",
        "targets": [{"provider": "json", "mode": "render"}],
    }
    return manifest


def _compliance_manifest() -> dict:
    manifest = _manifest()
    product = manifest["sink"]["options"]["data_product"]
    product["compliance"] = {
        "enabled": True,
        "mode": "gate",
        "profile": "prod_strict",
        "frameworks": [
            {
                "id": "finops",
                "version": "2026.1",
                "owner": "data-governance",
                "controls": [
                    {
                        "id": "FINOPS.1",
                        "title": "Data product release has cost guardrail evidence",
                        "severity": "high",
                        "require_artifacts": ["data_product_cost_gate"],
                        "allowed_statuses": {"data_product_cost_gate": ["allowed", "warning"]},
                    }
                ],
            }
        ],
    }
    return manifest
