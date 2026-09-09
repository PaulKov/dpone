from __future__ import annotations

import json

from dpone.readiness.data_product_compliance import ComplianceControlEvaluator, ComplianceControlPlanner, ComplianceGate
from dpone.readiness.data_product_governance import GovernanceExportPlanner
from dpone.readiness.data_product_rollout import (
    RingGateEvaluator,
    RolloutPlanBuilder,
    RolloutPromotionService,
    ShadowValidationEvaluator,
)
from dpone.readiness.data_product_rollout_rendering import RolloutRenderer
from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
)
from dpone.readiness.schema_migration_evidence_registry import MigrationEvidenceRecorder


def test_rollout_bundle_registry_compliance_and_governance_integration() -> None:
    pack = _pack()
    bundle_payload = {"bundle_id": "sha256:bundle", "pack_id": pack.pack_id, "summary": {}}
    plan = RolloutPlanBuilder().plan(manifest=_manifest(), bundle=bundle_payload, evidence=_evidence(pack.pack_id))
    shadow = ShadowValidationEvaluator().evaluate(
        plan=plan,
        runtime_artifact=_runtime(),
        baseline_runtime_artifact=_runtime(),
    )
    ring_gate = RingGateEvaluator().evaluate(
        plan=plan,
        ring_id="staging",
        evidence=_evidence(pack.pack_id),
        shadow_validation=None,
    )
    promotion = RolloutPromotionService().promote(
        ring_gate=ring_gate,
        target_ring="canary",
        authority_gate={"status": "allowed", "authority_gate_id": "sha256:authority"},
    )
    report = RolloutRenderer().report(promotion=promotion)

    bundle = MigrationBundleBuilder().build(
        artifacts=(
            _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
            _artifact("data_product_ring_gate", "ring-gate.json", ring_gate),
            _artifact("data_product_shadow_validation", "shadow.json", shadow),
            _artifact("data_product_rollout_promotion", "promotion.json", promotion),
            _artifact("data_product_rollout_report", "report.json", report),
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
                "required_artifacts": ["migration_pack", "data_product_ring_gate"],
            },
        ),
        artifact_payloads={"data_product_ring_gate": ring_gate},
    )
    record = MigrationEvidenceRecorder().build_record(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        gate=None,
        trust_verification=None,
        diff=None,
        data_product_ring_gate=ring_gate,
        data_product_shadow_validation=shadow,
        data_product_rollout_promotion=promotion,
        data_product_rollout_report=report,
        environment="prod",
        stage="rollout_ring_gate_passed",
    )
    compliance_plan = ComplianceControlPlanner().plan(
        manifest=_compliance_manifest(),
        evidence={"data_product_ring_gate": ring_gate},
    )
    compliance_eval = ComplianceControlEvaluator().evaluate(plan=compliance_plan)
    compliance_gate = ComplianceGate().evaluate(evaluation=compliance_eval, profile="prod_strict")
    governance = GovernanceExportPlanner().plan(
        manifest=_governance_manifest(),
        evidence={
            "data_product_ring_gate": ring_gate,
            "data_product_shadow_validation": shadow,
            "data_product_rollout_promotion": promotion,
        },
        registry_records=[record],
        targets=("json",),
    )

    assert bundle["summary"]["data_product_ring_gate_id"] == ring_gate["ring_gate_id"]
    assert decision["status"] == "allowed"
    assert "migration_bundle.unknown_artifact_kind:data_product_ring_gate" not in decision["warnings"]
    assert record["stage"] == "rollout_ring_gate_passed"
    assert {"data_product_ring_gate", "data_product_shadow_validation", "data_product_rollout_promotion"} <= {
        item["kind"] for item in record["artifact_refs"]
    }
    assert compliance_gate["status"] == "allowed"
    assert {"data_product_ring_gate", "data_product_shadow_validation", "data_product_rollout_promotion"} <= {
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
    from tests.test_data_product_rollout_contracts import _manifest

    return _manifest()


def _evidence(pack_id: str) -> dict[str, dict]:
    from tests.test_data_product_rollout_contracts import _green_canary_evidence

    evidence = _green_canary_evidence()
    for payload in evidence.values():
        payload["pack_id"] = pack_id
    return evidence


def _runtime() -> dict:
    from tests.test_data_product_rollout_contracts import _runtime

    return _runtime(row_count=1000, typed_hash="sha256:ok")


def _governance_manifest() -> dict:
    manifest = _manifest()
    manifest["sink"]["options"]["data_product"]["governance_export"] = {
        "enabled": True,
        "mode": "gate",
        "profile": "prod_strict",
        "targets": [{"provider": "json", "mode": "render"}],
    }
    return manifest


def _compliance_manifest() -> dict:
    manifest = _manifest()
    manifest["sink"]["options"]["data_product"]["compliance"] = {
        "enabled": True,
        "mode": "gate",
        "profile": "prod_strict",
        "frameworks": [
            {
                "id": "release",
                "version": "2026.1",
                "owner": "data-governance",
                "controls": [
                    {
                        "id": "RELEASE.1",
                        "title": "Data product release has rollout gate evidence",
                        "severity": "high",
                        "require_artifacts": ["data_product_ring_gate"],
                        "allowed_statuses": {"data_product_ring_gate": ["allowed", "warning"]},
                    }
                ],
            }
        ],
    }
    return manifest
