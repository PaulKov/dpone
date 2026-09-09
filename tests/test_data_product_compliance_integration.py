from __future__ import annotations

import json

from dpone.readiness.data_product_compliance import (
    AuditPackageRenderer,
    ComplianceControlEvaluator,
    ComplianceControlPlanner,
    ComplianceGate,
)
from dpone.readiness.data_product_governance import GovernanceExportPlanner
from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
)
from dpone.readiness.schema_migration_evidence_registry import MigrationEvidenceRecorder


def test_bundle_registry_and_governance_accept_compliance_artifacts() -> None:
    pack = _pack()
    plan = ComplianceControlPlanner().plan(manifest=_manifest(), evidence=_evidence(pack_id=pack.pack_id))
    evaluation = ComplianceControlEvaluator().evaluate(plan=plan)
    gate = ComplianceGate().evaluate(evaluation=evaluation, profile="regulated")
    package = AuditPackageRenderer().render(gate=gate)
    artifacts = (
        _bundle_artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
        _bundle_artifact("data_product_compliance_gate", "compliance-gate.json", gate, required=False),
        _bundle_artifact("data_product_audit_package", "audit-package.json", package, required=False),
    )
    bundle = MigrationBundleBuilder().build(artifacts=artifacts, attest=True)

    decision = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=MigrationBundlePolicyOptions.resolve(
            profile="prod_strict",
            policy_payload={"required_artifacts": ["migration_pack", "data_product_compliance_gate"]},
        ),
        artifact_payloads={item.kind: item.payload for item in artifacts},
    )
    record = MigrationEvidenceRecorder().build_record(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        gate=None,
        trust_verification=None,
        diff=None,
        data_product_compliance_gate={**gate, "pack_id": pack.pack_id, "bundle_id": bundle["bundle_id"]},
        data_product_audit_package={**package, "pack_id": pack.pack_id, "bundle_id": bundle["bundle_id"]},
        environment="prod",
        stage="compliance_gate_passed",
    )
    governance = GovernanceExportPlanner().plan(
        manifest=_manifest(),
        evidence={"data_product_compliance_gate": gate, "data_product_audit_package": package},
        registry_records=(record,),
        targets=("json",),
    )

    assert decision["status"] in {"allowed", "warning"}
    assert record["status"] == "ready"
    assert {"data_product_compliance_gate", "data_product_audit_package"} <= {
        ref["kind"] for ref in record["artifact_refs"]
    }
    assert {"data_product_compliance_gate", "data_product_audit_package"} <= {
        ref["kind"] for ref in governance["evidence_refs"]
    }


def _manifest() -> dict:
    return {
        "sink": {
            "options": {
                "data_product": {
                    "id": "analytics.orders",
                    "owner": "data-platform",
                    "tier": "regulated",
                    "criticality": "critical",
                    "compliance": {
                        "enabled": True,
                        "mode": "gate",
                        "profile": "regulated",
                        "frameworks": [
                            {
                                "id": "soc2",
                                "version": "2026.1",
                                "owner": "security-governance",
                                "controls": [
                                    {
                                        "id": "CC7.2",
                                        "title": "Release evidence",
                                        "severity": "critical",
                                        "require_artifacts": ["data_product_policy_gate"],
                                        "allowed_statuses": {
                                            "data_product_policy_gate": ["allowed", "warning", "waived"]
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    "governance_export": {
                        "enabled": True,
                        "mode": "gate",
                        "profile": "prod_strict",
                        "targets": [{"provider": "json", "mode": "render"}],
                    },
                }
            }
        }
    }


def _evidence(*, pack_id: str) -> dict[str, dict]:
    return {
        "data_product_policy_gate": {
            "schema_version": "dpone.data_product_policy_gate.v1",
            "policy_gate_id": "sha256:" + "1" * 64,
            "status": "waived",
            "product_id": "analytics.orders",
            "pack_id": pack_id,
            "blockers": [],
            "warnings": [],
        }
    }


def _pack() -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders", "order_by": ["id"]},
        actual={"sink_type": "clickhouse", "table": "analytics.orders", "order_by": ["id"]},
        changes=({"change_type": "table_setting", "path": "table_settings.index_granularity"},),
        ddl=("ALTER TABLE analytics.orders MODIFY SETTING index_granularity = 8192",),
        strategy="online_safe",
    )


def _bundle_artifact(kind: str, path: str, payload: dict, *, required: bool) -> MigrationEvidenceArtifact:
    content = json.dumps(payload, sort_keys=True).encode("utf-8")
    return MigrationEvidenceArtifact.from_bytes(kind=kind, path=path, content=content, required=required)
