from __future__ import annotations

import json

from dpone.readiness.data_product_access import (
    AccessClassificationBuilder,
    AccessGateEvaluator,
    EntitlementPlanBuilder,
    PrivacyImpactAssessor,
)
from dpone.readiness.data_product_governance import GovernanceExportPlanner
from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_migration_bundle import (
    MigrationBundleBuilder,
    MigrationEvidenceArtifact,
)
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
)
from dpone.readiness.schema_migration_evidence_registry import MigrationEvidenceRecorder


def test_access_gate_bundle_registry_and_governance_export_integration() -> None:
    manifest = _manifest()
    classification = AccessClassificationBuilder().build(manifest=manifest, schema_contract=_schema_contract())
    entitlement = EntitlementPlanBuilder().build(
        manifest=manifest,
        classification=classification,
        consumer_matrix=_consumer_matrix(),
    )
    privacy = PrivacyImpactAssessor().assess(
        manifest=manifest,
        entitlement_plan=entitlement,
        authority_gate=_authority_gate(),
    )
    access_gate = AccessGateEvaluator().evaluate(entitlement_plan=entitlement, privacy_impact=privacy)
    pack = _pack()

    bundle = MigrationBundleBuilder().build(
        artifacts=(
            _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
            _artifact("data_product_access_gate", "access-gate.json", access_gate, required=False),
            _artifact("data_product_privacy_impact_assessment", "privacy.json", privacy, required=False),
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
                "required_artifacts": ["migration_pack", "data_product_access_gate"],
            },
        ),
        artifact_payloads={"data_product_access_gate": access_gate},
    )
    record = MigrationEvidenceRecorder().build_record(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        gate=None,
        trust_verification=None,
        diff=None,
        data_product_access_gate=access_gate,
        data_product_privacy_impact_assessment=privacy,
        environment="prod",
        stage="access_gate_passed",
    )
    governance = GovernanceExportPlanner().plan(
        manifest=_governance_manifest(),
        evidence={
            "data_product_access_classification": classification,
            "data_product_entitlement_plan": entitlement,
            "data_product_privacy_impact_assessment": privacy,
            "data_product_access_gate": access_gate,
        },
        registry_records=[record],
        targets=("json",),
    )

    assert access_gate["status"] == "allowed"
    assert bundle["summary"]["data_product_access_gate_id"] == access_gate["access_gate_id"]
    assert decision["status"] == "allowed"
    assert "migration_bundle.unknown_artifact_kind:data_product_access_gate" not in decision["warnings"]
    assert "access_gate_passed" in {record["stage"]}
    assert "data_product_access_gate" in {item["kind"] for item in record["artifact_refs"]}
    assert {"data_product_access_gate", "data_product_privacy_impact_assessment"} <= {
        item["kind"] for item in governance["evidence_refs"]
    }


def _manifest() -> dict:
    return {
        "sink": {
            "options": {
                "data_product": {
                    "id": "analytics.orders",
                    "owner": "data-platform",
                    "tier": "gold",
                    "criticality": "high",
                    "access_governance": {
                        "enabled": True,
                        "mode": "gate",
                        "profile": "regulated",
                        "classification": {
                            "default": "internal",
                            "columns": {
                                "customer_email": {
                                    "class": "pii",
                                    "masking": "hash",
                                    "lawful_basis_required": True,
                                },
                                "amount": {"class": "financial", "masking": "none"},
                            },
                        },
                        "entitlements": [
                            {
                                "subject": "finance.daily_margin",
                                "type": "consumer",
                                "owner": "finance-analytics",
                                "purpose": "finance_close",
                                "actions": ["read"],
                                "columns": ["amount", "customer_id"],
                            },
                            {
                                "subject": "support.ops_debug",
                                "type": "group",
                                "owner": "support",
                                "purpose": "support_debug",
                                "lawful_basis": "support_contract",
                                "actions": ["read"],
                                "columns": ["customer_email"],
                                "masking_required": True,
                                "masking": "hash",
                            },
                        ],
                        "privacy": {
                            "require_lawful_basis_for": ["pii", "regulated"],
                            "require_authority_gate_for": ["pii", "regulated"],
                        },
                    },
                }
            }
        }
    }


def _governance_manifest() -> dict:
    manifest = _manifest()
    product = manifest["sink"]["options"]["data_product"]
    product["governance_export"] = {
        "enabled": True,
        "mode": "gate",
        "profile": "regulated",
        "targets": [{"provider": "json", "mode": "render"}],
    }
    return manifest


def _schema_contract() -> dict:
    return {
        "schema_version": "dpone.schema_contract_version.v1",
        "contract_id": "analytics.orders",
        "columns": [{"name": "customer_id"}, {"name": "customer_email"}, {"name": "amount"}],
    }


def _consumer_matrix() -> dict:
    return {
        "schema_version": "dpone.schema_contract_consumer_matrix.v1",
        "status": "compatible",
        "consumer_matrix_id": "sha256:" + "c" * 64,
        "consumers": [
            {"id": "finance.daily_margin", "owner": "finance-analytics", "reads": {"columns": ["amount"]}},
            {"id": "support.ops_debug", "owner": "support", "reads": {"columns": ["customer_email"]}},
        ],
        "summary": {"consumers_count": 2, "blocked_consumers": 0},
    }


def _authority_gate() -> dict:
    return {
        "schema_version": "dpone.data_product_authority_gate.v1",
        "status": "allowed",
        "authority_gate_id": "sha256:" + "a" * 64,
        "blockers": [],
        "warnings": [],
    }


def _pack() -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders"},
        actual={"sink_type": "clickhouse", "table": "analytics.orders"},
        changes=({"change_type": "column_added", "path": "status"},),
        strategy="online_safe",
    )


def _artifact(kind: str, path: str, payload: dict[str, object], *, required: bool) -> MigrationEvidenceArtifact:
    return MigrationEvidenceArtifact.from_bytes(
        kind=kind,
        path=path,
        content=json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8"),
        required=required,
    )
