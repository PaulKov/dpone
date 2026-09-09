from __future__ import annotations

from dpone.readiness.data_product_access_enforcement import (
    AccessDriftInspector,
    AccessEnforcementCertifier,
    AccessEnforcementPlanner,
    AccessEnforcementRunner,
)
from dpone.readiness.data_product_access_enforcement_clickhouse import ClickHouseAccessDialect
from dpone.readiness.data_product_governance import GovernanceExportPlanner
from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
)
from dpone.readiness.schema_migration_evidence_registry import MigrationEvidenceRecorder


def test_access_enforcement_bundle_registry_and_governance_integration() -> None:
    plan = AccessEnforcementPlanner().plan(
        manifest=_manifest(),
        classification=_classification(),
        entitlement_plan=_entitlement_plan(),
        privacy_impact=_privacy_impact(),
        access_gate=_access_gate(),
        authority_gate=_authority_gate(),
        target_connection=_target_connection(),
        dialect=ClickHouseAccessDialect(),
        environment="prod",
    )
    run = AccessEnforcementRunner().apply(
        plan=plan,
        approval={"status": "approved", "approved_by": "data-governance"},
        execute=True,
        executor=_FakeExecutor(),
    )
    drift = AccessDriftInspector().inspect(plan=plan, actual_state=plan["desired_state"])
    certificate = AccessEnforcementCertifier().certify(run=run, drift_report=drift, profile="regulated")
    pack = _pack()

    bundle = MigrationBundleBuilder().build(
        artifacts=(
            _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
            _artifact("data_product_access_enforcement_certificate", "certificate.json", certificate),
            _artifact("data_product_access_drift_report", "drift.json", drift),
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
                "required_artifacts": ["migration_pack", "data_product_access_enforcement_certificate"],
            },
        ),
        artifact_payloads={"data_product_access_enforcement_certificate": certificate},
    )
    record = MigrationEvidenceRecorder().build_record(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        gate=None,
        trust_verification=None,
        diff=None,
        data_product_access_enforcement_certificate=certificate,
        data_product_access_drift_report=drift,
        environment="prod",
        stage="access_enforcement_certified",
    )
    governance = GovernanceExportPlanner().plan(
        manifest=_governance_manifest(),
        evidence={
            "data_product_access_enforcement_certificate": certificate,
            "data_product_access_drift_report": drift,
        },
        registry_records=[record],
        targets=("json",),
    )

    assert certificate["status"] in {"certified", "warning"}
    assert (
        bundle["summary"]["data_product_access_enforcement_certificate_id"]
        == certificate["access_enforcement_certificate_id"]
    )
    assert decision["status"] == "allowed"
    assert "access_enforcement_certified" in {record["stage"]}
    assert "data_product_access_enforcement_certificate" in {item["kind"] for item in record["artifact_refs"]}
    assert {"data_product_access_enforcement_certificate", "data_product_access_drift_report"} <= {
        item["kind"] for item in governance["evidence_refs"]
    }


def _artifact(kind: str, path: str, payload: dict, *, required: bool = False) -> MigrationEvidenceArtifact:
    import json

    return MigrationEvidenceArtifact.from_bytes(
        kind=kind,
        path=path,
        content=json.dumps(payload, sort_keys=True).encode("utf-8"),
        required=required,
    )


class _FakeExecutor:
    def execute(self, operation: dict) -> dict:
        del operation
        return {"status": "executed"}


def _pack() -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"columns": ["amount", "customer_id", "customer_email"]},
        actual={"columns": ["amount", "customer_id", "customer_email"]},
        strategy="additive",
        changes=(),
        blockers=(),
        warnings=(),
        ddl=(),
    )


def _governance_manifest() -> dict:
    return {
        "sink": {
            "options": {
                "data_product": {
                    "id": "analytics.orders",
                    "owner": "data-platform",
                    "tier": "gold",
                    "criticality": "high",
                    "governance_export": {
                        "enabled": True,
                        "targets": [{"provider": "json", "mode": "render"}],
                    },
                }
            }
        }
    }


def _manifest() -> dict:
    from tests.test_data_product_access_enforcement_contracts import _manifest as _base_manifest

    return _base_manifest()


def _classification() -> dict:
    from tests.test_data_product_access_enforcement_contracts import _classification as _base_classification

    return _base_classification()


def _entitlement_plan() -> dict:
    from tests.test_data_product_access_enforcement_contracts import _entitlement_plan as _base_entitlement_plan

    return _base_entitlement_plan()


def _privacy_impact() -> dict:
    from tests.test_data_product_access_enforcement_contracts import _privacy_impact as _base_privacy_impact

    return _base_privacy_impact()


def _access_gate() -> dict:
    from tests.test_data_product_access_enforcement_contracts import _access_gate as _base_access_gate

    return _base_access_gate()


def _authority_gate() -> dict:
    from tests.test_data_product_access_enforcement_contracts import _authority_gate as _base_authority_gate

    return _base_authority_gate()


def _target_connection() -> dict:
    from tests.test_data_product_access_enforcement_contracts import _target_connection as _base_target_connection

    return _base_target_connection()
