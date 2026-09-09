from __future__ import annotations

import json

from dpone.readiness.data_product_governance import GovernanceExportPlanner
from dpone.readiness.data_product_remediation import (
    DataProductRemediationCloseout,
    DataProductRemediationGate,
    DataProductRemediationPlanner,
)
from dpone.readiness.data_product_remediation_rendering import RemediationRenderer
from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
)
from dpone.readiness.schema_migration_evidence_registry import MigrationEvidenceRecorder


def test_remediation_bundle_registry_and_governance_integration() -> None:
    pack = _pack()
    plan = DataProductRemediationPlanner().plan(
        manifest=_manifest(),
        trust_gate=_trust_gate(),
        trust_snapshot=_trust_snapshot(),
        evidence_payloads=[_assertion_gate(status="blocked")],
        pack_id=pack.pack_id,
        bundle_id="sha256:bundle",
    )
    gate = DataProductRemediationGate().evaluate(plan=plan, profile="prod_strict")
    runbook = RemediationRenderer().runbook(plan=plan)
    closeout = DataProductRemediationCloseout().evaluate(
        plan=plan,
        evidence_payloads=[_assertion_gate(status="allowed", evidence_id="sha256:fresh")],
    )
    report = RemediationRenderer().report(gate=gate, closeout=closeout)

    bundle = MigrationBundleBuilder().build(
        artifacts=(
            _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
            _artifact("data_product_remediation_gate", "remediation-gate.json", gate),
            _artifact("data_product_remediation_runbook", "runbook.json", runbook),
            _artifact("data_product_remediation_closeout", "closeout.json", closeout),
            _artifact("data_product_remediation_report", "report.json", report),
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
                "required_artifacts": ["migration_pack", "data_product_remediation_gate"],
            },
        ),
        artifact_payloads={"data_product_remediation_gate": gate},
    )
    record = MigrationEvidenceRecorder().build_record(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        gate=None,
        trust_verification=None,
        diff=None,
        data_product_remediation_gate=gate,
        data_product_remediation_runbook=runbook,
        data_product_remediation_closeout=closeout,
        data_product_remediation_report=report,
        environment="prod",
        stage="data_product_remediation_closed",
    )
    governance = GovernanceExportPlanner().plan(
        manifest=_governance_manifest(),
        evidence={
            "data_product_remediation_gate": gate,
            "data_product_remediation_closeout": closeout,
            "data_product_remediation_report": report,
        },
        registry_records=[record],
        targets=("json",),
    )

    assert bundle["summary"]["data_product_remediation_gate_id"] == gate["remediation_gate_id"]
    assert decision["status"] == "allowed"
    assert "migration_bundle_gate.unknown_artifact_kind:data_product_remediation_gate" not in decision["warnings"]
    assert record["stage"] == "data_product_remediation_closed"
    assert {"data_product_remediation_gate", "data_product_remediation_closeout"} <= {
        item["kind"] for item in record["artifact_refs"]
    }
    assert {"data_product_remediation_gate", "data_product_remediation_closeout"} <= {
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
    from tests.test_data_product_remediation_contracts import _manifest

    return _manifest()


def _governance_manifest() -> dict:
    manifest = _manifest()
    manifest["sink"]["options"]["data_product"]["governance_export"] = {
        "enabled": True,
        "mode": "gate",
        "profile": "prod_strict",
        "targets": [{"provider": "json", "mode": "render"}],
    }
    return manifest


def _trust_gate() -> dict:
    from tests.test_data_product_remediation_contracts import _trust_gate

    return _trust_gate(status="blocked")


def _trust_snapshot() -> dict:
    from tests.test_data_product_remediation_contracts import _trust_snapshot

    return _trust_snapshot()


def _assertion_gate(*, status: str, evidence_id: str = "sha256:assertion-old") -> dict:
    from tests.test_data_product_remediation_contracts import _assertion_gate

    return _assertion_gate(status=status, evidence_id=evidence_id)
