from __future__ import annotations

import json

from dpone.readiness.data_product_governance import GovernanceExportPlanner
from dpone.readiness.data_product_trust_index import TrustEvidenceLakeIndexer
from dpone.readiness.data_product_trust_rendering import TrustExportRenderer, TrustReportRenderer
from dpone.readiness.data_product_trust_snapshot import TrustGate, TrustSnapshotBuilder
from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
)
from dpone.readiness.schema_migration_evidence_registry import MigrationEvidenceRecorder


def test_trust_center_bundle_registry_and_governance_integration() -> None:
    pack = _pack()
    index = TrustEvidenceLakeIndexer().index(manifests=[_manifest()], evidence_payloads=_complete_evidence())
    snapshot = TrustSnapshotBuilder().snapshot(index=index, product_id="analytics.orders", profile="prod_strict")
    gate = TrustGate().evaluate(snapshot=snapshot, profile="prod_strict")
    report = TrustReportRenderer().report(snapshot=snapshot, gate=gate)
    export = TrustExportRenderer().export(snapshot=snapshot, target="json")

    bundle = MigrationBundleBuilder().build(
        artifacts=(
            _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
            _artifact("data_product_trust_gate", "trust-gate.json", gate),
            _artifact("data_product_trust_report", "trust-report.json", report),
            _artifact("data_product_trust_export", "trust-export.json", export),
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
                "required_artifacts": ["migration_pack", "data_product_trust_gate"],
            },
        ),
        artifact_payloads={"data_product_trust_gate": gate},
    )
    record = MigrationEvidenceRecorder().build_record(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        gate=None,
        trust_verification=None,
        diff=None,
        data_product_trust_gate=gate,
        data_product_trust_report=report,
        data_product_trust_export=export,
        environment="prod",
        stage="trust_gate_passed",
    )
    governance = GovernanceExportPlanner().plan(
        manifest=_governance_manifest(),
        evidence={
            "data_product_trust_gate": gate,
            "data_product_trust_report": report,
            "data_product_trust_export": export,
        },
        registry_records=[record],
        targets=("json",),
    )

    assert bundle["summary"]["data_product_trust_gate_id"] == gate["trust_gate_id"]
    assert decision["status"] == "allowed"
    assert "migration_bundle_gate.unknown_artifact_kind:data_product_trust_gate" not in decision["warnings"]
    assert record["stage"] == "trust_gate_passed"
    assert {"data_product_trust_gate", "data_product_trust_report", "data_product_trust_export"} <= {
        item["kind"] for item in record["artifact_refs"]
    }
    assert {"data_product_trust_gate", "data_product_trust_report", "data_product_trust_export"} <= {
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
    from tests.test_data_product_trust_contracts import _manifest

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


def _complete_evidence() -> tuple[dict, ...]:
    from tests.test_data_product_trust_cli import _complete_evidence

    return tuple(_complete_evidence().values())
