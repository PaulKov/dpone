from __future__ import annotations

import json

from dpone.readiness.data_product_governance import GovernanceExportPlanner
from dpone.readiness.data_product_remediation_execution import (
    RemediationExecutionCertifier,
    RemediationExecutionPlanner,
    RemediationExecutionRunner,
)
from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
)
from dpone.readiness.schema_migration_evidence_registry import MigrationEvidenceRecorder


class _FakeExecutor:
    def execute(self, argv: tuple[str, ...], *, timeout_seconds: int) -> dict:
        del argv, timeout_seconds
        return {"status": "succeeded", "exit_code": 0, "duration_ms": 1, "stdout": "ok", "stderr": ""}


def test_execution_bundle_registry_and_governance_integration() -> None:
    execution_plan = _execution_plan()
    run = RemediationExecutionRunner().run(
        execution_plan=execution_plan,
        executor=_FakeExecutor(),
        execute=True,
        idempotency_key="orders-remediation-1",
        lock=_lock(execution_plan),
    )
    certificate = RemediationExecutionCertifier().certify(
        run=run,
        evidence_payloads=[_assertion_gate(status="allowed", evidence_id="sha256:fresh")],
        profile="prod_strict",
    )
    bundle = MigrationBundleBuilder().build(
        artifacts=(
            _artifact("migration_pack", "pack.json", _pack().to_dict(command="plan"), required=True),
            _artifact("data_product_remediation_execution_run", "execution-run.json", run),
            _artifact("data_product_remediation_execution_certificate", "execution-certificate.json", certificate),
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
                "required_artifacts": ["migration_pack", "data_product_remediation_execution_certificate"],
            },
        ),
        artifact_payloads={"data_product_remediation_execution_certificate": certificate},
    )
    record = MigrationEvidenceRecorder().build_record(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        gate=None,
        trust_verification=None,
        diff=None,
        data_product_remediation_execution_run=run,
        data_product_remediation_execution_certificate=certificate,
        environment="prod",
        stage="data_product_remediation_execution_certified",
    )
    governance = GovernanceExportPlanner().plan(
        manifest=_governance_manifest(),
        evidence={
            "data_product_remediation_execution_run": run,
            "data_product_remediation_execution_certificate": certificate,
        },
        registry_records=[record],
        targets=("json",),
    )

    assert decision["status"] == "allowed"
    assert (
        "migration_bundle_gate.unknown_artifact_kind:data_product_remediation_execution_certificate"
        not in decision["warnings"]
    )
    assert record["stage"] == "data_product_remediation_execution_certified"
    assert {"data_product_remediation_execution_run", "data_product_remediation_execution_certificate"} <= {
        item["kind"] for item in record["artifact_refs"]
    }
    assert "data_product_remediation_execution_certificate_id" in bundle["summary"]
    assert "data_product_remediation_execution_certificate" in {item["kind"] for item in governance["evidence_refs"]}


def _execution_plan() -> dict:
    from tests.test_data_product_remediation_execution_contracts import (
        _authority_gate,
        _manifest,
        _remediation_gate,
        _remediation_plan,
    )

    remediation_plan = _remediation_plan()
    return RemediationExecutionPlanner().plan(
        manifest=_manifest(),
        remediation_plan=remediation_plan,
        remediation_gate=_remediation_gate(remediation_plan),
        authority_gate=_authority_gate(),
        parameters={"assertion-plan": "orders.assertion-plan.json"},
    )


def _lock(execution_plan: dict) -> dict:
    from tests.test_data_product_remediation_execution_contracts import _lock

    return _lock(execution_plan)


def _assertion_gate(*, status: str, evidence_id: str = "sha256:assertion-old") -> dict:
    from tests.test_data_product_remediation_execution_contracts import _assertion_gate

    return _assertion_gate(status=status, evidence_id=evidence_id)


def _governance_manifest() -> dict:
    from tests.test_data_product_remediation_execution_contracts import _manifest

    manifest = _manifest()
    manifest["sink"]["options"]["data_product"]["governance_export"] = {
        "enabled": True,
        "mode": "gate",
        "profile": "prod_strict",
        "targets": [{"provider": "json", "mode": "render"}],
    }
    return manifest


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
