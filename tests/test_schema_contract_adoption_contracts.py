from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_contract_adoption import (
    SchemaContractAdoptionPlanner,
    SchemaContractAdoptionStatusBuilder,
    SchemaContractRetirementGate,
)
from dpone.readiness.schema_contract_compatibility_views import CompatibilityViewPlanner
from dpone.readiness.schema_contract_consumer_matrix import SchemaConsumerMatrixBuilder
from dpone.readiness.schema_contract_consumer_test_kit import SchemaConsumerCertificationEvaluator
from dpone.readiness.schema_contract_registry import SchemaContractVersionBuilder
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
)
from dpone.readiness.schema_migration_evidence_registry import MigrationEvidenceRecorder


def test_adoption_plan_tracks_active_pinned_consumers_and_view_coverage() -> None:
    base, head, matrix, view_plan = _evidence()

    plan = SchemaContractAdoptionPlanner().plan(
        manifest=_manifest(version="2.0.0", serving=True, adoption=True),
        base_contract=base,
        head_contract=head,
        consumer_matrix=matrix,
        compatibility_view_plan=view_plan,
    )

    assert plan["schema_version"] == "dpone.schema_contract_adoption_plan.v1"
    assert plan["status"] == "planned"
    assert plan["contract_id"] == "analytics.orders"
    assert plan["summary"]["active_consumers"] == 1
    assert plan["summary"]["covered_by_compatibility_view"] == 1
    assert plan["consumers"][0]["state"] == "not_started"
    assert plan["consumers"][0]["compatibility_view"] == "analytics.orders__contract_v1"
    assert plan["blockers"] == []


def test_adoption_plan_blocks_missing_compatibility_view_for_active_consumer() -> None:
    base, head, matrix, _view_plan = _evidence(serving=False)

    plan = SchemaContractAdoptionPlanner().plan(
        manifest=_manifest(version="2.0.0", adoption=True),
        base_contract=base,
        head_contract=head,
        consumer_matrix=matrix,
        compatibility_view_plan=None,
    )

    assert plan["status"] == "blocked"
    assert "schema_contract_adoption.compatibility_view_missing:finance.daily_margin" in plan["blockers"]


def test_adoption_status_merges_certification_and_retirement_gate_passes() -> None:
    base, head, matrix, view_plan = _evidence()
    plan = SchemaContractAdoptionPlanner().plan(
        manifest=_manifest(version="2.0.0", serving=True, adoption=True),
        base_contract=base,
        head_contract=head,
        consumer_matrix=matrix,
        compatibility_view_plan=view_plan,
    )
    certification = SchemaConsumerCertificationEvaluator().evaluate(
        test_kit={
            "schema_version": "dpone.schema_contract_consumer_test_kit.v1",
            "test_kit_id": "sha256:" + "1" * 64,
            "contract_id": "analytics.orders",
            "contract_version_id": head["contract_version_id"],
            "status": "ready",
            "test_cases": [
                {
                    "test_case_id": "sha256:" + "6" * 64,
                    "consumer_id": "finance.daily_margin",
                    "required": True,
                }
            ],
            "blockers": [],
            "warnings": [],
        },
        result={"status": "passed"},
    )

    status = SchemaContractAdoptionStatusBuilder().build(
        plan=plan,
        registry_records=[_registry_record("consumer_migrated", "finance.daily_margin")],
        consumer_certifications=[certification],
    )
    gate = SchemaContractRetirementGate().evaluate(status=status, profile="prod_strict")
    retirement = SchemaContractRetirementGate().retire(gate=gate, compatibility_view_plan=view_plan)

    assert status["schema_version"] == "dpone.schema_contract_adoption_status.v1"
    assert status["status"] == "migrated"
    assert status["consumers"][0]["state"] == "migrated"
    assert gate["schema_version"] == "dpone.schema_contract_retirement_gate.v1"
    assert gate["status"] == "allowed"
    assert retirement["schema_version"] == "dpone.schema_contract_retirement_plan.v1"
    assert retirement["status"] == "ready"


def test_retirement_gate_blocks_active_or_unknown_consumers() -> None:
    base, head, matrix, view_plan = _evidence()
    plan = SchemaContractAdoptionPlanner().plan(
        manifest=_manifest(version="2.0.0", serving=True, adoption=True),
        base_contract=base,
        head_contract=head,
        consumer_matrix=matrix,
        compatibility_view_plan=view_plan,
    )

    status = SchemaContractAdoptionStatusBuilder().build(plan=plan, registry_records=[], consumer_certifications=[])
    gate = SchemaContractRetirementGate().evaluate(status=status, profile="prod_strict")

    assert status["status"] == "active"
    assert gate["status"] == "blocked"
    assert "schema_contract_retirement.active_consumer:finance.daily_margin" in gate["blockers"]


def test_bundle_policy_and_registry_accept_contract_retirement_gate() -> None:
    pack = _pack()
    gate = {
        "schema_version": "dpone.schema_contract_retirement_gate.v1",
        "status": "allowed",
        "pack_id": pack.pack_id,
        "contract_id": "analytics.orders",
        "retirement_gate_id": "sha256:" + "4" * 64,
        "adoption_status_id": "sha256:" + "5" * 64,
        "blockers": [],
        "warnings": [],
    }
    artifacts = (
        _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
        _artifact("contract_retirement_gate", "contract-retirement-gate.json", gate, required=False),
    )

    bundle = MigrationBundleBuilder().build(artifacts=artifacts, attest=True)
    decision = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=MigrationBundlePolicyOptions.resolve(
            profile="prod_strict",
            policy_payload={
                "required_artifacts": ["migration_pack", "contract_retirement_gate"],
                "fail_on_warnings": False,
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
        contract_retirement_gate=gate,
        environment="prod",
        stage="contract_retirement_ready",
    )

    assert bundle["summary"]["contract_retirement_gate_id"] == gate["retirement_gate_id"]
    assert decision["status"] == "allowed"
    assert record["status"] == "ready"
    assert "contract_retirement_gate" in {item["kind"] for item in record["artifact_refs"]}


def test_public_json_schemas_validate_adoption_artifacts() -> None:
    base, head, matrix, view_plan = _evidence()
    plan = SchemaContractAdoptionPlanner().plan(
        manifest=_manifest(version="2.0.0", serving=True, adoption=True),
        base_contract=base,
        head_contract=head,
        consumer_matrix=matrix,
        compatibility_view_plan=view_plan,
    )
    status = SchemaContractAdoptionStatusBuilder().build(plan=plan, registry_records=[], consumer_certifications=[])
    gate = SchemaContractRetirementGate().evaluate(status=status, profile="advisory")
    retirement = SchemaContractRetirementGate().retire(gate=gate, compatibility_view_plan=view_plan)

    for name, payload in (
        ("schema-contract-adoption-plan.schema.json", plan),
        ("schema-contract-adoption-status.schema.json", status),
        ("schema-contract-retirement-gate.schema.json", gate),
        ("schema-contract-retirement-plan.schema.json", retirement),
    ):
        schema = json.loads((Path("docs/schemas/schema-migration") / name).read_text(encoding="utf-8"))
        Draft202012Validator(schema).validate(payload)


def _evidence(
    *, serving: bool = True
) -> tuple[dict[str, object], dict[str, object], dict[str, object], dict[str, object]]:
    base = SchemaContractVersionBuilder().build(manifest=_manifest(version="1.5.0"))
    head = SchemaContractVersionBuilder().build(manifest=_manifest(version="2.0.0", drop_amount=True))
    matrix = SchemaConsumerMatrixBuilder().build(
        base=base,
        head=head,
        inventory={
            "schema_version": "dpone.schema_contract_consumer_inventory.v1",
            "contract_id": "analytics.orders",
            "status": "discovered",
            "blockers": [],
            "warnings": [],
            "consumers": [
                {
                    "id": "finance.daily_margin",
                    "type": "dashboard",
                    "owner": "finance-analytics",
                    "source": "manual",
                    "version_constraint": "1.x",
                    "reads": {"columns": ["amount", "customer_id"]},
                }
            ],
        },
        unknown_consumer="warn",
    )
    view_plan = CompatibilityViewPlanner().plan(
        manifest=_manifest(version="2.0.0", drop_amount=True, serving=serving),
        base_contract=base,
        head_contract=head,
        consumer_matrix=matrix,
    )
    return base, head, matrix, view_plan


def _manifest(
    *,
    version: str,
    drop_amount: bool = False,
    serving: bool = False,
    adoption: bool = False,
) -> dict[str, object]:
    columns: dict[str, object] = {
        "order_id": {"type": "integer", "nullable": False},
        "customer_id": {"type": "integer", "nullable": True},
    }
    if not drop_amount:
        columns["amount"] = {"type": "decimal", "precision": 18, "scale": 2, "nullable": True}
    contract: dict[str, object] = {
        "id": "analytics.orders",
        "version": version,
        "owner": "data-platform",
        "compatibility": "backward",
        "registry": {"enabled": True, "mode": "gate"},
        "versioning": {"semver": "strict", "unknown_consumer": "warn"},
        "columns": columns,
    }
    if adoption:
        contract["adoption"] = {
            "enabled": True,
            "mode": "gate",
            "profile": "prod_strict",
            "default_migration_window_days": 90,
            "expired_window_policy": "block",
            "require_consumer_certification": True,
            "unknown_consumer": "warn",
        }
    if serving:
        contract["serving"] = {
            "enabled": True,
            "mode": "gate",
            "default_strategy": "projection_view",
            "view_naming": "{schema}.{table}__contract_v{major}",
            "versions": [
                {
                    "constraint": "1.x",
                    "source_contract": "analytics.orders@1.5.0",
                    "view": "analytics.orders__contract_v1",
                    "remove_after": "2099-09-01",
                    "owner": "data-platform",
                }
            ],
        }
    return {
        "sink": {
            "type": "clickhouse",
            "table": {"schema": "analytics", "name": "orders"},
            "options": {
                "schema_contract": contract,
                "physical_design": {"storage": {"clickhouse": {"order_by": ["order_id"]}}},
            },
        }
    }


def _registry_record(stage: str, consumer_id: str) -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_migration_evidence_registry_record.v1",
        "record_id": "sha256:" + "2" * 64,
        "target": {"sink_type": "clickhouse", "table": "analytics.orders"},
        "environment": "prod",
        "stage": stage,
        "pack_id": "sha256:" + "3" * 64,
        "bundle_id": "sha256:" + "4" * 64,
        "status": "ready",
        "artifact_refs": [{"kind": "consumer", "path": consumer_id}],
        "blockers": [],
        "warnings": [],
    }


def _pack() -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders"},
        actual={"sink_type": "clickhouse", "table": "analytics.orders"},
        changes=({"change_type": "compatibility_view_retired", "path": "analytics.orders__contract_v1"},),
        strategy="expand_contract",
    )


def _artifact(kind: str, path: str, payload: dict[str, object], *, required: bool) -> MigrationEvidenceArtifact:
    content = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return MigrationEvidenceArtifact.from_bytes(kind=kind, path=path, content=content, required=required)
