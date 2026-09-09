from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_contract_compatibility_views import (
    CompatibilityViewGate,
    CompatibilityViewPlanner,
)
from dpone.readiness.schema_contract_consumer_matrix import SchemaConsumerMatrixBuilder
from dpone.readiness.schema_contract_registry import SchemaContractVersionBuilder
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
)
from dpone.readiness.schema_migration_evidence_registry import MigrationEvidenceRecorder


def test_major_drop_blocks_without_compatibility_view_coverage() -> None:
    base = SchemaContractVersionBuilder().build(manifest=_manifest(version="1.5.0"))
    head = SchemaContractVersionBuilder().build(manifest=_manifest(version="2.0.0", drop_amount=True))
    matrix = _consumer_matrix(base, head, reads=("amount", "customer_id"))

    plan = CompatibilityViewPlanner().plan(
        manifest=_manifest(version="2.0.0", drop_amount=True),
        base_contract=base,
        head_contract=head,
        consumer_matrix=matrix,
    )
    gate = CompatibilityViewGate().evaluate(plan=plan, consumer_gate=_consumer_gate(matrix), mode="gate")

    assert plan["schema_version"] == "dpone.schema_contract_compatibility_view_plan.v1"
    assert plan["status"] == "blocked"
    assert "schema_contract_view.version_uncovered:1.x" in plan["blockers"]
    assert gate["status"] == "blocked"


def test_projection_view_covers_major_drop_and_renders_clickhouse_ddl() -> None:
    base = SchemaContractVersionBuilder().build(manifest=_manifest(version="1.5.0"))
    head = SchemaContractVersionBuilder().build(manifest=_manifest(version="2.0.0", drop_amount=True))
    matrix = _consumer_matrix(base, head, reads=("amount", "customer_id"))

    plan = CompatibilityViewPlanner().plan(
        manifest=_manifest(version="2.0.0", drop_amount=True, include_serving=True),
        base_contract=base,
        head_contract=head,
        consumer_matrix=matrix,
    )
    gate = CompatibilityViewGate().evaluate(plan=plan, consumer_gate=_consumer_gate(matrix), mode="gate")

    assert plan["status"] == "planned"
    assert plan["summary"] == {"views_count": 1, "covered_versions": ["1.x"], "blocked_projections": 0}
    assert plan["views"][0]["version_constraint"] == "1.x"
    assert plan["views"][0]["projections"] == [
        {"column": "amount", "expression": "amount", "kind": "direct"},
        {"column": "customer_id", "expression": "customer_id", "kind": "direct"},
        {"column": "order_id", "expression": "order_id", "kind": "direct"},
    ]
    assert "CREATE OR REPLACE VIEW `analytics`.`orders__contract_v1` AS SELECT" in plan["views"][0]["ddl"][0]
    assert gate["status"] == "allowed"


def test_rename_alias_maps_current_column_to_legacy_name() -> None:
    base = SchemaContractVersionBuilder().build(manifest=_manifest(version="1.5.0", legacy_customer=True))
    head = SchemaContractVersionBuilder().build(manifest=_manifest(version="2.0.0"))

    plan = CompatibilityViewPlanner().plan(
        manifest=_manifest(version="2.0.0", include_serving=True),
        base_contract=base,
        head_contract=head,
        consumer_matrix=_consumer_matrix(base, head, reads=("client_id",)),
    )

    projections = {item["column"]: item for item in plan["views"][0]["projections"]}
    assert projections["client_id"] == {
        "column": "client_id",
        "expression": "customer_id",
        "kind": "alias",
    }
    assert "`customer_id` AS `client_id`" in plan["views"][0]["ddl"][0]


def test_expired_view_blocks_by_default_and_observe_gate_warns() -> None:
    base = SchemaContractVersionBuilder().build(manifest=_manifest(version="1.5.0"))
    head = SchemaContractVersionBuilder().build(manifest=_manifest(version="2.0.0", drop_amount=True))

    plan = CompatibilityViewPlanner().plan(
        manifest=_manifest(version="2.0.0", drop_amount=True, include_serving=True, remove_after="2020-01-01"),
        base_contract=base,
        head_contract=head,
        consumer_matrix=_consumer_matrix(base, head, reads=("amount",)),
    )
    gate = CompatibilityViewGate().evaluate(plan=plan, consumer_gate=_consumer_gate({}), mode="observe")

    assert plan["status"] == "blocked"
    assert "schema_contract_view.expired:analytics.orders__contract_v1" in plan["blockers"]
    assert gate["status"] == "warning"
    assert "schema_contract_view.expired:analytics.orders__contract_v1" in gate["warnings"]


def test_bundle_policy_and_registry_accept_compatibility_view_gate() -> None:
    pack = _pack()
    gate = {
        "schema_version": "dpone.schema_contract_compatibility_view_gate.v1",
        "status": "allowed",
        "pack_id": pack.pack_id,
        "contract_id": "analytics.orders",
        "compatibility_view_gate_id": "sha256:" + "4" * 64,
        "compatibility_view_plan_id": "sha256:" + "5" * 64,
        "blockers": [],
        "warnings": [],
    }
    artifacts = (
        _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
        _artifact("compatibility_view_gate", "compatibility-view-gate.json", gate, required=False),
    )

    bundle = MigrationBundleBuilder().build(artifacts=artifacts, attest=True)
    decision = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=MigrationBundlePolicyOptions.resolve(
            profile="prod_strict",
            policy_payload={
                "required_artifacts": ["migration_pack", "compatibility_view_gate"],
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
        compatibility_view_gate=gate,
        environment="prod",
        stage="compatibility_view_planned",
    )

    assert bundle["summary"]["compatibility_view_gate_id"] == gate["compatibility_view_gate_id"]
    assert decision["status"] == "allowed"
    assert record["status"] == "ready"
    assert "compatibility_view_gate" in {item["kind"] for item in record["artifact_refs"]}


def test_public_json_schemas_validate_plan_and_gate() -> None:
    base = SchemaContractVersionBuilder().build(manifest=_manifest(version="1.5.0"))
    head = SchemaContractVersionBuilder().build(manifest=_manifest(version="2.0.0", drop_amount=True))
    plan = CompatibilityViewPlanner().plan(
        manifest=_manifest(version="2.0.0", drop_amount=True, include_serving=True),
        base_contract=base,
        head_contract=head,
        consumer_matrix=_consumer_matrix(base, head, reads=("amount",)),
    )
    gate = CompatibilityViewGate().evaluate(plan=plan, consumer_gate=_consumer_gate({}), mode="gate")

    plan_schema = json.loads(
        Path("docs/schemas/schema-migration/schema-contract-compatibility-view-plan.schema.json").read_text(
            encoding="utf-8"
        )
    )
    gate_schema = json.loads(
        Path("docs/schemas/schema-migration/schema-contract-compatibility-view-gate.schema.json").read_text(
            encoding="utf-8"
        )
    )

    Draft202012Validator(plan_schema).validate(plan)
    Draft202012Validator(gate_schema).validate(gate)


def _consumer_matrix(base: dict[str, object], head: dict[str, object], *, reads: tuple[str, ...]) -> dict[str, object]:
    return SchemaConsumerMatrixBuilder().build(
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
                    "reads": {"columns": list(reads)},
                }
            ],
        },
        unknown_consumer="warn",
    )


def _consumer_gate(matrix: dict[str, object]) -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_contract_consumer_gate.v1",
        "status": "blocked" if matrix.get("blockers") else "allowed",
        "consumer_matrix_id": matrix.get("consumer_matrix_id"),
        "blockers": matrix.get("blockers", []),
        "warnings": matrix.get("warnings", []),
    }


def _manifest(
    *,
    version: str,
    drop_amount: bool = False,
    include_serving: bool = False,
    legacy_customer: bool = False,
    remove_after: str = "2099-09-01",
) -> dict[str, object]:
    columns: dict[str, object] = {"order_id": {"type": "integer", "nullable": False}}
    if legacy_customer:
        columns["client_id"] = {"type": "integer", "nullable": True}
    else:
        columns["customer_id"] = {"type": "integer", "nullable": True}
    if not drop_amount:
        columns["amount"] = {"type": "decimal", "precision": 18, "scale": 2, "nullable": True}
    contract: dict[str, object] = {
        "id": "analytics.orders",
        "version": version,
        "owner": "data-platform",
        "compatibility": "backward",
        "registry": {"enabled": True, "mode": "gate"},
        "columns": columns,
    }
    if include_serving:
        contract["serving"] = {
            "enabled": True,
            "mode": "gate",
            "default_strategy": "projection_view",
            "unknown_mapping": "block",
            "expired_view": "block",
            "view_naming": "{schema}.{table}__contract_v{major}",
            "versions": [
                {
                    "constraint": "1.x",
                    "source_contract": "analytics.orders@1.5.0",
                    "view": "analytics.orders__contract_v1",
                    "remove_after": remove_after,
                    "owner": "data-platform",
                    "columns": {"client_id": {"from": "customer_id"}},
                }
            ],
        }
    return {
        "sink": {
            "type": "clickhouse",
            "table": {"schema": "analytics", "name": "orders"},
            "options": {
                "schema_contract": contract,
                "schema_identity": {
                    "enabled": True,
                    "columns": {
                        "order_id": {"id": "orders.order_id"},
                        "customer_id": {
                            "id": "orders.customer_id",
                            "aliases": [{"name": "client_id", "remove_after": "2099-09-01"}],
                        },
                        "client_id": {"id": "orders.customer_id"},
                        "amount": {"id": "orders.amount"},
                    },
                },
                "physical_design": {"storage": {"clickhouse": {"order_by": ["order_id"]}}},
            },
        }
    }


def _pack() -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders"},
        actual={"sink_type": "clickhouse", "table": "analytics.orders"},
        changes=({"change_type": "column_removed", "path": "amount"},),
        strategy="expand_contract",
    )


def _artifact(kind: str, path: str, payload: dict[str, object], *, required: bool) -> MigrationEvidenceArtifact:
    content = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return MigrationEvidenceArtifact.from_bytes(kind=kind, path=path, content=content, required=required)
