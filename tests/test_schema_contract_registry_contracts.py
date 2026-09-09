from __future__ import annotations

import json
from pathlib import Path

from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_contract_registry import (
    SchemaCompatibilityClassifier,
    SchemaConsumerCompatibilityGate,
    SchemaContractComparator,
    SchemaContractVersionBuilder,
)
from dpone.readiness.schema_contract_registry_sqlite import SqliteSchemaContractRegistryStore
from dpone.readiness.schema_contract_registry_store import LocalJsonSchemaContractRegistryStore
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
)
from dpone.readiness.schema_migration_evidence_registry import MigrationEvidenceRecorder
from dpone.services.schema_migration import MigrationControlFacade


def test_first_publish_creates_stable_contract_version_and_store_is_idempotent(tmp_path: Path) -> None:
    manifest = _manifest(version="1.0.0")
    version = SchemaContractVersionBuilder().build(manifest=manifest)
    store = LocalJsonSchemaContractRegistryStore(tmp_path / "contracts.json")

    first = store.append(version)
    duplicate = store.append(version)
    latest = store.latest(contract_id="analytics.orders")

    assert version["schema_version"] == "dpone.schema_contract_version.v1"
    assert version["contract_id"] == "analytics.orders"
    assert version["version"] == "1.0.0"
    assert version["status"] == "published"
    assert version["contract_version_id"].startswith("sha256:")
    assert first["status"] == "recorded"
    assert duplicate["status"] == "duplicate"
    assert latest["contract_version_id"] == version["contract_version_id"]


def test_same_contract_version_with_different_fingerprint_blocks(tmp_path: Path) -> None:
    store = LocalJsonSchemaContractRegistryStore(tmp_path / "contracts.json")
    first = SchemaContractVersionBuilder().build(manifest=_manifest(version="1.0.0"))
    changed = SchemaContractVersionBuilder().build(manifest=_manifest(version="1.0.0", include_new_column=True))

    store.append(first)
    result = store.append(changed)

    assert result["status"] == "blocked"
    assert "schema_contract_registry.version_conflict" in result["blockers"]


def test_sqlite_store_matches_local_json_history_and_latest(tmp_path: Path) -> None:
    local = LocalJsonSchemaContractRegistryStore(tmp_path / "contracts.json")
    sqlite = SqliteSchemaContractRegistryStore(tmp_path / "contracts.sqlite3")
    first = SchemaContractVersionBuilder().build(manifest=_manifest(version="1.0.0"))
    second = SchemaContractVersionBuilder().build(manifest=_manifest(version="1.1.0", include_new_column=True))

    for store in (local, sqlite):
        store.append(first)
        store.append(second)

    assert local.history(contract_id="analytics.orders") == sqlite.history(contract_id="analytics.orders")
    assert local.latest(contract_id="analytics.orders") == sqlite.latest(contract_id="analytics.orders")


def test_classifier_assigns_minor_and_major_bumps() -> None:
    base = SchemaContractVersionBuilder().build(manifest=_manifest(version="1.0.0"))
    additive = SchemaContractVersionBuilder().build(manifest=_manifest(version="1.1.0", include_new_column=True))
    breaking = SchemaContractVersionBuilder().build(manifest=_manifest(version="2.0.0", amount_nullable=False))

    additive_plan = SchemaContractComparator().compare(base=base, head=additive, compatibility="backward")
    breaking_plan = SchemaContractComparator().compare(base=base, head=breaking, compatibility="backward")

    assert additive_plan["required_bump"] == "minor"
    assert additive_plan["status"] == "compatible"
    assert any(item["change_type"] == "column_added" for item in additive_plan["changes"])
    assert breaking_plan["required_bump"] == "major"
    assert breaking_plan["status"] == "breaking"
    assert "schema_contract.compatibility_breaking" in breaking_plan["risk_tags"]


def test_strict_semver_blocks_declared_version_lower_than_required_bump() -> None:
    base = SchemaContractVersionBuilder().build(manifest=_manifest(version="1.0.0"))
    head = SchemaContractVersionBuilder().build(manifest=_manifest(version="1.0.1", include_new_column=True))
    plan = SchemaCompatibilityClassifier().classify(
        base=base,
        head=head,
        changes=SchemaContractComparator().diff(base=base, head=head),
        compatibility="backward",
        semver_mode="strict",
    )

    assert plan["status"] == "blocked"
    assert plan["required_bump"] == "minor"
    assert "schema_contract.semver_bump_required:minor" in plan["blockers"]


def test_consumer_gate_blocks_version_constraint_and_removed_column() -> None:
    base = SchemaContractVersionBuilder().build(manifest=_manifest(version="1.4.0"))
    head = SchemaContractVersionBuilder().build(manifest=_manifest(version="2.0.0", drop_amount=True))
    plan = SchemaContractComparator().compare(base=base, head=head, compatibility="backward")

    decision = SchemaConsumerCompatibilityGate().evaluate(
        contract_version=head,
        compatibility_plan=plan,
        consumers=[
            {
                "id": "finance.daily_margin",
                "type": "dashboard",
                "owner": "finance-analytics",
                "version_constraint": ">=1.4,<2.0",
                "reads": {"columns": ["amount", "customer_id"]},
            }
        ],
        unknown_consumer="warn",
    )

    assert decision["status"] == "blocked"
    assert "schema_contract.consumer_version_incompatible:finance.daily_margin" in decision["blockers"]
    assert "schema_contract.consumer_column_removed:finance.daily_margin:amount" in decision["blockers"]


def test_bundle_policy_registry_and_migration_plan_integrate_contract_gate(tmp_path: Path) -> None:
    pack = _pack()
    gate = {
        "schema_version": "dpone.schema_contract_gate.v1",
        "status": "allowed",
        "contract_gate_id": "sha256:" + "1" * 64,
        "pack_id": pack.pack_id,
        "contract_id": "analytics.orders",
        "contract_version_id": "sha256:" + "2" * 64,
        "blockers": [],
        "warnings": [],
    }
    artifacts = (
        _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
        _artifact("contract_gate", "contract-gate.json", gate, required=False),
    )
    bundle = MigrationBundleBuilder().build(artifacts=artifacts, attest=True)
    decision = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=MigrationBundlePolicyOptions.resolve(
            profile="prod_strict",
            policy_payload={"required_artifacts": ["migration_pack", "contract_gate"], "fail_on_warnings": False},
        ),
        artifact_payloads={item.kind: item.payload for item in artifacts},
    )
    record = MigrationEvidenceRecorder().build_record(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        gate=None,
        trust_verification=None,
        diff=None,
        environment="prod",
        stage="contract_gate_passed",
        contract_gate=gate,
        actor="ci",
    )

    assert bundle["summary"]["contract_gate_id"] == gate["contract_gate_id"]
    assert decision["status"] == "allowed"
    assert record["status"] == "ready"
    assert "contract_gate" in {item["kind"] for item in record["artifact_refs"]}


def test_migration_plan_embeds_contract_compatibility_summary(tmp_path: Path) -> None:
    store = tmp_path / "contracts.json"
    base = SchemaContractVersionBuilder().build(manifest=_manifest(version="1.0.0", store_uri=str(store)))
    LocalJsonSchemaContractRegistryStore(store).append(base)
    manifest_path = _write_manifest(tmp_path, version="1.1.0", include_new_column=True, store_uri=str(store))

    payload = MigrationControlFacade().plan(manifest_path=str(manifest_path))

    summary = payload["contract_compatibility_summary"]
    assert summary["contract_id"] == "analytics.orders"
    assert summary["required_bump"] == "minor"
    assert summary["status"] == "compatible"
    assert payload["contract_version_id"].startswith("sha256:")


def _manifest(
    *,
    version: str,
    include_new_column: bool = False,
    amount_nullable: bool = True,
    drop_amount: bool = False,
    store_uri: str = ".dpone/schema-contracts/registry.json",
) -> dict[str, object]:
    columns: dict[str, object] = {
        "order_id": {"type": "integer", "nullable": False},
        "customer_id": {"type": "integer", "nullable": True},
    }
    if not drop_amount:
        columns["amount"] = {"type": "decimal", "precision": 18, "scale": 2, "nullable": amount_nullable}
    if include_new_column:
        columns["status"] = {"type": "string", "nullable": True}
    return {
        "source": {"options": {"columns": [{"name": name, "type": "string"} for name in columns]}},
        "sink": {
            "type": "clickhouse",
            "table": {"schema": "analytics", "name": "orders"},
            "options": {
                "schema_contract": {
                    "id": "analytics.orders",
                    "version": version,
                    "owner": "data-platform",
                    "enforcement": "strict",
                    "compatibility": "backward",
                    "registry": {
                        "enabled": True,
                        "mode": "gate",
                        "store_backend": "local_json",
                        "store_uri": store_uri,
                    },
                    "versioning": {"semver": "strict", "unknown_consumer": "warn"},
                    "columns": columns,
                },
                "schema_identity": {
                    "enabled": True,
                    "columns": {
                        "order_id": {"id": "orders.order_id"},
                        "customer_id": {"id": "orders.customer_id"},
                        "amount": {"id": "orders.amount"},
                    },
                },
                "physical_design": {"storage": {"clickhouse": {"order_by": ["order_id"]}}},
            },
        },
    }


def _write_manifest(tmp_path: Path, **kwargs: object) -> Path:
    import yaml

    path = tmp_path / "manifest.yaml"
    path.write_text(yaml.safe_dump(_manifest(**kwargs), sort_keys=False), encoding="utf-8")
    return path


def _pack() -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders"},
        actual={"sink_type": "clickhouse", "table": "analytics.orders"},
        changes=({"change_type": "column_added", "path": "status"},),
        strategy="online_safe",
    )


def _artifact(kind: str, path: str, payload: dict[str, object], *, required: bool) -> MigrationEvidenceArtifact:
    content = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return MigrationEvidenceArtifact.from_bytes(kind=kind, path=path, content=content, required=required)
