from __future__ import annotations

import json
from pathlib import Path

import yaml

from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_contract_consumers import (
    DbtManifestConsumerProvider,
    ManualConsumerProvider,
    OpenLineageConsumerProvider,
    SchemaConsumerDiscoveryOptions,
    SchemaConsumerInventoryBuilder,
    SchemaConsumerMatrixBuilder,
    SchemaConsumerMatrixGate,
)
from dpone.readiness.schema_contract_registry import SchemaContractVersionBuilder
from dpone.readiness.schema_contract_registry_store import LocalJsonSchemaContractRegistryStore
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
)
from dpone.readiness.schema_migration_evidence_registry import MigrationEvidenceRecorder


def test_manual_dbt_and_openlineage_providers_emit_stable_deduplicated_consumers(tmp_path: Path) -> None:
    manifest = _manifest(version="1.1.0", dbt_path=tmp_path / "dbt.json", openlineage_path=tmp_path / "ol.json")
    (tmp_path / "dbt.json").write_text(json.dumps(_dbt_manifest()), encoding="utf-8")
    (tmp_path / "ol.json").write_text(json.dumps(_openlineage_artifact()), encoding="utf-8")

    inventory = SchemaConsumerInventoryBuilder().build(
        contract_version=SchemaContractVersionBuilder().build(manifest=manifest),
        options=SchemaConsumerDiscoveryOptions.from_manifest(manifest, base_path=tmp_path),
        providers=(
            ManualConsumerProvider(manifest),
            DbtManifestConsumerProvider(tmp_path / "dbt.json"),
            OpenLineageConsumerProvider(tmp_path / "ol.json"),
        ),
    )

    assert inventory["schema_version"] == "dpone.schema_contract_consumer_inventory.v1"
    assert inventory["status"] == "discovered"
    assert inventory["consumer_inventory_id"].startswith("sha256:")
    assert {item["id"] for item in inventory["consumers"]} == {
        "finance.daily_margin",
        "model.project.daily_margin",
        "openlineage.finance_margin_job",
    }
    assert inventory["summary"]["consumers_count"] == 3


def test_required_source_missing_blocks_in_gate_and_warns_in_observe(tmp_path: Path) -> None:
    manifest = _manifest(version="1.1.0", dbt_path=tmp_path / "missing.json", required_sources=["dbt_manifest"])
    version = SchemaContractVersionBuilder().build(manifest=manifest)

    gate_inventory = SchemaConsumerInventoryBuilder().build(
        contract_version=version,
        options=SchemaConsumerDiscoveryOptions.from_manifest(manifest, base_path=tmp_path),
        providers=(DbtManifestConsumerProvider(tmp_path / "missing.json"),),
    )
    observe_manifest = _manifest(
        version="1.1.0",
        mode="observe",
        dbt_path=tmp_path / "missing.json",
        required_sources=["dbt_manifest"],
    )
    observe_inventory = SchemaConsumerInventoryBuilder().build(
        contract_version=version,
        options=SchemaConsumerDiscoveryOptions.from_manifest(observe_manifest, base_path=tmp_path),
        providers=(DbtManifestConsumerProvider(tmp_path / "missing.json"),),
    )

    assert gate_inventory["status"] == "blocked"
    assert "schema_contract_consumers.source_missing:dbt_manifest" in gate_inventory["blockers"]
    assert observe_inventory["status"] == "discovered"
    assert "schema_contract_consumers.source_missing:dbt_manifest" in observe_inventory["warnings"]


def test_consumer_matrix_blocks_removed_column_and_incompatible_version(tmp_path: Path) -> None:
    base = SchemaContractVersionBuilder().build(manifest=_manifest(version="1.5.0"))
    head = SchemaContractVersionBuilder().build(manifest=_manifest(version="2.0.0", drop_amount=True))
    inventory = {
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
    }

    matrix = SchemaConsumerMatrixBuilder().build(base=base, head=head, inventory=inventory, unknown_consumer="warn")

    assert matrix["schema_version"] == "dpone.schema_contract_consumer_matrix.v1"
    assert matrix["status"] == "blocked"
    assert matrix["summary"]["blocked_consumers"] == 1
    assert "schema_contract.consumer_version_incompatible:finance.daily_margin" in matrix["blockers"]
    assert "schema_contract.consumer_column_removed:finance.daily_margin:amount" in matrix["blockers"]


def test_consumer_gate_and_bundle_registry_integration_accept_compatible_matrix() -> None:
    pack = _pack()
    matrix = {
        "schema_version": "dpone.schema_contract_consumer_matrix.v1",
        "status": "compatible",
        "contract_id": "analytics.orders",
        "head_version": "1.6.0",
        "head_contract_version_id": "sha256:" + "2" * 64,
        "consumer_matrix_id": "sha256:" + "3" * 64,
        "blockers": [],
        "warnings": [],
        "summary": {"blocked_consumers": 0},
    }
    gate = SchemaConsumerMatrixGate().evaluate(pack=pack.to_dict(command="plan"), matrix=matrix, mode="gate")
    bundle = MigrationBundleBuilder().build(
        artifacts=(
            _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
            _artifact("consumer_gate", "consumer-gate.json", gate, required=False),
        ),
        attest=True,
    )
    decision = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=MigrationBundlePolicyOptions.resolve(
            profile="prod_strict",
            policy_payload={"required_artifacts": ["migration_pack", "consumer_gate"], "fail_on_warnings": False},
        ),
        artifact_payloads={"consumer_gate": gate},
    )
    record = MigrationEvidenceRecorder().build_record(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        gate=None,
        trust_verification=None,
        diff=None,
        consumer_gate=gate,
        environment="prod",
        stage="consumer_gate_passed",
    )

    assert gate["schema_version"] == "dpone.schema_contract_consumer_gate.v1"
    assert gate["status"] == "allowed"
    assert bundle["summary"]["consumer_gate_id"] == gate["consumer_gate_id"]
    assert decision["status"] == "allowed"
    assert "consumer_gate" in {item["kind"] for item in record["artifact_refs"]}


def test_migration_plan_embeds_consumer_matrix_summary_when_discovery_enabled(tmp_path: Path) -> None:
    store = tmp_path / "contracts.json"
    base = SchemaContractVersionBuilder().build(manifest=_manifest(version="1.5.0", store_uri=str(store)))
    LocalJsonSchemaContractRegistryStore(store).append(base)
    manifest_path = tmp_path / "manifest.yaml"
    manifest_path.write_text(
        yaml.safe_dump(_manifest(version="1.6.0", include_status=True, store_uri=str(store)), sort_keys=False),
        encoding="utf-8",
    )

    from dpone.services.schema_migration import MigrationControlFacade

    payload = MigrationControlFacade().plan(manifest_path=str(manifest_path))

    assert payload["consumer_matrix_summary"]["enabled"] is True
    assert payload["consumer_matrix_summary"]["status"] in {"compatible", "warning"}
    assert payload["consumer_matrix_summary"]["consumers_count"] == 1


def _manifest(
    *,
    version: str,
    include_status: bool = False,
    drop_amount: bool = False,
    mode: str = "gate",
    dbt_path: Path | None = None,
    openlineage_path: Path | None = None,
    required_sources: list[str] | None = None,
    store_uri: str = ".dpone/schema-contracts/registry.json",
) -> dict[str, object]:
    columns: dict[str, object] = {
        "order_id": {"type": "integer", "nullable": False},
        "customer_id": {"type": "integer", "nullable": True},
    }
    if not drop_amount:
        columns["amount"] = {"type": "decimal", "precision": 18, "scale": 2, "nullable": True}
    if include_status:
        columns["status"] = {"type": "string", "nullable": True}
    sources: dict[str, object] = {"manual": True}
    if dbt_path is not None:
        sources["dbt_manifest"] = str(dbt_path)
    if openlineage_path is not None:
        sources["openlineage"] = str(openlineage_path)
    return {
        "source": {"options": {"columns": [{"name": key, "type": "string"} for key in columns]}},
        "sink": {
            "type": "clickhouse",
            "table": {"schema": "analytics", "name": "orders"},
            "options": {
                "schema_contract": {
                    "id": "analytics.orders",
                    "version": version,
                    "owner": "data-platform",
                    "compatibility": "backward",
                    "registry": {
                        "enabled": True,
                        "mode": "gate",
                        "store_backend": "local_json",
                        "store_uri": store_uri,
                    },
                    "versioning": {"semver": "strict", "unknown_consumer": "warn"},
                    "consumers": {
                        "discovery": {
                            "enabled": True,
                            "mode": mode,
                            "unknown_consumer": "warn",
                            "required_sources": required_sources or ["manual"],
                            "sources": sources,
                        },
                        "manual": [
                            {
                                "id": "finance.daily_margin",
                                "type": "dashboard",
                                "owner": "finance-analytics",
                                "version_constraint": ">=1.4,<2.0",
                                "reads": {"columns": ["amount", "customer_id"]},
                            }
                        ],
                    },
                    "columns": columns,
                },
                "physical_design": {"storage": {"clickhouse": {"order_by": ["order_id"]}}},
            },
        },
    }


def _dbt_manifest() -> dict[str, object]:
    return {
        "sources": {
            "source.project.orders": {
                "unique_id": "source.project.orders",
                "database": "clickhouse",
                "schema": "analytics",
                "name": "orders",
            }
        },
        "nodes": {
            "model.project.daily_margin": {
                "unique_id": "model.project.daily_margin",
                "resource_type": "model",
                "name": "daily_margin",
                "depends_on": {"nodes": ["source.project.orders"]},
                "meta": {
                    "owner": "finance-analytics",
                    "dpone": {"reads": {"analytics.orders": ["amount", "customer_id"]}},
                },
            }
        },
    }


def _openlineage_artifact() -> dict[str, object]:
    return {
        "events": [
            {
                "job": {"namespace": "finance", "name": "finance_margin_job"},
                "inputs": [
                    {
                        "namespace": "clickhouse",
                        "name": "analytics.orders",
                        "facets": {"schema": {"fields": [{"name": "amount"}, {"name": "customer_id"}]}},
                    }
                ],
            }
        ]
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
