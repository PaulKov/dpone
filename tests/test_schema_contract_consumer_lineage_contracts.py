from __future__ import annotations

import json
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator

from dpone.readiness.schema_contract_catalog_providers import (
    DataHubConsumerProvider,
    DbtCompiledSqlConsumerProvider,
    GenericCatalogConsumerProvider,
)
from dpone.readiness.schema_contract_consumer_lineage import (
    CONSUMER_LINEAGE_SCHEMA,
    SchemaConsumerLineageInventoryBuilder,
)
from dpone.readiness.schema_contract_consumer_matrix import SchemaConsumerMatrixBuilder
from dpone.readiness.schema_contract_consumers import SchemaConsumerDiscoveryOptions
from dpone.readiness.schema_contract_registry import SchemaContractVersionBuilder
from dpone.readiness.schema_contract_registry_store import LocalJsonSchemaContractRegistryStore
from dpone.readiness.schema_contract_sql_lineage import SqlColumnLineageExtractor


def test_sqlglot_extractor_handles_select_alias_join_and_cte() -> None:
    sql = """
    with recent_orders as (
      select order_id, customer_id, amount from analytics.orders
    )
    select r.customer_id as client_id, sum(r.amount) as total_amount
    from recent_orders r
    join analytics.customers c on c.customer_id = r.customer_id
    group by r.customer_id
    """

    evidence = SqlColumnLineageExtractor().extract(
        sql=sql,
        consumer_id="model.project.daily_margin",
        target_table="analytics.orders",
        target_columns=("amount", "customer_id", "order_id"),
    )

    assert {item["column"] for item in evidence} == {"amount", "customer_id", "order_id"}
    assert {item["confidence"] for item in evidence} == {"parsed"}
    assert all(item["consumer_id"] == "model.project.daily_margin" for item in evidence)


def test_lineage_providers_emit_stable_confidence_evidence(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.yaml"
    manifest = _manifest(
        version="1.6.0",
        dbt_manifest=tmp_path / "target" / "manifest.json",
        dbt_compiled=tmp_path / "target" / "compiled",
        datahub=tmp_path / "catalog" / "datahub.json",
        generic_catalog=tmp_path / "catalog" / "consumers.json",
    )
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")
    _write_dbt_artifacts(tmp_path)
    _write_catalog_artifacts(tmp_path)
    contract = SchemaContractVersionBuilder().build(manifest=manifest)
    options = SchemaConsumerDiscoveryOptions.from_manifest(manifest, base_path=tmp_path)

    lineage = SchemaConsumerLineageInventoryBuilder().build(
        contract_version=contract,
        options=options,
        providers=(
            DbtCompiledSqlConsumerProvider(tmp_path / "target" / "manifest.json", tmp_path / "target" / "compiled"),
            DataHubConsumerProvider(tmp_path / "catalog" / "datahub.json"),
            GenericCatalogConsumerProvider(tmp_path / "catalog" / "consumers.json"),
        ),
    )

    assert lineage["schema_version"] == CONSUMER_LINEAGE_SCHEMA
    assert lineage["status"] == "discovered"
    assert lineage["consumer_lineage_id"].startswith("sha256:")
    assert lineage["summary"]["evidence_count"] == 5
    assert lineage["summary"]["by_confidence"] == {"explicit": 1, "parsed": 3, "table_only": 1}
    assert {(item["consumer_id"], item["column"], item["confidence"]) for item in lineage["evidence"]} >= {
        ("model.project.daily_margin", "amount", "parsed"),
        ("model.project.daily_margin", "customer_id", "parsed"),
        ("looker.finance_margin", "amount", "explicit"),
        ("catalog.table_only_margin", None, "table_only"),
    }


def test_generic_catalog_provider_reads_csv_exports(tmp_path: Path) -> None:
    catalog = tmp_path / "consumers.csv"
    catalog.write_text(
        "id,type,owner,dataset,columns,confidence\n"
        'bi.margin,dashboard,finance,analytics.orders,"amount,customer_id",explicit\n',
        encoding="utf-8",
    )

    evidence = GenericCatalogConsumerProvider(catalog).lineage(
        contract_id="analytics.orders",
        target_table="analytics.orders",
        target_columns=("amount", "customer_id"),
    )

    assert {(item["consumer_id"], item["column"]) for item in evidence} == {
        ("bi.margin", "amount"),
        ("bi.margin", "customer_id"),
    }


def test_matrix_blocks_low_confidence_major_change_when_policy_blocks() -> None:
    base = SchemaContractVersionBuilder().build(manifest=_manifest(version="1.5.0"))
    head = SchemaContractVersionBuilder().build(manifest=_manifest(version="2.0.0", drop_amount=True))
    lineage = {
        "schema_version": CONSUMER_LINEAGE_SCHEMA,
        "status": "discovered",
        "contract_id": "analytics.orders",
        "evidence": [
            {
                "consumer_id": "catalog.table_only_margin",
                "consumer_type": "dashboard",
                "owner": "finance-analytics",
                "dataset": "analytics.orders",
                "column": None,
                "source": "generic_catalog",
                "confidence": "table_only",
            }
        ],
        "blockers": [],
        "warnings": [],
    }

    matrix = SchemaConsumerMatrixBuilder().build(
        base=base,
        head=head,
        inventory={"consumers": [], "blockers": [], "warnings": []},
        lineage=lineage,
        unknown_consumer="warn",
        low_confidence_major_change="block",
    )
    warn_matrix = SchemaConsumerMatrixBuilder().build(
        base=base,
        head=head,
        inventory={"consumers": [], "blockers": [], "warnings": []},
        lineage=lineage,
        unknown_consumer="warn",
        low_confidence_major_change="warn",
    )

    assert matrix["status"] == "blocked"
    assert "schema_contract.consumer_low_confidence_major:catalog.table_only_margin" in matrix["blockers"]
    assert matrix["summary"]["lineage_confidence"] == {"table_only": 1}
    assert warn_matrix["status"] == "warning"
    assert "schema_contract.consumer_low_confidence_major:catalog.table_only_margin" in warn_matrix["warnings"]


def test_consumer_lineage_public_schema_validates_artifact(tmp_path: Path) -> None:
    manifest = _manifest(version="1.6.0", generic_catalog=tmp_path / "catalog" / "consumers.json")
    _write_catalog_artifacts(tmp_path)
    lineage = SchemaConsumerLineageInventoryBuilder().build(
        contract_version=SchemaContractVersionBuilder().build(manifest=manifest),
        options=SchemaConsumerDiscoveryOptions.from_manifest(manifest, base_path=tmp_path),
        providers=(GenericCatalogConsumerProvider(tmp_path / "catalog" / "consumers.json"),),
    )
    schema = json.loads(
        Path("docs/schemas/schema-migration/schema-contract-consumer-lineage.schema.json").read_text(encoding="utf-8")
    )

    Draft202012Validator(schema).validate(lineage)


def test_migration_plan_embeds_consumer_lineage_summary(tmp_path: Path) -> None:
    store = tmp_path / "contracts.json"
    base = SchemaContractVersionBuilder().build(manifest=_manifest(version="1.5.0", store_uri=store))
    LocalJsonSchemaContractRegistryStore(store).append(base)
    _write_dbt_artifacts(tmp_path)
    manifest = _manifest(
        version="1.6.0",
        store_uri=store,
        dbt_manifest=tmp_path / "target" / "manifest.json",
        dbt_compiled=tmp_path / "target" / "compiled",
    )
    manifest_path = tmp_path / "manifest.yaml"
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")

    from dpone.services.schema_migration import MigrationControlFacade

    payload = MigrationControlFacade().plan(manifest_path=str(manifest_path))

    summary = payload["consumer_matrix_summary"]
    assert summary["consumer_lineage_summary"]["summary"]["by_confidence"]["parsed"] == 3


def _manifest(
    *,
    version: str,
    drop_amount: bool = False,
    store_uri: Path | None = None,
    dbt_manifest: Path | None = None,
    dbt_compiled: Path | None = None,
    datahub: Path | None = None,
    generic_catalog: Path | None = None,
) -> dict[str, object]:
    columns: dict[str, object] = {
        "order_id": {"type": "integer", "nullable": False},
        "customer_id": {"type": "integer", "nullable": True},
    }
    if not drop_amount:
        columns["amount"] = {"type": "decimal", "precision": 18, "scale": 2, "nullable": True}
    sources: dict[str, object] = {"manual": True}
    if dbt_manifest is not None:
        sources["dbt_manifest"] = str(dbt_manifest)
    if dbt_compiled is not None:
        sources["dbt_compiled_sql"] = str(dbt_compiled)
    if datahub is not None:
        sources["datahub"] = str(datahub)
    if generic_catalog is not None:
        sources["generic_catalog"] = str(generic_catalog)
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
                        "store_uri": str(store_uri or ".dpone/schema-contracts/registry.json"),
                    },
                    "consumers": {
                        "discovery": {
                            "enabled": True,
                            "mode": "gate",
                            "unknown_consumer": "warn",
                            "low_confidence_major_change": "block",
                            "required_sources": ["manual"],
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
                }
            },
        },
    }


def _write_dbt_artifacts(tmp_path: Path) -> None:
    compiled = tmp_path / "target" / "compiled" / "models"
    compiled.mkdir(parents=True)
    (compiled / "daily_margin.sql").write_text(
        "select customer_id, amount, order_id from analytics.orders",
        encoding="utf-8",
    )
    manifest = {
        "sources": {
            "source.project.orders": {
                "unique_id": "source.project.orders",
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
                "compiled_path": "models/daily_margin.sql",
                "meta": {"owner": "finance-analytics"},
            }
        },
    }
    (tmp_path / "target").mkdir(exist_ok=True)
    (tmp_path / "target" / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


def _write_catalog_artifacts(tmp_path: Path) -> None:
    catalog = tmp_path / "catalog"
    catalog.mkdir()
    (catalog / "datahub.json").write_text(
        json.dumps(
            {
                "lineage": [
                    {
                        "consumer": {"id": "looker.finance_margin", "type": "dashboard", "owner": "finance"},
                        "dataset": "analytics.orders",
                        "columns": ["amount"],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (catalog / "consumers.json").write_text(
        json.dumps(
            {
                "consumers": [
                    {
                        "id": "catalog.table_only_margin",
                        "type": "dashboard",
                        "owner": "finance",
                        "dataset": "analytics.orders",
                        "confidence": "table_only",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
