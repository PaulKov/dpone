from __future__ import annotations

import json
from pathlib import Path

import yaml

from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_impact import (
    MigrationPackChangeExtractor,
    SchemaImpactFacade,
    SchemaImpactGate,
    SchemaImpactOptions,
)


def test_migration_pack_extractor_marks_destructive_as_compatibility_breaking() -> None:
    pack = _pack(
        changes=(
            {
                "change_type": "drop_column",
                "path": "columns.amount",
                "risk": "destructive",
            },
        )
    )

    subjects = MigrationPackChangeExtractor().extract(pack)

    assert subjects[0].path == "columns.amount"
    assert subjects[0].severity == "critical"
    assert set(subjects[0].risk_tags) == {"compatibility_breaking", "data_destructive"}


def test_impact_plan_routes_column_drop_to_manual_consumer_owner(tmp_path: Path) -> None:
    pack = _pack(
        changes=(
            {
                "change_type": "drop_column",
                "path": "columns.amount",
                "risk": "destructive",
            },
        )
    )
    manifest = _write_manifest(tmp_path, mode="gate")

    payload = SchemaImpactFacade().plan(pack=pack, manifest_path=str(manifest))

    assert payload["schema_version"] == "dpone.schema_impact_plan.v1"
    assert payload["pack_id"] == pack.pack_id
    assert payload["impact_plan_id"].startswith("sha256:")
    assert payload["summary"]["max_severity"] == "critical"
    assert payload["summary"]["impacted_consumers"] == 1
    assert payload["required_approvals"] == ["compatibility_breaking", "data_destructive"]
    assert payload["impacted_consumers"][0]["id"] == "finance.daily_margin"
    assert payload["impacted_consumers"][0]["owner"] == "finance-analytics"


def test_impact_gate_blocks_required_risk_without_approval(tmp_path: Path) -> None:
    pack = _pack(
        changes=(
            {
                "change_type": "direct_rename",
                "path": "columns.client_id",
                "risk": "unsafe_direct_rename",
            },
        )
    )
    manifest = _write_manifest(tmp_path, mode="gate")
    plan = SchemaImpactFacade().plan(pack=pack, manifest_path=str(manifest))

    result = SchemaImpactGate().evaluate(pack=pack, impact_plan=plan, approval=None)

    assert result["status"] == "blocked"
    assert "schema_impact.approval_required:direct_rename" in result["blockers"]


def test_impact_gate_accepts_pack_bound_approval(tmp_path: Path) -> None:
    pack = _pack(
        changes=(
            {
                "change_type": "shadow_cutover",
                "path": "target_table",
                "risk": "shadow_cutover",
            },
        )
    )
    manifest = _write_manifest(tmp_path, mode="gate")
    plan = SchemaImpactFacade().plan(pack=pack, manifest_path=str(manifest))
    approval = {
        "pack_id": pack.pack_id,
        "impact_plan_id": plan["impact_plan_id"],
        "approved_by": "finance-data-owner",
        "approved_risks": ["compatibility_breaking", "shadow_cutover"],
    }

    result = SchemaImpactGate().evaluate(pack=pack, impact_plan=plan, approval=approval)

    assert result["status"] == "allowed"
    assert result["blockers"] == []


def test_impact_gate_observe_mode_never_blocks(tmp_path: Path) -> None:
    pack = _pack(
        changes=(
            {
                "change_type": "drop_column",
                "path": "columns.amount",
                "risk": "destructive",
            },
        )
    )
    manifest = _write_manifest(tmp_path, mode="observe")
    plan = SchemaImpactFacade().plan(pack=pack, manifest_path=str(manifest))

    result = SchemaImpactGate().evaluate(pack=pack, impact_plan=plan, approval=None)

    assert result["status"] == "observed"
    assert result["blockers"] == []
    assert result["required_approvals"] == ["compatibility_breaking", "data_destructive"]


def test_unknown_dependency_block_policy_promotes_missing_source_to_blocker(tmp_path: Path) -> None:
    pack = _pack(changes=({"change_type": "add_column", "path": "columns.new_col", "risk": "additive"},))
    manifest = _write_manifest(
        tmp_path,
        mode="gate",
        sources={"dbt_manifest": "missing/manifest.json"},
        unknown_dependency="block",
    )

    payload = SchemaImpactFacade().plan(pack=pack, manifest_path=str(manifest))

    assert payload["status"] == "blocked"
    assert any(item.startswith("schema_impact.dbt_manifest.missing") for item in payload["blockers"])


def test_manifest_dependency_provider_adds_route_ownership_node(tmp_path: Path) -> None:
    pack = _pack(changes=({"change_type": "add_column", "path": "columns.new_col", "risk": "additive"},))
    manifest = _write_manifest(tmp_path, mode="gate", sources={"manifests": True})

    payload = SchemaImpactFacade().plan(pack=pack, manifest_path=str(manifest))

    consumer_ids = {item["id"] for item in payload["impacted_consumers"]}
    assert f"manifest:{manifest}" in consumer_ids


def test_dbt_manifest_and_openlineage_sources_build_dependency_graph(tmp_path: Path) -> None:
    pack = _pack(
        changes=(
            {
                "change_type": "alter_column_type",
                "path": "columns.amount",
                "risk": "type_narrowing",
            },
        )
    )
    dbt_manifest = tmp_path / "manifest.json"
    dbt_manifest.write_text(
        json.dumps(
            {
                "nodes": {
                    "model.finance.orders_mart": {
                        "unique_id": "model.finance.orders_mart",
                        "resource_type": "model",
                        "name": "orders_mart",
                        "database": "clickhouse",
                        "schema": "analytics",
                        "alias": "orders_mart",
                        "depends_on": {"nodes": ["source.analytics.orders"]},
                    }
                },
                "sources": {
                    "source.analytics.orders": {
                        "unique_id": "source.analytics.orders",
                        "database": "clickhouse",
                        "schema": "analytics",
                        "name": "orders",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    openlineage = tmp_path / "openlineage.json"
    openlineage.write_text(
        json.dumps(
            {
                "events": [
                    {
                        "job": {"namespace": "airflow", "name": "quality.orders_check"},
                        "inputs": [{"namespace": "clickhouse", "name": "analytics.orders"}],
                        "outputs": [],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    manifest = _write_manifest(
        tmp_path,
        mode="gate",
        sources={"dbt_manifest": str(dbt_manifest), "openlineage": str(openlineage)},
    )

    payload = SchemaImpactFacade().plan(pack=pack, manifest_path=str(manifest))

    consumer_ids = {item["id"] for item in payload["impacted_consumers"]}
    assert "model.finance.orders_mart" in consumer_ids
    assert "airflow.quality.orders_check" in consumer_ids


def test_impact_options_reject_invalid_policy() -> None:
    try:
        SchemaImpactOptions.from_config({"enabled": True, "mode": "panic"})
    except ValueError as exc:
        assert "schema_impact.mode" in str(exc)
    else:
        raise AssertionError("invalid impact mode should fail")


def _pack(*, changes: tuple[dict[str, object], ...]) -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders"},
        actual={"sink_type": "clickhouse", "table": "analytics.orders"},
        changes=changes,
        ddl=(),
    )


def _write_manifest(
    tmp_path: Path,
    *,
    mode: str,
    sources: dict[str, object] | None = None,
    unknown_dependency: str = "warn",
) -> Path:
    manifest = tmp_path / "manifest.yaml"
    payload = {
        "sink": {
            "type": "clickhouse",
            "table": {"schema": "analytics", "name": "orders"},
            "options": {
                "schema_impact": {
                    "enabled": True,
                    "mode": mode,
                    "unknown_dependency": unknown_dependency,
                    "approval": {
                        "required_for": [
                            "compatibility_breaking",
                            "data_destructive",
                            "direct_rename",
                            "shadow_cutover",
                        ]
                    },
                    "sources": {
                        "manual": {
                            "consumers": [
                                {
                                    "id": "finance.daily_margin",
                                    "type": "dashboard",
                                    "owner": "finance-analytics",
                                    "reads": [
                                        {
                                            "dataset": "clickhouse.analytics.orders",
                                            "columns": ["amount", "customer_id"],
                                        }
                                    ],
                                }
                            ]
                        },
                        **(sources or {}),
                    },
                }
            },
        }
    }
    manifest.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return manifest
