from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

from jsonschema import Draft202012Validator

from dpone.readiness.data_product_cost import (
    CostBudgetEvaluator,
    CostGovernanceGate,
    CostGovernancePlanner,
)
from dpone.readiness.data_product_cost_rendering import CostForecastEvaluator, CostGovernanceRenderer
from dpone.services.data_product_cost_clickhouse import ClickHouseCostProbe


def test_disabled_cost_governance_emits_noop_artifacts() -> None:
    plan = CostGovernancePlanner().plan(manifest=_manifest(enabled=False), bundle={})
    evaluation = CostBudgetEvaluator().evaluate(plan=plan, runtime_artifact={}, registry_records=())
    gate = CostGovernanceGate().evaluate(evaluation=evaluation, profile="prod_strict")
    forecast = CostForecastEvaluator().forecast(evaluation=evaluation, history={})
    report = CostGovernanceRenderer().report(gate=gate)

    assert plan["schema_version"] == "dpone.data_product_cost_plan.v1"
    assert plan["status"] == "disabled"
    assert evaluation["status"] == "disabled"
    assert gate["status"] == "allowed"
    assert forecast["status"] == "disabled"
    assert report["status"] == "allowed"


def test_cost_evaluation_blocks_budget_capacity_and_unbounded_full_refresh() -> None:
    plan = CostGovernancePlanner().plan(manifest=_manifest(), bundle=_bundle(full_refresh=True))
    evaluation = CostBudgetEvaluator().evaluate(
        plan=plan,
        runtime_artifact=_runtime_artifact(),
        registry_records=(_previous_cost_record(monthly_cost=90.0),),
    )
    gate = CostGovernanceGate().evaluate(evaluation=evaluation, profile="prod_strict")

    assert plan["status"] == "blocked"
    assert "data_product_cost.unbounded_full_refresh" in plan["blockers"]
    assert evaluation["schema_version"] == "dpone.data_product_cost_evaluation.v1"
    assert evaluation["status"] == "blocked"
    assert evaluation["estimated_costs"]["per_run_cost"] > 10
    assert "data_product_cost.budget_per_run_exceeded" in evaluation["blockers"]
    assert "data_product_cost.budget_monthly_exceeded" in evaluation["blockers"]
    assert "data_product_cost.capacity_table_growth_exceeded" in evaluation["blockers"]
    assert "data_product_cost.capacity_staging_gb_exceeded" in evaluation["blockers"]
    assert gate["status"] == "blocked"


def test_missing_cost_center_blocks_and_advisory_converts_to_warnings() -> None:
    manifest = _manifest()
    del manifest["sink"]["options"]["data_product"]["cost_governance"]["ownership"]["cost_center"]
    plan = CostGovernancePlanner().plan(manifest=manifest, bundle={})
    evaluation = CostBudgetEvaluator().evaluate(plan=plan, runtime_artifact={}, registry_records=())
    gate = CostGovernanceGate().evaluate(evaluation=evaluation, profile="advisory")

    assert "data_product_cost.cost_center_missing" in plan["blockers"]
    assert gate["status"] == "warning"
    assert "data_product_cost.cost_center_missing" in gate["warnings"]
    assert gate["blockers"] == []


def test_forecast_report_and_json_schemas_validate() -> None:
    plan = CostGovernancePlanner().plan(manifest=_manifest(), bundle={})
    evaluation = CostBudgetEvaluator().evaluate(
        plan=plan, runtime_artifact=_safe_runtime_artifact(), registry_records=()
    )
    gate = CostGovernanceGate().evaluate(evaluation=evaluation, profile="prod_strict")
    forecast = CostForecastEvaluator().forecast(
        evaluation=evaluation,
        history={"evaluations": [_safe_runtime_history(per_run_cost=6.0), _safe_runtime_history(per_run_cost=8.0)]},
    )
    report = CostGovernanceRenderer().report(gate=gate, forecast=forecast)

    assert evaluation["status"] == "allowed"
    assert gate["status"] == "allowed"
    assert forecast["schema_version"] == "dpone.data_product_cost_forecast.v1"
    assert forecast["summary"]["history_points"] == 2
    assert report["schema_version"] == "dpone.data_product_cost_report.v1"
    assert "# Data Product Cost Governance Report" in report["markdown"]

    for name, payload in (
        ("data-product-cost-plan.schema.json", plan),
        ("data-product-cost-evaluation.schema.json", evaluation),
        ("data-product-cost-gate.schema.json", gate),
        ("data-product-cost-forecast.schema.json", forecast),
        ("data-product-cost-report.schema.json", report),
    ):
        schema = json.loads((Path("docs/schemas/data-product") / name).read_text(encoding="utf-8"))
        Draft202012Validator(schema).validate(payload)


def test_clickhouse_cost_probe_reads_metadata_without_query_text_or_secrets(monkeypatch) -> None:
    fake_client = _FakeClickHouseClient()
    monkeypatch.setitem(
        sys.modules,
        "clickhouse_connect",
        SimpleNamespace(get_client=lambda **kwargs: fake_client),
    )

    result = ClickHouseCostProbe().collect(
        {"type": "clickhouse", "database": "analytics", "table": "orders", "password": "must-not-leak"}
    )

    assert result["blockers"] == []
    assert result["metrics"]["table_bytes"] == 1073741824
    assert result["metrics"]["query_bytes_read"] == 1099511627776
    assert "must-not-leak" not in json.dumps(result, sort_keys=True)
    assert "SELECT" not in json.dumps(result, sort_keys=True)
    assert "system.parts" in fake_client.sql
    assert "system.query_log" in fake_client.sql


class _FakeClickHouseClient:
    def __init__(self) -> None:
        self.sql = ""

    def query(self, sql: str):
        self.sql += sql
        if "system.parts" in sql:
            return SimpleNamespace(result_rows=[(1000, 1073741824)])
        if "system.query_log" in sql:
            return SimpleNamespace(result_rows=[(300000, 1099511627776)])
        return SimpleNamespace(result_rows=[])


def _manifest(*, enabled: bool = True) -> dict:
    return {
        "sink": {
            "options": {
                "data_product": {
                    "id": "analytics.orders",
                    "owner": "data-platform",
                    "tier": "gold",
                    "criticality": "high",
                    "cost_governance": {
                        "enabled": enabled,
                        "mode": "gate",
                        "profile": "prod_strict",
                        "currency": "USD",
                        "stale_evidence_policy": "block",
                        "ownership": {
                            "cost_center": "data-platform-prod",
                            "require_owner_cost_center": True,
                        },
                        "budgets": {
                            "monthly_max_cost": 100,
                            "per_run_max_cost": 10,
                            "per_release_max_cost_delta": 0.15,
                        },
                        "capacity": {
                            "max_table_growth_ratio": 0.25,
                            "max_staging_gb": 100,
                            "max_query_duration_ms": 300000,
                            "max_concurrent_runs": 4,
                        },
                        "rates": {
                            "storage_gb_month": 0.023,
                            "staging_gb_day": 0.01,
                            "query_tb_scanned": 20.0,
                            "run_minute": 1.0,
                        },
                        "policies": {
                            "block_unbounded_full_refresh": True,
                            "block_missing_owner_cost_center": True,
                            "require_waiver_for_budget_excess": True,
                        },
                    },
                }
            }
        }
    }


def _bundle(*, full_refresh: bool) -> dict:
    return {
        "bundle_id": "sha256:bundle",
        "pack_id": "sha256:pack",
        "summary": {"full_refresh": full_refresh},
        "artifacts": [{"kind": "migration_pack", "path": "pack.json"}],
    }


def _runtime_artifact() -> dict:
    return {
        "run_id": "run-1",
        "duration_ms": 1_200_000,
        "rows_written": 1000,
        "bytes_written": 1_073_741_824,
        "staging_bytes": 214_748_364_800,
        "query_bytes_read": 1_099_511_627_776,
        "table_bytes_before": 1_073_741_824,
        "table_bytes_after": 2_147_483_648,
        "max_concurrent_runs": 5,
        "full_refresh": True,
    }


def _safe_runtime_artifact() -> dict:
    return {
        "run_id": "run-safe",
        "duration_ms": 120_000,
        "bytes_written": 107_374_182,
        "staging_bytes": 107_374_182,
        "query_bytes_read": 10_737_418,
        "table_bytes_before": 1_073_741_824,
        "table_bytes_after": 1_181_116_006,
        "max_concurrent_runs": 1,
    }


def _previous_cost_record(*, monthly_cost: float) -> dict:
    return {
        "artifact_refs": [{"kind": "data_product_cost_evaluation", "evidence_id": "sha256:old"}],
        "monthly_cost": monthly_cost,
    }


def _safe_runtime_history(*, per_run_cost: float) -> dict:
    return {"estimated_costs": {"per_run_cost": per_run_cost}, "capacity": {"table_growth_ratio": 0.01}}
