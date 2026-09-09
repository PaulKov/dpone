from __future__ import annotations

from dpone.services.dbt_ci_report import render_dbt_ci_report


def test_dbt_mr_report_uses_frozen_compile_decisions_and_dag_link() -> None:
    report = {
        "passed": True,
        "models": [
            {
                "model": "model.analytics.orders",
                "source_relation": {
                    "database": "dwh",
                    "schema": "mart",
                    "name": "orders",
                },
                "intent": {
                    "target": {
                        "schema": "analytics",
                        "table": "orders",
                    }
                },
                "resolved_strategy": {"mode": "incremental_merge"},
                "route_capability": {
                    "support": "supported",
                    "evidence_status": "PASS",
                },
            }
        ],
        "workflows": [
            {
                "workflow": "daily_marts",
                "dag_id": "DAG__sales__daily_marts__refresh",
            }
        ],
        "warnings": [],
        "blockers": [],
    }

    rendered = render_dbt_ci_report(
        report,
        airflow_base_url="https://airflow.dev.example",
    )

    assert "model.analytics.orders" in rendered
    assert "dwh.mart.orders" in rendered
    assert "analytics.orders" in rendered
    assert "incremental_merge" in rendered
    assert "PASS" in rendered
    assert ("https://airflow.dev.example/dags/DAG__sales__daily_marts__refresh") in rendered


def test_dbt_mr_report_renders_blocker_and_next_action() -> None:
    rendered = render_dbt_ci_report(
        {
            "passed": False,
            "models": [],
            "workflows": [],
            "blockers": [
                {
                    "code": "DPONE_DBT_STRATEGY_UNRESOLVED",
                    "message": "No safe strategy.",
                    "remediation": "Choose an allowlisted strategy.",
                }
            ],
        }
    )

    assert "Status: **BLOCKED**" in rendered
    assert "`DPONE_DBT_STRATEGY_UNRESOLVED`" in rendered
    assert "Next: Choose an allowlisted strategy." in rendered
