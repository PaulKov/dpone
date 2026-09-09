from __future__ import annotations

from dpone.contracts.dbt_workflow_graph_policy import (
    DBT_CROSS_WORKFLOW_DEPENDENCY_UNSUPPORTED,
    DBT_WORKFLOW_GRAPH_OVERLAP,
    evaluate_dbt_workflow_graph_ownership,
)


def test_one_workflow_may_select_its_own_publish_models_and_upstream() -> None:
    report = evaluate_dbt_workflow_graph_ownership(
        publish_model_ids_by_workflow={
            "daily": ("model.analytics.dim_customer",),
        },
        selected_graph_ids_by_workflow={
            "daily": (
                "model.analytics.dim_customer",
                "model.analytics.stg_customer",
                "test.analytics.dim_customer_not_null",
            ),
        },
    )

    assert report.passed
    assert report.issues == ()


def test_foreign_publish_model_in_workflow_closure_fails_with_owner_context() -> None:
    report = evaluate_dbt_workflow_graph_ownership(
        publish_model_ids_by_workflow={
            "daily": ("model.analytics.dim_customer",),
            "hourly": ("model.analytics.fact_order",),
        },
        selected_graph_ids_by_workflow={
            "daily": ("model.analytics.dim_customer",),
            "hourly": (
                "model.analytics.dim_customer",
                "model.analytics.fact_order",
            ),
        },
    )

    assert not report.passed
    assert [issue.code for issue in report.issues] == [
        DBT_CROSS_WORKFLOW_DEPENDENCY_UNSUPPORTED,
    ]
    issue = report.issues[0]
    assert issue.unique_id == "model.analytics.dim_customer"
    assert issue.workflow == "hourly"
    assert issue.owner_workflow == "daily"
    assert issue.path == "workflow:hourly"
    assert "daily" in issue.message
    assert "hourly" in issue.message
    assert issue.remediation


def test_shared_non_publish_model_in_multiple_closures_fails_once() -> None:
    report = evaluate_dbt_workflow_graph_ownership(
        publish_model_ids_by_workflow={
            "daily": ("model.analytics.dim_customer",),
            "hourly": ("model.analytics.fact_order",),
        },
        selected_graph_ids_by_workflow={
            "daily": (
                "model.analytics.dim_customer",
                "model.analytics.stg_shared",
            ),
            "hourly": (
                "model.analytics.fact_order",
                "model.analytics.stg_shared",
            ),
        },
    )

    assert not report.passed
    assert [issue.code for issue in report.issues] == [
        DBT_WORKFLOW_GRAPH_OVERLAP,
    ]
    issue = report.issues[0]
    assert issue.unique_id == "model.analytics.stg_shared"
    assert issue.workflows == ("daily", "hourly")
    assert issue.path == "model.analytics.stg_shared"
    assert issue.remediation


def test_policy_ignores_shared_tests_and_is_deterministic() -> None:
    arguments = {
        "publish_model_ids_by_workflow": {
            "hourly": ("model.analytics.fact_order",),
            "daily": ("model.analytics.dim_customer",),
        },
        "selected_graph_ids_by_workflow": {
            "hourly": (
                "test.analytics.shared_quality",
                "model.analytics.fact_order",
            ),
            "daily": (
                "test.analytics.shared_quality",
                "model.analytics.dim_customer",
            ),
        },
    }

    first = evaluate_dbt_workflow_graph_ownership(**arguments)
    second = evaluate_dbt_workflow_graph_ownership(**arguments)

    assert first == second
    assert first.passed
