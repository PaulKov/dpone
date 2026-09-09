"""Pure authoring and visible-task policies for Airflow DAG specs."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from typing import Any

from dpone.gitops.airflow_step_visibility import (
    AirflowStepVisibilityError,
    VisibleTaskBudget,
    build_visible_task_plan,
    parse_visible_task_budget,
)
from dpone.gitops.workload_catalog_models import GitOpsWorkloadCatalogIssue, issue


def parse_dag_visible_task_budget(
    *,
    dag_id: str,
    raw: object,
    producer: str,
) -> tuple[VisibleTaskBudget, list[GitOpsWorkloadCatalogIssue]]:
    """Adapt one invalid budget into the aggregate DAG-spec issue contract."""

    try:
        return parse_visible_task_budget(raw), []
    except AirflowStepVisibilityError as exc:
        return VisibleTaskBudget(), [issue(code=exc.code, message=str(exc), path=dag_id, source=producer)]


def visible_task_plan_json(
    node_estimates: Sequence[int],
    *,
    budget: VisibleTaskBudget,
) -> dict[str, int | str]:
    """Render the deterministic task estimate used by a DAG-spec artifact."""

    return build_visible_task_plan(node_estimates, budget=budget).to_jsonable()


def parse_start_date(raw: object) -> str | None:
    """Normalize supported ISO-compatible start-date authoring values."""

    if isinstance(raw, datetime | date):
        return raw.isoformat()
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    return None


def parse_workload_source(
    *,
    dag_id: str,
    payload: dict[str, Any],
    producer: str,
) -> tuple[tuple[str, ...] | None, str | None, list[GitOpsWorkloadCatalogIssue]]:
    """Validate the mutually exclusive workload-list and group authoring modes."""

    raw_workloads = payload.get("workloads")
    raw_group = payload.get("group")
    if raw_workloads is not None and raw_group is not None:
        return (
            None,
            None,
            [
                _issue(
                    "dag_spec_workload_source_ambiguous",
                    dag_id,
                    "Declare either workloads or group, not both",
                    producer,
                )
            ],
        )
    if raw_workloads is None and raw_group is None:
        return (
            None,
            None,
            [
                _issue(
                    "dag_spec_workload_source_missing",
                    dag_id,
                    "dags entry needs a workloads list or a group ref",
                    producer,
                )
            ],
        )
    if raw_group is not None:
        if not isinstance(raw_group, str) or not raw_group.strip():
            return None, None, [_issue("dag_spec_field_invalid", dag_id, "group must be a non-empty string", producer)]
        return None, raw_group.strip(), []
    if not isinstance(raw_workloads, list) or not raw_workloads:
        return (
            None,
            None,
            [
                _issue(
                    "dag_spec_field_invalid",
                    dag_id,
                    "workloads must be a non-empty list of ids",
                    producer,
                )
            ],
        )
    return tuple(str(item) for item in raw_workloads), None, []


def typed_field_issues(
    *,
    dag_id: str,
    payload: dict[str, Any],
    producer: str,
) -> list[GitOpsWorkloadCatalogIssue]:
    """Return aggregate issues for simple typed DAG authoring fields."""

    checks: tuple[tuple[str, Any, type | tuple[type, ...]], ...] = (
        ("tags", payload.get("tags"), list),
        ("default_args", payload.get("default_args"), dict),
        ("operator_overrides", payload.get("operator_overrides"), dict),
        ("catchup", payload.get("catchup"), bool),
        ("max_active_runs", payload.get("max_active_runs"), int),
    )
    return [
        _issue(
            "dag_spec_field_invalid",
            dag_id,
            f"{name} has invalid type: {type(value).__name__}",
            producer,
        )
        for name, value, expected in checks
        if value is not None and not isinstance(value, expected)
    ]


def optional_authoring_str(value: object) -> str | None:
    """Normalize one optional non-empty authoring string."""

    return str(value) if isinstance(value, str) and value.strip() else None


def dag_spec_issue(
    *,
    code: str,
    dag_id: str,
    message: str,
    producer: str,
) -> GitOpsWorkloadCatalogIssue:
    """Create one DAG-spec issue without coupling the model module to its storage type."""

    return _issue(code, dag_id, message, producer)


def _issue(code: str, dag_id: str, message: str, producer: str) -> GitOpsWorkloadCatalogIssue:
    return issue(code=code, message=message, path=dag_id, source=producer)


__all__ = [
    "GitOpsWorkloadCatalogIssue",
    "VisibleTaskBudget",
    "dag_spec_issue",
    "optional_authoring_str",
    "parse_dag_visible_task_budget",
    "parse_start_date",
    "parse_workload_source",
    "typed_field_issues",
    "visible_task_plan_json",
]
