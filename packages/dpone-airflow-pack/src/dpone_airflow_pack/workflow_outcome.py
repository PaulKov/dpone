"""Terminal Airflow workflow outcome for compact dpone DAGs."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from typing import Any

from dpone_airflow_pack.airflow_compat import python_operator_class
from dpone_airflow_pack.deployment_identity import (
    AIRFLOW_DEPLOYMENT_IDENTITY_SCHEMA,
    deployment_identity_error,
)
from dpone_airflow_pack.deployment_index import AirflowDeploymentIndexError
from dpone_airflow_pack.dev_evidence_export import (
    DevEvidenceExportError,
    export_dev_evidence_if_requested,
)
from dpone_airflow_pack.init_fetch_contract import InitFetchDeliveryContext
from dpone_airflow_pack.workflow_terminal_states import (
    aggregate_terminal_task_states,
    terminal_task_states,
)

WORKFLOW_OUTCOME_SCHEMA = "dpone.dbt-workflow-outcome.v1"
WORKFLOW_OUTCOME_RESULT_SCHEMA = "dpone.dbt-workflow-outcome.v2"
_WORKFLOW_OUTCOME_FIELDS = frozenset(
    {
        "schema",
        "task_id",
        "workflow_id",
        "expected_terminal_task_ids",
    }
)
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


def wire_workflow_outcome(
    spec: Mapping[str, Any],
    *,
    dag: Any,
    wired_nodes: Mapping[str, Any],
    run_identity_context: Mapping[str, Any] | None,
    delivery_context: InitFetchDeliveryContext | None = None,
) -> Any | None:
    """Validate and wire one release-declared terminal workflow receipt."""

    config = spec.get("workflow_outcome")
    if not isinstance(config, Mapping):
        return None
    try:
        expected_task_ids = validated_workflow_outcome_config(config)
        upstream_ids = {
            str(edge.get("upstream") or "") for edge in _sequence(spec.get("edges")) if isinstance(edge, Mapping)
        }
        leaves = tuple(wired.terminal for node_id, wired in sorted(wired_nodes.items()) if node_id not in upstream_ids)
        actual_task_ids = validated_terminal_task_ids(
            [getattr(task, "task_id", None) for task in leaves],
            field="wired terminal task identities",
        )
    except ValueError as exc:
        raise _workflow_outcome_error(spec, str(exc)) from None
    if actual_task_ids != expected_task_ids:
        raise _workflow_outcome_error(
            spec,
            "configured and wired terminal task identities differ",
        )
    return build_workflow_outcome_task(
        config=config,
        dag=dag,
        upstream_tasks=leaves,
        expected_task_ids=expected_task_ids,
        run_identity_context=run_identity_context,
        workload_tasks=_workload_tasks(wired_nodes),
        delivery_context=delivery_context,
    )


def validated_workflow_outcome_config(raw: object) -> tuple[str, ...]:
    """Return the canonical terminal IDs from one closed static config."""

    if (
        not isinstance(raw, Mapping)
        or set(raw) != _WORKFLOW_OUTCOME_FIELDS
        or raw.get("schema") != WORKFLOW_OUTCOME_SCHEMA
        or not _non_empty_text(raw.get("task_id"))
        or not _non_empty_text(raw.get("workflow_id"))
        or not isinstance(raw.get("expected_terminal_task_ids"), list)
    ):
        raise ValueError("workflow_outcome contract is invalid")
    return validated_terminal_task_ids(
        raw["expected_terminal_task_ids"],
        field="expected terminal task identities",
    )


def validated_terminal_task_ids(
    raw: object,
    *,
    field: str,
) -> tuple[str, ...]:
    """Validate and canonicalize one explicit, non-inferred identity set."""

    if (
        isinstance(raw, (str, bytes))
        or not isinstance(raw, Sequence)
        or not raw
        or any(not _non_empty_text(item) for item in raw)
        or len(set(raw)) != len(raw)
    ):
        raise ValueError(f"{field} are invalid")
    return tuple(sorted(raw))


def build_workflow_outcome_task(
    *,
    config: Mapping[str, Any],
    dag: Any,
    upstream_tasks: Sequence[Any],
    expected_task_ids: Sequence[str],
    run_identity_context: Mapping[str, Any] | None,
    workload_tasks: Sequence[Mapping[str, str]] = (),
    delivery_context: InitFetchDeliveryContext | None = None,
) -> Any:
    """Materialize one parse-safe terminal evaluator from static DAG metadata."""

    task_ids = validated_terminal_task_ids(
        expected_task_ids,
        field="expected terminal task identities",
    )
    evidence_delivery = delivery_context.dev_evidence_delivery if delivery_context is not None else None
    task = python_operator_class()(
        dag=dag,
        task_id=str(config.get("task_id") or "workflow_outcome"),
        trigger_rule="all_done",
        retries=0,
        **({"queue": evidence_delivery.worker_queue} if evidence_delivery is not None else {}),
        python_callable=evaluate_workflow_outcome,
        op_kwargs={
            "workflow_id": str(config["workflow_id"]),
            "expected_task_ids": task_ids,
            "release_id": _identity(run_identity_context, "release_id"),
            "deployment_id": _identity(run_identity_context, "deployment_id"),
            "activation_id": _identity(run_identity_context, "_activation_id"),
            "workload_tasks": tuple(dict(item) for item in workload_tasks),
            "run_identity_context": (dict(run_identity_context) if isinstance(run_identity_context, Mapping) else None),
            "evidence_root": (evidence_delivery.mount_path if evidence_delivery is not None else None),
        },
    )
    for upstream in upstream_tasks:
        upstream >> task
    return task


def evaluate_workflow_outcome(
    *,
    workflow_id: str,
    expected_task_ids: Sequence[str],
    release_id: str | None,
    deployment_id: str | None,
    activation_id: str | None = None,
    workload_tasks: Sequence[Mapping[str, str]] = (),
    run_identity_context: Mapping[str, Any] | None = None,
    evidence_root: str | None = None,
    ti: Any = None,
    dag_run: Any = None,
    **_: Any,
) -> dict[str, Any]:
    """Persist a bounded receipt and fail when any required branch did not succeed."""

    if ti is None or not hasattr(ti, "xcom_push") or dag_run is None:
        raise RuntimeError("DPONE_DBT_WORKFLOW_OUTCOME_INVALID: Airflow runtime context is required")
    task_ids = validated_terminal_task_ids(
        expected_task_ids,
        field="expected terminal task identities",
    )
    identity_valid = _valid_digest(release_id) and _valid_digest(deployment_id)
    deployment_identity = {
        "schema": AIRFLOW_DEPLOYMENT_IDENTITY_SCHEMA,
        "release_id": release_id,
        "deployment_id": deployment_id,
        "activation_id": activation_id,
    }
    occurrence_valid = activation_id is None or not deployment_identity_error(deployment_identity)
    observed = terminal_task_states(
        dag_run=dag_run,
        ti=ti,
        expected_task_ids=task_ids,
    )
    tasks = tuple(
        {
            "task_id": task_id,
            "state": aggregate_terminal_task_states(observed.get(task_id, ())),
            "mapped_instances": len(observed.get(task_id, ())),
        }
        for task_id in task_ids
    )
    passed = (
        identity_valid and occurrence_valid and bool(task_ids) and all(item["state"] == "success" for item in tasks)
    )
    code = (
        "DPONE_DBT_WORKFLOW_PASSED"
        if passed
        else (
            "DPONE_DBT_WORKFLOW_FAILED"
            if identity_valid and occurrence_valid
            else "DPONE_DBT_WORKFLOW_IDENTITY_MISSING"
        )
    )
    payload: dict[str, Any] = {
        "schema": (
            WORKFLOW_OUTCOME_RESULT_SCHEMA
            if not deployment_identity_error(deployment_identity)
            else WORKFLOW_OUTCOME_SCHEMA
        ),
        "status": "passed" if passed else "failed",
        "code": code,
        "workflow_id": workflow_id,
        "release_id": release_id,
        "deployment_id": deployment_id,
        "dag_run_id": str(getattr(dag_run, "run_id", "") or ""),
        "tasks": list(tasks),
    }
    if not deployment_identity_error(deployment_identity):
        payload["deployment_identity"] = {
            "schema": AIRFLOW_DEPLOYMENT_IDENTITY_SCHEMA,
            "release_id": release_id,
            "deployment_id": deployment_id,
            "activation_id": activation_id,
        }
    if not passed:
        ti.xcom_push(key="dpone_workflow_outcome", value=payload)
        raise RuntimeError(json.dumps(payload, ensure_ascii=True, sort_keys=True))
    try:
        export = export_dev_evidence_if_requested(
            workflow_id=workflow_id,
            workflow_outcome=payload,
            workload_tasks=workload_tasks,
            run_identity_context=run_identity_context,
            ti=ti,
            dag_run=dag_run,
            evidence_root=evidence_root,
        )
    except DevEvidenceExportError:
        failed_payload = {
            **payload,
            "status": "failed",
            "code": "DPONE_DBT_WORKFLOW_EVIDENCE_EXPORT_FAILED",
        }
        ti.xcom_push(
            key="dpone_workflow_outcome",
            value=failed_payload,
        )
        raise
    ti.xcom_push(key="dpone_dev_evidence_export", value=export)
    ti.xcom_push(key="dpone_workflow_outcome", value=payload)
    return payload


def _workload_tasks(
    wired_nodes: Mapping[str, Any],
) -> tuple[dict[str, str], ...]:
    items = []
    for _, wired in sorted(wired_nodes.items()):
        workload_id = str(getattr(wired, "workload_id", "") or "")
        runtime_task_id = str(getattr(getattr(wired, "runtime", None), "task_id", "") or "")
        if workload_id and runtime_task_id:
            items.append(
                {
                    "workload_id": workload_id,
                    "runtime_task_id": runtime_task_id,
                }
            )
    return tuple(items)


def _valid_digest(value: object) -> bool:
    return isinstance(value, str) and _DIGEST_RE.fullmatch(value) is not None


def _identity(context: Mapping[str, Any] | None, field: str) -> str | None:
    value = context.get(field) if isinstance(context, Mapping) else None
    return value if isinstance(value, str) and value else None


def _workflow_outcome_error(
    spec: Mapping[str, Any],
    message: str,
) -> AirflowDeploymentIndexError:
    return AirflowDeploymentIndexError(
        "DPONE_DBT_WORKFLOW_OUTCOME_INVALID",
        message,
        path=str(spec.get("dag_id") or "unknown"),
    )


def _sequence(value: object) -> tuple[object, ...]:
    return tuple(value) if isinstance(value, list) else ()


def _non_empty_text(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


__all__ = [
    "WORKFLOW_OUTCOME_SCHEMA",
    "build_workflow_outcome_task",
    "evaluate_workflow_outcome",
    "validated_terminal_task_ids",
    "validated_workflow_outcome_config",
    "wire_workflow_outcome",
]
