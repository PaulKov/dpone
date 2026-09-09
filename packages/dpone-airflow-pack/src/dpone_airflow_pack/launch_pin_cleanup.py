"""Post-outcome_gate cleanup of pods retained for Authority C reads.

Lifecycle: create → freeze (keep_pod) → consume (outcome_gate) → cleanup.

Cleanup is occurrence-bound: it deletes **only** the verified handle published
by ``outcome_gate`` (try / uid / pin_sha256 / optional pointer resourceVersion).
It never reads the mutable ConfigMap head and never falls back to the runtime
XCom pin mirror for delete.

DAG status:

- Prefer native Airflow teardown (``as_teardown(on_failure_fail_dagrun=False)``)
  so cleanup success cannot mask a failed gate leaf/TaskGroup.
- Always rematerialize a failed gate result after soft delete so environments
  without teardown still end FAILED when the gate failed.
- Delete failures are soft and do not change a successful gate outcome.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Any

from dpone_airflow_pack.airflow_compat import python_operator_class
from dpone_airflow_pack.launch_pin_cleanup_handle import (
    AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_SCHEMA,
    build_cleanup_handle_from_pin,
    cleanup_handle_error,
)
from dpone_airflow_pack.launch_pin_codes import HEAD_PROVEN_CLOSED_STATUSES
from dpone_airflow_pack.launch_pin_locator import AIRFLOW_TASK_STATE_BACKEND
from dpone_airflow_pack.launch_pin_pod import delete_pod_for_launch_pin_typed

_LOG = logging.getLogger(__name__)

AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_XCOM_KEY = "dpone_launch_pin_cleanup_handle"
AIRFLOW_OUTCOME_GATE_RESULT_XCOM_KEY = "dpone_outcome_gate_result"


def build_pack_launch_pin_cleanup_task(
    *,
    pack: Mapping[str, Any],
    dag: Any,
    upstream_task_id: str,
    outcome_gate_task: Any = None,
    outcome_gate_task_id: str | None = None,
    node: Any = None,
    task_group: Any = None,
) -> Any:
    """Build the all_done cleanup task that deletes the gate-published handle."""

    del pack  # pack reserved for future retention policy knobs
    gate_task_id = _outcome_gate_task_id(
        outcome_gate_task=outcome_gate_task,
        outcome_gate_task_id=outcome_gate_task_id,
        upstream_task_id=upstream_task_id,
    )
    task = python_operator_class()(
        dag=dag,
        task_id=(node.task_id("launch_pin_cleanup") if node is not None else f"{upstream_task_id}__launch_pin_cleanup"),
        **({"task_group": task_group} if task_group is not None else {}),
        python_callable=cleanup_launch_pin_pod,
        op_kwargs={
            "upstream_task_id": upstream_task_id,
            "outcome_gate_task_id": gate_task_id,
        },
        trigger_rule="all_done",
    )
    return _mark_cleanup_teardown(task)


def _outcome_gate_task_id(
    *,
    outcome_gate_task: Any,
    outcome_gate_task_id: str | None,
    upstream_task_id: str,
) -> str:
    if isinstance(outcome_gate_task_id, str) and outcome_gate_task_id.strip():
        return outcome_gate_task_id.strip()
    direct = getattr(outcome_gate_task, "task_id", None)
    if isinstance(direct, str) and direct.strip():
        return direct.strip()
    outcome_kwargs = getattr(outcome_gate_task, "kwargs", None)
    if isinstance(outcome_kwargs, Mapping):
        nested = outcome_kwargs.get("task_id")
        if isinstance(nested, str) and nested.strip():
            return nested.strip()
    return f"{upstream_task_id}__outcome_gate"


def cleanup_launch_pin_pod(
    *,
    upstream_task_id: str,
    outcome_gate_task_id: str,
    ti: Any = None,
    **_: Any,
) -> dict[str, Any]:
    """Exact pod delete from the gate handle; rematerialize gate failure after."""

    del upstream_task_id  # occurrence is bound to the gate handle, not runtime XCom
    handle = _cleanup_handle_from_gate(ti=ti, outcome_gate_task_id=outcome_gate_task_id)
    result: dict[str, Any]
    if handle is None:
        result = {"status": "skipped", "reason": "cleanup_handle_absent"}
    else:
        result = _delete_from_handle(handle)
    _rematerialize_gate_failure(ti=ti, outcome_gate_task_id=outcome_gate_task_id)
    return result


def _mark_cleanup_teardown(task: Any) -> Any:
    """Prefer native teardown so cleanup cannot mask gate FAILED DAG status."""

    as_teardown = getattr(task, "as_teardown", None)
    if callable(as_teardown):
        try:
            return as_teardown(on_failure_fail_dagrun=False)
        except TypeError:
            try:
                marked = as_teardown()
                if hasattr(marked, "on_failure_fail_dagrun"):
                    marked.on_failure_fail_dagrun = False
                return marked
            except Exception:  # noqa: BLE001
                pass
        except Exception:  # noqa: BLE001
            pass
    # Fallback marker for tests / older Airflow — rematerialize covers DAG status.
    try:
        task.is_teardown = True
    except Exception:  # noqa: BLE001
        pass
    return task


def _cleanup_handle_from_gate(*, ti: Any, outcome_gate_task_id: str) -> dict[str, Any] | None:
    if ti is None or not hasattr(ti, "xcom_pull"):
        return None
    try:
        raw = ti.xcom_pull(task_ids=outcome_gate_task_id, key=AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_XCOM_KEY)
    except Exception:  # noqa: BLE001
        return None
    if raw is None:
        return None
    error = cleanup_handle_error(raw)
    if error:
        _LOG.warning("launch pin cleanup: ignoring invalid gate handle: %s", error)
        return None
    return dict(raw)


def _delete_from_handle(handle: Mapping[str, Any]) -> dict[str, Any]:
    """Pod delete → head ACTIVE→CONSUMED (+ readback) → per-try delete."""

    namespace = str(handle["pod_namespace"])
    name = str(handle["pod_name"])
    uid = str(handle["pod_uid"])
    store_coords = handle.get("store") if isinstance(handle.get("store"), Mapping) else {}
    frozen_conn = store_coords.get("kubernetes_conn_id") if isinstance(store_coords, Mapping) else None
    kubernetes_conn_id = str(frozen_conn).strip() if isinstance(frozen_conn, str) and frozen_conn.strip() else None
    base = {
        "pod_namespace": namespace,
        "pod_name": name,
        "pod_uid": uid,
        "try_number": int(handle["try_number"]),
        "pin_sha256": str(handle["pin_sha256"]),
    }
    try:
        pod_result = delete_pod_for_launch_pin_typed(
            namespace=namespace,
            name=name,
            expected_uid=uid,
            kubernetes_conn_id=kubernetes_conn_id,
        )
    except Exception as exc:  # noqa: BLE001
        _LOG.warning(
            "launch pin cleanup: delete %s/%s uid=%s try=%s failed (ignored): %s",
            namespace,
            name,
            uid,
            handle.get("try_number"),
            exc,
        )
        return {
            **base,
            "status": "delete_failed",
            "pod_delete": {"status": "unavailable", "detail": str(exc)},
            "head_transition": {"status": "skipped", "reason": "pod_delete_failed"},
            "per_try_delete": {"status": "skipped", "reason": "pod_delete_failed"},
            "detail": str(exc),
        }
    pod_status = str(pod_result.get("status") or "")
    if pod_status in {"uid_mismatch", "unavailable"}:
        _LOG.warning(
            "launch pin cleanup: delete %s/%s uid=%s try=%s blocked (%s): %s",
            namespace,
            name,
            uid,
            handle.get("try_number"),
            pod_status,
            pod_result.get("detail"),
        )
        return {
            **base,
            "status": "delete_failed",
            "pod_delete": dict(pod_result),
            "head_transition": {"status": "skipped", "reason": "pod_delete_failed"},
            "per_try_delete": {"status": "skipped", "reason": "pod_delete_failed"},
            "detail": str(pod_result.get("detail") or pod_status),
        }
    pod_delete: dict[str, Any] = dict(pod_result)
    store_backend = (
        str(store_coords.get("backend") if isinstance(store_coords, Mapping) else "") or "kubernetes_configmap"
    )
    if store_backend == AIRFLOW_TASK_STATE_BACKEND:
        return {
            **base,
            "status": "deleted",
            "pod_delete": pod_delete,
            "head_transition": {
                "status": "not_applicable",
                "backend": store_backend,
            },
            "per_try_delete": {
                "status": "not_applicable",
                "backend": store_backend,
            },
        }
    cm = _delete_configmap_occurrence(handle, kubernetes_conn_id=kubernetes_conn_id)
    head_transition = cm.get("head_transition")
    if not isinstance(head_transition, Mapping):
        head_transition = {"status": str(head_transition or "unknown")}
    per_try_delete = cm.get("per_try_delete")
    if not isinstance(per_try_delete, Mapping):
        per_try_delete = {
            "status": str(cm.get("status") or "unknown"),
            **{k: v for k, v in cm.items() if k != "status"},
        }
    overall = _overall_cleanup_status(head_transition=dict(head_transition), per_try_delete=dict(per_try_delete))
    return {
        **base,
        "status": overall,
        "pod_delete": pod_delete,
        "head_transition": dict(head_transition),
        "per_try_delete": dict(per_try_delete),
        "configmap": cm,
    }


def _overall_cleanup_status(
    *,
    head_transition: Mapping[str, Any],
    per_try_delete: Mapping[str, Any],
) -> str:
    """Map structured head/per-try phases to a single cleanup outcome."""

    head_st = str(head_transition.get("status") or "")
    per_try_st = str(per_try_delete.get("status") or "")
    if head_st not in HEAD_PROVEN_CLOSED_STATUSES:
        return "head_transition_failed"
    if per_try_st in {"deleted", "absent"}:
        return "deleted"
    return "per_try_delete_failed"


def _delete_configmap_occurrence(handle: Mapping[str, Any], *, kubernetes_conn_id: str | None) -> dict[str, Any]:
    """Head transition first, then occurrence-bound per-try delete (frozen store coords)."""

    del kubernetes_conn_id  # ConfigMap I/O uses store authority only
    resource_version = handle.get("pointer_resource_version")
    if not isinstance(resource_version, str) or not resource_version.strip():
        return {
            "status": "skipped",
            "head_transition": {"status": "skipped", "reason": "pointer_resource_version_absent"},
            "per_try_delete": {"status": "skipped", "reason": "pointer_resource_version_absent"},
        }
    dag_id = handle.get("dag_id")
    run_id = handle.get("run_id")
    task_id = handle.get("task_id")
    if not all(isinstance(value, str) and value.strip() for value in (dag_id, run_id, task_id)):
        return {
            "status": "skipped",
            "head_transition": {"status": "skipped", "reason": "subject_coordinates_absent"},
            "per_try_delete": {"status": "skipped", "reason": "subject_coordinates_absent"},
        }
    store_coords = handle.get("store") if isinstance(handle.get("store"), Mapping) else {}
    frozen_conn = store_coords.get("kubernetes_conn_id") if isinstance(store_coords, Mapping) else None
    frozen_ns = store_coords.get("namespace") if isinstance(store_coords, Mapping) else None
    conn = str(frozen_conn).strip() if isinstance(frozen_conn, str) and frozen_conn.strip() else None
    namespace = str(frozen_ns).strip() if isinstance(frozen_ns, str) and frozen_ns.strip() else None
    try:
        from dpone_airflow_pack.launch_pin import authoritative_launch_pin_store

        store = authoritative_launch_pin_store(
            kubernetes_conn_id=conn,
            namespace=namespace,
            allow_remote=True,
        )
        delete = getattr(store, "delete_occurrence", None)
        if not callable(delete):
            return {
                "status": "skipped",
                "head_transition": {"status": "skipped", "reason": "store_lacks_delete_occurrence"},
                "per_try_delete": {"status": "skipped", "reason": "store_lacks_delete_occurrence"},
            }
        result = delete(
            dag_id=str(dag_id),
            run_id=str(run_id),
            task_id=str(task_id),
            map_index=int(handle.get("map_index", -1)),
            pin_sha256=str(handle["pin_sha256"]),
            pointer_resource_version=resource_version.strip(),
            pod_uid=str(handle["pod_uid"]),
            try_number=int(handle["try_number"]),
        )
    except Exception as exc:  # noqa: BLE001 - ConfigMap GC must not fail a successful gate
        _LOG.warning("launch pin cleanup: ConfigMap occurrence delete ignored: %s", exc)
        return {
            "status": "delete_failed",
            "head_transition": {"status": "delete_failed", "detail": str(exc)},
            "per_try_delete": {"status": "delete_failed", "detail": str(exc)},
            "detail": str(exc),
        }
    if isinstance(result, Mapping):
        head_raw = result.get("head_transition")
        per_try = result.get("per_try_delete")
        head = dict(head_raw) if isinstance(head_raw, Mapping) else {"status": str(head_raw or "unknown")}
        if not isinstance(per_try, Mapping):
            per_try = {"status": "unknown"}
        return {
            "status": "deleted" if str(per_try.get("status")) == "deleted" else str(per_try.get("status") or "unknown"),
            "head_transition": head,
            "per_try_delete": dict(per_try),
            "store_namespace": namespace,
            "store_kubernetes_conn_id": conn,
        }
    return {
        "status": "deleted",
        "head_transition": {"status": "unknown"},
        "per_try_delete": {
            "status": "deleted",
            "pointer_resource_version": resource_version.strip(),
        },
        "store_namespace": namespace,
        "store_kubernetes_conn_id": conn,
    }


def _rematerialize_gate_failure(*, ti: Any, outcome_gate_task_id: str) -> None:
    """Fail this leaf when the gate published a failed outcome (no-teardown safety)."""

    if ti is None or not hasattr(ti, "xcom_pull"):
        return
    try:
        raw = ti.xcom_pull(task_ids=outcome_gate_task_id, key=AIRFLOW_OUTCOME_GATE_RESULT_XCOM_KEY)
    except Exception:  # noqa: BLE001
        return
    if not isinstance(raw, Mapping):
        return
    if raw.get("passed") is True:
        return
    raise RuntimeError(
        json.dumps(
            {
                "code": "DPONE_AIRFLOW_OUTCOME_GATE_REMATERIALIZED",
                "detail": "launch_pin_cleanup rematerialized failed outcome_gate result",
                "outcome_gate_task_id": outcome_gate_task_id,
                "gate_result": dict(raw),
            },
            ensure_ascii=True,
            sort_keys=True,
        )
    )


__all__ = [
    "AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_SCHEMA",
    "AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_XCOM_KEY",
    "AIRFLOW_OUTCOME_GATE_RESULT_XCOM_KEY",
    "build_cleanup_handle_from_pin",
    "build_pack_launch_pin_cleanup_task",
    "cleanup_handle_error",
    "cleanup_launch_pin_pod",
]
