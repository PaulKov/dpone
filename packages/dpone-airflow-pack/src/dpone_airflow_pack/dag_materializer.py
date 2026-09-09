"""Helpers to materialize one dpone dag-spec into an Airflow DAG object."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from importlib import import_module
from pathlib import Path
from typing import Any

from dpone_airflow_pack.airflow_compat import requested_module_is_absent
from dpone_airflow_pack.dag_materialization_metadata import (
    attach_delivery_context as _attach_delivery_context,
)
from dpone_airflow_pack.dag_materialization_metadata import (
    attach_partition_metadata as _attach_partition_metadata,
)
from dpone_airflow_pack.dag_materialization_metadata import (
    attach_run_identity_context as _attach_run_identity_context,
)
from dpone_airflow_pack.dag_materialization_metadata import (
    attach_spec_fingerprint as _attach_spec_fingerprint,
)
from dpone_airflow_pack.dag_materialization_metadata import dag_tags as _dag_tags
from dpone_airflow_pack.dag_schedule import dag_schedule_parameter_name, materialize_dag_timing
from dpone_airflow_pack.deployment_index import AirflowDeploymentIndexError
from dpone_airflow_pack.init_fetch_contract import InitFetchDeliveryContext
from dpone_airflow_pack.node_materialization import node_materialization_from_mapping
from dpone_airflow_pack.pack_wiring import WiredPackWorkload, wire_pack_workload
from dpone_airflow_pack.workflow_outcome import wire_workflow_outcome


def _resolve_node_pack_refs(spec: dict[str, Any], *, cache_resolver: Any) -> dict[str, Any]:
    """Resolve deployment-index node refs without admitting legacy raw paths."""

    nodes = spec.get("nodes")
    if not isinstance(nodes, list):
        return spec
    resolved_nodes: list[Any] = []
    changed = False
    for raw_node in nodes:
        if not isinstance(raw_node, Mapping):
            resolved_nodes.append(raw_node)
            continue
        pack_ref = raw_node.get("pack_ref")
        if not isinstance(pack_ref, str) or not pack_ref.startswith("cached://"):
            rejected_ref = pack_ref if isinstance(pack_ref, str) else raw_node.get("pack_path")
            raise AirflowDeploymentIndexError(
                "DPONE_CACHE_REF_INVALID",
                "deployment-index DAG nodes must use a cached:// workload pack_ref",
                path=str(rejected_ref or raw_node.get("workload_id") or "pack_ref"),
            )
        resolution = cache_resolver.resolve(pack_ref)
        if resolution.kind != "workload":
            raise AirflowDeploymentIndexError(
                "DPONE_CACHE_REF_INVALID",
                "dag-spec node pack_ref must resolve to a cached workload",
                path=pack_ref,
            )
        resolved_node = dict(raw_node)
        resolved_node["pack_ref"] = resolution.resolved_path.as_posix()
        resolved_node["pack_sha256"] = resolution.sha256
        resolved_node["_dpone_cache_root"] = resolution.cache_root.as_posix()
        resolved_node.pop("pack_path", None)
        resolved_nodes.append(resolved_node)
        changed = True
    if not changed:
        return spec
    resolved_spec = dict(spec)
    resolved_spec["nodes"] = resolved_nodes
    return resolved_spec


def _materialize_dag_spec(
    spec: Mapping[str, Any],
    *,
    repo_root: Path,
    operator_overrides: Mapping[str, Any],
    runtime_artifact_delivery: Mapping[str, Any] | None = None,
    run_identity_context: Mapping[str, Any] | None = None,
    delivery_context: InitFetchDeliveryContext | None = None,
) -> Any:
    dag_class, asset_schedule_class = _airflow_dag_types()
    dag_kwargs = {
        "dag_id": str(spec["dag_id"]),
        "description": spec.get("description"),
        **materialize_dag_timing(
            spec,
            asset_schedule_class=asset_schedule_class,
            dag_class=dag_class,
        ),
        "catchup": bool(spec.get("catchup", False)),
        "tags": _dag_tags(spec, run_identity_context),
        "max_active_runs": spec.get("max_active_runs"),
        "max_active_tasks": spec.get("max_active_tasks"),
    }
    dag_kwargs.update(_default_args(spec.get("default_args")))
    dag = dag_class(**{key: value for key, value in dag_kwargs.items() if value is not None})
    _attach_spec_fingerprint(dag, spec)
    _attach_partition_metadata(dag, spec)
    _attach_run_identity_context(dag, run_identity_context)
    _attach_delivery_context(dag, delivery_context)
    wired_nodes = _wire_nodes(
        spec,
        dag=dag,
        repo_root=repo_root,
        operator_overrides=operator_overrides,
        preview_mode=_is_local_preview_runtime_artifact_delivery(runtime_artifact_delivery),
        delivery_context=delivery_context,
    )
    for edge_index, edge in enumerate(_sequence(spec.get("edges"))):
        if not isinstance(edge, Mapping):
            raise _edge_wiring_error(spec, edge_index=edge_index, reason="edge must be a mapping")
        upstream_id = str(edge.get("upstream") or "")
        downstream_id = str(edge.get("downstream") or "")
        upstream = wired_nodes.get(upstream_id)
        downstream = wired_nodes.get(downstream_id)
        if upstream is None or downstream is None:
            raise _edge_wiring_error(
                spec,
                edge_index=edge_index,
                reason=f"wired endpoints are unavailable: {upstream_id!r} -> {downstream_id!r}",
            )
        for entrypoint in downstream.entrypoints:
            upstream.terminal >> entrypoint
    wire_workflow_outcome(
        spec,
        dag=dag,
        wired_nodes=wired_nodes,
        run_identity_context=run_identity_context,
        delivery_context=delivery_context,
    )
    return dag


def _edge_wiring_error(
    spec: Mapping[str, Any],
    *,
    edge_index: int,
    reason: str,
) -> AirflowDeploymentIndexError:
    dag_id = str(spec.get("dag_id") or "unknown")
    return AirflowDeploymentIndexError(
        "DPONE_AIRFLOW_DAG_EDGE_WIRING_FAILED",
        f"DAG {dag_id!r} edge {edge_index} could not be materialized: {reason}",
        path=dag_id,
    )


def _wire_nodes(
    spec: Mapping[str, Any],
    *,
    dag: Any,
    repo_root: Path,
    operator_overrides: Mapping[str, Any],
    preview_mode: bool = False,
    delivery_context: InitFetchDeliveryContext | None = None,
) -> dict[str, WiredPackWorkload]:
    if preview_mode:
        return _wire_preview_nodes(spec, dag=dag, operator_overrides=operator_overrides)
    wired: dict[str, WiredPackWorkload] = {}
    task_groups: dict[str, Any] = {}
    selected_processes: set[tuple[str, str]] = set()
    for raw_node in _sequence(spec.get("nodes")):
        if not isinstance(raw_node, Mapping):
            continue
        node = node_materialization_from_mapping(raw_node)
        if node.node_id in wired:
            raise ValueError(f"DPONE_AIRFLOW_TASK_ID_CONFLICT: duplicate node_id {node.node_id!r}")
        process_identity = (node.workload_id, node.selector or "")
        if node.selector is not None and process_identity in selected_processes:
            raise ValueError(
                "DPONE_AIRFLOW_NODE_SELECTOR_DUPLICATE: "
                f"workload {node.workload_id!r} selects process {node.selector!r} more than once"
            )
        selected_processes.add(process_identity)
        overrides = dict(operator_overrides)
        overrides.update(_mapping(spec.get("operator_overrides")))
        task_group = None
        if node.visibility == "group":
            task_group = task_groups.get(node.group_id)
            if task_group is None:
                task_group = _task_group_class()(group_id=node.group_id, dag=dag)
                task_groups[node.group_id] = task_group
        wired[node.node_id] = wire_pack_workload(
            node.workload_id,
            dag=dag,
            repo_root=repo_root,
            operator_overrides=overrides,
            pack_ref=str(raw_node.get("pack_ref") or "") or None,
            expected_sha256=str(raw_node.get("pack_sha256") or "") or None,
            confined_root=(
                Path(str(raw_node["_dpone_cache_root"])) if isinstance(raw_node.get("_dpone_cache_root"), str) else None
            ),
            node=node,
            task_group=task_group,
            delivery_context=delivery_context,
        )
    return wired


def _wire_preview_nodes(
    spec: Mapping[str, Any],
    *,
    dag: Any,
    operator_overrides: Mapping[str, Any],
) -> dict[str, WiredPackWorkload]:
    wired: dict[str, WiredPackWorkload] = {}
    for raw_node in _sequence(spec.get("nodes")):
        if not isinstance(raw_node, Mapping):
            continue
        node_id = str(raw_node.get("node_id") or "")
        workload_id = str(raw_node.get("workload_id") or node_id)
        if not node_id:
            continue
        overrides = dict(operator_overrides)
        overrides.update(_mapping(spec.get("operator_overrides")))
        task_id = _preview_task_id(
            node_id=node_id,
            workload_id=workload_id,
            operator_overrides=overrides,
        )
        task = _empty_operator_class()(task_id=task_id, dag=dag)
        wired[node_id] = WiredPackWorkload(
            entrypoints=(task,),
            terminal=task,
            workload_id=workload_id,
            runtime=task,
        )
    return wired


def _preview_task_id(*, node_id: str, workload_id: str, operator_overrides: Mapping[str, Any]) -> str:
    if node_id != workload_id:
        return f"{node_id}__dpone_runtime"
    task_id = operator_overrides.get("task_id")
    if isinstance(task_id, str) and task_id.strip():
        return task_id.strip()
    return f"{workload_id}__dpone_runtime"


def _is_local_preview_runtime_artifact_delivery(runtime_artifact_delivery: Mapping[str, Any] | None) -> bool:
    if runtime_artifact_delivery is None:
        return False
    return str(runtime_artifact_delivery.get("mode") or "").strip() == "local_preview"


def _quarantine_dag(*, dag_id: str, message: str, source: str) -> Any:
    dag_class, _ = _airflow_dag_types()
    operator_class = _empty_operator_class()
    dag = dag_class(
        dag_id=dag_id,
        **{
            dag_schedule_parameter_name(dag_class): None,
            "start_date": datetime(2026, 1, 1),
            "catchup": False,
            "is_paused_upon_creation": True,
            "tags": ["dpone_spec_error"],
            "doc_md": f"Quarantined dpone dag-spec from `{source}`: {message}",
        },
    )
    operator_class(task_id="dpone_spec_error", dag=dag)
    return dag


def _default_args(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        return {}
    args = dict(raw)
    retry_delay = args.pop("retry_delay_minutes", None)
    if retry_delay is not None:
        from datetime import timedelta

        args["retry_delay"] = timedelta(minutes=_retry_delay_minutes(retry_delay))
    return {"default_args": args} if args else {}


def _retry_delay_minutes(value: object) -> int:
    if isinstance(value, bool):
        raise ValueError("retry_delay_minutes must be a non-negative integer")
    if isinstance(value, int):
        minutes = value
    elif isinstance(value, str) and value.strip().isdigit():
        minutes = int(value)
    else:
        raise ValueError("retry_delay_minutes must be a non-negative integer")
    if minutes < 0:
        raise ValueError("retry_delay_minutes must be a non-negative integer")
    return minutes


def _airflow_dag_types() -> tuple[Any, Any | None]:
    try:
        sdk = import_module("airflow.sdk")
    except ModuleNotFoundError as exc:
        if not requested_module_is_absent(exc, "airflow.sdk"):
            raise
        try:
            legacy = import_module("airflow")
        except ModuleNotFoundError as legacy_exc:
            if not requested_module_is_absent(legacy_exc, "airflow"):
                raise
            raise RuntimeError("Airflow is required to load dpone dag-specs") from exc
        DAG = getattr(legacy, "DAG")
    else:
        DAG = getattr(sdk, "DAG")

    asset_schedule = None
    try:
        from airflow.timetables.assets import AssetSchedule

        asset_schedule = AssetSchedule
    except Exception:  # noqa: BLE001
        asset_schedule = None
    return DAG, asset_schedule


def _empty_operator_class() -> Any:
    try:
        primary = import_module("airflow.providers.standard.operators.empty")
    except ModuleNotFoundError as exc:
        if not requested_module_is_absent(exc, "airflow.providers.standard.operators.empty"):
            raise
        legacy = import_module("airflow.operators.empty")
        return getattr(legacy, "EmptyOperator")
    return getattr(primary, "EmptyOperator")


def _task_group_class() -> Any:
    try:
        sdk = import_module("airflow.sdk")
    except ModuleNotFoundError as exc:
        if not requested_module_is_absent(exc, "airflow.sdk"):
            raise
        legacy = import_module("airflow.utils.task_group")
        return getattr(legacy, "TaskGroup")
    return getattr(sdk, "TaskGroup")


def _sequence(value: Any) -> tuple[Any, ...]:
    if isinstance(value, list):
        return tuple(value)
    return ()


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


__all__ = [
    "_resolve_node_pack_refs",
    "_materialize_dag_spec",
]
