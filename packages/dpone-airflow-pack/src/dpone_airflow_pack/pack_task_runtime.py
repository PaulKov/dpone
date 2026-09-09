"""Operator/runtime helpers for compact pack task materialization."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone_airflow_pack.asset_partitions import partition_env_vars_from_pack
from dpone_airflow_pack.deployment_identity import (
    AIRFLOW_DEPLOYMENT_IDENTITY_ENV,
    deployment_identity_from_context,
    serialize_deployment_identity,
)
from dpone_airflow_pack.init_fetch_connection_bridge import (
    is_closed_init_fetch_connection_bridge,
)
from dpone_airflow_pack.init_fetch_pod_guard import (
    validate_strict_pack_extensions,
)
from dpone_airflow_pack.node_materialization import PackNodeMaterialization
from dpone_airflow_pack.operators import (
    AirflowConnectionSecretVolumeKubernetesPodOperator,
    PinnedXComSidecarKubernetesPodOperator,
    UnsafeAirflowConnectionEnvKubernetesPodOperator,
)
from dpone_airflow_pack.provider_execution import require_provider_execution
from dpone_airflow_pack.run_identity import (
    AIRFLOW_RUN_IDENTITY_ENV,
    WORKSPACE_AUTHORITY_CONNECTION_REF_CONTEXT_KEY,
    build_workload_run_identity,
    serialize_run_identity,
)
from dpone_airflow_pack.xcom_sidecar import (
    XComSidecarRuntimeConfig,
    require_strict_xcom_sidecar_image,
)

DBT_WORKSPACE_AUTHORITY_CONNECTION_REF_ENV = "DPONE_DBT_WORKSPACE_AUTHORITY_CONNECTION_REF"
_PACK_METADATA_FIELDS = frozenset({"outcome_mode", "resources_profile"})


def operator_kwargs(raw_kwargs: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in dict(raw_kwargs).items() if key not in _PACK_METADATA_FIELDS}


def runtime_operator_kwargs(
    pack: Mapping[str, Any],
    *,
    dag: Any = None,
    node: PackNodeMaterialization | None = None,
    task_group: Any = None,
    strict_provider_execution: bool = False,
    expected_workload_id: str | None = None,
) -> dict[str, Any]:
    if strict_provider_execution:
        validate_strict_pack_extensions(pack)
        raw_kwargs = require_provider_execution(
            pack,
            expected_workload_id=expected_workload_id,
        ).kpo_kwargs
    else:
        raw_kwargs = pack["kpo_kwargs"]
    kwargs = operator_kwargs(raw_kwargs)
    if strict_provider_execution:
        kwargs.pop("execution_timeout_seconds", None)
        default_args = getattr(dag, "default_args", None)
        if isinstance(default_args, Mapping) and "retries" in default_args:
            kwargs["retries"] = default_args["retries"]
    if node is not None:
        kwargs["task_id"] = node.task_id("dpone_runtime")
        kwargs["name"] = node.task_id("dpone_runtime").replace("_", "-")
    if task_group is not None:
        kwargs["task_group"] = task_group
    if not strict_provider_execution:
        apply_execution_policy(kwargs=kwargs, pack=pack)
    apply_partition_context(kwargs=kwargs, pack=pack, dag=dag)
    if not strict_provider_execution:
        command = str(pack.get("runtime_command") or "").strip()
        if command:
            kwargs["cmds"] = ["/bin/sh", "-ec"]
            kwargs["arguments"] = [command]
        pod_spec = pack.get("pod_spec")
        if isinstance(pod_spec, Mapping):
            kwargs["full_pod_spec"] = deserialize_pod_spec(pod_spec)
    return kwargs


def apply_partition_context(*, kwargs: dict[str, Any], pack: Mapping[str, Any], dag: Any) -> None:
    partition_env = partition_env_vars_from_pack(
        pack,
        partition_plan=getattr(dag, "_dpone_partition_plan", None),
        materialization_mode=dag_partition_mode(dag),
    )
    if not partition_env:
        return
    raw_env = kwargs.get("env_vars")
    if raw_env is not None and not isinstance(raw_env, Mapping):
        raise ValueError("DPONE_AIRFLOW_PARTITION_CAPABILITY_INVALID: kpo_kwargs.env_vars must be a mapping")
    env_vars = dict(raw_env or {})
    env_vars.update(partition_env)
    kwargs["env_vars"] = env_vars


def with_run_identity(
    pack: Mapping[str, Any],
    *,
    provenance: Mapping[str, Any],
    dag: Any,
    node: PackNodeMaterialization | None,
    run_identity_context: Mapping[str, Any] | None = None,
    workload_id: str | None = None,
) -> dict[str, Any]:
    context = run_identity_context
    if context is None:
        context = getattr(dag, "_dpone_run_identity_context", None)
    if not isinstance(context, Mapping):
        return dict(pack)
    pack_sha256 = provenance.get("pack_sha256")
    if not isinstance(pack_sha256, str):
        raise ValueError("DPONE_AIRFLOW_RUN_IDENTITY_INVALID: verified pack sha256 is missing")
    enriched = dict(pack)
    enriched["_dpone_run_identity"] = build_workload_run_identity(
        context,
        workload_id=(workload_id or (node.workload_id if node is not None else pack_workload_id(pack))),
        pack_sha256=pack_sha256,
    )
    workspace_ref = context.get(WORKSPACE_AUTHORITY_CONNECTION_REF_CONTEXT_KEY)
    if isinstance(workspace_ref, str):
        enriched[WORKSPACE_AUTHORITY_CONNECTION_REF_CONTEXT_KEY] = workspace_ref
    if deployment_identity := deployment_identity_from_context(context):
        enriched["_dpone_deployment_identity"] = deployment_identity
        # Exact activation always requires the full launch-envelope pin.
        existing_pin = enriched.get("deployment_identity_pin")
        if isinstance(existing_pin, Mapping) and existing_pin.get("required") is False:
            raise ValueError(
                "DPONE_AIRFLOW_DEPLOYMENT_IDENTITY_PIN_INVALID: "
                "exact activation cannot set deployment_identity_pin.required=false"
            )
        nested = enriched.get("outcome_gate")
        if isinstance(nested, Mapping):
            nested_pin = nested.get("deployment_identity_pin")
            if isinstance(nested_pin, Mapping) and nested_pin.get("required") is False:
                raise ValueError(
                    "DPONE_AIRFLOW_DEPLOYMENT_IDENTITY_PIN_INVALID: "
                    "exact activation cannot set outcome_gate.deployment_identity_pin.required=false"
                )
        enriched["deployment_identity_pin"] = {
            "schema": "dpone.airflow-runtime-launch-pin.v1",
            "required": True,
        }
    return enriched


def pack_workload_id(
    pack: Mapping[str, Any],
    *,
    strict_provider_execution: bool = False,
    expected_workload_id: str | None = None,
) -> str:
    if strict_provider_execution:
        return require_provider_execution(
            pack,
            expected_workload_id=expected_workload_id,
        ).workload_id
    workload = mapping(pack.get("workload"))
    workload_id = str(workload.get("workload_id") or workload.get("id") or "").strip()
    if workload_id:
        return workload_id
    task_id = str(mapping(pack.get("kpo_kwargs")).get("task_id") or "").strip()
    suffix = "__dpone_runtime"
    if task_id.endswith(suffix):
        return task_id[: -len(suffix)]
    if task_id:
        return task_id
    raise ValueError("DPONE_AIRFLOW_RUN_IDENTITY_INVALID: workload id is missing")


def apply_run_identity(*, kwargs: dict[str, Any], pack: Mapping[str, Any]) -> None:
    identity = pack.get("_dpone_run_identity")
    if not isinstance(identity, Mapping):
        return
    raw_env = kwargs.get("env_vars")
    if raw_env is not None and not isinstance(raw_env, Mapping):
        raise ValueError("DPONE_AIRFLOW_RUN_IDENTITY_INVALID: kpo_kwargs.env_vars must be a mapping")
    env_vars = dict(raw_env or {})
    env_vars[AIRFLOW_RUN_IDENTITY_ENV] = serialize_run_identity(identity)
    deployment_identity = pack.get("_dpone_deployment_identity")
    if isinstance(deployment_identity, Mapping):
        env_vars[AIRFLOW_DEPLOYMENT_IDENTITY_ENV] = serialize_deployment_identity(deployment_identity)
    workspace_ref = pack.get(WORKSPACE_AUTHORITY_CONNECTION_REF_CONTEXT_KEY)
    if isinstance(workspace_ref, str):
        env_vars[DBT_WORKSPACE_AUTHORITY_CONNECTION_REF_ENV] = workspace_ref
    kwargs["env_vars"] = env_vars
    raw_params = kwargs.get("params")
    if raw_params is not None and not isinstance(raw_params, Mapping):
        raise ValueError("DPONE_AIRFLOW_RUN_IDENTITY_INVALID: kpo_kwargs.params must be a mapping")
    params = dict(raw_params or {})
    params["dpone_run_identity"] = dict(identity)
    if isinstance(deployment_identity, Mapping):
        params["dpone_deployment_identity"] = dict(deployment_identity)
    kwargs["params"] = params


def dag_partition_mode(dag: Any) -> str:
    declared = getattr(dag, "_dpone_partition_mode", None)
    if declared in {"native", "degraded_unpartitioned"}:
        return str(declared)
    timetable = getattr(dag, "timetable", None)
    if timetable is not None and timetable.__class__.__name__ in {
        "CronPartitionTimetable",
        "PartitionedAssetTimetable",
    }:
        return "native"
    return "degraded_unpartitioned"


def apply_execution_policy(*, kwargs: dict[str, Any], pack: Mapping[str, Any]) -> None:
    execution = mapping(mapping(pack.get("airflow")).get("execution"))
    task_executor = str(execution.get("task_executor") or "").strip()
    if task_executor:
        kwargs["executor"] = task_executor
    copy_policy_value(kwargs, execution, "deferrable")
    copy_policy_value(kwargs, execution, "on_finish_action")
    copy_policy_value(kwargs, execution, "get_logs")
    if "logging_interval_seconds" in execution:
        kwargs["logging_interval"] = execution["logging_interval_seconds"]


def copy_policy_value(kwargs: dict[str, Any], execution: Mapping[str, Any], field: str) -> None:
    if field in execution:
        kwargs[field] = execution[field]


def operator_class(pack: Mapping[str, Any]) -> type[Any]:
    projection = mapping(pack.get("connection_projection"))
    if projection.get("mode") == "unsafe_airflow_env":
        return UnsafeAirflowConnectionEnvKubernetesPodOperator
    if is_airflow_connection_secret_volume_projection(projection):
        return AirflowConnectionSecretVolumeKubernetesPodOperator
    return PinnedXComSidecarKubernetesPodOperator


def operator_selection_pack(pack: Mapping[str, Any], *, strict: bool) -> Mapping[str, Any]:
    """Return the pack surface used to choose a KPO class.

    Strict init-fetch ignores legacy unsafe projection surfaces. The closed
    Airflow Connection bridge is the only projection that may select a specialized
    operator while a delivery context is present.
    """

    if not strict:
        return pack
    projection = mapping(pack.get("connection_projection"))
    if is_closed_init_fetch_connection_bridge(projection):
        return {"connection_projection": dict(projection)}
    return {}


def operator_init_kwargs(
    pack: Mapping[str, Any],
    kwargs: Mapping[str, Any],
    *,
    inline_required_status: str | None = None,
    strict_runtime_image_ref: str | None = None,
    pin_deployment_identity_for_separate_outcome_gate: bool = False,
    launch_pin_store: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    init_kwargs = dict(kwargs)
    if strict_runtime_image_ref is not None:
        validate_strict_pack_extensions(pack)
        init_kwargs["xcom_sidecar"] = XComSidecarRuntimeConfig(
            image=require_strict_xcom_sidecar_image(pack),
            strict_runtime_image=(strict_runtime_image_ref if kwargs.get("do_xcom_push") is True else None),
        )
    else:
        xcom = mapping(pack.get("xcom"))
        if xcom.get("sidecar_image"):
            init_kwargs["xcom_sidecar"] = XComSidecarRuntimeConfig(image=str(xcom["sidecar_image"]))
    if inline_required_status is not None:
        init_kwargs["inline_outcome_required_status"] = inline_required_status
    if pin_deployment_identity_for_separate_outcome_gate:
        init_kwargs["pin_deployment_identity_for_separate_outcome_gate"] = True
    if launch_pin_store is not None:
        init_kwargs["launch_pin_store"] = dict(launch_pin_store)
    projection = mapping(pack.get("connection_projection"))
    if strict_runtime_image_ref is None and projection.get("mode") == "unsafe_airflow_env":
        init_kwargs.update(
            {
                "unsafe_airflow_connection_ids": tuple(
                    str(item) for item in sequence(projection.get("connection_ids"))
                ),
                "unsafe_runtime_database_overrides": string_mapping(projection.get("database_overrides")),
                "unsafe_runtime_query_overrides": nested_string_mapping(projection.get("query_overrides")),
                "unsafe_runtime_scheme_overrides": string_mapping(projection.get("scheme_overrides")),
            }
        )
    if is_airflow_connection_secret_volume_projection(projection) and (
        strict_runtime_image_ref is None or is_closed_init_fetch_connection_bridge(projection)
    ):
        init_kwargs["airflow_connection_projection"] = dict(projection)
    return init_kwargs


def is_airflow_connection_secret_volume_projection(projection: Mapping[str, Any]) -> bool:
    return (
        projection.get("mode") == "kubernetes_secret_volume"
        and projection.get("payload_format") == "airflow_connection_uri"
    )


def deserialize_pod_spec(pod_spec: Mapping[str, Any]) -> object:
    payload = dict(pod_spec)
    try:
        from kubernetes.client import ApiClient
        from kubernetes.client import models as k8s
    except Exception:
        return payload
    return ApiClient()._ApiClient__deserialize_model(payload, k8s.V1Pod)


def mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def sequence(value: object) -> tuple[object, ...]:
    return tuple(value) if isinstance(value, list | tuple) else ()


def string_mapping(value: object) -> dict[str, str]:
    return {str(key): str(item) for key, item in mapping(value).items()}


def nested_string_mapping(value: object) -> dict[str, dict[str, str]]:
    return {str(key): string_mapping(item) for key, item in mapping(value).items()}


__all__ = [
    "apply_execution_policy",
    "apply_partition_context",
    "apply_run_identity",
    "dag_partition_mode",
    "deserialize_pod_spec",
    "is_airflow_connection_secret_volume_projection",
    "operator_class",
    "operator_init_kwargs",
    "operator_kwargs",
    "operator_selection_pack",
    "pack_workload_id",
    "runtime_operator_kwargs",
    "with_run_identity",
]
