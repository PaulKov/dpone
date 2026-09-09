"""Lightweight Airflow-side helpers for consuming dpone GitOps packs."""

from __future__ import annotations

import sys
import warnings
from typing import Any

from dpone_airflow_pack.pack_provenance import load_dpone_airflow_pack_with_provenance
from dpone_airflow_pack.runtime_adapter import (
    DponeAirflowArtifactError,
    DponeAirflowContractError,
    build_dpone_gitops_task_from_artifacts,
    load_dpone_kpo_kwargs,
    validate_dpone_artifacts,
)
from dpone_airflow_pack.workload_catalog import (
    load_gitops_domain_catalog,
    load_gitops_workload_groups,
    workload_ids_from_gitops_domain,
)

_PROVIDER_COMPAT_EXPORTS = {
    "AcknowledgedDagLoad": "dpone_airflow_pack.loader_ack.AcknowledgedDagLoad",
    "AirflowDeploymentIndex": "dpone_airflow_pack.provider.AirflowDeploymentIndex",
    "AirflowDeploymentIndexError": "dpone_airflow_pack.provider.AirflowDeploymentIndexError",
    "AirflowIndexArtifact": "dpone_airflow_pack.provider.AirflowIndexArtifact",
    "CacheResolution": "dpone_airflow_pack.provider.CacheResolution",
    "CacheResolver": "dpone_airflow_pack.provider.CacheResolver",
    "DponeDag": "dpone_airflow_pack.provider.DponeDag",
    "DponeTaskGroup": "dpone_airflow_pack.provider.DponeTaskGroup",
    "LoadReport": "dpone_airflow_pack.provider.LoadReport",
    "LoaderAcknowledgement": "dpone_airflow_pack.loader_ack.LoaderAcknowledgement",
    "LoaderAcknowledgementError": "dpone_airflow_pack.loader_ack.LoaderAcknowledgementError",
    "SemanticRefreshDagProjectionArtifact": ("dpone_airflow_pack.provider.SemanticRefreshDagProjectionArtifact"),
    "get_provider_info": "dpone_airflow_pack.provider.get_provider_info",
    "load_airflow_deployment_index": "dpone_airflow_pack.provider.load_airflow_deployment_index",
    "load_semantic_refresh_dag_projection_artifact": (
        "dpone_airflow_pack.provider.load_semantic_refresh_dag_projection_artifact"
    ),
    "load_and_acknowledge_dpone_dags": "dpone_airflow_pack.provider.load_and_acknowledge_dpone_dags",
    "load_dpone_dags": "dpone_airflow_pack.provider.load_dpone_dags",
    "write_dpone_loader_ack": "dpone_airflow_pack.loader_ack.write_dpone_loader_ack",
}
_LOCAL_LAZY_EXPORTS = {
    "WiredPackWorkload": "dpone_airflow_pack.pack_wiring.WiredPackWorkload",
}
_provider_compat_warning_emitted = False


def build_dpone_gitops_task_group_from_pack(*args: Any, **kwargs: Any) -> Any:
    from dpone_airflow_pack.pack_tasks import build_dpone_gitops_task_group_from_pack as build

    return build(*args, **kwargs)


def load_dpone_airflow_pack(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from dpone_airflow_pack.pack_tasks import load_dpone_airflow_pack as load

    return load(*args, **kwargs)


def wire_pack_workload(*args: Any, **kwargs: Any) -> Any:
    from dpone_airflow_pack.pack_wiring import wire_pack_workload as wire

    return wire(*args, **kwargs)


def wire_workloads_in_waves(*args: Any, **kwargs: Any) -> Any:
    from dpone_airflow_pack.pack_wiring import wire_workloads_in_waves as wire

    return wire(*args, **kwargs)


def __getattr__(name: str) -> Any:
    target = _PROVIDER_COMPAT_EXPORTS.get(name)
    if target is not None:
        _warn_provider_compat_export()
        value = _load_export(target)
        globals()[name] = value
        return value
    target = _LOCAL_LAZY_EXPORTS.get(name)
    if target is not None:
        value = _load_export(target)
        globals()[name] = value
        return value
    module_name = f"{__name__}.{name}"
    module = sys.modules.get(module_name)
    if module is not None and getattr(module, "__package__", "") == __name__:
        globals()[name] = module
        return module
    raise AttributeError(f"module 'dpone_airflow_pack' has no attribute {name!r}")


def _load_export(target: str) -> Any:
    module_name, attribute = target.rsplit(".", 1)
    module = __import__(module_name, fromlist=[attribute])
    return getattr(module, attribute)


def _warn_provider_compat_export() -> None:
    global _provider_compat_warning_emitted
    if _provider_compat_warning_emitted:
        return
    warnings.warn(
        "Import provider facade objects from airflow.providers.dpone; "
        "dpone_airflow_pack compatibility exports are deprecated.",
        DeprecationWarning,
        stacklevel=3,
    )
    _provider_compat_warning_emitted = True


__all__ = [
    "AcknowledgedDagLoad",
    "AirflowDeploymentIndex",
    "AirflowDeploymentIndexError",
    "AirflowIndexArtifact",
    "CacheResolution",
    "CacheResolver",
    "DponeAirflowArtifactError",
    "DponeAirflowContractError",
    "DponeDag",
    "DponeTaskGroup",
    "LoadReport",
    "LoaderAcknowledgement",
    "LoaderAcknowledgementError",
    "SemanticRefreshDagProjectionArtifact",
    "WiredPackWorkload",
    "build_dpone_gitops_task_group_from_pack",
    "build_dpone_gitops_task_from_artifacts",
    "get_provider_info",
    "load_airflow_deployment_index",
    "load_semantic_refresh_dag_projection_artifact",
    "load_and_acknowledge_dpone_dags",
    "load_dpone_airflow_pack",
    "load_dpone_airflow_pack_with_provenance",
    "load_dpone_dags",
    "load_gitops_domain_catalog",
    "load_gitops_workload_groups",
    "load_dpone_kpo_kwargs",
    "wire_pack_workload",
    "wire_workloads_in_waves",
    "workload_ids_from_gitops_domain",
    "validate_dpone_artifacts",
    "write_dpone_loader_ack",
]
