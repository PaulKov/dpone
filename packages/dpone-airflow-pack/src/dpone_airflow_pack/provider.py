"""Public Airflow provider facade for dpone deployments."""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping, Sequence
from importlib import import_module
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

from dpone_airflow_pack.airflow_compat import (
    requested_module_is_absent as _requested_module_is_absent,
)
from dpone_airflow_pack.deployment_index import (
    AirflowDeploymentIndex,
    AirflowDeploymentIndexError,
    AirflowIndexArtifact,
    CacheResolution,
    CacheResolver,
    LoadReport,
    SemanticRefreshDagProjectionArtifact,
    load_airflow_deployment_index,
    load_semantic_refresh_dag_projection_artifact,
)
from dpone_airflow_pack.deployment_index_contract import (
    _load_airflow_deployment_index_descriptor,
)
from dpone_airflow_pack.init_fetch_contract import InitFetchDeliveryContext
from dpone_airflow_pack.loader_ack import AcknowledgedDagLoad
from dpone_airflow_pack.run_identity import build_task_group_run_identity_context

if TYPE_CHECKING:
    from airflow.models.dag import DAG as AirflowDAG
    from airflow.utils.task_group import TaskGroup as AirflowTaskGroup

    from dpone_airflow_pack.semantic_refresh_airflow import (
        SemanticRefreshAirflowCallables,
    )

_PROVIDER_PACKAGE_NAME = "apache-airflow-providers-dpone"
_READER_PACKAGE_NAME = "dpone-airflow-pack"


class DponeDag:
    """Escape hatch for materializing one DAG from a deployment index spec."""

    @staticmethod
    def from_spec(
        spec_ref: str | Path,
        *,
        index_path: str | Path | None = None,
        globals_dict: MutableMapping[str, Any] | None = None,
    ) -> AirflowDAG:
        if index_path is None:
            raise AirflowDeploymentIndexError(
                "DPONE_AIRFLOW_INDEX_REQUIRED",
                "index_path is required for DponeDag.from_spec in the deployment-index API",
                path=str(spec_ref),
            )
        index = _load_airflow_deployment_index_descriptor(index_path)
        target = _resolve_spec_ref(spec_ref, index=index)
        namespace: dict[str, Any] = {}
        from dpone_airflow_pack.dag_loader import load_dpone_dags_from_index

        report = load_dpone_dags_from_index(
            namespace,
            index=index,
            operator_overrides=None,
            duplicate_policy="skip_and_report",
            invalid_dag_policy="skip_and_report",
        )
        dag = _find_dag(target, namespace)
        if dag is not None:
            if globals_dict is not None:
                globals_dict[target] = dag
            return cast("AirflowDAG", dag)
        if report.errors:
            _raise_dag_load_error(target, report)
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_DAG_SPEC_NOT_FOUND",
            f"dpone DAG spec was not loaded: {target}",
            path=str(spec_ref),
        )


class DponeTaskGroup:
    """Escape hatch for embedding one dpone workload into a custom DAG."""

    @staticmethod
    def from_pack(
        pack_ref: str | Path,
        *,
        dag: AirflowDAG | None = None,
        index_path: str | Path | None = None,
        deployment_id: str | None = None,
        operator_overrides: Mapping[str, Any] | None = None,
    ) -> AirflowTaskGroup:
        (
            resolved_pack_ref,
            expected_sha256,
            run_identity_context,
            confined_root,
            delivery_context,
        ) = _resolve_pack_ref(
            pack_ref,
            index_path=index_path,
            deployment_id=deployment_id,
        )
        task_group_class = _airflow_task_group_class()
        if task_group_class is not None:
            group_id = _group_id(pack_ref, operator_overrides)
            group = task_group_class(group_id=group_id, dag=dag)
            overrides = dict(operator_overrides or {})
            overrides.pop("group_id", None)
            build_kwargs: dict[str, Any] = {
                "dag": dag,
                "operator_overrides": overrides,
                "task_group": group,
                "expected_sha256": expected_sha256,
                "run_identity_context": run_identity_context,
                "confined_root": confined_root,
            }
            if delivery_context is not None:
                build_kwargs["delivery_context"] = delivery_context
            build_dpone_gitops_task_group_from_pack(
                resolved_pack_ref,
                **build_kwargs,
            )
            return cast("AirflowTaskGroup", group)
        build_kwargs = {
            "dag": dag,
            "operator_overrides": operator_overrides,
            "expected_sha256": expected_sha256,
            "run_identity_context": run_identity_context,
            "confined_root": confined_root,
        }
        if delivery_context is not None:
            build_kwargs["delivery_context"] = delivery_context
        return cast(
            "AirflowTaskGroup",
            build_dpone_gitops_task_group_from_pack(
                resolved_pack_ref,
                **build_kwargs,
            ),
        )


def get_provider_info() -> dict[str, Any]:
    """Return minimal Airflow provider metadata."""

    return {
        "package-name": _PROVIDER_PACKAGE_NAME,
        "name": "dpone",
        "description": "Airflow provider facade for dpone release/deployment indexes",
        "versions": [_provider_version()],
    }


def load_dpone_dags(
    globals_dict: MutableMapping[str, Any],
    repo_root: str | Path | None = None,
    *,
    index_path: str | Path | None = None,
    domains: Sequence[str] | None = None,
    operator_overrides: Mapping[str, Any] | None = None,
    duplicate_policy: Literal["skip_and_report", "fail_all", "replace_if_same_fingerprint"] = "skip_and_report",
    invalid_dag_policy: Literal["skip_and_report", "fail_all", "create_diagnostic_dag"] = "skip_and_report",
    semantic_refresh_callables: SemanticRefreshAirflowCallables | None = None,
) -> LoadReport:
    from dpone_airflow_pack.dag_loader import load_dpone_dags as load

    return load(
        globals_dict,
        repo_root,
        index_path=index_path,
        domains=domains,
        operator_overrides=operator_overrides,
        duplicate_policy=duplicate_policy,
        invalid_dag_policy=invalid_dag_policy,
        semantic_refresh_callables=semantic_refresh_callables,
    )


def load_and_acknowledge_dpone_dags(
    globals_dict: MutableMapping[str, Any],
    *,
    index_path: str | Path,
    ack_path: str | Path,
    ack_root: str | Path | None = None,
    operator_overrides: Mapping[str, Any] | None = None,
    duplicate_policy: Literal["skip_and_report", "fail_all", "replace_if_same_fingerprint"] = "skip_and_report",
    invalid_dag_policy: Literal["skip_and_report", "fail_all", "create_diagnostic_dag"] = "skip_and_report",
    semantic_refresh_callables: SemanticRefreshAirflowCallables | None = None,
) -> AcknowledgedDagLoad:
    from dpone_airflow_pack.dag_loader import (
        load_and_acknowledge_dpone_dags as load_and_acknowledge,
    )

    return load_and_acknowledge(
        globals_dict,
        index_path=index_path,
        ack_path=ack_path,
        ack_root=ack_root,
        operator_overrides=operator_overrides,
        duplicate_policy=duplicate_policy,
        invalid_dag_policy=invalid_dag_policy,
        semantic_refresh_callables=semantic_refresh_callables,
    )


def build_dpone_gitops_task_group_from_pack(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from dpone_airflow_pack.pack_tasks import build_dpone_gitops_task_group_from_pack as build

    return build(*args, **kwargs)


def _provider_version() -> str:
    for distribution in (_PROVIDER_PACKAGE_NAME, _READER_PACKAGE_NAME):
        try:
            return version(distribution)
        except PackageNotFoundError:
            continue
    return "0+unknown"


def _find_dag(target: str, namespace: Mapping[str, Any]) -> Any | None:
    if target in namespace:
        return namespace[target]
    for dag in namespace.values():
        if getattr(dag, "dag_id", None) == target or getattr(dag, "kwargs", {}).get("dag_id") == target:
            return dag
    return None


def _raise_dag_load_error(target: str, report: LoadReport) -> None:
    for error in report.errors:
        if _error_matches_target(error, target):
            code = _provider_error_code(error)
            raise AirflowDeploymentIndexError(
                code,
                error.get("message") or f"dpone DAG spec is invalid: {target}",
                path=error.get("path"),
            )


def _provider_error_code(error: dict[str, str]) -> str:
    code = error.get("code")
    if isinstance(code, str) and code.startswith("DPONE_"):
        return code
    return "DPONE_AIRFLOW_DAG_SPEC_INVALID"


def _error_matches_target(error: dict[str, str], target: str) -> bool:
    if error.get("dag_id") == target:
        return True
    path = error.get("path")
    return bool(path and Path(path).name.startswith(f"{target}."))


def _airflow_task_group_class() -> Any | None:
    try:
        sdk = import_module("airflow.sdk")
    except ModuleNotFoundError as exc:
        if not _requested_module_is_absent(exc, "airflow.sdk"):
            raise
    else:
        return getattr(sdk, "TaskGroup")
    try:
        legacy = import_module("airflow.utils.task_group")
    except ModuleNotFoundError as exc:
        if not _requested_module_is_absent(exc, "airflow.utils.task_group"):
            raise
        return None
    return getattr(legacy, "TaskGroup")


def _group_id(pack_ref: str | Path, operator_overrides: Mapping[str, Any] | None) -> str:
    if operator_overrides and operator_overrides.get("group_id"):
        return str(operator_overrides["group_id"])
    raw = str(pack_ref).rstrip("/")
    if raw.startswith("cached://workloads/"):
        return raw.removeprefix("cached://workloads/").split("?", 1)[0]
    if raw.startswith("cached://deployments/"):
        segments = raw.removeprefix("cached://deployments/").split("?", 1)[0].split("/")
        if len(segments) == 3 and segments[1] == "workloads":
            return segments[2]
    if raw.startswith("cached://"):
        return raw.removeprefix("cached://").split("?", 1)[0]
    return Path(raw).stem.replace(".", "_") or "dpone_workload"


def _resolve_pack_ref(
    pack_ref: str | Path,
    *,
    index_path: str | Path | None,
    deployment_id: str | None,
) -> tuple[
    str | Path,
    str | None,
    Mapping[str, Any] | None,
    Path | None,
    InitFetchDeliveryContext | None,
]:
    raw = str(pack_ref)
    if not raw.startswith("cached://"):
        return pack_ref, None, None, None, None
    if index_path is None:
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_INDEX_REQUIRED",
            "index_path is required for cached workload refs in DponeTaskGroup.from_pack",
            path=raw,
        )
    index = _load_airflow_deployment_index_descriptor(index_path)
    resolver = CacheResolver(index)
    if deployment_id is not None and deployment_id != resolver.deployment_id:
        raise AirflowDeploymentIndexError(
            "DPONE_DEPLOYMENT_ID_MISMATCH",
            f"deployment_id {deployment_id} does not match deployment index {resolver.deployment_id}",
            path=raw,
        )
    resolution = resolver.resolve(raw)
    if resolution.kind != "workload":
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_REF_INVALID",
            "DponeTaskGroup.from_pack requires cached://workloads/<workload_id>",
            path=raw,
        )
    return (
        resolution.resolved_path,
        resolution.sha256,
        build_task_group_run_identity_context(index),
        resolution.cache_root,
        index.delivery_context,
    )


def _resolve_spec_ref(spec_ref: str | Path, *, index: AirflowDeploymentIndex) -> str:
    raw = str(spec_ref)
    if raw.startswith("cached://"):
        resolution = CacheResolver(index).resolve(raw)
        if resolution.kind != "dag":
            raise AirflowDeploymentIndexError(
                "DPONE_CACHE_REF_INVALID",
                "DponeDag.from_spec requires cached://dags/<dag_id>",
                path=raw,
            )
        return resolution.logical_id
    return raw.removeprefix("cached://dags/")


__all__ = [
    "AirflowDeploymentIndex",
    "AirflowDeploymentIndexError",
    "AirflowIndexArtifact",
    "CacheResolution",
    "CacheResolver",
    "DponeDag",
    "DponeTaskGroup",
    "LoadReport",
    "SemanticRefreshDagProjectionArtifact",
    "get_provider_info",
    "load_airflow_deployment_index",
    "load_semantic_refresh_dag_projection_artifact",
    "load_and_acknowledge_dpone_dags",
    "load_dpone_dags",
]
