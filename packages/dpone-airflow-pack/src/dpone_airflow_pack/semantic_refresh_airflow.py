"""Lazy Airflow 3.3 materializer for an immutable run-neutral DAG sidecar."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from importlib import import_module
from typing import Any

from dpone_airflow_pack.dag_schedule import materialize_dag_timing
from dpone_airflow_pack.semantic_refresh_dag_projection import (
    AuthenticatedSemanticRefreshDagProjection,
    SemanticRefreshDagProjectionAuthority,
    SemanticRefreshDagTaskProjection,
    SemanticRefreshProjectionError,
    SemanticRefreshTask,
    authenticate_semantic_refresh_dag_projection,
)
from dpone_airflow_pack.semantic_refresh_projection import (
    DurableModelPublication,
    persist_and_project_success_assets,
    reduce_durable_workflow_summary,
)

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_DAG_RUN_TEMPLATE = "{{ dag_run.run_id }}"


@dataclass(frozen=True)
class SemanticRefreshAirflowCallables:
    """Worker-only authorities; constructing this DTO performs no I/O."""

    dbt_build_test: Callable[..., object]
    resolve_execution_binding: Callable[..., str]
    prepare: Callable[..., object]
    commit: Callable[..., object]
    read_publications: Callable[..., tuple[DurableModelPublication, ...]]
    persist_summary: Callable[[Mapping[str, object]], Mapping[str, object]]

    def __post_init__(self) -> None:
        for field_name in self.__dataclass_fields__:
            if not callable(getattr(self, field_name)):
                raise TypeError(f"semantic-refresh Airflow {field_name} must be callable")


@dataclass(frozen=True)
class _RunAuthorityCoordinates:
    dag_projection_sha256: str
    release_id: str
    deployment_id: str
    plan_bundle_sha256: str
    workflow_plan_sha256: str
    topology_sha256: str
    pre_release_bundle_sha256: str
    package_artifacts_sha256: str
    template_pack_fingerprint: str

    @classmethod
    def from_projection(cls, projection: AuthenticatedSemanticRefreshDagProjection) -> _RunAuthorityCoordinates:
        identity = projection.identity
        return cls(
            dag_projection_sha256=identity.dag_projection_sha256,
            release_id=identity.release_id,
            deployment_id=identity.deployment_id,
            plan_bundle_sha256=identity.plan_bundle_sha256,
            workflow_plan_sha256=identity.workflow_plan_sha256,
            topology_sha256=identity.topology_sha256,
            pre_release_bundle_sha256=identity.pre_release_bundle_sha256,
            package_artifacts_sha256=identity.package_artifacts_sha256,
            template_pack_fingerprint=identity.template_pack_fingerprint,
        )

    def to_kwargs(self, workflow_execution_id: str) -> dict[str, str]:
        return {
            "dag_projection_sha256": self.dag_projection_sha256,
            "deployment_id": self.deployment_id,
            "package_artifacts_sha256": self.package_artifacts_sha256,
            "plan_bundle_sha256": self.plan_bundle_sha256,
            "pre_release_bundle_sha256": self.pre_release_bundle_sha256,
            "release_id": self.release_id,
            "topology_sha256": self.topology_sha256,
            "template_pack_fingerprint": self.template_pack_fingerprint,
            "workflow_execution_id": workflow_execution_id,
            "workflow_plan_sha256": self.workflow_plan_sha256,
        }

    def to_projection_identity(self) -> dict[str, str]:
        """Return the closed run-neutral identity admitted by the dbt worker."""

        return {
            "dag_projection_sha256": self.dag_projection_sha256,
            "deployment_id": self.deployment_id,
            "package_artifacts_sha256": self.package_artifacts_sha256,
            "plan_bundle_sha256": self.plan_bundle_sha256,
            "pre_release_bundle_sha256": self.pre_release_bundle_sha256,
            "release_id": self.release_id,
            "template_pack_fingerprint": self.template_pack_fingerprint,
            "topology_sha256": self.topology_sha256,
            "workflow_plan_sha256": self.workflow_plan_sha256,
        }


@dataclass(frozen=True)
class _DbtTask:
    coordinates: _RunAuthorityCoordinates
    plan_bundle: Mapping[str, object]
    dbt_execution_pack: Mapping[str, object]
    project_config_overlay: Mapping[str, object]
    profile_sha256: str
    delegate: Callable[..., object]

    def __call__(self, **kwargs: object) -> object:
        run_id = _bound_dag_run_id(kwargs)
        return self.delegate(
            dbt_execution_pack=self.dbt_execution_pack,
            project_config_overlay=self.project_config_overlay,
            profile_sha256=self.profile_sha256,
            projection_identity=self.coordinates.to_projection_identity(),
            topology_sha256=self.coordinates.topology_sha256,
            plan_bundle=self.plan_bundle,
            workflow_execution_id=run_id,
        )


@dataclass(frozen=True)
class _RunBoundExecutionTask:
    coordinates: _RunAuthorityCoordinates
    resolver: Callable[..., str]
    delegate: Callable[..., object]

    def __call__(self, **kwargs: object) -> object:
        run_id = _bound_dag_run_id(kwargs)
        binding = _binding_digest(self.resolver(**self.coordinates.to_kwargs(run_id)))
        return self.delegate(**kwargs, workflow_execution_binding_sha256=binding)


@dataclass(frozen=True)
class _DurableSummaryTask:
    projection: SemanticRefreshDagTaskProjection
    coordinates: _RunAuthorityCoordinates
    operation_ids: tuple[str, ...]
    resolver: Callable[..., str]
    read_publications: Callable[..., tuple[DurableModelPublication, ...]]
    persist_summary: Callable[[Mapping[str, object]], Mapping[str, object]]

    def __call__(self) -> dict[str, object]:
        run_id = _current_dag_run_id()
        binding = _binding_digest(self.resolver(**self.coordinates.to_kwargs(run_id)))
        publications = self.read_publications(
            workflow_execution_id=run_id,
            workflow_execution_binding_sha256=binding,
            topology_sha256=self.projection.topology_sha256,
            expected_operation_ids=self.operation_ids,
        )
        if not isinstance(publications, tuple) or any(
            not isinstance(publication, DurableModelPublication) for publication in publications
        ):
            raise SemanticRefreshProjectionError("durable publication reader returned an invalid closure")
        summary = reduce_durable_workflow_summary(
            workflow_execution_id=run_id,
            workflow_plan_sha256=self.coordinates.workflow_plan_sha256,
            workflow_execution_binding_sha256=binding,
            expected_operation_ids=self.operation_ids,
            publications=publications,
        )
        if summary.status != "FULLY_COMPLETE":
            raise SemanticRefreshProjectionError("durable workflow summary is not fully complete")
        asset_uris = tuple(sorted(self.projection.asset_uris))
        if (
            persist_and_project_success_assets(
                summary,
                asset_uris=asset_uris,
                persist_summary=self.persist_summary,
            )
            != asset_uris
        ):
            raise SemanticRefreshProjectionError("durable workflow summary acknowledgement differs")
        return summary.to_mapping()


def materialize_semantic_refresh_dag(
    *,
    dag_projection: Mapping[str, object],
    dag_projection_authority: SemanticRefreshDagProjectionAuthority,
    callables: SemanticRefreshAirflowCallables,
) -> Any:
    """Build a static DAG; logical DagRun admission happens only in workers."""

    authenticated = authenticate_semantic_refresh_dag_projection(
        dag_projection,
        authority=dag_projection_authority,
    )
    projection = authenticated.projection
    coordinates = _RunAuthorityCoordinates.from_projection(authenticated)
    operation_ids = _operation_ids(projection)
    dag_class, asset_class, operator_class = _airflow_33_types()
    dag_policy = authenticated.dag_policy
    dag = dag_class(
        dag_id=projection.dag_id,
        **materialize_dag_timing(
            dag_policy.to_mapping(),
            asset_schedule_class=None,
            dag_class=dag_class,
        ),
        catchup=dag_policy.catchup,
        default_args={"owner": dag_policy.owner},
        max_active_runs=dag_policy.max_active_runs,
        max_active_tasks=dag_policy.max_active_tasks,
        tags=list(dag_policy.tags),
    )
    operators: dict[str, Any] = {}
    for task in _projected_tasks(projection):
        if task.task_kind == "DURABLE_WORKFLOW_SUMMARY":
            python_callable: Callable[..., object] = _DurableSummaryTask(
                projection=projection,
                coordinates=coordinates,
                operation_ids=operation_ids,
                resolver=callables.resolve_execution_binding,
                read_publications=callables.read_publications,
                persist_summary=callables.persist_summary,
            )
            kwargs: dict[str, object] = {
                "do_xcom_push": True,
                "outlets": [asset_class(uri) for uri in sorted(projection.asset_uris)],
                "retries": 0,
            }
        else:
            op_kwargs: dict[str, object] = {
                **coordinates.to_kwargs(_DAG_RUN_TEMPLATE),
                **({"operation_id": task.operation_id} if task.operation_id is not None else {}),
                **({"model_unique_id": task.model_unique_id} if task.model_unique_id is not None else {}),
            }
            if task.task_kind == "DBT_BUILD_TEST_GATE":
                op_kwargs.update(
                    {
                        "dbt_execution_pack": authenticated.dbt_execution_pack,
                        "profile_sha256": authenticated.profile_sha256,
                        "projection_identity": coordinates.to_projection_identity(),
                        "project_config_overlay": authenticated.project_config_overlay,
                    }
                )
                python_callable = _DbtTask(
                    coordinates,
                    authenticated.plan_bundle,
                    authenticated.dbt_execution_pack,
                    authenticated.project_config_overlay,
                    authenticated.profile_sha256,
                    callables.dbt_build_test,
                )
            else:
                python_callable = _RunBoundExecutionTask(
                    coordinates,
                    callables.resolve_execution_binding,
                    _task_callable(task, callables),
                )
            kwargs = {"do_xcom_push": False, "op_kwargs": op_kwargs, "retries": 0}
        operators[task.task_id] = operator_class(
            task_id=task.task_id,
            python_callable=python_callable,
            trigger_rule=task.trigger_rule,
            dag=dag,
            **kwargs,
        )
    for task in _projected_tasks(projection):
        for upstream_task_id in task.upstream_task_ids:
            operators[upstream_task_id] >> operators[task.task_id]
    return dag


def _airflow_33_types() -> tuple[Any, Any, Any]:
    try:
        sdk = import_module("airflow.sdk")
        standard = import_module("airflow.providers.standard.operators.python")
        return sdk.DAG, sdk.Asset, standard.PythonOperator
    except (AttributeError, ModuleNotFoundError) as exc:
        raise RuntimeError("Airflow 3.3 with the standard provider is required") from exc


def _current_dag_run_id() -> str:
    try:
        context = import_module("airflow.sdk").get_current_context()
        run_id = context["dag_run"].run_id
    except (AttributeError, KeyError, ModuleNotFoundError, TypeError) as exc:
        raise SemanticRefreshProjectionError("Airflow logical DagRun identity is unavailable") from exc
    if not isinstance(run_id, str) or not run_id.strip():
        raise SemanticRefreshProjectionError("Airflow logical DagRun identity is invalid")
    return run_id


def _bound_dag_run_id(kwargs: Mapping[str, object]) -> str:
    run_id = _current_dag_run_id()
    if kwargs.get("workflow_execution_id") != run_id:
        raise SemanticRefreshProjectionError("rendered Airflow logical DagRun differs from current context")
    return run_id


def _binding_digest(value: object) -> str:
    if not isinstance(value, str) or _DIGEST_RE.fullmatch(value) is None:
        raise SemanticRefreshProjectionError("worker execution binding authority is invalid")
    return value


def _projected_tasks(projection: SemanticRefreshDagTaskProjection) -> tuple[SemanticRefreshTask, ...]:
    return (
        projection.dbt_task,
        *projection.prepare_tasks,
        *projection.commit_tasks,
        projection.summary_task,
    )


def _operation_ids(projection: SemanticRefreshDagTaskProjection) -> tuple[str, ...]:
    result = tuple(task.operation_id for task in projection.prepare_tasks)
    if any(operation_id is None for operation_id in result):
        raise SemanticRefreshProjectionError("semantic-refresh projection omitted operation identity")
    operation_ids = tuple(str(operation_id) for operation_id in result)
    if tuple(task.operation_id for task in projection.commit_tasks) != operation_ids:
        raise SemanticRefreshProjectionError("prepare/commit operation closures differ")
    return tuple(sorted(operation_ids))


def _task_callable(
    task: SemanticRefreshTask,
    callables: SemanticRefreshAirflowCallables,
) -> Callable[..., object]:
    if task.task_kind == "CLICKHOUSE_PREPARE":
        return callables.prepare
    if task.task_kind == "CLICKHOUSE_COMMIT":
        return callables.commit
    raise SemanticRefreshProjectionError(f"unsupported semantic-refresh task kind: {task.task_kind}")


__all__ = ["SemanticRefreshAirflowCallables", "materialize_semantic_refresh_dag"]
