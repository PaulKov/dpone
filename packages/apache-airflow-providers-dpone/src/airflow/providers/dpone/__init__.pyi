from collections.abc import Mapping, MutableMapping, Sequence
from pathlib import Path
from typing import Any, Literal

from airflow.models.dag import DAG as _AirflowDAG
from airflow.utils.task_group import TaskGroup as _AirflowTaskGroup
from dpone_airflow_pack.deployment_index import (
    AirflowDeploymentIndex as AirflowDeploymentIndex,
)
from dpone_airflow_pack.deployment_index import (
    AirflowDeploymentIndexError as AirflowDeploymentIndexError,
)
from dpone_airflow_pack.deployment_index import (
    AirflowIndexArtifact as AirflowIndexArtifact,
)
from dpone_airflow_pack.deployment_index import (
    CacheResolution as CacheResolution,
)
from dpone_airflow_pack.deployment_index import (
    CacheResolver as CacheResolver,
)
from dpone_airflow_pack.deployment_index import (
    LoadReport as LoadReport,
)
from dpone_airflow_pack.deployment_index import (
    load_airflow_deployment_index as load_airflow_deployment_index,
)
from dpone_airflow_pack.loader_ack import (
    AcknowledgedDagLoad as AcknowledgedDagLoad,
)
from dpone_airflow_pack.loader_ack import (
    LoaderAcknowledgement as LoaderAcknowledgement,
)
from dpone_airflow_pack.loader_ack import (
    LoaderAcknowledgementError as LoaderAcknowledgementError,
)
from dpone_airflow_pack.semantic_refresh_airflow import (
    SemanticRefreshAirflowCallables as SemanticRefreshAirflowCallables,
)

class DponeDag:
    @staticmethod
    def from_spec(
        spec_ref: str | Path,
        *,
        index_path: str | Path | None = ...,
        globals_dict: MutableMapping[str, Any] | None = ...,
    ) -> _AirflowDAG: ...

class DponeTaskGroup:
    @staticmethod
    def from_pack(
        pack_ref: str | Path,
        *,
        dag: _AirflowDAG | None = ...,
        index_path: str | Path | None = ...,
        deployment_id: str | None = ...,
        operator_overrides: Mapping[str, Any] | None = ...,
    ) -> _AirflowTaskGroup: ...

def get_provider_info() -> dict[str, Any]: ...
def load_and_acknowledge_dpone_dags(
    globals_dict: MutableMapping[str, Any],
    *,
    index_path: str | Path,
    ack_path: str | Path,
    ack_root: str | Path | None = ...,
    operator_overrides: Mapping[str, Any] | None = ...,
    duplicate_policy: Literal["skip_and_report", "fail_all", "replace_if_same_fingerprint"] = ...,
    invalid_dag_policy: Literal["skip_and_report", "fail_all", "create_diagnostic_dag"] = ...,
    semantic_refresh_callables: SemanticRefreshAirflowCallables | None = ...,
) -> AcknowledgedDagLoad: ...
def load_dpone_dags(
    globals_dict: MutableMapping[str, Any],
    repo_root: str | Path | None = ...,
    *,
    index_path: str | Path | None = ...,
    domains: Sequence[str] | None = ...,
    operator_overrides: Mapping[str, Any] | None = ...,
    duplicate_policy: Literal["skip_and_report", "fail_all", "replace_if_same_fingerprint"] = ...,
    invalid_dag_policy: Literal["skip_and_report", "fail_all", "create_diagnostic_dag"] = ...,
    semantic_refresh_callables: SemanticRefreshAirflowCallables | None = ...,
) -> LoadReport: ...
def write_dpone_loader_ack(
    report: LoadReport,
    *,
    index_path: str | Path,
    ack_path: str | Path,
    ack_root: str | Path | None = ...,
) -> LoaderAcknowledgement: ...

__all__: list[str]
