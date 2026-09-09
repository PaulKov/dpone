"""Optional Airflow task callables for the protected semantic-refresh runtime."""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from dpone_airflow_pack.semantic_refresh_airflow import (
        SemanticRefreshAirflowCallables,
    )
    from dpone_airflow_pack.semantic_refresh_projection import (
        DurableModelPublication,
    )

    from dpone.adapters.semantic_refresh_mssql_workflow_summary import (
        MssqlSemanticRefreshWorkflowSummaryState,
    )
    from dpone.app.semantic_refresh_composition import (
        SemanticRefreshPublicationRuntime,
    )
    from dpone.ports.semantic_refresh_workflow_publication import (
        SemanticRefreshWorkflowPublicationPort,
    )
    from dpone.readiness.airflow_semantic_refresh_projection import (
        SemanticRefreshDeploymentSidecar,
    )


@dataclass(frozen=True, slots=True)
class SemanticRefreshDagProjectionSource:
    """One template/final-plan pair supplied by the protected controller."""

    template_pack: Mapping[str, object]
    plan_bundle: Mapping[str, object]


class SemanticRefreshPostDeploymentProjectionPort(Protocol):
    """Produce final plan inputs only after the deployment ID exists."""

    def load(
        self,
        *,
        release_id: str,
        deployment_id: str,
    ) -> tuple[SemanticRefreshDagProjectionSource, ...]: ...


class SemanticRefreshAirflowWorkerAuthority(Protocol):
    """One correlated worker capability for admission, dbt, and binding lookup."""

    def dbt_build_test(self, **kwargs: object) -> object:
        """Admit the actual DagRun and execute its exact protected dbt plan."""

    def resolve_execution_binding(self, **kwargs: object) -> str:
        """Resolve the durable binding for a later worker task."""


@dataclass(frozen=True, slots=True)
class SemanticRefreshVerifiedDagSidecarFactory:
    """Adapt protected post-deployment plans into immutable index sidecars."""

    source: SemanticRefreshPostDeploymentProjectionPort

    def build(
        self,
        *,
        release_id: str,
        deployment_id: str,
    ) -> tuple[SemanticRefreshDeploymentSidecar, ...]:
        from dpone_airflow_pack.semantic_refresh_dag_authority import (  # noqa: PLC0415
            LocalSemanticRefreshDagProjectionAuthority,
        )
        from dpone_airflow_pack.semantic_refresh_dag_projection import (  # noqa: PLC0415
            build_semantic_refresh_dag_projection,
        )

        from dpone.readiness.airflow_semantic_refresh_projection import (  # noqa: PLC0415
            SemanticRefreshDeploymentSidecar,
        )

        values = self.source.load(
            release_id=release_id,
            deployment_id=deployment_id,
        )
        if not isinstance(values, tuple):
            raise TypeError("semantic-refresh sidecar source must return a tuple")
        result: list[SemanticRefreshDeploymentSidecar] = []
        for value in values:
            if not isinstance(value, SemanticRefreshDagProjectionSource):
                raise TypeError("semantic-refresh sidecar source entry is invalid")
            projection = build_semantic_refresh_dag_projection(
                template_pack=value.template_pack,
                plan_bundle=value.plan_bundle,
            )
            if projection.identity.release_id != release_id or projection.identity.deployment_id != deployment_id:
                raise ValueError("semantic-refresh sidecar differs from its deployment")
            result.append(
                SemanticRefreshDeploymentSidecar(
                    content=projection.canonical_bytes(),
                    descriptor=projection.descriptor(),
                    authority=LocalSemanticRefreshDagProjectionAuthority.build(projection.identity).to_mapping(),
                )
            )
        return tuple(result)


@dataclass(frozen=True, slots=True)
class SemanticRefreshAirflowRuntime:
    """Bound worker callables backed only by durable runtime authorities."""

    publication: SemanticRefreshPublicationRuntime
    workflow_publications: SemanticRefreshWorkflowPublicationPort
    workflow_summary: MssqlSemanticRefreshWorkflowSummaryState

    def prepare(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
        **_: object,
    ) -> object:
        """Seal and persist PREPARED; the task return is not an authority."""

        return self.publication.models.prepare(
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            operation_id=operation_id,
        )

    def commit(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
        **_: object,
    ) -> object:
        """Load durable PREPARED documents and reconcile the target commit."""

        return self.publication.models.commit(
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            operation_id=operation_id,
        )

    def read_publications(
        self,
        *,
        workflow_execution_id: str,
        workflow_execution_binding_sha256: str,
        topology_sha256: str,
        expected_operation_ids: tuple[str, ...],
    ) -> tuple[DurableModelPublication, ...]:
        """Map exact SQL Server terminal rows to the optional Airflow package DTO."""

        _digest(topology_sha256, "topology_sha256")
        records = self.workflow_publications.read(
            workflow_execution_id=workflow_execution_id,
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            expected_operation_ids=expected_operation_ids,
        )
        from dpone_airflow_pack.semantic_refresh_projection import (  # noqa: PLC0415
            DurableModelPublication,
        )

        return tuple(
            DurableModelPublication(
                operation_id=record.operation_id,
                operation_plan_sha256=record.operation_plan_sha256,
                workflow_execution_binding_sha256=record.workflow_execution_binding_sha256,
                attempt_binding_sha256=record.attempt_binding_sha256,
                status=record.status,
                artifact_manifest_sha256=record.artifact_manifest_sha256,
                clickhouse_terminal_receipt_sha256=(record.clickhouse_terminal_receipt_sha256),
                terminal_receipt_sha256=record.terminal_receipt_sha256,
                target_generation=record.target_generation,
                scope_revision=record.scope_revision,
            )
            for record in records
        )

    def persist_summary(self, summary: Mapping[str, object]) -> Mapping[str, object]:
        """Persist the canonical fully-complete summary and exact guard release."""

        return self.workflow_summary.persist(summary)


def build_semantic_refresh_airflow_callables(
    *,
    runtime: SemanticRefreshAirflowRuntime,
    worker: SemanticRefreshAirflowWorkerAuthority,
) -> SemanticRefreshAirflowCallables:
    """Build parse-inert callables from one correlated worker authority."""

    from dpone_airflow_pack.semantic_refresh_airflow import (  # noqa: PLC0415
        SemanticRefreshAirflowCallables,
    )

    return SemanticRefreshAirflowCallables(
        dbt_build_test=worker.dbt_build_test,
        resolve_execution_binding=worker.resolve_execution_binding,
        prepare=runtime.prepare,
        commit=runtime.commit,
        read_publications=runtime.read_publications,
        persist_summary=runtime.persist_summary,
    )


def materialize_verified_semantic_refresh_dag(
    *,
    artifact: object,
    callables: SemanticRefreshAirflowCallables,
) -> object:
    """Build a static DAG only from a sidecar in the verified local index."""

    from dpone_airflow_pack.semantic_refresh_airflow import (  # noqa: PLC0415
        materialize_semantic_refresh_dag,
    )
    from dpone_airflow_pack.semantic_refresh_index_artifacts import (  # noqa: PLC0415
        SemanticRefreshDagProjectionArtifact,
        load_semantic_refresh_dag_projection_artifact,
    )

    if not isinstance(artifact, SemanticRefreshDagProjectionArtifact):
        raise TypeError("semantic-refresh DAG materialization requires a verified index artifact")
    projection = load_semantic_refresh_dag_projection_artifact(
        artifact,
        max_artifact_bytes=max(artifact.artifact_bytes, 1),
    )
    return materialize_semantic_refresh_dag(
        dag_projection=projection.to_mapping(),
        dag_projection_authority=artifact.authority,
        callables=callables,
    )


def load_verified_semantic_refresh_airflow_dags(
    globals_dict: MutableMapping[str, object],
    *,
    index_path: str | Path,
    ack_path: str | Path,
    ack_root: str | Path | None = None,
    runtime: SemanticRefreshAirflowRuntime,
    worker: SemanticRefreshAirflowWorkerAuthority,
) -> object:
    """Load and acknowledge the index with the concrete semantic worker graph.

    The scheduler performs no control-store or connector I/O here.  The bound
    capabilities open their injected resources only when Airflow executes a
    worker task for the actual ``dag_run.run_id``.
    """

    from dpone_airflow_pack.provider import load_and_acknowledge_dpone_dags  # noqa: PLC0415

    return load_and_acknowledge_dpone_dags(
        globals_dict,
        index_path=index_path,
        ack_path=ack_path,
        ack_root=ack_root,
        duplicate_policy="fail_all",
        invalid_dag_policy="fail_all",
        semantic_refresh_callables=build_semantic_refresh_airflow_callables(
            runtime=runtime,
            worker=worker,
        ),
    )


def _digest(value: object, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or not value.startswith("sha256:")
        or len(value) != 71
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise ValueError(f"{field_name} must be a canonical lowercase sha256 digest")
    return value


__all__ = [
    "SemanticRefreshAirflowWorkerAuthority",
    "SemanticRefreshAirflowRuntime",
    "SemanticRefreshDagProjectionSource",
    "SemanticRefreshPostDeploymentProjectionPort",
    "SemanticRefreshVerifiedDagSidecarFactory",
    "build_semantic_refresh_airflow_callables",
    "load_verified_semantic_refresh_airflow_dags",
    "materialize_verified_semantic_refresh_dag",
]
