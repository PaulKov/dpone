"""Complete production application root for semantic-refresh Airflow DAGs."""

from __future__ import annotations

from collections.abc import Callable, MutableMapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.adapters.semantic_refresh_clickhouse_connection import (
    ClickHouseConnectionAuthorityVerifier,
)
from dpone.adapters.semantic_refresh_clickhouse_quiescence import (
    ClickHouseHttpAttemptQuiescenceObserver,
)
from dpone.adapters.semantic_refresh_mssql_run_authority import (
    MssqlSemanticRefreshWorkerRunAuthority,
)
from dpone.adapters.semantic_refresh_mssql_workflow_publication import (
    MssqlSemanticRefreshWorkflowPublicationReader,
)
from dpone.adapters.semantic_refresh_mssql_workflow_summary import (
    MssqlSemanticRefreshWorkflowSummaryState,
)
from dpone.app.semantic_refresh_airflow_composition import (
    SemanticRefreshAirflowRuntime,
    load_verified_semantic_refresh_airflow_dags,
)
from dpone.app.semantic_refresh_composition import (
    build_semantic_refresh_publication_runtime,
)
from dpone.app.semantic_refresh_worker_composition import (
    build_semantic_refresh_airflow_worker_runtime,
)

if TYPE_CHECKING:
    from dpone.adapters.semantic_refresh_clickhouse_http_client import (
        ClickHousePublicationHttpClient,
    )
    from dpone.app.semantic_refresh_worker_composition import (
        SemanticRefreshAirflowWorkerRuntime,
    )
    from dpone.contracts.dbt_semantic_refresh_run_contracts import (
        SemanticRefreshRunAdmissionVerifierPort,
    )
    from dpone.ports.semantic_refresh_artifact_store import (
        SemanticRefreshArtifactStoreResolver,
    )
    from dpone.ports.semantic_refresh_clickhouse_connection import (
        ClickHouseClusterConnectionAuthority,
    )
    from dpone.ports.semantic_refresh_seal_policy import (
        SemanticRefreshSealPolicyAuthorityPort,
    )
    from dpone.readiness.dbt_semantic_refresh_runtime_proof import (
        SemanticRefreshImmutableProofRechecker,
    )


@dataclass(frozen=True, slots=True)
class SemanticRefreshAirflowApplication:
    """One correlated parse-inert graph for scheduler and worker boundaries."""

    runtime: SemanticRefreshAirflowRuntime
    worker: SemanticRefreshAirflowWorkerRuntime

    def load_dags(
        self,
        globals_dict: MutableMapping[str, object],
        *,
        index_path: str | Path,
        ack_path: str | Path,
        ack_root: str | Path | None = None,
    ) -> object:
        """Load and acknowledge one strict index with the protected worker graph."""

        return load_verified_semantic_refresh_airflow_dags(
            globals_dict,
            index_path=index_path,
            ack_path=ack_path,
            ack_root=ack_root,
            runtime=self.runtime,
            worker=self.worker,
        )


def build_semantic_refresh_airflow_application(
    *,
    mssql_connection_factory: Callable[[], Any],
    mssql_connection_authority_id: str,
    authority_store_ref: str,
    artifact_stores: SemanticRefreshArtifactStoreResolver,
    seal_policy: SemanticRefreshSealPolicyAuthorityPort,
    clickhouse_http_client: ClickHousePublicationHttpClient,
    clickhouse_connection_authority: ClickHouseClusterConnectionAuthority,
    run_admission_verifier: SemanticRefreshRunAdmissionVerifierPort,
    immutable_proof_rechecker: SemanticRefreshImmutableProofRechecker,
    package_source_root: Path,
    pod_identity_root: Path,
    pod_uid_relative_path: str,
    clock: Callable[[], datetime],
    control_schema: str = "dpone_control",
) -> SemanticRefreshAirflowApplication:
    """Compose every V2 task from injected protected capabilities.

    Construction is deliberately I/O-free. Connection factories, clients and
    proof authorities are invoked only at worker execution time.
    """

    publication = build_semantic_refresh_publication_runtime(
        mssql_connection_factory=mssql_connection_factory,
        mssql_connection_authority_id=mssql_connection_authority_id,
        artifact_stores=artifact_stores,
        seal_policy=seal_policy,
        clickhouse_http_client=clickhouse_http_client,
        clickhouse_connection_authority=clickhouse_connection_authority,
        now=clock,
        control_schema=control_schema,
    )
    runtime = SemanticRefreshAirflowRuntime(
        publication=publication,
        workflow_publications=MssqlSemanticRefreshWorkflowPublicationReader(
            mssql_connection_factory,
            control_schema=control_schema,
        ),
        workflow_summary=MssqlSemanticRefreshWorkflowSummaryState(
            mssql_connection_factory,
            control_schema=control_schema,
        ),
    )
    worker = build_semantic_refresh_airflow_worker_runtime(
        mssql_connection_factory=mssql_connection_factory,
        authority_store_ref=authority_store_ref,
        run_admission_verifier=run_admission_verifier,
        immutable_proof_rechecker=immutable_proof_rechecker,
        run_authority=MssqlSemanticRefreshWorkerRunAuthority(
            mssql_connection_factory,
            control_schema=control_schema,
        ),
        package_source_root=package_source_root,
        pod_identity_root=pod_identity_root,
        pod_uid_relative_path=pod_uid_relative_path,
        clock=clock,
        clickhouse_quiescence=ClickHouseHttpAttemptQuiescenceObserver(
            client=clickhouse_http_client,
            connection=ClickHouseConnectionAuthorityVerifier(
                client=clickhouse_http_client,
                authority=clickhouse_connection_authority,
            ),
        ),
        control_schema=control_schema,
    )
    return SemanticRefreshAirflowApplication(runtime=runtime, worker=worker)


__all__ = [
    "SemanticRefreshAirflowApplication",
    "build_semantic_refresh_airflow_application",
]
