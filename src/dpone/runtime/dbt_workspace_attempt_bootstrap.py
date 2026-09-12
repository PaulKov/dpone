"""Runtime composition of protected dbt workspace task-attempt fencing."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.adapters.dbt_workspace_attempt_request import DbtWorkspaceAttemptRequestFactory
from dpone.adapters.dbt_workspace_mssql_attempt_admission import MssqlDbtWorkspaceAttemptAdmission
from dpone.contracts.dbt_runtime import (
    AIRFLOW_DEPLOYMENT_IDENTITY_ENV,
    DBT_EXECUTION_PACK_SCHEMA_V2,
    DBT_WORKSPACE_AUTHORITY_CONNECTION_REF_ENV,
    AirflowDeploymentIdentity,
    AirflowRunIdentity,
    DbtExecutionPack,
    DbtPublishingError,
    parse_airflow_deployment_identity_json,
    require_workspace_authority_connection_ref,
    required_runtime_environment,
)
from dpone.runtime.credentials.resolved_connector_factory import ResolvedConnectorFactory


def workspace_attempt_dependencies(
    pack: DbtExecutionPack,
    *,
    environment: Mapping[str, str],
    run_identity: AirflowRunIdentity,
    resolver: Any,
) -> tuple[DbtWorkspaceAttemptRequestFactory | None, MssqlDbtWorkspaceAttemptAdmission | None]:
    """Compose V2 fencing from exact deployment identity and control binding."""

    if pack.schema != DBT_EXECUTION_PACK_SCHEMA_V2:
        return None, None
    deployment_identity = _deployment_identity(environment, run_identity)
    try:
        authority_connection_ref = require_workspace_authority_connection_ref(
            environment.get(DBT_WORKSPACE_AUTHORITY_CONNECTION_REF_ENV)
        )
    except ValueError as exc:
        raise DbtPublishingError(
            "DPONE_DBT_WORKSPACE_ADMISSION_UNAVAILABLE",
            "A projected workspace authority connection_ref is required",
        ) from exc
    if authority_connection_ref == pack.profile.connection_ref:
        raise DbtPublishingError(
            "DPONE_DBT_WORKSPACE_ADMISSION_UNAVAILABLE",
            "Workspace authority and dbt target connection_ref must be distinct",
        )
    resolved = resolver.resolve(authority_connection_ref)

    def connection_factory() -> Any:
        connector = ResolvedConnectorFactory.create(resolved, autocommit=False)
        return connector.connection

    return (
        DbtWorkspaceAttemptRequestFactory(deployment_identity),
        MssqlDbtWorkspaceAttemptAdmission(connection_factory),
    )


def _deployment_identity(
    environment: Mapping[str, str],
    run_identity: AirflowRunIdentity,
) -> AirflowDeploymentIdentity:
    raw_identity = str(environment.get(AIRFLOW_DEPLOYMENT_IDENTITY_ENV) or "")
    if not raw_identity:
        raise DbtPublishingError(
            "DPONE_DBT_WORKSPACE_ADMISSION_UNAVAILABLE",
            "Airflow deployment activation identity is required for workspace execution",
        )
    try:
        identity = parse_airflow_deployment_identity_json(raw_identity)
    except ValueError as exc:
        raise DbtPublishingError(
            "DPONE_DBT_WORKSPACE_ADMISSION_UNAVAILABLE",
            "Airflow deployment activation identity is invalid",
        ) from exc
    if (identity.release_id, identity.deployment_id) != (
        run_identity.release_id,
        run_identity.deployment_id,
    ):
        raise DbtPublishingError(
            "DPONE_DBT_WORKSPACE_ADMISSION_UNAVAILABLE",
            "Airflow deployment activation identity differs from the run identity",
        )
    return identity


__all__ = ["required_runtime_environment", "workspace_attempt_dependencies"]
