"""Read one pinned durable occurrence without selecting the current deployment."""

from __future__ import annotations

from collections.abc import Callable

from dpone.adapters.dbapi_lifecycle import close, rollback, row
from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.contracts.airflow_run_identity import AirflowDeploymentIdentity
from dpone.contracts.dbt_workspace_runtime_authority import DbtWorkspaceRuntimeAuthority
from dpone.contracts.mssql_object_name import native_control_schema
from dpone.ports.sql_connection import SqlControlConnection


class MssqlNativeActivationLookup:
    """Recover only recorded previous-deployment coordinates for exact admission.

    The injected factory must use bounded control connection/statement timeouts.
    This read does not grant execution or prove guard ownership. The application
    must follow it with the existing coordinator's full require_active readback.
    """

    def __init__(self, connection_factory: Callable[[], SqlControlConnection], *, control_schema: str) -> None:
        self._connect = connection_factory
        self._schema = native_control_schema(control_schema)

    def previous_deployment(
        self,
        *,
        identity: AirflowDeploymentIdentity,
        authority: DbtWorkspaceRuntimeAuthority,
        source_inventory_sha256: str,
    ) -> str | None:
        """Read the pinned UUID; absent, foreign and inactive rows fail closed."""
        if type(identity) is not AirflowDeploymentIdentity or type(authority) is not DbtWorkspaceRuntimeAuthority:
            raise ValueError("native activation requires exact launch and authority contracts")
        identity = AirflowDeploymentIdentity.from_mapping(identity.to_dict())
        authority.__post_init__()
        if (identity.release_id, identity.deployment_id) != (
            authority.release_id,
            authority.deployment_id,
        ) or not is_canonical_sha256_digest(source_inventory_sha256):
            raise ValueError("native activation coordinates differ from the verified authority")
        connection = cursor = None
        try:
            connection = self._connect()
            cursor = connection.cursor()
            cursor.execute(
                f"SELECT TOP (2) activation_id, request_sha256, environment, release_id, deployment_id, "
                f"previous_deployment_id, source_inventory_sha256, runtime_context_sha256, state "
                f"FROM [{self._schema}].[dbt_workspace_activations] WHERE activation_id = ?;",
                identity.activation_id,
            )
            observed = row(cursor)
            if observed is None or len(observed) != 9 or row(cursor) is not None:
                raise ValueError("native activation occurrence is missing, incomplete or ambiguous")
            previous = observed[5]
            if not is_canonical_sha256_digest(observed[1]) or (
                previous is not None and not is_canonical_sha256_digest(previous)
            ):
                raise ValueError("native activation occurrence contains invalid identity coordinates")
            if observed != (
                identity.activation_id,
                observed[1],
                authority.environment,
                identity.release_id,
                identity.deployment_id,
                previous,
                source_inventory_sha256,
                authority.authority_subject_sha256,
                "ACTIVE",
            ):
                raise ValueError("native activation occurrence is foreign or no longer active")
            return previous
        finally:
            close(cursor)
            rollback(connection)
            close(connection)
