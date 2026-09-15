"""Compose a lazy, pinned activation check after native original preflight."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

from dpone.adapters.native_activation_lookup import MssqlNativeActivationLookup
from dpone.contracts.airflow_run_identity import AirflowDeploymentIdentity
from dpone.contracts.dbt_workspace_activation import DbtWorkspaceActiveActivation
from dpone.contracts.dbt_workspace_attempt import require_workspace_authority_connection_ref
from dpone.contracts.mssql_object_name import native_control_schema
from dpone.ports.dbt_workspace_activation import DbtWorkspaceActivationCoordinatorPort, DbtWorkspaceActivationInputPort
from dpone.runtime.credentials.resolved_connector_factory import ResolvedConnectorFactory


def build_native_originals_activation_callback(
    *,
    inputs: DbtWorkspaceActivationInputPort,
    coordinator: DbtWorkspaceActivationCoordinatorPort,
    invocation_identity: AirflowDeploymentIdentity,
    projection_root: Path,
    environment: str,
    authority_connection_ref: str,
    control_schema: str,
    connect_timeout_seconds: int,
    statement_timeout_seconds: int,
) -> Callable[[], DbtWorkspaceActiveActivation]:
    """Bind launch coordinates without opening files, resolving secrets or SQL.

    Use the SAME input instance used to construct the existing coordinator and
    native verifier. Only the verifier calls the returned capability, after its
    complete policy/source preflight. Timeouts bound the new occurrence lookup;
    the existing coordinator owns its independent physical/guard readback.
    """
    if type(invocation_identity) is not AirflowDeploymentIdentity:
        raise ValueError("native activation requires an exact pinned deployment identity")
    identity = AirflowDeploymentIdentity.from_mapping(invocation_identity.to_dict())
    if not isinstance(projection_root, Path) or not projection_root.is_absolute() or ".." in projection_root.parts:
        raise ValueError("native activation projection root must be absolute without traversal")
    reference = require_workspace_authority_connection_ref(authority_connection_ref)
    schema = native_control_schema(control_schema)
    for seconds in (connect_timeout_seconds, statement_timeout_seconds):
        if type(seconds) is not int or not 1 <= seconds <= 2147483647:
            raise ValueError("native activation lookup timeouts must be explicit positive integers")

    def require_active() -> DbtWorkspaceActiveActivation:
        sources = inputs.load_sources(projection_root=projection_root, release_id=identity.release_id)
        authority = inputs.load_runtime_authority(
            projection_root=projection_root,
            environment=environment,
            release_id=identity.release_id,
            deployment_id=identity.deployment_id,
        )
        authority.__post_init__()
        if (sources.release_id, authority.release_id, authority.deployment_id, authority.environment) != (
            identity.release_id,
            identity.release_id,
            identity.deployment_id,
            environment,
        ):
            raise ValueError("native activation inputs differ from pinned invocation coordinates")
        resolved = inputs.resolve_connection(authority, reference)
        if resolved.descriptor is None or resolved.descriptor.connection_type != "mssql":
            raise ValueError("native activation control binding must resolve to SQL Server")
        # Preserve the resolved immutable binding and copy credential options.
        # Remove timeout aliases so they cannot override these explicit bounds.
        aliases = {"connect_timeout", "login_timeout", "logintimeout", "query_timeout", "querytimeout"}
        parameters = {
            k: v for k, v in (resolved.credentials.additional_params or {}).items() if k.lower() not in aliases
        }
        bounded = replace(
            resolved,
            credentials=replace(
                resolved.credentials,
                connect_timeout=connect_timeout_seconds,
                query_timeout=statement_timeout_seconds,
                additional_params=parameters,
            ),
        )
        lookup = MssqlNativeActivationLookup(
            lambda: ResolvedConnectorFactory.create(bounded, autocommit=False).connection,
            control_schema=schema,
        )
        previous = lookup.previous_deployment(
            identity=identity,
            authority=authority,
            source_inventory_sha256=sources.inventory.snapshot_sha256,
        )
        active = coordinator.require_active(
            projection_root=projection_root,
            activation_id=identity.activation_id,
            environment=environment,
            release_id=identity.release_id,
            deployment_id=identity.deployment_id,
            previous_deployment_id=previous,
        )
        if type(active) is not DbtWorkspaceActiveActivation:
            raise ValueError("native activation requires an exact active occurrence")
        active.request.__post_init__()
        active.__post_init__()
        request = active.request
        if (
            request.activation_id,
            request.environment,
            request.release_id,
            request.deployment_id,
            request.previous_deployment_id,
            request.source_inventory_sha256,
            request.runtime_context_sha256,
        ) != (
            identity.activation_id,
            environment,
            identity.release_id,
            identity.deployment_id,
            previous,
            sources.inventory.snapshot_sha256,
            authority.authority_subject_sha256,
        ):
            raise ValueError("native activation readback differs from the preflight observation")
        return active

    return require_active
