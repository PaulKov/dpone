"""Production composition for stateless dbt workspace activation phases."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from dpone.adapters.dbt_workspace_mssql_physical_authority import MssqlDbtWorkspacePhysicalAuthority
from dpone.app.dbt_workspace_mssql_admission_factory import DbtWorkspaceMssqlAdmissionFactory
from dpone.app.dbt_workspace_runtime_resolver import DbtWorkspaceRuntimeResolverFactory
from dpone.app.deployment_cache_workspace_activation_inputs import DeploymentCacheWorkspaceActivationInputs
from dpone.runtime.dbt_workspace_mssql_observer import MssqlWorkspaceCatalogObserver
from dpone.services.dbt_workspace_activation_coordinator import DbtWorkspaceActivationCoordinator
from dpone.services.dbt_workspace_activation_preparation import DbtWorkspaceActivationPreparation

if TYPE_CHECKING:
    from dpone.ports.dbt_workspace_activation import DbtWorkspaceActivationInputPort


def build_dbt_workspace_activation_coordinator(
    *,
    inputs: DbtWorkspaceActivationInputPort,
    authority_connection_ref: str,
    control_schema: str = "dpone_control",
) -> DbtWorkspaceActivationCoordinator:
    """Compose read-only physical proof and durable shared-guard admission."""

    return DbtWorkspaceActivationCoordinator(
        inputs=inputs,
        preparation=DbtWorkspaceActivationPreparation(
            physical_authority=MssqlDbtWorkspacePhysicalAuthority(),
            mssql_observer=MssqlWorkspaceCatalogObserver(),
        ),
        admission_factory=DbtWorkspaceMssqlAdmissionFactory(
            authority_connection_ref,
            control_schema=control_schema,
        ),
    )


def build_deployment_cache_workspace_activation_coordinator(
    *,
    cache_root: Path,
    authority_connection_ref: str,
    control_schema: str = "dpone_control",
) -> DbtWorkspaceActivationCoordinator:
    """Compose production cache inputs without retaining phase-local connections."""

    from dpone.app.dbt_promotion_composition import build_dbt_release_source_reader

    return build_dbt_workspace_activation_coordinator(
        inputs=DeploymentCacheWorkspaceActivationInputs(
            cache_root=cache_root,
            source_reader=build_dbt_release_source_reader(),
            resolver_factory=DbtWorkspaceRuntimeResolverFactory(),
        ),
        authority_connection_ref=authority_connection_ref,
        control_schema=control_schema,
    )


__all__ = [
    "build_dbt_workspace_activation_coordinator",
    "build_deployment_cache_workspace_activation_coordinator",
]
