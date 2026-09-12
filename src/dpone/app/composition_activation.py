"""Production composition root for complete-parent deployment activation."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.adapters.composition_clickhouse_enrollment import ClickHouseCompositionEnrollmentReader
from dpone.adapters.composition_mssql_enrollment import MssqlCompositionEnrollmentReader
from dpone.app.composition_activation_inputs import DeploymentCacheCompositionActivationInputs
from dpone.app.composition_authority_connections import CompositionAuthorityConnections
from dpone.app.composition_clickhouse_admission import require_protected_clickhouse_enrollment
from dpone.app.composition_execution_cells import (
    InstalledCompositionExecutionCapabilities,
    build_installed_execution_capabilities,
)
from dpone.app.composition_materialization_seams import bind_composition_materialization_seams
from dpone.app.composition_physical_backend import CompositionProtectedPhysicalBackend
from dpone.app.composition_store_factory import CompositionMssqlStoreFactory
from dpone.app.dbt_workspace_runtime_resolver import DbtWorkspaceRuntimeResolverFactory
from dpone.app.release_composition import build_composition_source_reader
from dpone.contracts.composition_activation import CompositionOccurrenceContext
from dpone.contracts.composition_physical import CompositionPhysicalDomain
from dpone.runtime.dbt_workspace_mssql_observer import MssqlWorkspaceCatalogObserver
from dpone.services.composition_activation_coordinator import (
    CompositionActivationCoordinator as _ServiceCoordinator,
)
from dpone.services.composition_activation_coordinator import CompositionActivationPreparation
from dpone.services.composition_physical_admission import CompositionPhysicalAdmissionService

if TYPE_CHECKING:
    from dpone.app.composition_materialization_seams import RequireTargetService
    from dpone.ports.dbt_workspace_activation import DbtWorkspaceRuntimeResolverFactory as RuntimeResolverFactory

ClickHouseEnrollmentVerifier = Callable[
    [CompositionPhysicalDomain, CompositionOccurrenceContext],
    None,
]


class CompositionActivationCoordinator(_ServiceCoordinator):
    """Service coordinator plus the callable factories for every installed cell."""

    def __init__(
        self,
        *,
        execution_capabilities: InstalledCompositionExecutionCapabilities,
        require_target_service: RequireTargetService,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._execution_capabilities = execution_capabilities
        self._require_target_service = require_target_service

    @property
    def execution_cells(self) -> frozenset[str]:
        return self._execution_capabilities.execution_cells

    def execution_factory(self, cell: str) -> Any:
        return self._execution_capabilities.factory(cell)

    def materialization_seams(self, **kwargs: Any) -> Any:
        kwargs.setdefault("require_target_service", self._require_target_service)
        return bind_composition_materialization_seams(**kwargs)


def build_composition_activation_coordinator(
    *,
    cache_root: str | Path,
    authority_connection_ref: str,
    control_schema: str = "dpone_control",
) -> CompositionActivationCoordinator:
    """Build lazy, credential-safe parent admission for deployment promotion.

    The public factory keeps the approved three-parameter signature. Production
    construction always installs the protected ClickHouse enrollment callback
    and binds target-service authority into materialization seams.
    """

    return _compose_composition_activation_coordinator(
        cache_root=cache_root,
        authority_connection_ref=authority_connection_ref,
        control_schema=control_schema,
    )


def _compose_composition_activation_coordinator(
    *,
    cache_root: str | Path,
    authority_connection_ref: str,
    control_schema: str = "dpone_control",
    resolver_factory: RuntimeResolverFactory | None = None,
    require_clickhouse_enrollment: ClickHouseEnrollmentVerifier | None = None,
    require_mssql_target_service: RequireTargetService | None = None,
) -> CompositionActivationCoordinator:
    """Package-private builder for production wiring and offline test doubles."""

    inputs = DeploymentCacheCompositionActivationInputs(
        cache_root=Path(cache_root),
        source_reader=build_composition_source_reader(),
        resolver_factory=resolver_factory or DbtWorkspaceRuntimeResolverFactory(),
    )
    connections = CompositionAuthorityConnections(
        inputs=inputs,
        authority_connection_ref=authority_connection_ref,
        control_schema=control_schema,
    )
    capabilities = build_installed_execution_capabilities()
    clickhouse_verifier = require_clickhouse_enrollment or (
        lambda domain, context: require_protected_clickhouse_enrollment(
            domain,
            context,
            connections=connections,
            control_schema=control_schema,
        )
    )
    target_service = require_mssql_target_service or connections.require_mssql_target_service
    backend = CompositionProtectedPhysicalBackend(
        inputs=inputs,
        execution_capabilities=capabilities,
        mssql_enrollment=MssqlCompositionEnrollmentReader(
            connection_factory=connections.control_connection,
            require_target_service=connections.require_mssql_target_service,
            control_schema=control_schema,
        ),
        clickhouse_enrollment=ClickHouseCompositionEnrollmentReader(
            query=connections.query_clickhouse,
            require_enrollment=clickhouse_verifier,
        ),
        mssql_observer=MssqlWorkspaceCatalogObserver(),
    )
    return CompositionActivationCoordinator(
        execution_capabilities=capabilities,
        require_target_service=target_service,
        inputs=inputs,
        preparation=CompositionActivationPreparation(physical=CompositionPhysicalAdmissionService(backend=backend)),
        stores=CompositionMssqlStoreFactory(
            inputs=inputs,
            authority_connection_ref=authority_connection_ref,
            control_schema=control_schema,
        ),
    )


__all__ = [
    "CompositionActivationCoordinator",
    "InstalledCompositionExecutionCapabilities",
    "build_composition_activation_coordinator",
]
