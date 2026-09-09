"""Compose stateless workspace admission from a protected control binding."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dpone.adapters.dbt_workspace_mssql_activation_admission import MssqlDbtWorkspaceActivationAdmission
from dpone.contracts.dbt_workspace_control import require_workspace_authority_connection_ref
from dpone.runtime.credentials.resolved_connector_factory import ResolvedConnectorFactory

if TYPE_CHECKING:
    from dpone.ports.dbt_workspace_activation import DbtWorkspaceConnectionResolver


class DbtWorkspaceMssqlAdmissionFactory:
    """Rebuild independent SQL Server sessions from one logical authority ref."""

    def __init__(self, authority_connection_ref: str, *, control_schema: str = "dpone_control") -> None:
        self._authority_connection_ref = require_workspace_authority_connection_ref(authority_connection_ref)
        self._control_schema = control_schema

    def build(self, resolver: DbtWorkspaceConnectionResolver) -> MssqlDbtWorkspaceActivationAdmission:
        """Resolve through the sealed deployment and return a fresh durable adapter."""

        resolved = resolver.resolve(self._authority_connection_ref)
        descriptor = resolved.descriptor
        if descriptor is None or descriptor.connection_type != "mssql":
            raise ValueError("workspace authority connection must resolve to SQL Server")

        def connection_factory() -> Any:
            connector = ResolvedConnectorFactory.create(resolved, autocommit=False)
            return connector.connection

        return MssqlDbtWorkspaceActivationAdmission(
            connection_factory,
            control_schema=self._control_schema,
        )


__all__ = ["DbtWorkspaceMssqlAdmissionFactory"]
