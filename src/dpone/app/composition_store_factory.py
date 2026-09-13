"""Construct parent persistence from reopened sealed authority, without provisioning."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID

from dpone.adapters.composition_mssql_schema import require_control_schema
from dpone.adapters.composition_mssql_store import MssqlCompositionActivationStore
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.dbt_workspace_control import require_workspace_authority_connection_ref
from dpone.runtime.credentials.resolved_connector_factory import ResolvedConnectorFactory

if TYPE_CHECKING:
    from dpone.contracts.composition_activation import CompositionOccurrenceContext
    from dpone.ports.composition_activation import CompositionRuntimeInputs
    from dpone.ports.sql_connection import SqlControlConnection


class CompositionMssqlStoreFactory:
    """Rebuild phase-local connections to an explicitly enrolled control service.

    The service UUID comes from the verified registry descriptor. The SQL store
    independently checks protected enrollment on every transaction; this factory
    neither enrolls a database nor grants writer authority. Native-v2 stores are
    deliberately constructed by their separate composition root.
    """

    def __init__(
        self,
        *,
        inputs: CompositionRuntimeInputs,
        authority_connection_ref: str,
        control_schema: str = "dpone_control",
    ) -> None:
        self._inputs = inputs
        self._authority_ref = require_workspace_authority_connection_ref(authority_connection_ref)
        self._schema = require_control_schema(control_schema)

    def build(self, *, projection_root: Path, context: CompositionOccurrenceContext) -> MssqlCompositionActivationStore:
        """Reopen context before credential resolution and defer SQL until use."""
        context.__post_init__()
        checked = self._inputs.load_context(
            projection_root=projection_root,
            activation_id=context.activation_id,
            environment=context.environment,
            release_id=context.release_id,
            deployment_id=context.deployment_id,
            previous_deployment_id=context.previous_deployment_id,
        )
        if checked != context:
            raise CompositionAdmissionError("projection_context")
        resolved = self._inputs.resolve_connection(checked, self._authority_ref)
        descriptor = resolved.descriptor
        if descriptor is None or descriptor.connection_type != "mssql":
            raise CompositionAdmissionError("control_connection")
        service_id = descriptor.properties.get("composition_service_id")
        if not isinstance(service_id, str):
            raise CompositionAdmissionError("control_service_id")
        try:
            valid = str(UUID(service_id)) == service_id
        except (ValueError, AttributeError, TypeError):
            valid = False
        if not valid:
            raise CompositionAdmissionError("control_service_id")

        def connection_factory() -> SqlControlConnection:
            connector = ResolvedConnectorFactory.create(resolved, autocommit=False)
            return connector.connection

        return MssqlCompositionActivationStore(
            connection_factory,
            expected_service_id=service_id,
            control_schema=self._schema,
        )
