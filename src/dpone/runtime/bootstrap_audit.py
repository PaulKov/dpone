"""Compose and admit audit stores independently of checkpoint state.

The hydrator retains ownership until all runtime objects have been built.
Successful hydration transfers the connector to normal runtime disposal;
preflight or later endpoint failures release it without a business fallback.
"""

from __future__ import annotations

from contextlib import ExitStack, suppress
from typing import TYPE_CHECKING, Any

from dpone.ports.runtime_hydrator import RuntimeAuditBindings
from dpone.runtime.etl.audit_policy import audit_policy

if TYPE_CHECKING:
    from dpone.config.audit import ResolvedMssqlAuditLocation
    from dpone.config.load_config import LoadConfig
    from dpone.runtime.credentials.authority import RuntimeResolvedConnections


def build_independent_audit_bindings(
    *,
    connections: RuntimeResolvedConnections,
    load_config: LoadConfig,
    ownership: ExitStack,
) -> RuntimeAuditBindings | None:
    """Preflight both selected tables before source/sink construction."""

    if connections.audit is None:
        return None
    if connections.audit_location is None:
        raise ValueError("Independent audit location was not resolved")
    from dpone.runtime.credentials.resolved_connector_factory import ResolvedConnectorFactory
    from dpone.runtime.state.factory import StateFactory

    connector = ResolvedConnectorFactory.create(connections.audit)
    ownership.callback(_close, connector)
    location = connections.audit_location
    loads = build_mssql_load_audit(
        StateFactory,
        connector,
        database=location.database,
        schema=location.schema,
        table=location.loads_table,
        provisioning=location.provisioning,
    )
    steps = build_mssql_step_audit(connector, location=location) if audit_policy(load_config).enabled else None
    return RuntimeAuditBindings(loads=loads, steps=steps, connector=connector)


def build_mssql_load_audit(
    state_factory: Any,
    connector: Any,
    *,
    database: str,
    schema: str,
    table: str,
    provisioning: str,
) -> Any:
    """Build and preflight the canonical load identity ledger at explicit coordinates."""

    storage = state_factory.create_mssql_load_audit_storage(
        mssql_connector=connector,
        state_table=table,
        schema=schema,
        database=database,
        provisioning=provisioning,
    )
    storage.create_load_table()
    return storage


def build_mssql_step_audit(connector: Any, *, location: ResolvedMssqlAuditLocation) -> Any:
    """Build and preflight the canonical step store; no endpoint inference."""

    from dpone.runtime.state.mssql_load_step_audit import MSSQLLoadStepAuditStorage

    storage = MSSQLLoadStepAuditStorage(
        connector,
        database=location.database,
        schema=location.schema,
        table=location.steps_table,
        provisioning=location.provisioning,
    )
    storage.create_step_table()
    return storage


def _close(connector: Any) -> None:
    with suppress(Exception):
        connector.close()
