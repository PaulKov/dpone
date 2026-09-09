"""Route-aware MSSQL state composition for runtime hydration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dpone.runtime.errors import RuntimeConfigurationError
from dpone.runtime.postgres_xmin_execution import (
    PostgresXminExecutionMode,
    postgres_xmin_execution_policy,
)
from dpone.runtime.sinks.mssql_transaction_requirement import is_snapshot_envelope_route

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig
    from dpone.config.state import ResolvedMssqlStateConfig


def is_generic_mssql_transaction_route(*, sink_type: str | None, load_config: LoadConfig) -> bool:
    """Select generic governance for every non-key-snapshot MSSQL sink route."""

    return str(sink_type or "").strip().lower() == "mssql" and not is_snapshot_envelope_route(load_config)


def require_generic_mssql_state_policy(
    *,
    generic_route: bool,
    state_type: str,
    atomicity: str | None = None,
    provisioning: str | None = None,
) -> None:
    """Fail during composition when generic governance cannot be target-atomic."""

    if not generic_route:
        return
    if state_type != "mssql":
        raise RuntimeConfigurationError("Governed MSSQL routes require state.type=mssql.")
    if atomicity is not None and atomicity != "target_atomic":
        raise RuntimeConfigurationError("Governed MSSQL routes require state.atomicity=target_atomic.")
    if provisioning is not None and provisioning != "external":
        raise RuntimeConfigurationError("Governed MSSQL routes require state.provisioning=external.")


def build_mssql_route_state_storage(
    *,
    state_factory: Any,
    connector: Any,
    location: ResolvedMssqlStateConfig,
    generic_route: bool,
) -> Any:
    """Build either generic governance state or the XMin snapshot state store."""

    require_generic_mssql_state_policy(
        generic_route=generic_route,
        state_type="mssql",
        atomicity=location.atomicity,
        provisioning=location.provisioning,
    )
    if generic_route:
        return state_factory.create_mssql_generic_transaction_state_storage(
            mssql_connector=connector,
            database=location.location.database,
            schema=location.location.schema,
        )
    return state_factory.create_mssql_xmin_state_storage(
        mssql_connector=connector,
        state_table=location.location.table,
        schema=location.location.schema,
        database=location.location.database,
        receipt_table=location.location.receipt_table,
        repair_authority_table=location.location.repair_authority_table,
        repair_consumption_table=location.location.repair_consumption_table,
        run_table=location.run_table,
        audit_table=location.audit_table,
        atomicity=location.atomicity,
        provisioning=location.provisioning,
    )


def build_mssql_xmin_handoff_state_storage(
    *,
    state_factory: Any,
    connector: Any,
    location: ResolvedMssqlStateConfig,
    load_config: LoadConfig,
    generic_route: bool,
) -> Any | None:
    """Add the six-object XMin authority only for an explicit initial campaign."""

    policy = postgres_xmin_execution_policy(getattr(load_config, "options", None))
    if not generic_route or policy.mode is not PostgresXminExecutionMode.INITIAL:
        return None
    return state_factory.create_mssql_xmin_state_storage(
        mssql_connector=connector,
        state_table=location.location.table,
        schema=location.location.schema,
        database=location.location.database,
        receipt_table=location.location.receipt_table,
        repair_authority_table=location.location.repair_authority_table,
        repair_consumption_table=location.location.repair_consumption_table,
        run_table=location.run_table,
        audit_table=location.audit_table,
        atomicity=location.atomicity,
        provisioning=location.provisioning,
    )


def build_mssql_route_state_storages(
    *,
    state_factory: Any,
    connector: Any,
    location: ResolvedMssqlStateConfig,
    load_config: LoadConfig,
    generic_route: bool,
) -> tuple[Any, Any | None]:
    """Build the route's primary store and optional initial handoff store."""

    return (
        build_mssql_route_state_storage(
            state_factory=state_factory,
            connector=connector,
            location=location,
            generic_route=generic_route,
        ),
        build_mssql_xmin_handoff_state_storage(
            state_factory=state_factory,
            connector=connector,
            location=location,
            load_config=load_config,
            generic_route=generic_route,
        ),
    )


__all__ = [
    "build_mssql_route_state_storage",
    "build_mssql_route_state_storages",
    "build_mssql_xmin_handoff_state_storage",
    "is_generic_mssql_transaction_route",
    "require_generic_mssql_state_policy",
]
