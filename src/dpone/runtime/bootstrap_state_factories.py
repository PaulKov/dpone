"""Connector-specific state service factories for runtime bootstrap."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Protocol

from dpone.runtime.errors import RuntimeConfigurationError

if TYPE_CHECKING:
    from dpone.config.state import ResolvedMssqlStateConfig


class _RuntimeStateBindingsPort(Protocol):
    """Structural view needed by connector-specific state factories."""

    @property
    def state_type(self) -> str: ...

    @property
    def shared_bq_connector(self) -> Any: ...

    @property
    def shared_mssql_state_connector(self) -> Any: ...

    @property
    def shared_postgres_state_connector(self) -> Any: ...

    @property
    def mssql_state_location(self) -> Any: ...

    @property
    def xmin_state_storage(self) -> Any: ...


def build_run_state_storage(
    *,
    state_factory: Any,
    state_bindings: _RuntimeStateBindingsPort,
    state_cfg: Mapping[str, Any],
    sink_obj: Any,
) -> Any:
    """Build the backend-specific run ledger from resolved state bindings."""

    if state_bindings.state_type == "disabled":
        return None
    if state_bindings.state_type == "mssql" and state_bindings.shared_mssql_state_connector is not None:
        from dpone.runtime.state.mssql_generic_transaction_storage import (
            is_mssql_generic_transaction_storage,
        )

        if is_mssql_generic_transaction_storage(state_bindings.xmin_state_storage):
            return None
        location = state_bindings.mssql_state_location
        if location is None:
            raise RuntimeConfigurationError("Resolved MSSQL state location is missing")
        process_scoped = location.atomicity == "target_atomic"
        storage = state_factory.create_mssql_run_state_storage(
            mssql_connector=state_bindings.shared_mssql_state_connector,
            state_table=location.run_table,
            schema=location.location.schema,
            database=location.location.database,
            provisioning=location.provisioning,
            identity_policy=("process_scoped_v2_required" if process_scoped else "legacy_compatible"),
        )
        if process_scoped:
            storage.create_state_table()
        return storage
    if state_bindings.state_type == "postgres" and state_bindings.shared_postgres_state_connector is not None:
        state_table_cfg = state_cfg.get("run_table") or state_cfg.get("table", {})
        return state_factory.create_postgres_run_state_storage(
            postgres_connector=state_bindings.shared_postgres_state_connector,
            state_table=state_table_cfg.get("run_name", "etl_run_state"),
            schema=state_table_cfg.get("schema", "etl_state"),
        )
    if sink_obj and hasattr(sink_obj, "connector"):
        try:
            return state_factory.create_run_state_storage(
                bigquery_connector=state_bindings.shared_bq_connector,
                state_table="etl_run_state",
                schema="etl_state",
            )
        except Exception:
            return None
    return None


def build_mssql_state_connector(
    *,
    state_factory: Any,
    connection_id: str,
    state_credentials_source: Any,
    mount_point: str,
    vault_path: str,
) -> Any:
    """Build an MSSQL state connector from prevalidated bootstrap inputs."""

    return state_factory.create_mssql_connector(
        connection_id=connection_id,
        credentials_source=state_credentials_source,
        mount_point=mount_point,
        path=vault_path,
    )


def build_postgres_state_connector(
    *,
    state_factory: Any,
    connection_id: str,
    state_credentials_source: Any,
    mount_point: str,
    vault_path: str,
) -> Any:
    """Build a PostgreSQL state connector from prevalidated bootstrap inputs."""

    return state_factory.create_postgres_connector(
        connection_id=connection_id,
        credentials_source=state_credentials_source,
        mount_point=mount_point,
        path=vault_path,
    )


def build_kafka_offset_state_storage(
    *,
    state_factory: Any,
    state_type: str,
    state_cfg: Mapping[str, Any],
    shared_bq_connector: Any,
    shared_mssql_state_connector: Any,
    shared_postgres_state_connector: Any,
) -> Any:
    """Build the Kafka offset ledger on the selected shared state backend."""

    kafka_state_table_cfg = state_cfg.get("kafka_table") or state_cfg.get("table", {})
    kafka_state_table = kafka_state_table_cfg.get("kafka_name", "etl_kafka_offsets")
    kafka_state_schema = kafka_state_table_cfg.get("schema", "etl_state")
    if state_type == "mssql" and shared_mssql_state_connector is not None:
        return state_factory.create_mssql_kafka_offset_state_storage(
            mssql_connector=shared_mssql_state_connector,
            state_table=kafka_state_table,
            schema=kafka_state_schema,
        )
    if state_type == "postgres" and shared_postgres_state_connector is not None:
        return state_factory.create_postgres_kafka_offset_state_storage(
            postgres_connector=shared_postgres_state_connector,
            state_table=kafka_state_table,
            schema=kafka_state_schema,
        )
    if shared_bq_connector is not None:
        return state_factory.create_bigquery_kafka_offset_state_storage(
            bigquery_connector=shared_bq_connector,
            state_table=kafka_state_table,
            schema=kafka_state_schema,
        )
    return None


def build_mssql_load_audit_storage(
    state_factory: Any,
    connector: Any,
    location: ResolvedMssqlStateConfig,
) -> Any:
    """Resolve and preflight the canonical three-part MSSQL audit table."""

    storage = state_factory.create_mssql_load_audit_storage(
        mssql_connector=connector,
        state_table=location.audit_table,
        schema=location.location.schema,
        database=location.location.database,
        provisioning=location.provisioning,
    )
    storage.create_load_table()
    return storage


__all__ = [
    "build_kafka_offset_state_storage",
    "build_mssql_load_audit_storage",
    "build_mssql_state_connector",
    "build_postgres_state_connector",
    "build_run_state_storage",
]
