"""Runtime state-storage bootstrap wiring."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig
    from dpone.contracts.runtime_connection import ResolvedBindingConnection


from collections.abc import Mapping
from typing import Any

from dpone.config.state import (
    StateConfigError,
    resolve_mssql_state_location,
    resolve_mssql_state_location_defaults,
)
from dpone.runtime.bootstrap_checkpoints import build_sql_partition_checkpoint_store
from dpone.runtime.bootstrap_config import (
    require_state_vault_mount,
    resolve_proxy_config,
    resolve_state_vault_path,
)
from dpone.runtime.bootstrap_mssql_state import (
    build_mssql_route_state_storages,
    is_generic_mssql_transaction_route,
    require_generic_mssql_state_policy,
)
from dpone.runtime.bootstrap_state_factories import (
    build_kafka_offset_state_storage,
    build_mssql_load_audit_storage,
    build_mssql_state_connector,
    build_postgres_state_connector,
    build_run_state_storage,
)
from dpone.runtime.bootstrap_state_models import RuntimeStateBindings
from dpone.runtime.bootstrap_state_policy import (
    STATELESS_LOAD_STRATEGIES,
    legacy_state_connection_id,
    validate_disabled_state,
)
from dpone.runtime.errors import RuntimeConfigurationError
from dpone.runtime.state.mssql_generic_transaction_storage import (
    is_mssql_generic_transaction_storage,
)

_DISABLED_STATE_TYPES = frozenset({"disabled", "noop", "none", "off"})
# Backfill extraction is bounded by chunk predicates and its resume ledger is
# file-based, so it never reads or writes xmin/offset state storages.


class RuntimeStateBootstrap:
    """Build state-storage connectors and state stores for runtime hydration."""

    def build_resolved(
        self,
        *,
        state_cfg: Mapping[str, Any],
        load_config: LoadConfig,
        state_connection: ResolvedBindingConnection | None,
        proxy_connection: ResolvedBindingConnection | None = None,
        state_configured: bool = True,
        sink_type: str | None = None,
    ) -> RuntimeStateBindings:
        """Build state services from an already-resolved connection snapshot."""

        generic_mssql_route = is_generic_mssql_transaction_route(
            sink_type=sink_type,
            load_config=load_config,
        )
        if not state_configured and load_config.load_strategy in STATELESS_LOAD_STRATEGIES and not generic_mssql_route:
            return RuntimeStateBindings(
                state_type="disabled",
                proxy_config={},
                xmin_state_storage=None,
            )
        configured_type = str(state_cfg.get("type") or "").strip().lower()
        if configured_type in _DISABLED_STATE_TYPES:
            require_generic_mssql_state_policy(
                generic_route=generic_mssql_route,
                state_type="disabled",
            )
            validate_disabled_state(load_config)
            return RuntimeStateBindings(
                state_type="disabled",
                proxy_config={},
                xmin_state_storage=None,
            )
        if state_connection is None or state_connection.descriptor is None:
            raise RuntimeConfigurationError("Strict stateful runtime requires a resolved state connection.")
        state_type = configured_type or state_connection.descriptor.connection_type.strip().lower()
        require_generic_mssql_state_policy(
            generic_route=generic_mssql_route,
            state_type=state_type,
        )
        from dpone.runtime.credentials.resolved_connector_factory import (
            ResolvedConnectorFactory,
        )
        from dpone.runtime.state.factory import StateFactory

        connector = ResolvedConnectorFactory.create(
            state_connection,
            proxy_connection=proxy_connection,
        )
        state_table_cfg = state_cfg.get("table", {})
        shared_bq_connector = None
        shared_mssql_state_connector = None
        shared_postgres_state_connector = None
        partition_checkpoint_store = None
        mssql_state_location = None
        load_audit_storage = None
        xmin_handoff_state_storage = None
        if state_type == "mssql":
            shared_mssql_state_connector = connector
            try:
                mssql_state_location = resolve_mssql_state_location(state_cfg, state_connection)
            except StateConfigError as exc:
                raise RuntimeConfigurationError(str(exc)) from exc
            xmin_state_storage, xmin_handoff_state_storage = build_mssql_route_state_storages(
                state_factory=StateFactory,
                connector=connector,
                location=mssql_state_location,
                load_config=load_config,
                generic_route=generic_mssql_route,
            )
            if not is_mssql_generic_transaction_storage(xmin_state_storage):
                partition_checkpoint_store = build_sql_partition_checkpoint_store(
                    state_type=state_type,
                    connector=connector,
                    state_cfg=state_cfg,
                )
                load_audit_storage = build_mssql_load_audit_storage(
                    StateFactory,
                    connector,
                    mssql_state_location,
                )
        elif state_type == "postgres":
            shared_postgres_state_connector = connector
            xmin_state_storage = StateFactory.create_postgres_xmin_state_storage(
                postgres_connector=connector,
                state_table=state_table_cfg.get("name", "etl_xmin_state"),
                schema=state_table_cfg.get("schema", "etl_state"),
            )
            partition_checkpoint_store = build_sql_partition_checkpoint_store(
                state_type=state_type,
                connector=connector,
                state_cfg=state_cfg,
            )
        elif state_type == "bigquery":
            shared_bq_connector = connector
            xmin_state_storage = StateFactory.create_xmin_state_storage(
                bigquery_connector=connector,
                state_table=state_table_cfg.get("name", "etl_xmin_state"),
                schema=state_table_cfg.get("schema", "etl_state"),
            )
        else:
            raise RuntimeConfigurationError(f"Resolved state connection type {state_type!r} is unsupported.")
        kafka_offset_state_storage = (
            None
            if is_mssql_generic_transaction_storage(xmin_state_storage)
            else build_kafka_offset_state_storage(
                state_factory=StateFactory,
                state_type=state_type,
                state_cfg=state_cfg,
                shared_bq_connector=shared_bq_connector,
                shared_mssql_state_connector=shared_mssql_state_connector,
                shared_postgres_state_connector=shared_postgres_state_connector,
            )
        )
        return RuntimeStateBindings(
            state_type=state_type,
            proxy_config={},
            xmin_state_storage=xmin_state_storage,
            xmin_handoff_state_storage=xmin_handoff_state_storage,
            kafka_offset_state_storage=kafka_offset_state_storage,
            partition_checkpoint_store=partition_checkpoint_store,
            shared_bq_connector=shared_bq_connector,
            shared_mssql_state_connector=shared_mssql_state_connector,
            shared_postgres_state_connector=shared_postgres_state_connector,
            mssql_state_location=mssql_state_location,
            load_audit_storage=load_audit_storage,
        )

    def build(
        self,
        *,
        config: Mapping[str, Any],
        sink_cfg: Mapping[str, Any],
        state_cfg: Mapping[str, Any],
        load_config: LoadConfig,
        state_configured: bool = True,
    ) -> RuntimeStateBindings:
        proxy_config = resolve_proxy_config(config)
        generic_mssql_route = is_generic_mssql_transaction_route(
            sink_type=str(sink_cfg.get("type") or ""),
            load_config=load_config,
        )
        if not state_configured and load_config.load_strategy in STATELESS_LOAD_STRATEGIES and not generic_mssql_route:
            return RuntimeStateBindings(
                state_type="disabled",
                proxy_config=proxy_config,
                xmin_state_storage=None,
            )
        raw_state_type = state_cfg.get("type")
        if raw_state_type is None or str(raw_state_type).strip() == "":
            raise RuntimeConfigurationError(
                "state.type must be set explicitly; implicit default 'bigquery' is disabled"
            )
        state_type = str(raw_state_type).lower()
        state_credentials_source = state_cfg.get("credentials_source") or state_cfg.get("connection_type")
        if state_type not in _DISABLED_STATE_TYPES and (
            state_credentials_source is None or str(state_credentials_source).strip() == ""
        ):
            raise RuntimeConfigurationError(
                "state.credentials_source (or connection_type) must be set explicitly; "
                "implicit default 'vault' is disabled"
            )

        if state_type in _DISABLED_STATE_TYPES:
            require_generic_mssql_state_policy(
                generic_route=generic_mssql_route,
                state_type="disabled",
            )
            validate_disabled_state(load_config)
            return RuntimeStateBindings(
                state_type="disabled",
                proxy_config=proxy_config,
                xmin_state_storage=None,
            )
        require_generic_mssql_state_policy(
            generic_route=generic_mssql_route,
            state_type=state_type,
        )

        from dpone.runtime.state.factory import StateFactory

        shared_bq_connector = None
        shared_mssql_state_connector = None
        shared_postgres_state_connector = None
        partition_checkpoint_store = None
        mssql_state_location = None
        load_audit_storage = None
        xmin_handoff_state_storage = None

        if state_type == "mssql":
            connection_id = legacy_state_connection_id(state_type, state_cfg, sink_cfg)
            shared_mssql_state_connector = build_mssql_state_connector(
                state_factory=StateFactory,
                connection_id=connection_id,
                state_credentials_source=state_credentials_source,
                mount_point=require_state_vault_mount(state_cfg, state_credentials_source),
                vault_path=str(state_cfg.get("vault_path") or ""),
            )
            try:
                mssql_state_location = resolve_mssql_state_location_defaults(
                    state_cfg,
                    default_database=str(getattr(shared_mssql_state_connector, "database", "") or "") or None,
                    default_schema=None,
                )
            except StateConfigError as exc:
                raise RuntimeConfigurationError(str(exc)) from exc
            xmin_state_storage, xmin_handoff_state_storage = build_mssql_route_state_storages(
                state_factory=StateFactory,
                connector=shared_mssql_state_connector,
                location=mssql_state_location,
                load_config=load_config,
                generic_route=generic_mssql_route,
            )
            if not is_mssql_generic_transaction_storage(xmin_state_storage):
                partition_checkpoint_store = build_sql_partition_checkpoint_store(
                    state_type=state_type,
                    connector=shared_mssql_state_connector,
                    state_cfg=state_cfg,
                )
                load_audit_storage = build_mssql_load_audit_storage(
                    StateFactory,
                    shared_mssql_state_connector,
                    mssql_state_location,
                )
        elif state_type == "postgres":
            connection_id = legacy_state_connection_id(state_type, state_cfg, sink_cfg)
            shared_postgres_state_connector = build_postgres_state_connector(
                state_factory=StateFactory,
                connection_id=connection_id,
                state_credentials_source=state_credentials_source,
                mount_point=require_state_vault_mount(state_cfg, state_credentials_source),
                vault_path=str(state_cfg.get("vault_path") or ""),
            )
            state_table_cfg = state_cfg.get("table", {})
            xmin_state_storage = StateFactory.create_postgres_xmin_state_storage(
                postgres_connector=shared_postgres_state_connector,
                state_table=state_table_cfg.get("name", "etl_xmin_state"),
                schema=state_table_cfg.get("schema", "etl_state"),
            )
            partition_checkpoint_store = build_sql_partition_checkpoint_store(
                state_type=state_type,
                connector=shared_postgres_state_connector,
                state_cfg=state_cfg,
            )
        else:
            vault_mount_point, vault_path = resolve_state_vault_path(state_cfg, sink_cfg)
            shared_bq_connector = StateFactory.create_bigquery_connector(
                connection_id=load_config.target_conn_id,
                credentials_source=str(state_credentials_source),
                mount_point=vault_mount_point,
                path=vault_path,
                proxy_enable=proxy_config["proxy_enable"],
                proxy_mount_point=proxy_config["vault_mount_point"],
                proxy_path=proxy_config["vault_path"],
            )
            xmin_state_storage = StateFactory.create_xmin_state_storage(
                bigquery_connector=shared_bq_connector,
                state_table="etl_xmin_state",
                schema="etl_state",
            )

        kafka_offset_state_storage = (
            None
            if is_mssql_generic_transaction_storage(xmin_state_storage)
            else build_kafka_offset_state_storage(
                state_factory=StateFactory,
                state_type=state_type,
                state_cfg=state_cfg,
                shared_bq_connector=shared_bq_connector,
                shared_mssql_state_connector=shared_mssql_state_connector,
                shared_postgres_state_connector=shared_postgres_state_connector,
            )
        )
        return RuntimeStateBindings(
            state_type=state_type,
            proxy_config=proxy_config,
            xmin_state_storage=xmin_state_storage,
            xmin_handoff_state_storage=xmin_handoff_state_storage,
            kafka_offset_state_storage=kafka_offset_state_storage,
            partition_checkpoint_store=partition_checkpoint_store,
            shared_bq_connector=shared_bq_connector,
            shared_mssql_state_connector=shared_mssql_state_connector,
            shared_postgres_state_connector=shared_postgres_state_connector,
            mssql_state_location=mssql_state_location,
            load_audit_storage=load_audit_storage,
        )

    @staticmethod
    def build_run_state_storage(
        *, state_bindings: RuntimeStateBindings, state_cfg: Mapping[str, Any], sink_obj: Any
    ) -> Any:
        from dpone.runtime.state.factory import StateFactory

        return build_run_state_storage(
            state_factory=StateFactory,
            state_bindings=state_bindings,
            state_cfg=state_cfg,
            sink_obj=sink_obj,
        )


__all__ = ["RuntimeStateBindings", "RuntimeStateBootstrap"]
