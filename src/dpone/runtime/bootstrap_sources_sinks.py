"""Runtime source and sink builders used by the default hydrator."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.contracts.runtime_connection import ResolvedBindingConnection
    from dpone.runtime.storage_policy import RuntimeStoragePolicy


from collections.abc import Mapping
from typing import Any

from dpone.config.env import ENV_CODE
from dpone.contracts.api_sources import get_api_source_defaults
from dpone.runtime.errors import RuntimeConfigurationError


class RuntimeEndpointFactory:
    """Build source and sink runtime endpoints from manifest configuration."""

    @staticmethod
    def build_source_resolved(
        source_cfg: Mapping[str, Any],
        connection: ResolvedBindingConnection | None,
        xmin_state_storage: Any,
        *,
        sink_connector: Any = None,
    ) -> Any:
        """Build a source from the composition root's resolved snapshot."""

        if not source_cfg:
            raise RuntimeConfigurationError("Не задан блок source")
        if str(source_cfg.get("type") or "").lower() == "api":
            from dpone.runtime.api_registry import (
                build_api_runtime_source_from_connection,
            )
            from dpone.runtime.process_logging import etl_logger

            return build_api_runtime_source_from_connection(
                source_cfg=source_cfg,
                resolved_connection=connection,
                sink_connector=sink_connector,
                logger=etl_logger,
            )
        if connection is None:
            raise RuntimeConfigurationError("Resolved source connection is required for strict runtime.")
        from dpone.runtime.credentials.resolved_endpoint_factory import (
            ResolvedEndpointFactory,
        )

        return ResolvedEndpointFactory.create_source(
            connection,
            xmin_state_storage,
            sink_connector=sink_connector,
        )

    @staticmethod
    def build_sink_resolved(
        sink_cfg: Mapping[str, Any],
        connection: ResolvedBindingConnection | None,
        xmin_state_storage: Any,
        *,
        proxy_connection: ResolvedBindingConnection | None = None,
        shared_bq_connector: Any = None,
        runtime_storage_policy: RuntimeStoragePolicy | None = None,
    ) -> Any:
        """Build a sink without consulting an ambient credential backend."""

        if not sink_cfg:
            raise RuntimeConfigurationError("Не задан блок sink")
        if connection is None:
            raise RuntimeConfigurationError("Resolved sink connection is required for strict runtime.")
        descriptor = connection.descriptor
        if (
            descriptor is not None
            and descriptor.connection_type.strip().lower() == "bigquery"
            and shared_bq_connector is not None
        ):
            from dpone.runtime.sinks.bigquery import BigQuerySink

            return BigQuerySink(
                connector=shared_bq_connector,
                state_storage=xmin_state_storage,
            )
        from dpone.runtime.credentials.resolved_endpoint_factory import (
            ResolvedEndpointFactory,
        )

        return ResolvedEndpointFactory.create_sink(
            connection,
            xmin_state_storage,
            proxy_connection=proxy_connection,
            runtime_storage_policy=runtime_storage_policy,
        )

    @staticmethod
    def build_source(
        source_cfg: Mapping[str, Any],
        xmin_state_storage: Any,
        *,
        sink_connector: Any = None,
    ) -> Any:
        if not source_cfg:
            raise RuntimeConfigurationError("Не задан блок source")

        conn_id = source_cfg.get("connection_id")
        conn_type = source_cfg.get("type", "postgres")
        connection_type = source_cfg.get("connection_type")

        if conn_type == "api":
            api_defaults = get_api_source_defaults(source_cfg.get("api_type"))
            connection_type = connection_type or api_defaults.default_connection_type
            vault_mount_point = source_cfg.get("vault_mount_point")
            vault_path = source_cfg.get("vault_path")
            if api_defaults.credentials_mode != "none" and not conn_id:
                raise RuntimeConfigurationError(
                    "Для api source необходимо явно указать connection_id; implicit api__* defaults are disabled"
                )
            if api_defaults.credentials_mode == "vault":
                if connection_type not in ("airflow", "vault", "env", "params"):
                    raise RuntimeConfigurationError(
                        f"Неверное значение connection_type='{connection_type}' для api source '{conn_id}'. "
                        "Допустимые значения: 'airflow', 'vault', 'env' или 'params'"
                    )
                if connection_type == "vault" and not vault_path:
                    raise RuntimeConfigurationError(
                        f"Для source '{conn_id}' с connection_type='vault' необходимо указать vault_path"
                    )
                if connection_type == "vault" and not vault_mount_point:
                    raise RuntimeConfigurationError(
                        f"Для source '{conn_id}' с connection_type='vault' необходимо явно указать vault_mount_point"
                    )
            return RuntimeEndpointFactory.build_api_source(
                source_cfg={**source_cfg, "connection_id": conn_id, "connection_type": connection_type},
                vault_path=vault_path,
                vault_mount_point=vault_mount_point if vault_mount_point is not None else ENV_CODE,
                sink_connector=sink_connector,
            )

        if not conn_id:
            raise RuntimeConfigurationError("Для source необходимо указать connection_id")
        RuntimeEndpointFactory._validate_connection_type(connection_type, endpoint="source", conn_id=conn_id)

        vault_mount_point = source_cfg.get("vault_mount_point", ENV_CODE)
        vault_path = source_cfg.get("vault_path")
        if connection_type == "vault" and not vault_path:
            raise RuntimeConfigurationError(
                f"Для source '{conn_id}' с connection_type='vault' необходимо указать vault_path"
            )

        from dpone.runtime.credentials.factory import SourceFactory

        return SourceFactory.create(
            connection_id=conn_id,
            connection_type=conn_type,
            credentials_source=str(connection_type),
            mount_point=vault_mount_point,
            path=vault_path or "",
            state_storage=xmin_state_storage,
            sink_connector=sink_connector,
        )

    @staticmethod
    def build_sink(
        sink_cfg: Mapping[str, Any],
        xmin_state_storage: Any,
        *,
        shared_bq_connector: Any = None,
        proxy_config: Mapping[str, Any] | None = None,
        runtime_storage_policy: RuntimeStoragePolicy | None = None,
    ) -> Any:
        from dpone.runtime.credentials.factory import SinkFactory

        if not sink_cfg:
            raise RuntimeConfigurationError("Не задан блок sink")

        conn_id = sink_cfg.get("connection_id")
        conn_type = sink_cfg.get("type", "postgres")
        connection_type = sink_cfg.get("connection_type")
        if not conn_id:
            raise RuntimeConfigurationError("Для sink необходимо указать connection_id")
        RuntimeEndpointFactory._validate_connection_type(connection_type, endpoint="sink", conn_id=conn_id)

        vault_mount_point = sink_cfg.get("vault_mount_point", ENV_CODE)
        vault_path = sink_cfg.get("vault_path")
        if connection_type == "vault" and not vault_path:
            raise RuntimeConfigurationError(
                f"Для sink '{conn_id}' с connection_type='vault' необходимо указать vault_path"
            )

        if conn_type.lower() == "bigquery" and shared_bq_connector is not None:
            from dpone.runtime.sinks.bigquery import BigQuerySink

            return BigQuerySink(
                connector=shared_bq_connector,
                state_storage=xmin_state_storage,
            )

        proxy_config = dict(proxy_config or {})
        return SinkFactory.create(
            connection_id=conn_id,
            connection_type=conn_type,
            credentials_source=str(connection_type),
            mount_point=vault_mount_point,
            path=vault_path or "",
            state_storage=xmin_state_storage,
            proxy_enable=proxy_config.get("proxy_enable", False),
            proxy_mount_point=proxy_config.get("vault_mount_point") or "",
            proxy_path=proxy_config.get("vault_path", "network/proxy/gcp/current"),
            runtime_storage_policy=runtime_storage_policy,
        )

    @staticmethod
    def build_api_source(
        *,
        source_cfg: Mapping[str, Any],
        vault_path: str | None,
        vault_mount_point: str | None = None,
        sink_connector: Any = None,
    ) -> Any:
        from dpone.runtime.api_registry import build_api_runtime_source
        from dpone.runtime.process_logging import etl_logger

        return build_api_runtime_source(
            source_cfg=source_cfg,
            vault_path=vault_path,
            vault_mount_point=vault_mount_point,
            sink_connector=sink_connector,
            logger=etl_logger,
        )

    @staticmethod
    def _validate_connection_type(connection_type: Any, *, endpoint: str, conn_id: Any) -> None:
        if not connection_type:
            raise RuntimeConfigurationError(
                f"Для {endpoint} '{conn_id}' необходимо явно указать connection_type "
                "(возможные значения: 'airflow' или 'vault')"
            )
        if connection_type not in ("airflow", "vault", "env", "params"):
            raise RuntimeConfigurationError(
                f"Неверное значение connection_type='{connection_type}' для {endpoint} '{conn_id}'. "
                "Допустимые значения: 'airflow', 'vault', 'env' или 'params'"
            )


__all__ = ["RuntimeEndpointFactory"]
