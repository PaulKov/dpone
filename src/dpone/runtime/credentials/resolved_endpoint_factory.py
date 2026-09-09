"""Build runtime endpoints from already-resolved connection snapshots."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.contracts.runtime_connection import ResolvedBindingConnection
    from dpone.runtime.storage_policy import RuntimeStoragePolicy


from typing import Any

from dpone.runtime.connector_logging import etl_logger
from dpone.runtime.credentials.config import ConnectionType
from dpone.runtime.credentials.resolved_connector_factory import (
    ResolvedConnectorFactory,
)


class ResolvedEndpointFactory:
    """Create source and sink adapters without credential discovery."""

    @classmethod
    def create_source(
        cls,
        connection: ResolvedBindingConnection,
        state_storage: Any,
        *,
        autocommit: bool = True,
        sink_connector: Any = None,
    ) -> Any:
        """Create a source from one immutable resolved connection."""

        connection_type = cls._connection_type(connection, endpoint="source")
        connector = ResolvedConnectorFactory.create(
            connection,
            autocommit=autocommit,
        )
        if connection_type == ConnectionType.POSTGRES:
            from dpone.runtime.sources.postgres import PostgresSource

            return PostgresSource(
                connector=connector,
                state_storage=state_storage,
                logger=etl_logger,
                sink_connector=sink_connector,
            )
        if connection_type == ConnectionType.CLICKHOUSE:
            from dpone.runtime.sources import ClickHouseSource

            return ClickHouseSource(
                connector=connector,
                logger=etl_logger,
                sink_connector=sink_connector,
            )
        if connection_type == ConnectionType.MSSQL:
            from dpone.runtime.sources.mssql import MSSQLSource

            return MSSQLSource(
                connector=connector,
                logger=etl_logger,
                sink_connector=sink_connector,
            )
        if connection_type == ConnectionType.KAFKA:
            from dpone.runtime.sources.kafka import KafkaSource

            return KafkaSource(
                connector=connector,
                state_storage=state_storage,
                logger=etl_logger,
                sink_connector=sink_connector,
            )
        raise NotImplementedError(f"Unsupported resolved source type: {connection_type}")

    @classmethod
    def create_sink(
        cls,
        connection: ResolvedBindingConnection,
        state_storage: Any,
        *,
        autocommit: bool = True,
        proxy_connection: ResolvedBindingConnection | None = None,
        runtime_storage_policy: RuntimeStoragePolicy | None = None,
    ) -> Any:
        """Create a sink from one immutable resolved connection."""

        connection_type = cls._connection_type(connection, endpoint="sink")
        connector = ResolvedConnectorFactory.create(
            connection,
            autocommit=autocommit,
            proxy_connection=proxy_connection,
        )
        if connection_type == ConnectionType.POSTGRES:
            from dpone.runtime.sinks.postgres import PostgresSink

            return PostgresSink(
                connector=connector,
                state_storage=state_storage,
                logger=etl_logger,
            )
        if connection_type == ConnectionType.BIGQUERY:
            from dpone.runtime.sinks.bigquery import BigQuerySink

            return BigQuerySink(
                connector=connector,
                state_storage=state_storage,
                logger=etl_logger,
            )
        if connection_type == ConnectionType.MSSQL:
            from dpone.runtime.sinks.mssql import MSSQLSink

            return MSSQLSink(
                connector=connector,
                state_storage=state_storage,
                logger=etl_logger,
                runtime_storage_policy=runtime_storage_policy,
            )
        if connection_type == ConnectionType.CLICKHOUSE:
            from dpone.runtime.sinks.clickhouse import ClickHouseSink

            return ClickHouseSink(
                connector=connector,
                state_storage=state_storage,
                logger=etl_logger,
            )
        if connection_type == ConnectionType.KAFKA:
            from dpone.runtime.sinks.kafka import KafkaSink

            return KafkaSink(
                connector=connector,
                state_storage=state_storage,
                logger=etl_logger,
            )
        raise NotImplementedError(f"Unsupported resolved sink type: {connection_type}")

    @staticmethod
    def _connection_type(
        connection: ResolvedBindingConnection,
        *,
        endpoint: str,
    ) -> ConnectionType:
        descriptor = connection.descriptor
        if descriptor is None:
            raise ValueError(f"Resolved {endpoint} connection descriptor is required")
        return ConnectionType(descriptor.connection_type)


__all__ = ["ResolvedEndpointFactory"]
