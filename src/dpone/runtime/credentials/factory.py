"""Factories for runtime sources and sinks."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dpone.runtime.connector_logging import etl_logger
from dpone.runtime.credentials.config import ConnectionType, CredentialsSource
from dpone.runtime.credentials.connector_factory import BaseFactory

if TYPE_CHECKING:
    from dpone.contracts.runtime_connection import ResolvedBindingConnection
    from dpone.runtime.sources.postgres import PostgresSource
    from dpone.runtime.state.xmin_storage import XMinStateStorage
    from dpone.runtime.storage_policy import RuntimeStoragePolicy


class SourceFactory(BaseFactory):
    @classmethod
    def create_resolved(
        cls,
        connection: ResolvedBindingConnection,
        state_storage: XMinStateStorage,
        *,
        autocommit: bool = True,
        sink_connector: Any = None,
    ) -> PostgresSource | Any:
        """Create a source without consulting ambient credential providers."""

        from dpone.runtime.credentials.resolved_endpoint_factory import (
            ResolvedEndpointFactory,
        )

        return ResolvedEndpointFactory.create_source(
            connection,
            state_storage,
            autocommit=autocommit,
            sink_connector=sink_connector,
        )

    @classmethod
    def create(
        cls,
        connection_id: str,
        state_storage: XMinStateStorage,
        credentials_source: str = CredentialsSource.AIRFLOW,
        connection_type: str = ConnectionType.POSTGRES,
        autocommit: bool = True,
        mount_point: str | None = None,
        path: str | None = None,
        sink_connector=None,
    ) -> PostgresSource | Any:
        source_enum = CredentialsSource(credentials_source)
        type_enum = ConnectionType(connection_type)

        if type_enum == ConnectionType.POSTGRES:
            from dpone.runtime.sources.postgres import PostgresSource

            postgres_connector = cls._create_postgres_connector(
                connection_id,
                source_enum,
                autocommit,
                mount_point,
                path,
            )
            return PostgresSource(
                connector=postgres_connector,
                state_storage=state_storage,
                logger=etl_logger,
                sink_connector=sink_connector,
            )

        if type_enum == ConnectionType.CLICKHOUSE:
            from dpone.runtime.sources import ClickHouseSource

            clickhouse_connector = cls._create_clickhouse_connector(
                connection_id,
                source_enum,
                mount_point,
                path,
            )
            return ClickHouseSource(
                connector=clickhouse_connector,
                logger=etl_logger,
                sink_connector=sink_connector,
            )

        if type_enum == ConnectionType.MSSQL:
            from dpone.runtime.sources.mssql import MSSQLSource

            mssql_connector = cls._create_mssql_connector(
                connection_id,
                source_enum,
                autocommit,
                mount_point,
                path,
            )
            return MSSQLSource(
                connector=mssql_connector,
                logger=etl_logger,
                sink_connector=sink_connector,
            )

        if type_enum == ConnectionType.MYSQL:
            from dpone.runtime.sources.mysql import MySQLSource

            mysql_connector = cls._create_mysql_connector(
                connection_id,
                source_enum,
                autocommit,
                mount_point,
                path,
            )
            return MySQLSource(
                connector=mysql_connector,
                logger=etl_logger,
                sink_connector=sink_connector,
            )

        if type_enum == ConnectionType.KAFKA:
            from dpone.runtime.sources.kafka import KafkaSource

            kafka_connector = cls._create_kafka_connector(
                connection_id,
                source_enum,
                mount_point,
                path,
            )
            return KafkaSource(
                connector=kafka_connector,
                state_storage=state_storage,
                logger=etl_logger,
                sink_connector=sink_connector,
            )

        raise NotImplementedError(f"Пока не реализован источник для {type_enum}")


class SinkFactory(BaseFactory):
    @classmethod
    def create_resolved(
        cls,
        connection: ResolvedBindingConnection,
        state_storage: XMinStateStorage,
        *,
        autocommit: bool = True,
        proxy_connection: ResolvedBindingConnection | None = None,
        runtime_storage_policy: RuntimeStoragePolicy | None = None,
    ) -> Any:
        """Create a sink without consulting ambient credential providers."""

        from dpone.runtime.credentials.resolved_endpoint_factory import (
            ResolvedEndpointFactory,
        )

        return ResolvedEndpointFactory.create_sink(
            connection,
            state_storage,
            autocommit=autocommit,
            proxy_connection=proxy_connection,
            runtime_storage_policy=runtime_storage_policy,
        )

    @classmethod
    def create(
        cls,
        connection_id: str,
        state_storage: XMinStateStorage,
        credentials_source: str = CredentialsSource.AIRFLOW,
        connection_type: str = ConnectionType.POSTGRES,
        autocommit: bool = True,
        mount_point: str | None = None,
        path: str | None = None,
        proxy_enable: bool = False,
        proxy_mount_point: str | None = None,
        proxy_path: str = "network/proxy/gcp/current",
        runtime_storage_policy: RuntimeStoragePolicy | None = None,
    ):
        source_enum = CredentialsSource(credentials_source)
        type_enum = ConnectionType(connection_type)

        if type_enum == ConnectionType.POSTGRES:
            from dpone.runtime.sinks.postgres import PostgresSink

            postgres_connector = cls._create_postgres_connector(
                connection_id,
                source_enum,
                autocommit,
                mount_point,
                path,
            )
            return PostgresSink(
                connector=postgres_connector,
                state_storage=state_storage,
                logger=etl_logger,
            )

        if type_enum == ConnectionType.BIGQUERY:
            from dpone.runtime.sinks.bigquery import BigQuerySink

            bigquery_connector = cls._create_bigquery_connector(
                connection_id,
                source_enum,
                mount_point,
                path,
                proxy_enable,
                proxy_mount_point,
                proxy_path,
            )
            return BigQuerySink(
                connector=bigquery_connector,
                state_storage=state_storage,
                logger=etl_logger,
            )

        if type_enum == ConnectionType.MSSQL:
            from dpone.runtime.sinks.mssql import MSSQLSink

            mssql_connector = cls._create_mssql_connector(
                connection_id,
                source_enum,
                autocommit,
                mount_point,
                path,
            )
            return MSSQLSink(
                connector=mssql_connector,
                state_storage=state_storage,
                logger=etl_logger,
                runtime_storage_policy=runtime_storage_policy,
            )

        if type_enum == ConnectionType.CLICKHOUSE:
            from dpone.runtime.sinks.clickhouse import ClickHouseSink

            clickhouse_connector = cls._create_clickhouse_connector(
                connection_id,
                source_enum,
                mount_point,
                path,
            )
            return ClickHouseSink(
                connector=clickhouse_connector,
                state_storage=state_storage,
                logger=etl_logger,
            )

        if type_enum == ConnectionType.KAFKA:
            from dpone.runtime.sinks.kafka import KafkaSink

            kafka_connector = cls._create_kafka_connector(
                connection_id,
                source_enum,
                mount_point,
                path,
            )
            return KafkaSink(
                connector=kafka_connector,
                state_storage=state_storage,
                logger=etl_logger,
            )

        raise NotImplementedError(f"Пока не реализован приёмник для {type_enum}")
