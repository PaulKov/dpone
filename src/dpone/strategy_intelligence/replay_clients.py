from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from dpone.runtime.credentials.config import CredentialsSource
from dpone.runtime.credentials.factory import BaseFactory
from dpone.strategy_intelligence.live_backends import (
    BigQueryReplayBackend,
    ClickHouseReplayBackend,
    KafkaLiveReplayBackend,
    MssqlReplayBackend,
    PostgresReplayBackend,
)
from dpone.strategy_intelligence.replay_adapters import ReplayBackend


class SqlRuntimeConnector(Protocol):
    def get_records(self, query: str, params: tuple[object, ...] | None = None) -> list[Any]: ...

    def execute_query(self, query: str) -> int: ...


class KafkaRuntimeConnector(Protocol):
    def create_producer(self, options: dict[str, Any] | None = None) -> Any: ...


@dataclass(frozen=True, slots=True)
class ReplayBackendConnection:
    """Connection reference used to build a live replay backend."""

    sink_type: str
    connection_id: str
    target_table: str
    credentials_source: CredentialsSource | str = CredentialsSource.ENVIRONMENT
    target_schema: str = "public"
    staging_schema: str = "staging"
    mount_point: str | None = None
    path: str | None = None


class RuntimeSqlReplayClient:
    """Adapter from runtime SQL connectors to the replay SQL port."""

    def __init__(self, *, connector: SqlRuntimeConnector, dialect: str) -> None:
        self._connector = connector
        self._dialect = _normalize_sink(dialect)

    def exists(self, schema: str, table: str) -> bool:
        table_exists = getattr(self._connector, "table_exists", None)
        if callable(table_exists):
            return bool(table_exists(schema, table))
        query, params = _exists_query(self._dialect, schema, table)
        return bool(self._connector.get_records(query, params))

    def scalar(self, statement: str) -> int:
        rows = self._connector.get_records(statement)
        if not rows:
            return 0
        return int(_first_value(rows[0]) or 0)

    def execute(self, statement: str) -> None:
        self._connector.execute_query(statement)


class RuntimeKafkaReplayClient:
    """Adapter from runtime Kafka connector to the replay Kafka port."""

    def __init__(
        self,
        *,
        connector: KafkaRuntimeConnector,
        producer_options: dict[str, Any] | None = None,
        command_handler: Callable[[str], int] | None = None,
    ) -> None:
        self._connector = connector
        self._producer_options = producer_options
        self._command_handler = command_handler
        self._producer: Any | None = None

    def produce(self, topic: str, value: dict[str, Any]) -> None:
        payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        self._producer_instance().produce(topic, value=payload)

    def flush(self) -> None:
        self._producer_instance().flush()

    def scalar(self, statement: str) -> int:
        if self._command_handler is not None:
            return int(self._command_handler(statement))
        return 1

    def _producer_instance(self) -> Any:
        if self._producer is None:
            self._producer = self._connector.create_producer(self._producer_options)
        return self._producer


class RuntimeReplayBackendFactory:
    """Build replay backends from existing runtime connector factories."""

    def __init__(self, *, connector_factory: Any = BaseFactory) -> None:
        self._connector_factory = connector_factory

    def build(self, connection: ReplayBackendConnection) -> ReplayBackend:
        sink = _normalize_sink(connection.sink_type)
        credentials_source = _credentials_source(connection.credentials_source)
        if sink == "mssql":
            connector = self._connector_factory._create_mssql_connector(
                connection.connection_id,
                credentials_source,
                mount_point=connection.mount_point,
                path=connection.path,
            )
            return MssqlReplayBackend(
                client=RuntimeSqlReplayClient(connector=connector, dialect=sink),
                target_schema=connection.target_schema,
                target_table=connection.target_table,
                staging_schema=connection.staging_schema,
            )
        if sink == "postgres":
            connector = self._connector_factory._create_postgres_connector(
                connection.connection_id,
                credentials_source,
                mount_point=connection.mount_point,
                path=connection.path,
            )
            return PostgresReplayBackend(
                client=RuntimeSqlReplayClient(connector=connector, dialect=sink),
                target_schema=connection.target_schema,
                target_table=connection.target_table,
                staging_schema=connection.staging_schema,
            )
        if sink == "clickhouse":
            connector = self._connector_factory._create_clickhouse_connector(
                connection.connection_id,
                credentials_source,
                mount_point=connection.mount_point,
                path=connection.path,
            )
            return ClickHouseReplayBackend(
                client=RuntimeSqlReplayClient(connector=connector, dialect=sink),
                target_schema=connection.target_schema,
                target_table=connection.target_table,
                staging_schema=connection.staging_schema,
            )
        if sink == "bigquery":
            connector = self._connector_factory._create_bigquery_connector(
                connection.connection_id,
                credentials_source,
                mount_point=connection.mount_point,
                path=connection.path,
            )
            return BigQueryReplayBackend(
                client=RuntimeSqlReplayClient(connector=connector, dialect=sink),
                target_schema=connection.target_schema,
                target_table=connection.target_table,
                staging_schema=connection.staging_schema,
            )
        if sink == "kafka":
            connector = self._connector_factory._create_kafka_connector(
                connection.connection_id,
                credentials_source,
                mount_point=connection.mount_point,
                path=connection.path,
            )
            return KafkaLiveReplayBackend(
                client=RuntimeKafkaReplayClient(connector=connector),
                topic=connection.target_table,
            )
        raise ValueError(f"Unsupported runtime replay sink: {connection.sink_type}")


def _credentials_source(value: CredentialsSource | str) -> CredentialsSource:
    if isinstance(value, CredentialsSource):
        return value
    return CredentialsSource(str(value))


def _normalize_sink(value: str) -> str:
    normalized = str(value).strip().lower().replace("-", "_")
    aliases = {"sqlserver": "mssql", "sql_server": "mssql", "bq": "bigquery"}
    return aliases.get(normalized, normalized)


def _exists_query(dialect: str, schema: str, table: str) -> tuple[str, tuple[object, ...] | None]:
    if dialect == "clickhouse":
        return (
            f"SELECT 1 FROM system.tables WHERE database = '{_sql_literal(schema)}' AND name = '{_sql_literal(table)}'",
            None,
        )
    if dialect == "bigquery":
        return (
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
            f"WHERE table_schema = '{_sql_literal(schema)}' AND table_name = '{_sql_literal(table)}'",
            None,
        )
    placeholder = "?" if dialect == "mssql" else "%s"
    return (
        f"SELECT 1 FROM information_schema.tables WHERE table_schema = {placeholder} AND table_name = {placeholder}",
        (schema, table),
    )


def _first_value(row: Any) -> Any:
    if isinstance(row, dict):
        return next(iter(row.values()), None)
    if isinstance(row, tuple | list):
        return row[0] if row else None
    return row


def _sql_literal(value: str) -> str:
    return str(value).replace("'", "''")
