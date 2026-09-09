from __future__ import annotations

from typing import Any

from dpone.runtime.credentials.config import CredentialsSource
from dpone.strategy_intelligence.live_backends import KafkaLiveReplayBackend, MssqlReplayBackend
from dpone.strategy_intelligence.replay import ReplayExecutionRequest
from dpone.strategy_intelligence.replay_adapters import DbReplayAdapter, KafkaReplayAdapter
from dpone.strategy_intelligence.replay_clients import (
    ReplayBackendConnection,
    RuntimeKafkaReplayClient,
    RuntimeReplayBackendFactory,
    RuntimeSqlReplayClient,
)


def test_runtime_sql_replay_client_uses_connector_table_exists_scalar_and_execute() -> None:
    connector = _FakeSqlConnector(records=[(7,)])
    client = RuntimeSqlReplayClient(connector=connector, dialect="mssql")

    assert client.exists("staging", "orders__replay_staging") is True
    assert client.scalar("SELECT COUNT(*) FROM staging.orders__replay_staging") == 7
    client.execute("UPDATE etl_state.__dpone__loads SET status = 'committed'")

    assert connector.table_exists_calls == [("staging", "orders__replay_staging")]
    assert connector.record_queries == ["SELECT COUNT(*) FROM staging.orders__replay_staging"]
    assert connector.execute_queries == ["UPDATE etl_state.__dpone__loads SET status = 'committed'"]


def test_runtime_sql_replay_client_falls_back_to_information_schema_exists() -> None:
    connector = _FakeSqlConnectorWithoutTableExists(records=[{"exists": 1}])
    client = RuntimeSqlReplayClient(connector=connector, dialect="postgres")

    assert client.exists("public", "orders__replay_staging") is True

    assert connector.record_queries
    assert "information_schema.tables" in connector.record_queries[0].lower()
    assert connector.record_params == [("public", "orders__replay_staging")]


def test_runtime_kafka_replay_client_serializes_json_and_uses_command_handler() -> None:
    connector = _FakeKafkaConnector()
    commands: list[str] = []
    client = RuntimeKafkaReplayClient(
        connector=connector,
        command_handler=lambda statement: commands.append(statement) or 1,
    )

    client.produce("dwh.orders", {"run_id": "01JREPLAY000000000000000010", "op": "replay"})
    client.flush()
    assert client.scalar("state_commit:01JREPLAY000000000000000010") == 1

    assert connector.producer_created is True
    assert connector.producer.produced == [
        (
            "dwh.orders",
            b'{"op":"replay","run_id":"01JREPLAY000000000000000010"}',
        )
    ]
    assert connector.producer.flushed is True
    assert commands == ["state_commit:01JREPLAY000000000000000010"]


def test_runtime_replay_backend_factory_builds_mssql_backend_from_connection_ref() -> None:
    connector_factory = _FakeRuntimeConnectorFactory()
    backend_factory = RuntimeReplayBackendFactory(connector_factory=connector_factory)

    backend = backend_factory.build(
        ReplayBackendConnection(
            sink_type="mssql",
            connection_id="mssql_dwh",
            credentials_source=CredentialsSource.ENVIRONMENT,
            target_schema="dbo",
            target_table="orders",
            staging_schema="staging",
        )
    )

    assert isinstance(backend, MssqlReplayBackend)
    assert connector_factory.calls == [
        ("mssql", "mssql_dwh", CredentialsSource.ENVIRONMENT, None, None),
    ]


def test_runtime_replay_backend_factory_result_executes_with_injected_runtime_connector() -> None:
    connector_factory = _FakeRuntimeConnectorFactory()
    backend = RuntimeReplayBackendFactory(connector_factory=connector_factory).build(
        ReplayBackendConnection(
            sink_type="mssql",
            connection_id="mssql_dwh",
            credentials_source="env",
            target_schema="dbo",
            target_table="orders",
            staging_schema="staging",
        )
    )
    request = ReplayExecutionRequest(
        action="resync",
        run_id="01JREPLAY000000000000000011",
        source_type="postgres",
        sink_type="mssql",
        strategy_mode="incremental_merge",
        yes=True,
    )

    result = DbReplayAdapter(backend=backend).execute(request)

    assert result.status == "executed"
    assert result.state_committed is True
    assert connector_factory.mssql_connector.execute_queries[-1].startswith("UPDATE etl_state.__dpone__loads")


def test_runtime_replay_backend_factory_builds_kafka_backend() -> None:
    connector_factory = _FakeRuntimeConnectorFactory()
    backend = RuntimeReplayBackendFactory(connector_factory=connector_factory).build(
        ReplayBackendConnection(
            sink_type="kafka",
            connection_id="kafka_cluster",
            credentials_source="params",
            target_table="dwh.orders",
        )
    )
    request = ReplayExecutionRequest(
        action="resume",
        run_id="01JREPLAY000000000000000012",
        source_type="postgres",
        sink_type="kafka",
        strategy_mode="incremental_merge",
        yes=True,
    )

    result = KafkaReplayAdapter(backend=backend).execute(request)

    assert isinstance(backend, KafkaLiveReplayBackend)
    assert result.status == "executed"
    assert connector_factory.calls == [
        ("kafka", "kafka_cluster", CredentialsSource.PARAMS, None, None),
    ]
    assert connector_factory.kafka_connector.producer.produced == [
        (
            "dwh.orders",
            b'{"op":"replay","partitions":[],"run_id":"01JREPLAY000000000000000012","strategy":"incremental_merge"}',
        )
    ]


class _FakeSqlConnector:
    def __init__(self, records: list[Any]) -> None:
        self.records = records
        self.table_exists_calls: list[tuple[str, str]] = []
        self.record_queries: list[str] = []
        self.record_params: list[tuple[object, ...]] = []
        self.execute_queries: list[str] = []

    def table_exists(self, schema: str, table: str) -> bool:
        self.table_exists_calls.append((schema, table))
        return True

    def get_records(self, query: str, params: tuple[object, ...] | None = None) -> list[Any]:
        self.record_queries.append(query)
        self.record_params.append(tuple(params or ()))
        return list(self.records)

    def execute_query(self, query: str) -> int:
        self.execute_queries.append(query)
        return 1


class _FakeSqlConnectorWithoutTableExists:
    def __init__(self, records: list[Any]) -> None:
        self.records = records
        self.record_queries: list[str] = []
        self.record_params: list[tuple[object, ...]] = []

    def get_records(self, query: str, params: tuple[object, ...] | None = None) -> list[Any]:
        self.record_queries.append(query)
        self.record_params.append(tuple(params or ()))
        return list(self.records)


class _FakeProducer:
    def __init__(self) -> None:
        self.produced: list[tuple[str, bytes]] = []
        self.flushed = False

    def produce(self, topic: str, value: bytes) -> None:
        self.produced.append((topic, value))

    def flush(self) -> None:
        self.flushed = True


class _FakeKafkaConnector:
    def __init__(self) -> None:
        self.producer = _FakeProducer()
        self.producer_created = False

    def create_producer(self, options: dict[str, Any] | None = None) -> _FakeProducer:
        assert options is None
        self.producer_created = True
        return self.producer


class _FakeRuntimeConnectorFactory:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, CredentialsSource, str | None, str | None]] = []
        self.mssql_connector = _FakeSqlConnector(records=[(1,)])
        self.kafka_connector = _FakeKafkaConnector()

    def _create_mssql_connector(
        self,
        connection_id: str,
        credentials_source: CredentialsSource,
        autocommit: bool = True,
        mount_point: str | None = None,
        path: str | None = None,
    ) -> _FakeSqlConnector:
        del autocommit
        self.calls.append(("mssql", connection_id, credentials_source, mount_point, path))
        return self.mssql_connector

    def _create_kafka_connector(
        self,
        connection_id: str,
        credentials_source: CredentialsSource,
        mount_point: str | None = None,
        path: str | None = None,
    ) -> _FakeKafkaConnector:
        self.calls.append(("kafka", connection_id, credentials_source, mount_point, path))
        return self.kafka_connector
