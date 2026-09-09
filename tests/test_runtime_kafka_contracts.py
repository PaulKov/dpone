from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.dag.load_config_builder import LoadConfigBuilder
from dpone.runtime.artifacts import InMemoryRowsArtifact, StreamingRowsArtifact
from dpone.runtime.credentials.config import ConnectionType, CredentialsConfig, CredentialsSource
from dpone.runtime.credentials.factory import SinkFactory, SourceFactory
from dpone.runtime.kafka.codecs import CodecContext, JsonRowCodec, build_message_codec, infer_schema_from_rows
from dpone.runtime.kafka.config import (
    KafkaDeliveryMode,
    KafkaEnvelopeMode,
    KafkaKeyMode,
    KafkaReadMode,
    KafkaSinkOptions,
    KafkaSourceOptions,
)
from dpone.runtime.kafka.envelope import KafkaEnvelopeBuilder, KafkaKeyBuilder
from dpone.runtime.kafka.offsets import KafkaBatchPlanner, KafkaOffsetState
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.kafka import KafkaSink
from dpone.runtime.sources.kafka import KafkaSource


def _load_config(**options) -> LoadConfig:
    return LoadConfig(
        source_conn_id="src",
        target_conn_id="kafka_cluster",
        source_schema="public",
        source_table="orders",
        target_schema="kafka",
        target_table="orders_topic",
        load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        unique_key="id",
        batch_size=2,
        options=options,
    )


class FakeProducer:
    def __init__(self):
        self.messages = []
        self.flushed = False
        self.polled = []

    def produce(self, topic, key=None, value=None, on_delivery=None, headers=None):
        self.messages.append({"topic": topic, "key": key, "value": value, "headers": headers})
        if on_delivery:
            on_delivery(
                None, SimpleNamespace(topic=lambda: topic, partition=lambda: 0, offset=lambda: len(self.messages) - 1)
            )

    def poll(self, timeout):
        self.polled.append(timeout)

    def flush(self, timeout=None):
        self.flushed = True
        return 0


class FakeKafkaConnector:
    def __init__(self):
        self.producer = FakeProducer()
        self.committed = []
        self.value_schema = []
        self.registered = []
        self.end_offsets = {0: 5, 1: 7}
        self.timestamp_offsets = {0: 2, 1: 3}
        self.messages = [
            SimpleNamespace(
                error=lambda: None,
                value=lambda: json.dumps({"id": 1, "name": "Ada"}).encode(),
                key=lambda: b"1",
                topic=lambda: "orders_events",
                partition=lambda: 0,
                offset=lambda: 0,
                timestamp=lambda: (1, 1710000000000),
                headers=lambda: [],
            ),
            SimpleNamespace(
                error=lambda: None,
                value=lambda: json.dumps({"id": 2, "name": "Bob"}).encode(),
                key=lambda: b"2",
                topic=lambda: "orders_events",
                partition=lambda: 0,
                offset=lambda: 1,
                timestamp=lambda: (1, 1710000000001),
                headers=lambda: [],
            ),
        ]

    def create_producer(self, options=None):
        self.producer_options = options or {}
        return self.producer

    def create_consumer(self, group_id, options=None):
        self.consumer_group_id = group_id
        self.consumer_options = options or {}
        return SimpleNamespace(
            assign=lambda partitions: setattr(self, "assigned", partitions),
            poll=self._poll,
            close=lambda: setattr(self, "closed", True),
        )

    def _poll(self, timeout):
        return self.messages.pop(0) if self.messages else None

    def topic_partitions(self, topic):
        assert topic == "orders_events"
        return [0, 1]

    def watermarks(self, topic, partition):
        return (0, self.end_offsets[partition])

    def offsets_for_times(self, topic, timestamps_by_partition):
        return {partition: self.timestamp_offsets[partition] for partition in timestamps_by_partition}

    def commit_offsets(self, consumer, topic, offsets):
        self.committed.append((topic, dict(offsets)))

    def latest_value_schema(self, topic):
        return self.value_schema

    def register_value_schema(self, topic, schema, message_format="json"):
        self.registered.append((topic, tuple(schema), message_format))


class MemoryKafkaStateStorage:
    def __init__(self, state=None):
        self.state = state
        self.saved = []

    def load_state(self, topic, group_id):
        return self.state

    def save_state(self, state):
        self.saved.append(state)
        self.state = state


def test_kafka_options_defaults_are_batch_etl_safe() -> None:
    source = KafkaSourceOptions.from_mapping({})
    sink = KafkaSinkOptions.from_mapping({})

    assert source.read_mode == KafkaReadMode.OFFSETS
    assert source.offset_storage == "dpone"
    assert source.start_from == "stored"
    assert sink.delivery.mode == KafkaDeliveryMode.AT_LEAST_ONCE
    assert sink.key.mode == KafkaKeyMode.UNIQUE_KEY
    assert sink.envelope == KafkaEnvelopeMode.FLAT
    assert sink.deletes_enabled is False


def test_kafka_key_builder_uses_unique_key_and_hash_row_fallback() -> None:
    row = {"id": 42, "name": "Ada"}

    assert KafkaKeyBuilder(KafkaSinkOptions.from_mapping({"key": {"mode": "unique_key"}})).build(row, "id") == b"42"
    assert KafkaKeyBuilder(KafkaSinkOptions.from_mapping({"key": {"mode": "null"}})).build(row, "id") is None
    hashed = KafkaKeyBuilder(KafkaSinkOptions.from_mapping({"key": {"mode": "hash_row"}})).build(row, None)
    assert hashed is not None and len(hashed) == 64


def test_json_codec_round_trips_rows_and_infers_schema() -> None:
    codec = JsonRowCodec()
    ctx = CodecContext(topic="orders", message_format="json")
    payload = codec.serialize({"id": 1, "active": True, "amount": 10.5}, [("id", "bigint")], ctx)

    assert codec.deserialize(payload, ctx) == {"id": 1, "active": True, "amount": 10.5}
    assert infer_schema_from_rows([{"id": 1, "active": True, "amount": 10.5, "meta": {"x": 1}}]) == [
        ("id", "bigint"),
        ("active", "boolean"),
        ("amount", "double precision"),
        ("meta", "json"),
    ]


def test_registry_codecs_require_schema_registry_configuration() -> None:
    with pytest.raises(ValueError, match="Schema Registry"):
        build_message_codec("avro", schema_registry_client=None)
    assert isinstance(build_message_codec("json", schema_registry_client=None), JsonRowCodec)


def test_batch_planner_uses_stored_offsets_and_fixed_high_watermarks() -> None:
    connector = FakeKafkaConnector()
    storage = MemoryKafkaStateStorage(
        KafkaOffsetState(
            topic="orders_events",
            group_id="dpone.orders.batch",
            partition_offsets={0: 2},
            high_watermarks={0: 2},
            read_mode="offsets",
        )
    )
    planner = KafkaBatchPlanner(connector, storage)

    window = planner.plan("orders_events", KafkaSourceOptions.from_mapping({"group_id": "dpone.orders.batch"}))

    assert window.start_offsets == {0: 2, 1: 0}
    assert window.end_offsets == {0: 5, 1: 7}


def test_kafka_sink_produces_flat_rows_and_flushes_at_least_once() -> None:
    connector = FakeKafkaConnector()
    sink = KafkaSink(connector)
    cfg = _load_config(topic="orders_topic", message_format="json")

    result = sink.load(
        cfg,
        LoadPayload(
            artifact=InMemoryRowsArtifact([{"id": 1, "name": "Ada"}, {"id": 2, "name": "Bob"}]),
            schema=[("id", "bigint"), ("name", "text")],
        ),
    )

    assert result.inserted_rows == 2
    assert connector.producer.flushed is True
    assert connector.producer.messages[0]["key"] == b"1"
    assert json.loads(connector.producer.messages[0]["value"].decode()) == {"id": 1, "name": "Ada"}


def test_kafka_sink_supports_dpone_envelope_and_tombstones() -> None:
    connector = FakeKafkaConnector()
    sink = KafkaSink(connector)
    cfg = _load_config(
        topic="orders_topic",
        envelope="dpone",
        deletes={"enabled": True, "tombstone": True},
    )

    result = sink.load(
        cfg,
        LoadPayload(
            artifact=InMemoryRowsArtifact([{"id": 1, "name": "Ada"}, {"id": 2, "__dpone__op": "delete"}]),
            schema=[("id", "bigint"), ("name", "text")],
        ),
    )

    first = json.loads(connector.producer.messages[0]["value"].decode())
    assert first["op"] == "upsert"
    assert first["data"] == {"id": 1, "name": "Ada"}
    assert connector.producer.messages[1]["key"] == b"2"
    assert connector.producer.messages[1]["value"] is None
    assert result.inserted_rows == 2


@pytest.mark.parametrize(
    "strategy",
    [LoadStrategy.SNAPSHOT_DIFF, LoadStrategy.CDC_APPLY, LoadStrategy.BACKFILL],
)
def test_kafka_sink_supports_event_log_production_strategies(strategy: LoadStrategy) -> None:
    connector = FakeKafkaConnector()
    sink = KafkaSink(connector)
    cfg = _load_config(topic="orders_topic", message_format="json", envelope="dpone")
    cfg.load_strategy = strategy

    result = sink.load(
        cfg,
        LoadPayload(
            artifact=InMemoryRowsArtifact([{"id": 1, "__dpone__op": "delete"}]),
            schema=[("id", "bigint"), ("__dpone__op", "text")],
        ),
    )

    value = json.loads(connector.producer.messages[0]["value"].decode())
    assert value["op"] == "delete"
    assert value["metadata"]["strategy"] == strategy.value
    assert result.inserted_rows == 1


def test_kafka_sink_uses_canonical_dpone_operation_column_for_envelope() -> None:
    connector = FakeKafkaConnector()
    sink = KafkaSink(connector)
    cfg = _load_config(topic="orders_topic", envelope="dpone")

    sink.load(
        cfg,
        LoadPayload(
            artifact=InMemoryRowsArtifact([{"id": 1, "__dpone__op": "delete"}]),
            schema=[("id", "bigint"), ("__dpone__op", "text")],
        ),
    )

    value = json.loads(connector.producer.messages[0]["value"].decode())
    assert value["op"] == "delete"


def test_kafka_source_extracts_bounded_streaming_rows_and_saves_offset_state_after_load() -> None:
    connector = FakeKafkaConnector()
    connector.end_offsets = {0: 2, 1: 0}
    storage = MemoryKafkaStateStorage()
    source = KafkaSource(connector=connector, state_storage=storage, logger=None)
    cfg = _load_config(
        topic="orders_events",
        group_id="dpone.orders.batch",
        read_mode="offsets",
        offset_storage="dpone",
        message_format="json",
    )

    state = source.get_incremental_state(cfg)
    result = source.extract(cfg, state)

    assert isinstance(result.artifact, StreamingRowsArtifact)
    rows = list(result.artifact._iterator)
    assert rows == [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Bob"}]
    assert result.schema == [("id", "bigint"), ("name", "text")]
    assert result.state.partition_offsets == {0: 2, 1: 0}


def test_kafka_source_can_commit_offsets_when_configured() -> None:
    connector = FakeKafkaConnector()
    source = KafkaSource(connector=connector, state_storage=MemoryKafkaStateStorage(), logger=None)
    cfg = _load_config(
        topic="orders_events",
        group_id="dpone.orders.batch",
        read_mode="max_records",
        offset_storage="kafka",
        max_records=1,
    )

    result = source.extract(cfg, source.get_incremental_state(cfg))
    source.save_state(cfg, result.state)

    assert connector.committed == [("orders_events", {0: 1, 1: 0})]


def test_load_config_builder_maps_kafka_topic_to_source_and_target_tables() -> None:
    cfg = LoadConfigBuilder().build(
        {
            "source": {
                "type": "kafka",
                "connection_id": "kafka_cluster",
                "connection_type": "vault",
                "topic": "orders_events",
                "options": {"batch_size": 50000},
            },
            "sink": {
                "type": "kafka",
                "connection_id": "kafka_cluster",
                "connection_type": "vault",
                "topic": "dwh.orders",
                "strategy": {"mode": "incremental_append", "unique_key": "order_id"},
            },
        }
    )

    assert cfg.source_schema == "kafka"
    assert cfg.source_table == "orders_events"
    assert cfg.target_schema == "kafka"
    assert cfg.target_table == "dwh.orders"
    assert cfg.unique_key == "order_id"
    assert cfg.batch_size == 50000


def test_factories_create_kafka_source_and_sink(monkeypatch) -> None:
    class Manager:
        def get_credentials(self, connection_id, source, mount_point=None, path=None):
            assert source == CredentialsSource.PARAMS
            return CredentialsConfig(additional_params={"bootstrap_servers": "localhost:9092"})

    monkeypatch.setattr(SourceFactory, "manager", Manager())
    monkeypatch.setattr(SinkFactory, "manager", Manager())

    source = SourceFactory.create(
        connection_id="kafka_cluster",
        state_storage=MemoryKafkaStateStorage(),
        credentials_source="params",
        connection_type="kafka",
    )
    sink = SinkFactory.create(
        connection_id="kafka_cluster",
        state_storage=MemoryKafkaStateStorage(),
        credentials_source="params",
        connection_type="kafka",
    )

    assert ConnectionType.KAFKA == "kafka"
    assert isinstance(source, KafkaSource)
    assert isinstance(sink, KafkaSink)


def test_schema_registry_plan_is_applied_for_kafka_sink() -> None:
    connector = FakeKafkaConnector()
    sink = KafkaSink(connector)
    cfg = _load_config(topic="orders_topic", schema_registry={"enabled": True, "auto_register_schemas": True})

    sink.apply_schema_plan(cfg, SimpleNamespace(mapped_schema=[("id", "bigint")]))

    assert connector.registered == [("orders_topic", (("id", "bigint"),), "json")]


def test_envelope_builder_can_force_always_envelope_for_plain_rows() -> None:
    options = KafkaSinkOptions.from_mapping({"always_envelope": True, "envelope": "flat"})
    value = KafkaEnvelopeBuilder(options).build_value(
        {"id": 1},
        op="upsert",
        metadata={"run_id": "r1"},
        source={"schema": "public", "table": "orders"},
    )

    assert value["data"] == {"id": 1}
    assert value["metadata"]["run_id"] == "r1"
    assert value["source"]["table"] == "orders"


def test_etl_processor_persists_kafka_offsets_only_after_sink_load() -> None:
    from dpone.runtime.etl.processor import ETLProcessor
    from dpone.runtime.sinks.base import LoadResult

    events: list[str] = []
    state = KafkaOffsetState(
        topic="orders_events",
        group_id="dpone.orders.batch",
        partition_offsets={0: 2},
        high_watermarks={0: 2},
        read_mode="offsets",
    )

    class Source:
        def get_incremental_state(self, load_config):
            return None

        def extract(self, load_config, last_state):
            events.append("extract")
            return SimpleNamespace(
                artifact=InMemoryRowsArtifact([{"id": 1}]),
                schema=[("id", "bigint")],
                state=state,
                force_full_refresh=False,
            )

        def save_state(self, load_config, saved_state):
            events.append("save_state")
            assert saved_state == state

    class Sink:
        def get_target_schema(self, load_config):
            return [("id", "bigint")]

        def apply_schema_plan(self, load_config, plan):
            events.append("apply_schema")

        def load(self, load_config, payload):
            events.append("load")
            return LoadResult(inserted_rows=1, updated_rows=0, total_rows=1, staging_rows=1)

    cfg = _load_config(topic="orders_events")
    result = ETLProcessor(Source(), Sink()).run(cfg)

    assert result["status"] == "success"
    assert events == ["extract", "apply_schema", "load", "save_state"]
