from __future__ import annotations

import json
import os
import uuid

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.connectors.kafka import KafkaConnector
from dpone.runtime.kafka.offsets import InMemoryKafkaOffsetStateStorage
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.kafka import KafkaSink
from dpone.runtime.sources.kafka import KafkaSource

pytestmark = pytest.mark.integration_kafka


def _connector() -> KafkaConnector:
    bootstrap = os.getenv("DPONE_KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap:
        pytest.skip("DPONE_KAFKA_BOOTSTRAP_SERVERS is not configured")
    return KafkaConnector(
        bootstrap_servers=bootstrap,
        schema_registry_url=os.getenv("DPONE_SCHEMA_REGISTRY_URL"),
        client_id="dpone-it",
    )


def test_kafka_json_sink_and_source_round_trip() -> None:
    connector = _connector()
    topic = f"dpone_it_{uuid.uuid4().hex}"
    sink = KafkaSink(connector)
    source = KafkaSource(connector, state_storage=InMemoryKafkaOffsetStateStorage())
    cfg = LoadConfig(
        source_conn_id="kafka",
        target_conn_id="kafka",
        source_schema="kafka",
        source_table=topic,
        target_schema="kafka",
        target_table=topic,
        load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        unique_key="id",
        batch_size=100,
        options={
            "topic": topic,
            "group_id": f"dpone-it-{uuid.uuid4().hex}",
            "message_format": "json",
            "read_mode": "offsets",
            "offset_storage": "dpone",
            "max_empty_polls": 5,
        },
    )

    loaded = sink.load(
        cfg,
        LoadPayload(
            artifact=InMemoryRowsArtifact([{"id": 1, "name": "Ada"}, {"id": 2, "name": "Bob"}]),
            schema=[("id", "bigint"), ("name", "text")],
        ),
    )
    assert loaded.inserted_rows == 2

    result = source.extract(cfg, source.get_incremental_state(cfg))
    rows = list(result.artifact._iterator)
    assert {json.dumps(row, sort_keys=True) for row in rows} >= {
        json.dumps({"id": 1, "name": "Ada"}, sort_keys=True),
        json.dumps({"id": 2, "name": "Bob"}, sort_keys=True),
    }
    source.save_state(cfg, result.state)


def test_schema_registry_client_can_be_created_when_url_is_configured() -> None:
    url = os.getenv("DPONE_SCHEMA_REGISTRY_URL")
    if not url:
        pytest.skip("DPONE_SCHEMA_REGISTRY_URL is not configured")
    connector = KafkaConnector(
        bootstrap_servers=os.getenv("DPONE_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), schema_registry_url=url
    )
    client = connector.create_schema_registry_client()
    assert client is not None
