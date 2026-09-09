"""Kafka connector with lazy confluent-kafka imports."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

_SECRET_KEYS = {"password", "sasl.password", "basic.auth.user.info", "schema_registry_password"}


@dataclass
class KafkaConnector:
    bootstrap_servers: str
    security_protocol: str | None = None
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None
    ssl_ca_location: str | None = None
    client_id: str = "dpone"
    schema_registry_url: str | None = None
    schema_registry_username: str | None = None
    schema_registry_password: str | None = None
    additional_config: dict[str, Any] = field(default_factory=dict)

    def base_config(self) -> dict[str, Any]:
        cfg: dict[str, Any] = {"bootstrap.servers": self.bootstrap_servers, "client.id": self.client_id}
        if self.security_protocol:
            cfg["security.protocol"] = self.security_protocol
        if self.sasl_mechanism:
            cfg["sasl.mechanism"] = self.sasl_mechanism
        if self.sasl_username:
            cfg["sasl.username"] = self.sasl_username
        if self.sasl_password:
            cfg["sasl.password"] = self.sasl_password
        if self.ssl_ca_location:
            cfg["ssl.ca.location"] = self.ssl_ca_location
        cfg.update(self.additional_config or {})
        return cfg

    def redacted_config(self) -> dict[str, Any]:
        return redact_secrets(self.base_config())

    def create_producer(self, options: dict[str, Any] | None = None):
        from confluent_kafka import Producer

        cfg = self.base_config()
        cfg.update(options or {})
        return Producer(cfg)

    def create_consumer(self, group_id: str, options: dict[str, Any] | None = None):
        from confluent_kafka import Consumer

        cfg = self.base_config()
        cfg.update(
            {
                "group.id": group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        cfg.update(options or {})
        return Consumer(cfg)

    def create_schema_registry_client(self):
        if not self.schema_registry_url:
            return None
        from confluent_kafka.schema_registry import SchemaRegistryClient

        cfg: dict[str, Any] = {"url": self.schema_registry_url}
        if self.schema_registry_username or self.schema_registry_password:
            cfg["basic.auth.user.info"] = f"{self.schema_registry_username or ''}:{self.schema_registry_password or ''}"
        return SchemaRegistryClient(cfg)

    def topic_partitions(self, topic: str) -> list[int]:
        from confluent_kafka.admin import AdminClient

        metadata = AdminClient(self.base_config()).list_topics(topic=topic, timeout=10)
        if topic not in metadata.topics:
            raise ValueError(f"Kafka topic not found: {topic}")
        return sorted(metadata.topics[topic].partitions.keys())

    def watermarks(self, topic: str, partition: int) -> tuple[int, int]:
        from confluent_kafka import Consumer, TopicPartition

        consumer = Consumer({**self.base_config(), "group.id": f"dpone-watermark-{topic}", "enable.auto.commit": False})
        try:
            return consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
        finally:
            consumer.close()

    def offsets_for_times(self, topic: str, timestamps_by_partition: dict[int, int]) -> dict[int, int]:
        from confluent_kafka import Consumer, TopicPartition

        consumer = Consumer({**self.base_config(), "group.id": f"dpone-timestamp-{topic}", "enable.auto.commit": False})
        try:
            requests = [
                TopicPartition(topic, partition, timestamp) for partition, timestamp in timestamps_by_partition.items()
            ]
            results = consumer.offsets_for_times(requests, timeout=10)
            return {int(item.partition): int(item.offset) for item in results if item.offset >= 0}
        finally:
            consumer.close()

    def build_assignments(self, topic: str, offsets: dict[int, int]):
        from confluent_kafka import TopicPartition

        return [TopicPartition(topic, partition, offset) for partition, offset in offsets.items()]

    def commit_offsets(self, consumer: Any, topic: str, offsets: dict[int, int]) -> None:
        from confluent_kafka import TopicPartition

        consumer.commit(
            offsets=[TopicPartition(topic, partition, offset) for partition, offset in offsets.items()],
            asynchronous=False,
        )

    def latest_value_schema(self, topic: str) -> list[tuple[str, str]]:
        client = self.create_schema_registry_client()
        if client is None:
            return []
        subject = f"{topic}-value"
        try:
            registered = client.get_latest_version(subject)
        except Exception:
            return []
        return _schema_to_columns(getattr(registered, "schema", None))

    def register_value_schema(self, topic: str, schema: list[tuple[str, str]], message_format: str = "json") -> None:
        client = self.create_schema_registry_client()
        if client is None:
            return
        from confluent_kafka.schema_registry import Schema

        schema_type = {"avro": "AVRO", "protobuf": "PROTOBUF", "json_schema": "JSON"}.get(message_format, "JSON")
        schema_text = _columns_to_json_schema(topic, schema)
        client.register_schema(f"{topic}-value", Schema(schema_text, schema_type))


def redact_secrets(config: dict[str, Any]) -> dict[str, Any]:
    return {key: ("***" if key.lower() in _SECRET_KEYS else value) for key, value in config.items()}


def _columns_to_json_schema(topic: str, schema: list[tuple[str, str]]) -> str:
    import json

    return json.dumps(
        {
            "title": topic.replace(".", "_"),
            "type": "object",
            "properties": {name: {"type": _json_type(dtype)} for name, dtype in schema},
        },
        sort_keys=True,
    )


def _json_type(dtype: str) -> str:
    t = dtype.lower()
    if any(token in t for token in ("int", "numeric", "decimal", "float", "double")):
        return "number"
    if "bool" in t:
        return "boolean"
    if "json" in t:
        return "object"
    return "string"


def _schema_to_columns(schema: Any) -> list[tuple[str, str]]:
    if schema is None:
        return []
    text = getattr(schema, "schema_str", None) or getattr(schema, "schema", None) or ""
    try:
        import json

        payload = json.loads(text)
    except Exception:
        return []
    props = payload.get("properties") or {}
    return [(str(name), _dpone_type(spec.get("type"))) for name, spec in props.items()]


def _dpone_type(json_type: Any) -> str:
    if json_type == "number":
        return "numeric"
    if json_type == "integer":
        return "bigint"
    if json_type == "boolean":
        return "boolean"
    if json_type in {"object", "array"}:
        return "json"
    return "text"
