"""Kafka bounded batch source."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from datetime import datetime
from typing import TYPE_CHECKING, Any

from dpone._compat import UTC
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.kafka.codecs import CodecContext, build_message_codec, infer_schema_from_rows
from dpone.runtime.kafka.config import KafkaReadMode, KafkaSourceOptions
from dpone.runtime.kafka.offsets import InMemoryKafkaOffsetStateStorage, KafkaBatchPlanner, KafkaOffsetState
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.source_protocol import AbstractSource
from dpone.runtime.streaming_rows import StreamingRowsArtifact

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


class KafkaSource(AbstractSource):
    """Reads finite Kafka batches through the standard dpone Source contract."""

    def __init__(self, connector: Any, state_storage: Any = None, logger: Any = None, sink_connector: Any = None):
        self.connector = connector
        self.state_storage = state_storage or InMemoryKafkaOffsetStateStorage()
        self.logger = logger
        self.sink_connector = sink_connector
        self._last_consumer: Any = None

    def get_incremental_state(self, load_config: LoadConfig) -> KafkaOffsetState | None:
        options = self._options(load_config)
        topic = self._topic(load_config, options)
        if options.offset_storage != "dpone" or not hasattr(self.state_storage, "load_state"):
            return None
        return self.state_storage.load_state(topic, options.resolved_group_id(topic))

    def mssql_transaction_checkpoint_mode(self, load_config: LoadConfig) -> str:
        """Kafka offsets cannot join a local SQL Server target transaction."""

        del load_config
        return "external_nonatomic"

    def extract(self, load_config: LoadConfig, last_state: KafkaOffsetState | None) -> ExtractResult:
        del last_state  # planner reloads canonical state from storage to avoid stale caller data
        options = self._options(load_config)
        topic = self._topic(load_config, options)
        planner = KafkaBatchPlanner(self.connector, self.state_storage if options.offset_storage == "dpone" else None)
        window = planner.plan(topic, options)
        codec = build_message_codec(
            options.message_format,
            schema_registry_client=self._schema_registry_client(options),
            protobuf_message_class=load_config.options.get("protobuf_message_class"),
        )
        consumer = self.connector.create_consumer(window.group_id, options={"enable.auto.commit": False})
        self._last_consumer = consumer
        assignments = (
            self.connector.build_assignments(topic, window.start_offsets)
            if hasattr(self.connector, "build_assignments")
            else list(window.start_offsets.items())
        )
        consumer.assign(assignments)

        if options.read_mode == KafkaReadMode.MAX_RECORDS:
            rows, end_offsets = self._consume_to_list(
                consumer, topic, window.start_offsets, window.end_offsets, options, codec
            )
            schema = infer_schema_from_rows(rows)
            state = KafkaOffsetState(
                topic=topic,
                group_id=window.group_id,
                partition_offsets=end_offsets,
                high_watermarks=window.end_offsets,
                read_mode=options.read_mode.value,
                started_at=datetime.now(UTC),
                completed_at=datetime.now(UTC),
            )
            return ExtractResult(artifact=InMemoryRowsArtifact(rows), schema=schema, state=state)

        preview_rows, preview_offsets, empty_polls = self._consume_preview(
            consumer,
            topic,
            window.end_offsets,
            options,
            codec,
        )
        schema = infer_schema_from_rows(preview_rows) if preview_rows else self._registry_schema(topic)
        artifact = StreamingRowsArtifact(
            self._stream_remaining(
                consumer,
                topic,
                window.end_offsets,
                options,
                codec,
                preview_rows=preview_rows,
                preview_offsets=preview_offsets,
                initial_empty_polls=empty_polls,
            ),
            batch_size=options.batch_size,
            estimated_rows=sum(
                max(0, window.end_offsets[p] - window.start_offsets.get(p, 0)) for p in window.end_offsets
            ),
            cleanup_callback=lambda: self._close_consumer(consumer),
        )
        return ExtractResult(artifact=artifact, schema=schema, state=window.to_state())

    def save_state(self, load_config: LoadConfig, state: KafkaOffsetState) -> None:
        options = self._options(load_config)
        topic = self._topic(load_config, options)
        if options.offset_storage == "kafka":
            consumer = self._last_consumer
            if consumer is not None and hasattr(self.connector, "commit_offsets"):
                self.connector.commit_offsets(consumer, topic, state.partition_offsets)
            self._close_consumer(consumer)
            return
        if hasattr(self.state_storage, "save_state"):
            self.state_storage.save_state(state)

    def _consume_preview(
        self,
        consumer: Any,
        topic: str,
        end_offsets: Mapping[int, int],
        options: KafkaSourceOptions,
        codec: Any,
    ) -> tuple[list[dict[str, Any]], dict[int, int], int]:
        rows: list[dict[str, Any]] = []
        offsets: dict[int, int] = {}
        empty_polls = 0
        while len(rows) < options.schema_sample_size and empty_polls < options.max_empty_polls:
            msg = consumer.poll(options.poll_timeout_ms / 1000.0)
            if msg is None:
                empty_polls += 1
                continue
            parsed = self._message_to_row(msg, topic, end_offsets, options, codec)
            if parsed is None:
                continue
            row, partition, next_offset = parsed
            offsets[partition] = next_offset
            rows.append(row)
        return rows, offsets, empty_polls

    def _stream_remaining(
        self,
        consumer: Any,
        topic: str,
        end_offsets: Mapping[int, int],
        options: KafkaSourceOptions,
        codec: Any,
        *,
        preview_rows: list[dict[str, Any]],
        preview_offsets: dict[int, int],
        initial_empty_polls: int,
    ) -> Iterator[Mapping[str, Any]]:
        del preview_offsets
        for row in preview_rows:
            yield row
        empty_polls = initial_empty_polls
        while empty_polls < options.max_empty_polls:
            msg = consumer.poll(options.poll_timeout_ms / 1000.0)
            if msg is None:
                empty_polls += 1
                continue
            parsed = self._message_to_row(msg, topic, end_offsets, options, codec)
            if parsed is None:
                continue
            empty_polls = 0
            row, _partition, _next_offset = parsed
            yield row
        self._close_consumer(consumer)

    def _consume_to_list(
        self,
        consumer: Any,
        topic: str,
        start_offsets: Mapping[int, int],
        end_offsets: Mapping[int, int],
        options: KafkaSourceOptions,
        codec: Any,
    ) -> tuple[list[dict[str, Any]], dict[int, int]]:
        rows: list[dict[str, Any]] = []
        offsets = dict(start_offsets)
        empty_polls = 0
        max_records = options.max_records or options.batch_size
        while len(rows) < max_records and empty_polls < options.max_empty_polls:
            msg = consumer.poll(options.poll_timeout_ms / 1000.0)
            if msg is None:
                empty_polls += 1
                continue
            parsed = self._message_to_row(msg, topic, end_offsets, options, codec)
            if parsed is None:
                continue
            empty_polls = 0
            row, partition, next_offset = parsed
            offsets[partition] = next_offset
            rows.append(row)
        return rows, offsets

    def _message_to_row(
        self,
        msg: Any,
        topic: str,
        end_offsets: Mapping[int, int],
        options: KafkaSourceOptions,
        codec: Any,
    ) -> tuple[dict[str, Any], int, int] | None:
        if msg.error():
            raise RuntimeError(f"Kafka consumer error: {msg.error()}")
        partition = int(msg.partition())
        offset = int(msg.offset())
        if offset >= int(end_offsets.get(partition, offset + 1)):
            return None
        value = codec.deserialize(msg.value(), CodecContext(topic=topic, message_format=options.message_format))
        if value is None:
            return ({"__dpone__op": "delete", "__dpone__kafka_key": _decode_key(msg.key())}, partition, offset + 1)
        row = _unwrap_envelope(value, options)
        return row, partition, offset + 1

    def _registry_schema(self, topic: str) -> list[tuple[str, str]]:
        if hasattr(self.connector, "latest_value_schema"):
            return list(self.connector.latest_value_schema(topic))
        return []

    def _schema_registry_client(self, options: KafkaSourceOptions) -> Any:
        if not options.schema_registry.enabled:
            return None
        if hasattr(self.connector, "create_schema_registry_client"):
            return self.connector.create_schema_registry_client()
        return None

    def _options(self, load_config: LoadConfig) -> KafkaSourceOptions:
        return KafkaSourceOptions.from_mapping(load_config.options)

    def _topic(self, load_config: LoadConfig, options: KafkaSourceOptions) -> str:
        topic = options.topic or load_config.source_table
        if not topic:
            raise ValueError("Kafka source requires source.topic or source.table.name")
        return str(topic)

    def _close_consumer(self, consumer: Any) -> None:
        if consumer is not None and hasattr(consumer, "close"):
            try:
                consumer.close()
            except Exception:
                pass


def _unwrap_envelope(value: dict[str, Any], options: KafkaSourceOptions) -> dict[str, Any]:
    if options.envelope.value in {"dpone", "auto"} and "data" in value and "op" in value:
        row = dict(value.get("data") or {})
        row["__dpone__op"] = value.get("op")
        return row
    return dict(value)


def _decode_key(value: bytes | None) -> str | None:
    if value is None:
        return None
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return value.hex()
