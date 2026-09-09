"""Kafka sink implementation."""

from __future__ import annotations

import csv
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.file_artifacts import (
    FileExportArtifact,
    PartitionedFileExportArtifact,
)
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.kafka.codecs import CodecContext, build_message_codec
from dpone.runtime.kafka.config import KafkaSinkOptions
from dpone.runtime.kafka.envelope import KafkaEnvelopeBuilder, KafkaKeyBuilder
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.merge_policy import MergePolicy, resolve_merge_policy
from dpone.runtime.sinks.sink_protocol import AbstractSink
from dpone.runtime.streaming_rows import StreamingRowsArtifact

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


class KafkaDeliveryTracker:
    """Aggregates producer delivery callbacks."""

    def __init__(self) -> None:
        self.delivered = 0
        self.errors: list[str] = []

    def callback(self, err: Any, msg: Any) -> None:
        if err is not None:
            self.errors.append(str(err))
            return
        self.delivered += 1

    def raise_if_failed(self) -> None:
        if self.errors:
            raise RuntimeError("Kafka delivery failed: " + "; ".join(self.errors[:5]))


class KafkaSink(AbstractSink):
    """Produces dpone load payload rows to a Kafka topic."""

    def __init__(self, connector: Any, state_storage: Any = None, logger: Any = None):
        self.connector = connector
        self.state_storage = state_storage
        self.logger = logger

    def load(self, load_config: LoadConfig, payload: LoadPayload) -> LoadResult:
        if load_config.load_strategy == LoadStrategy.PARTITION_REPLACE:
            raise ValueError("Kafka sink does not support partition_replace because Kafka topics are append-only logs")
        if load_config.load_strategy not in {
            LoadStrategy.FULL_REFRESH,
            LoadStrategy.INCREMENTAL_APPEND,
            LoadStrategy.INCREMENTAL_MERGE,
            LoadStrategy.REPLACE,
            LoadStrategy.SNAPSHOT_DIFF,
            LoadStrategy.CDC_APPLY,
            LoadStrategy.BACKFILL,
        }:
            raise ValueError(f"Unsupported Kafka load strategy: {load_config.load_strategy.value}")
        if load_config.load_strategy == LoadStrategy.INCREMENTAL_MERGE:
            policy = resolve_merge_policy(load_config, "kafka")
            if policy != MergePolicy.EVENT_UPSERT:
                raise ValueError("Kafka incremental_merge supports only merge_policy=event_upsert")
        if load_config.load_strategy == LoadStrategy.BACKFILL:
            self._validate_backfill_options(load_config)
        options = KafkaSinkOptions.from_mapping(load_config.options)
        topic = self._topic(load_config, options)
        producer = self.connector.create_producer(options=options.delivery.to_producer_config())
        registry_client = self.connector.create_schema_registry_client() if options.schema_registry.enabled else None
        codec = build_message_codec(
            options.message_format,
            schema_registry_client=registry_client,
            protobuf_message_class=load_config.options.get("protobuf_message_class"),
        )
        key_builder = KafkaKeyBuilder(options)
        envelope_builder = KafkaEnvelopeBuilder(options)
        tracker = KafkaDeliveryTracker()
        produced = 0
        for row in self._iter_rows(payload):
            op = self._operation(row, load_config)
            key = key_builder.build(row, load_config.unique_key)
            if options.deletes.enabled and options.deletes.tombstone and op == "delete":
                if key is None:
                    raise ValueError("Kafka tombstone delete requires a non-null message key")
                value = None
            else:
                value_row = envelope_builder.build_value(
                    row,
                    op=op,
                    metadata={"strategy": load_config.load_strategy.value},
                    source={"schema": load_config.source_schema, "table": load_config.source_table},
                )
                value = codec.serialize(
                    value_row, payload.schema, CodecContext(topic=topic, message_format=options.message_format)
                )
            producer.produce(
                topic, key=key, value=value, headers=list(options.headers.items()), on_delivery=tracker.callback
            )
            produced += 1
            producer.poll(0)
        remaining = producer.flush(options.delivery.flush_timeout)
        tracker.raise_if_failed()
        if remaining:
            raise RuntimeError(f"Kafka producer flush left {remaining} undelivered message(s)")
        return LoadResult(inserted_rows=produced, updated_rows=0, total_rows=produced, staging_rows=produced)

    def get_target_schema(self, load_config: LoadConfig) -> list[tuple[str, str]]:
        options = KafkaSinkOptions.from_mapping(load_config.options)
        if not options.schema_registry.enabled or not hasattr(self.connector, "latest_value_schema"):
            return []
        return list(self.connector.latest_value_schema(self._topic(load_config, options)))

    def apply_schema_plan(self, load_config: LoadConfig, plan: Any) -> None:
        options = KafkaSinkOptions.from_mapping(load_config.options)
        if not options.schema_registry.enabled or not options.schema_registry.auto_register_schemas:
            return
        schema = list(getattr(plan, "mapped_schema", []) or [])
        if hasattr(self.connector, "register_value_schema"):
            self.connector.register_value_schema(self._topic(load_config, options), schema, options.message_format)

    def save_state(self, load_config: LoadConfig, state: Any) -> None:
        if self.state_storage is not None and hasattr(self.state_storage, "save_state"):
            self.state_storage.save_state(state)

    def _validate_backfill_options(self, load_config: LoadConfig) -> None:
        """Kafka backfill replays chunk rows as keyed upsert events.

        Topics are append-only logs, so sink-side inner modes like
        ``partition_replace``/``replace``/``full_refresh`` cannot apply; only
        the event-upsert replay semantics is supported.
        """

        backfill_options = (load_config.options or {}).get("backfill") or {}
        inner_mode = str(backfill_options.get("inner_mode") or "").strip().lower()
        if inner_mode and inner_mode != "incremental_merge":
            raise ValueError(
                "Kafka backfill supports only keyed upsert replay: omit backfill.inner_mode or set it to "
                f"incremental_merge (got {inner_mode!r})"
            )

    def _topic(self, load_config: LoadConfig, options: KafkaSinkOptions) -> str:
        topic = options.topic or load_config.target_table
        if not topic:
            raise ValueError("Kafka sink requires sink.topic or sink.table.name")
        return str(topic)

    def _operation(self, row: Mapping[str, Any], load_config: LoadConfig) -> str:
        raw = row.get("__dpone__op")
        if raw:
            return str(raw).lower()
        if load_config.load_strategy == LoadStrategy.CDC_APPLY:
            return "upsert"
        if load_config.load_strategy == LoadStrategy.INCREMENTAL_MERGE:
            return "upsert"
        if load_config.load_strategy == LoadStrategy.SNAPSHOT_DIFF:
            return "upsert"
        if load_config.load_strategy == LoadStrategy.BACKFILL:
            return "upsert"
        if load_config.load_strategy == LoadStrategy.REPLACE:
            return "replace"
        return "upsert"

    def _iter_rows(self, payload: LoadPayload) -> Iterator[Mapping[str, Any]]:
        artifact = payload.artifact
        if isinstance(artifact, InMemoryRowsArtifact):
            yield from artifact._rows
            return
        if isinstance(artifact, StreamingRowsArtifact):
            yield from artifact._iterator
            lifecycle = artifact.extraction_lifecycle
            if lifecycle is not None:
                lifecycle.complete()
            return
        if isinstance(artifact, FileExportArtifact):
            yield from self._iter_file_rows(artifact, payload.schema)
            return
        if isinstance(artifact, PartitionedFileExportArtifact):
            for partition in artifact.partitions:
                yield from self._iter_file_rows(partition, payload.schema)
            return
        raise TypeError(f"KafkaSink cannot iterate artifact type {type(artifact).__name__}")

    def _iter_file_rows(
        self,
        artifact: FileExportArtifact,
        schema: Sequence[tuple[str, str]],
    ) -> Iterable[Mapping[str, Any]]:
        delimiter = "\t" if artifact.format == "mssql-delimited" else ","
        columns = [column for column, _ in schema] or list(artifact.columns)
        with open(artifact.file_path, encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle, delimiter=delimiter)
            for values in reader:
                yield {column: values[index] if index < len(values) else None for index, column in enumerate(columns)}
