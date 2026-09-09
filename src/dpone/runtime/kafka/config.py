"""Kafka source/sink option models.

The models keep manifest parsing out of connector/source/sink classes and make
Kafka defaults explicit. They intentionally accept mappings so runtime code can
work with compiled YAML manifests, tests, and programmatic LoadConfig objects.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from dpone._compat import StrEnum


class KafkaReadMode(StrEnum):
    OFFSETS = "offsets"
    TIME_WINDOW = "time_window"
    MAX_RECORDS = "max_records"


class KafkaDeliveryMode(StrEnum):
    AT_LEAST_ONCE = "at_least_once"
    IDEMPOTENT = "idempotent"


class KafkaKeyMode(StrEnum):
    UNIQUE_KEY = "unique_key"
    HASH_ROW = "hash_row"
    NULL = "null"


class KafkaEnvelopeMode(StrEnum):
    FLAT = "flat"
    DPONE = "dpone"
    AUTO = "auto"


@dataclass(frozen=True)
class KafkaSchemaRegistryOptions:
    enabled: bool = False
    subject_name_strategy: str = "topic"
    auto_register_schemas: bool = False
    url: str | None = None

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any] | None) -> KafkaSchemaRegistryOptions:
        data = dict(raw or {})
        return cls(
            enabled=bool(data.get("enabled", False)),
            subject_name_strategy=str(data.get("subject_name_strategy", "topic")),
            auto_register_schemas=bool(data.get("auto_register_schemas", False)),
            url=data.get("url"),
        )


@dataclass(frozen=True)
class KafkaSourceOptions:
    topic: str | None = None
    group_id: str | None = None
    read_mode: KafkaReadMode = KafkaReadMode.OFFSETS
    offset_storage: str = "dpone"
    start_from: str = "stored"
    message_format: str = "json"
    envelope: KafkaEnvelopeMode = KafkaEnvelopeMode.AUTO
    batch_size: int = 50_000
    max_records: int | None = None
    poll_timeout_ms: int = 1000
    max_empty_polls: int = 3
    time_window_start_ms: int | None = None
    time_window_end_ms: int | None = None
    schema_sample_size: int = 100
    schema_registry: KafkaSchemaRegistryOptions = field(default_factory=KafkaSchemaRegistryOptions)

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any] | None) -> KafkaSourceOptions:
        data = dict(raw or {})
        read_mode = KafkaReadMode(str(data.get("read_mode", KafkaReadMode.OFFSETS.value)).lower())
        envelope = KafkaEnvelopeMode(str(data.get("envelope", KafkaEnvelopeMode.AUTO.value)).lower())
        return cls(
            topic=data.get("topic"),
            group_id=data.get("group_id"),
            read_mode=read_mode,
            offset_storage=str(data.get("offset_storage", "dpone")).lower(),
            start_from=str(data.get("start_from", "stored")).lower(),
            message_format=str(data.get("message_format", "json")).lower(),
            envelope=envelope,
            batch_size=int(data.get("batch_size", 50_000)),
            max_records=_optional_int(data.get("max_records")),
            poll_timeout_ms=int(data.get("poll_timeout_ms", 1000)),
            max_empty_polls=int(data.get("max_empty_polls", 3)),
            time_window_start_ms=_optional_int(data.get("time_window_start_ms") or data.get("start_timestamp_ms")),
            time_window_end_ms=_optional_int(data.get("time_window_end_ms") or data.get("end_timestamp_ms")),
            schema_sample_size=int(data.get("schema_sample_size", 100)),
            schema_registry=KafkaSchemaRegistryOptions.from_mapping(data.get("schema_registry")),
        )

    def resolved_group_id(self, topic: str) -> str:
        return self.group_id or f"dpone.{topic}.batch"


@dataclass(frozen=True)
class KafkaKeyOptions:
    mode: KafkaKeyMode = KafkaKeyMode.UNIQUE_KEY
    fields: tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any] | None) -> KafkaKeyOptions:
        data = dict(raw or {})
        mode = KafkaKeyMode(str(data.get("mode", KafkaKeyMode.UNIQUE_KEY.value)).lower())
        fields_raw = data.get("fields") or data.get("field") or ()
        fields: tuple[str, ...]
        if isinstance(fields_raw, str):
            fields = (fields_raw,)
        else:
            fields = tuple(str(item) for item in fields_raw)
        return cls(mode=mode, fields=fields)


@dataclass(frozen=True)
class KafkaDeliveryOptions:
    mode: KafkaDeliveryMode = KafkaDeliveryMode.AT_LEAST_ONCE
    compression_type: str | None = None
    linger_ms: int | None = None
    batch_num_messages: int | None = None
    request_timeout_ms: int | None = None
    flush_timeout: float = 30.0

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any] | None) -> KafkaDeliveryOptions:
        data = dict(raw or {})
        return cls(
            mode=KafkaDeliveryMode(str(data.get("mode", KafkaDeliveryMode.AT_LEAST_ONCE.value)).lower()),
            compression_type=data.get("compression_type"),
            linger_ms=_optional_int(data.get("linger_ms")),
            batch_num_messages=_optional_int(data.get("batch_num_messages")),
            request_timeout_ms=_optional_int(data.get("request_timeout_ms")),
            flush_timeout=float(data.get("flush_timeout", 30.0)),
        )

    def to_producer_config(self) -> dict[str, Any]:
        cfg: dict[str, Any] = {}
        if self.compression_type:
            cfg["compression.type"] = self.compression_type
        if self.linger_ms is not None:
            cfg["linger.ms"] = self.linger_ms
        if self.batch_num_messages is not None:
            cfg["batch.num.messages"] = self.batch_num_messages
        if self.request_timeout_ms is not None:
            cfg["request.timeout.ms"] = self.request_timeout_ms
        if self.mode == KafkaDeliveryMode.IDEMPOTENT:
            cfg["enable.idempotence"] = True
            cfg.setdefault("acks", "all")
        return cfg


@dataclass(frozen=True)
class KafkaDeleteOptions:
    enabled: bool = False
    tombstone: bool = False
    op_field: str = "__dpone__op"

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any] | None) -> KafkaDeleteOptions:
        data = dict(raw or {})
        return cls(
            enabled=bool(data.get("enabled", False)),
            tombstone=bool(data.get("tombstone", False)),
            op_field=str(data.get("op_field", "__dpone__op")),
        )


@dataclass(frozen=True)
class KafkaSinkOptions:
    topic: str | None = None
    message_format: str = "json"
    envelope: KafkaEnvelopeMode = KafkaEnvelopeMode.FLAT
    always_envelope: bool = False
    key: KafkaKeyOptions = field(default_factory=KafkaKeyOptions)
    delivery: KafkaDeliveryOptions = field(default_factory=KafkaDeliveryOptions)
    deletes: KafkaDeleteOptions = field(default_factory=KafkaDeleteOptions)
    schema_registry: KafkaSchemaRegistryOptions = field(default_factory=KafkaSchemaRegistryOptions)
    headers: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any] | None) -> KafkaSinkOptions:
        data = dict(raw or {})
        envelope = KafkaEnvelopeMode(str(data.get("envelope", KafkaEnvelopeMode.FLAT.value)).lower())
        headers = data.get("headers") or {}
        return cls(
            topic=data.get("topic"),
            message_format=str(data.get("message_format", "json")).lower(),
            envelope=envelope,
            always_envelope=bool(data.get("always_envelope", False)),
            key=KafkaKeyOptions.from_mapping(data.get("key")),
            delivery=KafkaDeliveryOptions.from_mapping(data.get("delivery")),
            deletes=KafkaDeleteOptions.from_mapping(data.get("deletes")),
            schema_registry=KafkaSchemaRegistryOptions.from_mapping(data.get("schema_registry")),
            headers={str(key): str(value) for key, value in headers.items()},
        )

    @property
    def deletes_enabled(self) -> bool:
        return self.deletes.enabled

    def should_envelope(self) -> bool:
        return self.always_envelope or self.envelope == KafkaEnvelopeMode.DPONE


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)
