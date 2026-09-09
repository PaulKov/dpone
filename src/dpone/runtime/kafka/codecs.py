"""Kafka message codecs.

The registry-backed codecs are intentionally thin wrappers. They validate that a
Schema Registry client exists and keep the public dpone contract stable while
allowing real Confluent serializers to be wired in future/live paths without
changing source/sink code.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from importlib import import_module
from typing import Any, Protocol


@dataclass(frozen=True)
class CodecContext:
    topic: str
    message_format: str = "json"
    schema_registry_subject: str | None = None
    protobuf_message_class: type | None = None


class MessageCodec(Protocol):
    def serialize(self, row: Mapping[str, Any], schema: Sequence[tuple[str, str]], ctx: CodecContext) -> bytes: ...

    def deserialize(self, payload: bytes | None, ctx: CodecContext) -> dict[str, Any] | None: ...


class JsonRowCodec:
    """Plain JSON row codec used as the default Kafka payload format."""

    def serialize(self, row: Mapping[str, Any], schema: Sequence[tuple[str, str]], ctx: CodecContext) -> bytes:
        del schema, ctx
        return json.dumps(dict(row), ensure_ascii=False, default=_json_default, separators=(",", ":")).encode("utf-8")

    def deserialize(self, payload: bytes | None, ctx: CodecContext) -> dict[str, Any] | None:
        del ctx
        if payload is None:
            return None
        value = json.loads(payload.decode("utf-8"))
        if value is None:
            return None
        if not isinstance(value, dict):
            raise ValueError("Kafka JSON value must decode to an object")
        return value


class _RegistryCodecBase(JsonRowCodec):
    """Base class for Confluent Schema Registry backed codecs."""

    def __init__(self, schema_registry_client: Any, message_format: str):
        if schema_registry_client is None:
            raise ValueError(f"Schema Registry client is required for Kafka message_format={message_format!r}")
        self.schema_registry_client = schema_registry_client
        self.message_format = message_format

    def _serialization_context(self, ctx: CodecContext) -> Any:
        from confluent_kafka.serialization import MessageField, SerializationContext

        return SerializationContext(ctx.topic, MessageField.VALUE)


class JsonSchemaRegistryCodec(_RegistryCodecBase):
    def __init__(self, schema_registry_client: Any):
        super().__init__(schema_registry_client, "json_schema")
        self._serializers: dict[str, Any] = {}
        self._deserializers: dict[str, Any] = {}

    def serialize(self, row: Mapping[str, Any], schema: Sequence[tuple[str, str]], ctx: CodecContext) -> bytes:
        from confluent_kafka.schema_registry.json_schema import JSONSerializer

        schema_text = _columns_to_json_schema(ctx.topic, schema)
        serializer = self._serializers.get(schema_text)
        if serializer is None:
            serializer = JSONSerializer(schema_text, self.schema_registry_client, to_dict=lambda obj, _ctx: dict(obj))
            self._serializers[schema_text] = serializer
        return serializer(dict(row), self._serialization_context(ctx))

    def deserialize(self, payload: bytes | None, ctx: CodecContext) -> dict[str, Any] | None:
        if payload is None:
            return None
        from confluent_kafka.schema_registry.json_schema import JSONDeserializer

        key = ctx.schema_registry_subject or ctx.topic
        deserializer = self._deserializers.get(key)
        if deserializer is None:
            deserializer = JSONDeserializer(None, schema_registry_client=self.schema_registry_client)
            self._deserializers[key] = deserializer
        value = deserializer(payload, self._serialization_context(ctx))
        return dict(value) if value is not None else None


class AvroSchemaRegistryCodec(_RegistryCodecBase):
    def __init__(self, schema_registry_client: Any):
        super().__init__(schema_registry_client, "avro")
        self._serializers: dict[str, Any] = {}
        self._deserializers: dict[str, Any] = {}

    def serialize(self, row: Mapping[str, Any], schema: Sequence[tuple[str, str]], ctx: CodecContext) -> bytes:
        from confluent_kafka.schema_registry.avro import AvroSerializer

        schema_text = _columns_to_avro_schema(ctx.topic, schema)
        serializer = self._serializers.get(schema_text)
        if serializer is None:
            serializer = AvroSerializer(self.schema_registry_client, schema_text, to_dict=lambda obj, _ctx: dict(obj))
            self._serializers[schema_text] = serializer
        return serializer(dict(row), self._serialization_context(ctx))

    def deserialize(self, payload: bytes | None, ctx: CodecContext) -> dict[str, Any] | None:
        if payload is None:
            return None
        from confluent_kafka.schema_registry.avro import AvroDeserializer

        key = ctx.schema_registry_subject or ctx.topic
        deserializer = self._deserializers.get(key)
        if deserializer is None:
            deserializer = AvroDeserializer(self.schema_registry_client, from_dict=lambda obj, _ctx: dict(obj))
            self._deserializers[key] = deserializer
        value = deserializer(payload, self._serialization_context(ctx))
        return dict(value) if value is not None else None


class ProtobufSchemaRegistryCodec(_RegistryCodecBase):
    def __init__(self, schema_registry_client: Any, message_class: type | None = None):
        super().__init__(schema_registry_client, "protobuf")
        self.message_class = message_class
        self._serializer: Any | None = None
        self._deserializer: Any | None = None

    def serialize(self, row: Mapping[str, Any], schema: Sequence[tuple[str, str]], ctx: CodecContext) -> bytes:
        del schema
        from confluent_kafka.schema_registry.protobuf import ProtobufSerializer

        message_class = ctx.protobuf_message_class or self.message_class
        if message_class is None:
            raise ValueError("Kafka protobuf message_format requires protobuf_message_class")
        message = message_class(**dict(row))
        if self._serializer is None:
            self._serializer = ProtobufSerializer(message_class, self.schema_registry_client)
        return self._serializer(message, self._serialization_context(ctx))

    def deserialize(self, payload: bytes | None, ctx: CodecContext) -> dict[str, Any] | None:
        if payload is None:
            return None
        from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer

        message_class = ctx.protobuf_message_class or self.message_class
        if message_class is None:
            raise ValueError("Kafka protobuf message_format requires protobuf_message_class")
        if self._deserializer is None:
            self._deserializer = ProtobufDeserializer(message_class, schema_registry_client=self.schema_registry_client)
        message = self._deserializer(payload, self._serialization_context(ctx))
        return _protobuf_to_dict(message)


def build_message_codec(
    message_format: str,
    *,
    schema_registry_client: Any = None,
    protobuf_message_class: type | str | None = None,
) -> MessageCodec:
    fmt = str(message_format or "json").lower()
    if fmt == "json":
        return JsonRowCodec()
    if fmt == "json_schema":
        return JsonSchemaRegistryCodec(schema_registry_client)
    if fmt == "avro":
        return AvroSchemaRegistryCodec(schema_registry_client)
    if fmt == "protobuf":
        return ProtobufSchemaRegistryCodec(
            schema_registry_client,
            message_class=resolve_protobuf_message_class(protobuf_message_class),
        )
    raise ValueError(f"Unsupported Kafka message_format: {message_format}")


def resolve_protobuf_message_class(value: type | str | None) -> type | None:
    """Resolve a Protobuf generated message class from a class or import path."""

    if value is None or isinstance(value, type):
        return value
    module_name, separator, class_name = value.partition(":")
    if not separator:
        module_name, separator, class_name = value.rpartition(".")
    if not module_name or not class_name:
        raise ValueError(
            "protobuf_message_class must be a generated message class or an "
            "import path like 'package.module:MessageClass'"
        )
    module = import_module(module_name)
    resolved = getattr(module, class_name)
    if not isinstance(resolved, type):
        raise TypeError(f"Resolved protobuf_message_class {value!r} is not a class")
    return resolved


def infer_schema_from_rows(rows: Sequence[Mapping[str, Any]]) -> list[tuple[str, str]]:
    """Infer a deterministic dpone schema from sample rows."""

    ordered: dict[str, str] = {}
    for row in rows:
        for key, value in row.items():
            if key not in ordered or ordered[key] == "text":
                ordered[key] = _infer_type(value)
    return list(ordered.items())


def _infer_type(value: Any) -> str:
    if value is None:
        return "text"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "bigint"
    if isinstance(value, float):
        return "double precision"
    if isinstance(value, Decimal):
        return "numeric"
    if isinstance(value, datetime):
        return "timestamp"
    if isinstance(value, date) and not isinstance(value, datetime):
        return "date"
    if isinstance(value, time):
        return "time"
    if isinstance(value, bytes | bytearray):
        return "bytea"
    if isinstance(value, list | tuple | dict):
        return "json"
    return "text"


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime | date | time):
        return value.isoformat()
    if isinstance(value, bytes | bytearray):
        return value.hex()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _columns_to_json_schema(topic: str, schema: Sequence[tuple[str, str]]) -> str:
    return json.dumps(
        {
            "title": topic.replace(".", "_"),
            "type": "object",
            "properties": {name: {"type": _json_schema_type(dtype)} for name, dtype in schema},
        },
        sort_keys=True,
    )


def _columns_to_avro_schema(topic: str, schema: Sequence[tuple[str, str]]) -> str:
    return json.dumps(
        {
            "type": "record",
            "name": topic.replace(".", "_").replace("-", "_"),
            "fields": [{"name": name, "type": ["null", _avro_type(dtype)], "default": None} for name, dtype in schema],
        },
        sort_keys=True,
    )


def _json_schema_type(dtype: str) -> str:
    t = dtype.lower()
    if any(token in t for token in ("int", "numeric", "decimal", "float", "double")):
        return "number"
    if "bool" in t:
        return "boolean"
    if any(token in t for token in ("json", "array")):
        return "object"
    return "string"


def _avro_type(dtype: str) -> str:
    t = dtype.lower()
    if "bool" in t:
        return "boolean"
    if any(token in t for token in ("bigint", "int8", "int64")):
        return "long"
    if "int" in t:
        return "int"
    if any(token in t for token in ("numeric", "decimal", "float", "double", "real")):
        return "double"
    if "bytes" in t or "bytea" in t or "binary" in t:
        return "bytes"
    return "string"


def _protobuf_to_dict(message: Any) -> dict[str, Any]:
    if hasattr(message, "items"):
        return dict(message.items())
    if hasattr(message, "ListFields"):
        return {field.name: value for field, value in message.ListFields()}
    if hasattr(message, "DESCRIPTOR"):
        return {field.name: getattr(message, field.name) for field in message.DESCRIPTOR.fields}
    return dict(message)
