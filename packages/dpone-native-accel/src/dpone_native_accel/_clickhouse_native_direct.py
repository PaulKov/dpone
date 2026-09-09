"""Direct ClickHouse Native TCP ingest for pre-encoded Native blocks."""

from __future__ import annotations

import socket
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from time import perf_counter
from typing import Any, BinaryIO

from . import _clickhouse_native_protocol as native_protocol

_CLIENT_REVISION = 54_453
_TEMPORARY_TABLES_REVISION = 50_264
_BLOCK_INFO_REVISION = 51_903
_CLIENT_INFO_REVISION = 54_032
_SETTINGS_AS_STRINGS_REVISION = 54_429
_INTERSERVER_SECRET_REVISION = 54_441
_INITIAL_TIME_REVISION = 54_449
_QUOTA_KEY_REVISION = 54_060
_DISTRIBUTED_DEPTH_REVISION = 54_448
_VERSION_PATCH_REVISION = 54_401
_OPENTELEMETRY_REVISION = 54_442
_PARALLEL_REPLICAS_REVISION = 54_453


@dataclass(frozen=True, slots=True)
class NativeInsertRequest:
    """Direct Native insert request without credentials."""

    table: str
    columns: Sequence[str]
    query_id: str | None = None
    settings: Mapping[str, Any] | None = None

    @property
    def query(self) -> str:
        columns_sql = ", ".join(native_protocol.quote_identifier(column) for column in self.columns)
        return f"INSERT INTO {native_protocol.format_table(self.table)} ({columns_sql}) VALUES"


@dataclass(frozen=True, slots=True)
class NativeInsertResult:
    """Direct Native insert evidence returned to dpone core."""

    rows: int
    source_bytes: int
    compressed_bytes: int | None
    blocks: int
    duration_seconds: float
    query_id: str | None
    compression: str
    provider_version: str

    def to_provider_dict(self) -> dict[str, Any]:
        return {
            "backend": "direct",
            "rows": self.rows,
            "source_bytes": self.source_bytes,
            "compressed_bytes": self.compressed_bytes,
            "blocks": self.blocks,
            "duration_seconds": self.duration_seconds,
            "query_id": self.query_id,
            "compression": self.compression,
            "provider_version": self.provider_version,
        }


class NativeBlockFramer:
    """Frame ClickHouse FORMAT Native blocks for Native TCP Data packets."""

    def frame(self, block_payload: bytes, *, protocol_revision: int = _CLIENT_REVISION) -> bytes:
        if protocol_revision < _BLOCK_INFO_REVISION:
            return block_payload
        return native_protocol.block_info() + block_payload

    def empty(self, *, protocol_revision: int = _CLIENT_REVISION) -> bytes:
        return self.frame(
            native_protocol.var_uint(0) + native_protocol.var_uint(0), protocol_revision=protocol_revision
        )

    def row_count(self, block_payload: bytes) -> int:
        _, offset = native_protocol.read_var_uint(block_payload, 0)
        rows, _ = native_protocol.read_var_uint(block_payload, offset)
        return rows


class NativeCompressionCodec:
    """Encode Native protocol compressed blocks using clickhouse-driver codecs."""

    def __init__(self, method: str | None) -> None:
        normalized = str(method or "none").strip().lower()
        self.method = "none" if normalized in {"", "none", "false", "0"} else normalized

    @property
    def enabled(self) -> bool:
        return self.method != "none"

    def encode(self, payload: bytes) -> bytes:
        if not self.enabled:
            return payload
        compressor_cls, city_hash = _compression_dependencies(self.method)
        compressor = compressor_cls()
        compressor.write(payload)
        compressed = bytearray()
        if compressor.method_byte is not None:
            compressed.extend(native_protocol.uint8(compressor.method_byte))
            extra_header_size = 1
        else:
            extra_header_size = 0
        compressed.extend(compressor.get_compressed_data(extra_header_size))
        compressed_payload = bytes(compressed)
        return native_protocol.uint128(city_hash(compressed_payload)) + compressed_payload


class NativePacketWriter:
    """Write ClickHouse Native protocol query and Data packets."""

    def __init__(
        self,
        stream: BinaryIO,
        *,
        include_temporary_table_name: bool,
        compression: str | None,
        protocol_revision: int = _CLIENT_REVISION,
    ) -> None:
        self._stream = stream
        self._include_temporary_table_name = include_temporary_table_name
        self._protocol_revision = protocol_revision
        self._framer = NativeBlockFramer()
        self._codec = NativeCompressionCodec(compression)

    def write_query(self, request: NativeInsertRequest) -> None:
        self._stream.write(native_protocol.var_uint(1))
        native_protocol.write_string(self._stream, request.query_id or "")
        if self._protocol_revision >= _CLIENT_INFO_REVISION:
            self._write_client_info()
        self._write_settings(request.settings or {})
        if self._protocol_revision >= _INTERSERVER_SECRET_REVISION:
            native_protocol.write_string(self._stream, "")
        self._stream.write(native_protocol.var_uint(2))
        self._stream.write(native_protocol.var_uint(1 if self._codec.enabled else 0))
        native_protocol.write_string(self._stream, request.query)
        native_protocol.flush(self._stream)

    def write_data(self, block_payload: bytes, *, table_name: str = "") -> int:
        self._stream.write(native_protocol.var_uint(2))
        if self._include_temporary_table_name:
            native_protocol.write_string(self._stream, table_name)
        framed = self._framer.frame(block_payload, protocol_revision=self._protocol_revision)
        encoded = self._codec.encode(framed)
        self._stream.write(encoded)
        native_protocol.flush(self._stream)
        return len(encoded)

    def write_empty_data(self) -> int:
        self._stream.write(native_protocol.var_uint(2))
        if self._include_temporary_table_name:
            native_protocol.write_string(self._stream, "")
        encoded = self._codec.encode(self._framer.empty(protocol_revision=self._protocol_revision))
        self._stream.write(encoded)
        native_protocol.flush(self._stream)
        return len(encoded)

    def _write_client_info(self) -> None:
        self._stream.write(native_protocol.uint8(1))
        native_protocol.write_string(self._stream, "")
        native_protocol.write_string(self._stream, "")
        native_protocol.write_string(self._stream, "0.0.0.0:0")
        if self._protocol_revision >= _INITIAL_TIME_REVISION:
            self._stream.write(native_protocol.uint64(0))
        self._stream.write(native_protocol.uint8(1))
        native_protocol.write_string(self._stream, native_protocol.safe_user())
        native_protocol.write_string(self._stream, socket.gethostname())
        native_protocol.write_string(self._stream, "dpone-native-accel")
        self._stream.write(native_protocol.var_uint(20))
        self._stream.write(native_protocol.var_uint(10))
        self._stream.write(native_protocol.var_uint(self._protocol_revision))
        if self._protocol_revision >= _QUOTA_KEY_REVISION:
            native_protocol.write_string(self._stream, "")
        if self._protocol_revision >= _DISTRIBUTED_DEPTH_REVISION:
            self._stream.write(native_protocol.var_uint(0))
        if self._protocol_revision >= _VERSION_PATCH_REVISION:
            self._stream.write(native_protocol.var_uint(0))
        if self._protocol_revision >= _OPENTELEMETRY_REVISION:
            self._stream.write(native_protocol.uint8(0))
        if self._protocol_revision >= _PARALLEL_REPLICAS_REVISION:
            self._stream.write(native_protocol.var_uint(0))
            self._stream.write(native_protocol.var_uint(0))
            self._stream.write(native_protocol.var_uint(0))

    def _write_settings(self, settings: Mapping[str, Any]) -> None:
        for key, value in settings.items():
            native_protocol.write_string(self._stream, str(key))
            if self._protocol_revision >= _SETTINGS_AS_STRINGS_REVISION:
                self._stream.write(native_protocol.uint8(0))
                native_protocol.write_string(self._stream, str(value))
            else:
                native_protocol.write_string(self._stream, str(value))
        native_protocol.write_string(self._stream, "")


class ClickHouseNativeProtocolClient:
    """Direct ClickHouse Native protocol client for pre-encoded blocks."""

    def __init__(self, *, connection_factory: Any | None = None, provider_version: str = "unknown") -> None:
        self._connection_factory = connection_factory or _connection_factory()
        self._provider_version = provider_version

    def insert(self, request: Mapping[str, Any]) -> NativeInsertResult:
        credentials = _mapping(request.get("credentials"))
        options = _mapping(request.get("options"))
        insert_request = NativeInsertRequest(
            table=str(request["table"]),
            columns=tuple(str(column) for column in request.get("columns") or ()),
            query_id=str(options["query_id"]) if options.get("query_id") else None,
            settings=_mapping(options.get("settings")) if isinstance(options.get("settings"), Mapping) else {},
        )
        compression = _compression(options.get("compression"))
        connection = self._connection_factory(
            host=str(credentials["host"]),
            port=int(credentials.get("port") or 9000),
            database=str(credentials.get("database") or "default"),
            user=str(credentials.get("user") or "default"),
            password=str(credentials.get("password") or ""),
            secure=bool(credentials.get("secure")),
            compression=False if compression == "none" else compression,
            send_receive_timeout=int(options.get("timeout_seconds") or 3600),
            client_revision=_CLIENT_REVISION,
            disable_reconnect=True,
        )
        started_at = perf_counter()
        rows = source_bytes = encoded_bytes = blocks = 0
        try:
            connection.connect()
            _initialize_driver_context(connection, insert_request.settings or {})
            revision = int(connection.server_info.used_revision)
            writer = NativePacketWriter(
                connection.fout,
                include_temporary_table_name=revision >= _TEMPORARY_TABLES_REVISION,
                compression=compression,
                protocol_revision=revision,
            )
            connection.send_query(insert_request.query, query_id=insert_request.query_id)
            writer.write_empty_data()
            _receive_insert_sample(connection)
            framer = NativeBlockFramer()
            for chunk in request["byte_stream"]:
                payload = bytes(chunk)
                rows += framer.row_count(payload)
                source_bytes += len(payload)
                encoded_bytes += writer.write_data(payload)
                blocks += 1
            encoded_bytes += writer.write_empty_data()
            _drain(connection)
        finally:
            disconnect = getattr(connection, "disconnect", None)
            if callable(disconnect):
                disconnect()
        return NativeInsertResult(
            rows=rows,
            source_bytes=source_bytes,
            compressed_bytes=encoded_bytes if compression != "none" else None,
            blocks=blocks,
            duration_seconds=perf_counter() - started_at,
            query_id=insert_request.query_id,
            compression=compression,
            provider_version=self._provider_version,
        )


def _drain(connection: Any) -> None:
    protocol = _protocol_module()
    benign_packets = {
        protocol.ServerPacketTypes.LOG,
        protocol.ServerPacketTypes.PROGRESS,
        protocol.ServerPacketTypes.PROFILE_EVENTS,
        protocol.ServerPacketTypes.TABLE_COLUMNS,
    }
    timezone_update = getattr(protocol.ServerPacketTypes, "TIMEZONE_UPDATE", None)
    if timezone_update is not None:
        benign_packets.add(timezone_update)
    while True:
        packet = connection.receive_packet()
        if packet.type == protocol.ServerPacketTypes.EXCEPTION:
            raise packet.exception
        if packet.type == protocol.ServerPacketTypes.END_OF_STREAM:
            return
        if packet.type in benign_packets:
            continue
        message = connection.unexpected_packet_message(
            "EndOfStream, Exception, Progress, ProfileEvents, Log", packet.type
        )
        raise RuntimeError(message)


def _receive_insert_sample(connection: Any) -> None:
    protocol = _protocol_module()
    while True:
        packet = connection.receive_packet()
        if packet.type == protocol.ServerPacketTypes.DATA:
            return
        if packet.type == protocol.ServerPacketTypes.EXCEPTION:
            raise packet.exception
        if packet.type in {
            protocol.ServerPacketTypes.LOG,
            protocol.ServerPacketTypes.TABLE_COLUMNS,
        }:
            continue
        message = connection.unexpected_packet_message("Data, Exception, Log or TableColumns", packet.type)
        raise RuntimeError(message)


def _initialize_driver_context(connection: Any, settings: Mapping[str, Any]) -> None:
    context = getattr(connection, "context", None)
    if context is None:
        return
    context.settings = dict(settings)
    context.client_settings = {
        "insert_block_size": 1_048_576,
        "strings_as_bytes": False,
        "strings_encoding": "utf-8",
        "use_numpy": False,
        "opentelemetry_traceparent": None,
        "opentelemetry_tracestate": "",
        "quota_key": "",
        "input_format_null_as_default": False,
        "namedtuple_as_json": True,
        "server_side_params": False,
    }


def _connection_factory() -> Any:
    from clickhouse_driver.connection import Connection

    return Connection


def _protocol_module() -> Any:
    from clickhouse_driver import protocol

    return protocol


def _compression_dependencies(method: str) -> tuple[type[Any], Any]:
    from clickhouse_cityhash.cityhash import CityHash128
    from clickhouse_driver.compression import get_compressor_cls

    return get_compressor_cls(method), CityHash128


def _compression(value: Any) -> str:
    normalized = str(value or "auto").strip().lower()
    if normalized == "auto":
        return "lz4"
    return "none" if normalized in {"", "none", "false", "0"} else normalized


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


__all__ = [
    "ClickHouseNativeProtocolClient",
    "NativeBlockFramer",
    "NativeCompressionCodec",
    "NativeInsertRequest",
    "NativeInsertResult",
    "NativePacketWriter",
]
