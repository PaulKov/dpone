from __future__ import annotations

from io import BytesIO

import pytest

from dpone.runtime.byte_stream_artifacts import ByteStreamArtifact
from dpone.runtime.native_transfer_execution import NativeTransferExecutionPolicy
from dpone.runtime.native_transfer_transport import StreamCapability, TransferTransportResolver


def test_transport_policy_parses_stream_defaults_and_byte_sizes() -> None:
    policy = NativeTransferExecutionPolicy.from_mapping(
        {
            "mode": "auto",
            "transport": {
                "mode": "stream",
                "prefer_streaming": True,
                "fallback_to_file": False,
                "stream_buffer_bytes": "32MiB",
                "checksum": "sha256",
                "max_stream_seconds": 900,
            },
        }
    )

    assert policy.transport.mode == "stream"
    assert policy.transport.prefer_streaming is True
    assert policy.transport.fallback_to_file is False
    assert policy.transport.stream_buffer_bytes == 32 * 1024 * 1024
    assert policy.transport.checksum == "sha256"
    assert policy.transport.max_stream_seconds == 900
    assert policy.to_dict()["transport"]["mode"] == "stream"


def test_transport_resolver_selects_stream_only_when_all_capabilities_match() -> None:
    policy = NativeTransferExecutionPolicy.from_mapping({"transport": {"mode": "auto"}})
    resolver = TransferTransportResolver()

    plan = resolver.resolve(
        policy.transport,
        source=StreamCapability.supported("postgres_copy_stdout"),
        sink=StreamCapability.supported("clickhouse_http_body"),
        codec=StreamCapability.supported("tabseparated_codec"),
    )

    assert plan.transport == "stream"
    assert plan.fallback_reason is None
    assert plan.eligibility.to_dict() == {
        "source": True,
        "sink": True,
        "codec": True,
        "reasons": [],
    }


def test_transport_resolver_preserves_forced_object_transport() -> None:
    policy = NativeTransferExecutionPolicy.from_mapping({"transport": {"mode": "object"}})

    plan = TransferTransportResolver().resolve(
        policy.transport,
        source=StreamCapability.supported("postgres_copy_stdout"),
        sink=StreamCapability.supported("clickhouse_http_body"),
        codec=StreamCapability.supported("tabseparated_codec"),
    )

    assert plan.transport == "object"
    assert plan.eligibility.supported is True


def test_transport_resolver_records_file_fallback_reason() -> None:
    policy = NativeTransferExecutionPolicy.from_mapping({"transport": {"mode": "auto", "fallback_to_file": True}})
    resolver = TransferTransportResolver()

    plan = resolver.resolve(
        policy.transport,
        source=StreamCapability.unsupported("bcp_queryout_is_file_transport"),
        sink=StreamCapability.supported("clickhouse_http_body"),
        codec=StreamCapability.supported("tabseparated_codec"),
    )

    assert plan.transport == "file"
    assert plan.fallback_allowed is True
    assert plan.fallback_reason == "native_transfer_stream_fallback_file_only_source"
    assert "bcp_queryout_is_file_transport" in plan.eligibility.reasons


def test_transport_resolver_uses_canonical_codec_failure_code() -> None:
    policy = NativeTransferExecutionPolicy.from_mapping({"transport": {"mode": "auto", "fallback_to_file": True}})

    plan = TransferTransportResolver().resolve(
        policy.transport,
        source=StreamCapability.supported("postgres_copy_stdout"),
        sink=StreamCapability.supported("clickhouse_http_body"),
        codec=StreamCapability.unsupported("temporal_epoch_codec_not_stream_safe"),
    )

    assert plan.transport == "file"
    assert plan.fallback_reason == "native_transfer_codec_not_stream_safe"


def test_transport_resolver_forced_stream_fails_fast_when_source_is_file_only() -> None:
    policy = NativeTransferExecutionPolicy.from_mapping({"transport": {"mode": "stream", "fallback_to_file": False}})

    with pytest.raises(RuntimeError, match="native_transfer_stream_unsupported"):
        TransferTransportResolver().resolve(
            policy.transport,
            source=StreamCapability.unsupported("bcp_queryout_is_file_transport"),
            sink=StreamCapability.supported("clickhouse_http_body"),
            codec=StreamCapability.supported("tabseparated_codec"),
        )


def test_byte_stream_artifact_counts_bytes_checksums_and_closes_once() -> None:
    closed: list[str] = []
    artifact = ByteStreamArtifact(
        lambda: iter((b"1\talpha\n", b"2\tbeta\n")),
        columns=("id", "name"),
        format="mssql-delimited",
        estimated_rows=2,
        cleanup_callback=lambda: closed.append("closed"),
    )

    assert b"".join(artifact.iter_bytes()) == b"1\talpha\n2\tbeta\n"
    artifact.cleanup()
    artifact.cleanup()

    assert artifact.stats.size_bytes == 15
    assert artifact.stats.sha256.startswith("sha256:")
    assert artifact.stats.chunks == 2
    assert closed == ["closed"]


def test_byte_stream_artifact_can_wrap_binary_file_like_objects() -> None:
    artifact = ByteStreamArtifact.from_binary_reader(
        BytesIO(b"payload"),
        columns=("payload",),
        format="raw",
        chunk_size=3,
    )

    assert list(artifact.iter_bytes()) == [b"pay", b"loa", b"d"]
    assert artifact.stats.size_bytes == 7


def test_byte_stream_artifact_closes_when_consumer_stops_early() -> None:
    closed: list[str] = []
    artifact = ByteStreamArtifact(
        lambda: iter((b"first", b"second")),
        columns=("payload",),
        format="raw",
        cleanup_callback=lambda: closed.append("closed"),
    )

    iterator = artifact.iter_bytes()
    assert next(iterator) == b"first"
    iterator.close()

    assert closed == ["closed"]
