from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

from dpone.runtime.bulk_options import ClickHouseBulkOptionsResolver
from dpone.runtime.physical_chunk_policy import PHYSICAL_CHUNK_SIZE_LITERAL_PATTERN
from dpone.runtime.streaming_transfer import (
    DEFAULT_STREAM_READ_BUFFER_BYTES,
    LEGACY_TARGET_CHUNK_LITERAL_PATTERN,
    MAX_STREAM_READ_BUFFER_BYTES,
    MIN_STREAM_READ_BUFFER_BYTES,
    STREAM_READ_BUFFER_LITERAL_PATTERN,
    STREAMING_TARGET_CHUNK_BYTES_ALIAS,
    StreamingTransferPolicy,
    decide_streaming_route,
)


def test_streaming_transfer_policy_parses_public_manifest_options() -> None:
    policy = StreamingTransferPolicy.from_options(
        {
            "native_transfer": {
                "snapshot": {
                    "streaming": {
                        "mode": "required",
                        "provider": "bcp_pipe",
                        "pipe_mode": "fifo",
                        "read_buffer_bytes": "1MiB",
                        "cleanup_policy": "eager",
                        "delimiter_safety": "advisory",
                    }
                }
            }
        }
    )

    assert policy.required is True
    assert policy.read_buffer_bytes == 1024 * 1024
    assert policy.configured_read_buffer_bytes == 1024 * 1024
    assert policy.deprecated_aliases == ()
    assert policy.pipe_mode == "fifo"


@pytest.mark.parametrize(
    "read_buffer_bytes",
    [MIN_STREAM_READ_BUFFER_BYTES - 1, MAX_STREAM_READ_BUFFER_BYTES + 1],
)
def test_streaming_transfer_policy_rejects_read_buffer_outside_bounds(read_buffer_bytes: int) -> None:
    with pytest.raises(ValueError, match="streaming_read_buffer_bytes_out_of_range"):
        StreamingTransferPolicy(read_buffer_bytes=read_buffer_bytes)


def test_streaming_transfer_policy_rejects_zero_manifest_buffer() -> None:
    with pytest.raises(ValueError, match="streaming_read_buffer_bytes_out_of_range"):
        StreamingTransferPolicy.from_options({"native_transfer": {"snapshot": {"streaming": {"read_buffer_bytes": 0}}}})


def test_streaming_transfer_policy_normalizes_malformed_canonical_buffer_error() -> None:
    with pytest.raises(ValueError, match="streaming_read_buffer_bytes_out_of_range"):
        StreamingTransferPolicy.from_options(
            {"native_transfer": {"snapshot": {"streaming": {"read_buffer_bytes": "garbage"}}}}
        )


def test_streaming_transfer_policy_rejects_fractional_numeric_buffer_without_truncation() -> None:
    with pytest.raises(ValueError, match="streaming_read_buffer_bytes_out_of_range"):
        StreamingTransferPolicy.from_options(
            {"native_transfer": {"snapshot": {"streaming": {"read_buffer_bytes": 1024 * 1024 + 0.5}}}}
        )


@pytest.mark.parametrize("read_buffer_bytes", ["32KiB", "17MiB", "0.5MiB", "65536B"])
def test_streaming_transfer_policy_rejects_manifest_string_outside_canonical_literal_contract(
    read_buffer_bytes: str,
) -> None:
    with pytest.raises(ValueError, match="streaming_read_buffer_bytes_out_of_range"):
        StreamingTransferPolicy.from_options(
            {"native_transfer": {"snapshot": {"streaming": {"read_buffer_bytes": read_buffer_bytes}}}}
        )


@pytest.mark.parametrize(
    "read_buffer_bytes",
    [MIN_STREAM_READ_BUFFER_BYTES, DEFAULT_STREAM_READ_BUFFER_BYTES, MAX_STREAM_READ_BUFFER_BYTES],
)
def test_streaming_transfer_policy_accepts_buffer_boundaries(read_buffer_bytes: int) -> None:
    assert StreamingTransferPolicy(read_buffer_bytes=read_buffer_bytes).read_buffer_bytes == read_buffer_bytes


def test_streaming_transfer_policy_keeps_python_target_constructor_compatibility() -> None:
    policy = StreamingTransferPolicy(target_chunk_bytes=1024 * 1024)

    assert policy.target_chunk_bytes == 1024 * 1024
    assert policy.read_buffer_bytes == DEFAULT_STREAM_READ_BUFFER_BYTES


def test_streaming_transfer_policy_derived_evidence_metadata_is_not_constructor_input() -> None:
    with pytest.raises(TypeError, match="configured_read_buffer_bytes"):
        StreamingTransferPolicy(configured_read_buffer_bytes=2 * 1024 * 1024)  # type: ignore[call-arg]


def test_streaming_transfer_policy_keeps_legacy_alias_with_historical_effective_buffer() -> None:
    policy = StreamingTransferPolicy.from_options(
        {
            "native_transfer": {
                "snapshot": {
                    "streaming": {
                        "mode": "required",
                        "target_chunk_bytes": "512MiB",
                        "delimiter_safety": "advisory",
                    }
                }
            }
        }
    )

    decision = decide_streaming_route(
        policy=policy,
        artifact_format="mssql-delimited",
        bulk_wire_contract=type("Contract", (), {"selected_route": "typed_raw_direct"})(),
        sink_type="ClickHouseConnector",
    )

    assert policy.target_chunk_bytes == 512 * 1024 * 1024
    assert policy.read_buffer_bytes == DEFAULT_STREAM_READ_BUFFER_BYTES
    assert policy.configured_read_buffer_bytes is None
    assert policy.deprecated_aliases == (STREAMING_TARGET_CHUNK_BYTES_ALIAS,)
    assert decision is not None
    assert decision.to_evidence()["configured_read_buffer_bytes"] is None
    assert decision.to_evidence()["effective_read_buffer_bytes"] == DEFAULT_STREAM_READ_BUFFER_BYTES
    assert decision.to_evidence()["deprecated_aliases"] == [STREAMING_TARGET_CHUNK_BYTES_ALIAS]
    assert "streaming_target_chunk_bytes_deprecated_use_read_buffer_bytes" in decision.warnings


def test_streaming_transfer_policy_prefers_canonical_buffer_when_both_fields_are_present() -> None:
    policy = StreamingTransferPolicy.from_options(
        {
            "native_transfer": {
                "snapshot": {
                    "streaming": {
                        "mode": "required",
                        "read_buffer_bytes": "1MiB",
                        "target_chunk_bytes": "512MiB",
                        "delimiter_safety": "advisory",
                    }
                }
            }
        }
    )

    assert policy.read_buffer_bytes == 1024 * 1024
    assert policy.configured_read_buffer_bytes == 1024 * 1024
    assert policy.deprecated_aliases == (STREAMING_TARGET_CHUNK_BYTES_ALIAS,)

    decision = decide_streaming_route(
        policy=policy,
        artifact_format="mssql-delimited",
        bulk_wire_contract=type("Contract", (), {"selected_route": "typed_raw_direct"})(),
        sink_type="ClickHouseConnector",
    )
    assert decision is not None
    assert "streaming_target_chunk_bytes_deprecated_use_read_buffer_bytes" in decision.warnings


def test_streaming_transfer_policy_ignores_malformed_legacy_value_when_canonical_is_present() -> None:
    policy = StreamingTransferPolicy.from_options(
        {
            "native_transfer": {
                "snapshot": {
                    "streaming": {
                        "read_buffer_bytes": "1MiB",
                        "target_chunk_bytes": "garbage",
                    }
                }
            }
        }
    )

    assert policy.read_buffer_bytes == 1024 * 1024
    assert policy.deprecated_aliases == (STREAMING_TARGET_CHUNK_BYTES_ALIAS,)


def test_streaming_transfer_policy_rejects_malformed_legacy_only_value() -> None:
    with pytest.raises(ValueError, match="streaming_target_chunk_bytes_invalid"):
        StreamingTransferPolicy.from_options(
            {"native_transfer": {"snapshot": {"streaming": {"target_chunk_bytes": "garbage"}}}}
        )


@pytest.mark.parametrize("legacy_value", ["", " ", "0", "0MiB", "0.1B", 0, -1, 1.5, "1 2MiB"])
def test_streaming_transfer_policy_rejects_non_positive_or_malformed_legacy_only_value(legacy_value: object) -> None:
    with pytest.raises(ValueError, match="streaming_target_chunk_bytes_invalid"):
        StreamingTransferPolicy.from_options(
            {"native_transfer": {"snapshot": {"streaming": {"target_chunk_bytes": legacy_value}}}}
        )


def test_clickhouse_streaming_async_options_feed_insert_settings() -> None:
    bulk = ClickHouseBulkOptionsResolver.resolve(
        {
            "clickhouse_bulk": {
                "mode": "client",
                "streaming": {
                    "async_insert": True,
                    "wait_for_async_insert": True,
                },
            }
        }
    )

    assert bulk.insert_settings["async_insert"] == 1
    assert bulk.insert_settings["wait_for_async_insert"] == 1


def test_streaming_route_blocks_certified_only_without_delimiter_probe() -> None:
    decision = decide_streaming_route(
        policy=StreamingTransferPolicy(mode="required", delimiter_safety="certified_only"),
        artifact_format="mssql-delimited",
        bulk_wire_contract=type("Contract", (), {"selected_route": "typed_raw_direct"})(),
        sink_type="ClickHouseConnector",
    )

    assert decision is not None
    assert decision.selected_route == "blocked"
    assert "streaming_delimiter_safety_uncertified" in decision.blockers


def test_schema_accepts_altinity_class_streaming_contract() -> None:
    streaming_policies: list[dict[str, object]] = []
    streaming_contracts: list[dict[str, object]] = []
    for path in ("src/dpone/schema/etl-config.schema.json", "src/dpone/schema/etl-batch-manifest.schema.json"):
        schema = json.loads(Path(path).read_text(encoding="utf-8"))
        snapshot_props = schema["definitions"]["native_transfer_snapshot_policy"]["properties"]
        streaming_contract = schema["definitions"]["native_transfer_streaming_policy"]
        streaming = streaming_contract["properties"]
        streaming_contracts.append(streaming_contract)
        streaming_policies.append(streaming)

        assert "streaming" in snapshot_props
        assert _schema_has_enum_value(schema, "typed_raw_streaming_staging")
        assert _schema_has_property(schema, "async_insert")
        assert _schema_has_property(schema, "wait_for_async_insert")
        assert streaming["pipe_mode"]["enum"] == ["fifo", "stdout"]
        read_buffer = streaming["read_buffer_bytes"]
        assert read_buffer["default"] == "4MiB"
        assert read_buffer["oneOf"][0]["pattern"] == STREAM_READ_BUFFER_LITERAL_PATTERN
        assert read_buffer["oneOf"][1] == {
            "type": "integer",
            "minimum": 64 * 1024,
            "maximum": 16 * 1024 * 1024,
        }
        assert read_buffer["description"] == (
            "BCP FIFO read buffer. String literals use whole KiB or MiB values from 64KiB through 16MiB; "
            "integer values are bytes."
        )
        assert streaming["target_chunk_bytes"]["deprecated"] is True
        assert "default" not in streaming["target_chunk_bytes"]
        assert "read_buffer_bytes" in streaming["target_chunk_bytes"]["description"]

        with pytest.raises(ValidationError):
            Draft202012Validator(read_buffer).validate("garbage")
        for invalid_buffer in ("32KiB", "17MiB", "0.5MiB", "65536B"):
            with pytest.raises(ValidationError):
                Draft202012Validator(read_buffer).validate(invalid_buffer)
        for valid_buffer in ("64KiB", "4MiB", "16MiB", MIN_STREAM_READ_BUFFER_BYTES):
            Draft202012Validator(read_buffer).validate(valid_buffer)
        physical = schema["definitions"]["native_transfer_physical_chunking_policy"]["properties"]
        for field in ("target_chunk_bytes", "max_chunk_bytes"):
            assert physical[field]["oneOf"][0]["pattern"] == PHYSICAL_CHUNK_SIZE_LITERAL_PATTERN
            with pytest.raises(ValidationError):
                Draft202012Validator(physical[field]).validate(0)
            with pytest.raises(ValidationError):
                Draft202012Validator(physical[field]).validate("garbage")

        # Decimal-unit syntax is lexical schema validation. The policy parser
        # performs the computed whole-byte lower bound before source I/O.
        Draft202012Validator(physical["max_chunk_bytes"]).validate("0.1B")

        streaming_policy = schema["definitions"]["native_transfer_streaming_policy"]
        legacy_only = streaming_policy["allOf"][0]["then"]["properties"]["target_chunk_bytes"]
        assert legacy_only["oneOf"][0]["pattern"] == LEGACY_TARGET_CHUNK_LITERAL_PATTERN
        for invalid_legacy in ("", " ", "0", "0MiB", 0, -1, "1 2MiB"):
            with pytest.raises(ValidationError):
                Draft202012Validator(streaming_policy).validate({"target_chunk_bytes": invalid_legacy})
        Draft202012Validator(streaming_policy).validate({"target_chunk_bytes": "0.1B"})
        for ignored_legacy in ("garbage", "", "0MiB", 0, -1, 1.5, {"unexpected": "shape"}):
            Draft202012Validator(streaming_policy).validate(
                {"read_buffer_bytes": "1MiB", "target_chunk_bytes": ignored_legacy}
            )

    assert streaming_policies[0] == streaming_policies[1]
    assert streaming_contracts[0] == streaming_contracts[1]


def _schema_has_enum_value(node: object, expected: str) -> bool:
    if isinstance(node, dict):
        values = node.get("enum")
        if isinstance(values, list) and expected in values:
            return True
        return any(_schema_has_enum_value(value, expected) for value in node.values())
    if isinstance(node, list):
        return any(_schema_has_enum_value(value, expected) for value in node)
    return False


def _schema_has_property(node: object, expected: str) -> bool:
    if isinstance(node, dict):
        props = node.get("properties")
        if isinstance(props, dict) and expected in props:
            return True
        return any(_schema_has_property(value, expected) for value in node.values())
    if isinstance(node, list):
        return any(_schema_has_property(value, expected) for value in node)
    return False
