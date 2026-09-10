"""MSSQL method presence cannot promote legacy paths to staged governance."""

from types import SimpleNamespace

import pytest

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.sinks.mssql import MSSQLSink


def config():
    return LoadConfig(
        "source",
        "target",
        "default",
        "events",
        "dbo",
        "events",
        options={
            "source_type": "clickhouse",
            "sink_type": "mssql",
            "native_transfer": {
                "wire": {"mode": "typed_binary", "binary_format": "mssql_native"},
                "execution": {
                    "chunking": {"mode": "bounded_stream", "checkpointing": "resumable"},
                    "native_chunks": {
                        "max_total_encoded_bytes": 10000000,
                        "stage_allocated_bytes_stop_threshold": 10000000,
                    },
                },
            },
        },
    )


def test_existing_configs_keep_legacy_method_selection():
    sink = MSSQLSink(SimpleNamespace())
    value = config()
    value.options = {}
    assert sink.supports_staged_load_for(value) is False


def test_native_requires_composition_before_source_preflight():
    sink = MSSQLSink(SimpleNamespace())
    with pytest.raises(ValueError, match="staging_composition_required"):
        sink.preflight_before_extract(load_config=config())


def test_native_append_is_not_admitted_by_service_presence():
    sink = MSSQLSink(SimpleNamespace(), native_staged_load_service=object())
    value = config()
    value.load_strategy = LoadStrategy.INCREMENTAL_APPEND
    with pytest.raises(ValueError, match="explicit_window_required"):
        sink.supports_staged_load_for(value)


def test_combined_load_dispatches_the_same_native_lifecycle():
    events = []
    result = object()

    class Service:
        def load(self, config, payload):
            events.append(payload)
            return result

    sink = MSSQLSink(SimpleNamespace(), native_staged_load_service=Service())
    payload = object()
    assert sink.load(config(), payload) is result
    assert events == [payload]
