"""Admission failures must precede extraction and preserve legacy sink selection."""

from types import SimpleNamespace

import pytest

from dpone.manifest.mssql_native_policy import native_limits, native_requested, native_window, validate_native_config
from dpone.runtime.etl.payload_loader_staged_load import supports_staged_load


def config(mode="full_refresh", **limits):
    return SimpleNamespace(
        load_strategy=mode,
        options={
            "source_type": "clickhouse",
            "sink_type": "mssql",
            "native_transfer": {
                "wire": {"mode": "typed_binary", "binary_format": "mssql_native"},
                "execution": {
                    "chunking": {"mode": "bounded_stream", "checkpointing": "resumable", "parallelism": 2},
                    "native_chunks": {
                        "max_total_encoded_bytes": 100000,
                        "stage_allocated_bytes_stop_threshold": 100000,
                        **limits,
                    },
                },
            },
        },
    )


def test_strict_limits_and_legacy_admission():
    value = config()
    assert native_requested(value)
    assert native_limits(value).parallelism == 2
    assert native_window(value) is None
    validate_native_config(value)
    assert not native_requested(SimpleNamespace(options={}))


@pytest.mark.parametrize(
    "values",
    [{"max_rows": True}, {"max_bytes": 0}, {"max_row_bytes": 20000000}, {"max_pending": 65}, {"unexpected": 1}],
)
def test_invalid_bounds_fail(values):
    with pytest.raises(ValueError):
        native_limits(config(**values))


def test_interval_requires_authored_window():
    with pytest.raises(ValueError, match="window"):
        native_window(config("partition_replace"))


def test_staged_port_admission_is_per_config():
    sink = SimpleNamespace(
        stage_payload=lambda: None,
        finalize_staged_load=lambda: None,
        abort_staged_load=lambda: None,
        supports_staged_load_for=lambda cfg: cfg == "native",
    )
    assert supports_staged_load(sink, "native")
    assert not supports_staged_load(sink, "legacy")
    legacy = SimpleNamespace(
        stage_payload=lambda: None, finalize_staged_load=lambda: None, abort_staged_load=lambda: None
    )
    assert supports_staged_load(legacy, "anything")


def test_manifest_scope_is_native_partition_window():
    from dpone.dag.load_config_scope import resolve_load_scopes

    options = {}
    resolve_load_scopes(
        strategy_config={
            "mode": "partition_replace",
            "atomicity": "target_atomic",
            "window": {"column": "observed_at", "anchor": "data_interval_end", "lookback": "P7D"},
        },
        source_options=config().options,
        options=options,
        parse_tracer=None,
    )
    assert "mssql_native_window" in options and "rolling_window" not in options


def test_uncomposed_native_request_fails_before_hydration():
    from dpone.contracts.process_errors import ETLProcessError
    from dpone.runtime.bootstrap_runner import DefaultProcessRunner

    process = SimpleNamespace(
        config=SimpleNamespace(
            name="native", load_config=config(), ensure_runtime_bindings=lambda: pytest.fail("row I/O before admission")
        )
    )
    with pytest.raises(ETLProcessError, match="composition_required"):
        DefaultProcessRunner().run(process)


def test_plan_never_reports_rowbinary_for_mssql_native():
    from dpone.runtime.bulk_wire import BulkWirePlanner

    value = BulkWirePlanner().plan(
        source_type="clickhouse", sink_type="mssql", schema=(), source_options=config().options, sink_options={}
    )
    assert value.binary_format == "mssql_native"
    assert value.input_format == "MSSQLNative"
    assert value.selected_route == "typed_binary_mssql_native"
    with pytest.raises(ValueError, match="route"):
        BulkWirePlanner().plan(
            source_type="mssql", sink_type="clickhouse", schema=(), source_options=config().options, sink_options={}
        )


@pytest.mark.parametrize("name", ["etl-config.schema.json", "etl-batch-manifest.schema.json"])
def test_public_schema_native_limits_and_wire(name):
    import json
    from pathlib import Path

    import jsonschema

    schema = json.loads((Path(__file__).parents[1] / "src/dpone/schema" / name).read_text())
    definitions = schema["definitions"]
    native = config().options["native_transfer"]
    for field, definition in [("wire", "native_transfer_wire_policy"), ("execution", "native_transfer_execution")]:
        validator = jsonschema.Draft7Validator({**definitions[definition], "definitions": definitions})
        assert not list(validator.iter_errors(native[field]))
    invalid = dict(native["execution"], native_chunks={"max_total_encoded_bytes": True})
    assert list(validator.iter_errors(invalid))


def test_native_window_policy_never_derives_scope_from_staging():
    from dpone.config.mssql_strategy_contract_policies import partition_replace_policy

    value = config("partition_replace")
    value.options["mssql_native_window"] = {"column": "observed_at", "anchor": "data_interval_end", "lookback": "P1D"}
    policy = partition_replace_policy(value)
    assert policy.column == "observed_at"
    assert not policy.values_from_staging and not policy.native
    value.options["partition"] = {"column": "observed_at"}
    with pytest.raises(ValueError):
        partition_replace_policy(value)
