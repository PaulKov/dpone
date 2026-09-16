"""An authored transport must never silently execute through the BCP default."""

from types import SimpleNamespace

import pytest

from dpone.contracts.bounded_window import WindowContractError
from dpone.manifest.mssql_native_policy import native_requested, native_transport_policy, validate_native_config
from dpone.readiness.mssql_native_planning import project_mssql_native
from dpone.runtime.native_transfer_execution import NativeTransferExecutionPolicy
from tests.test_mssql_native_policy import config
from tests.test_mssql_native_runtime import runtime


@pytest.mark.parametrize("backend", ["mssql_python", "mssql_sqlclient"])
def test_explicit_transport_roundtrips_without_enabling_execution(backend):
    authored = {"backend": backend, "input": "rows", "max_worker_address_space_bytes": 8 << 30}
    value = config(transport=authored)
    resolved = native_transport_policy(value)
    execution = value.options["native_transfer"]["execution"]
    policy = NativeTransferExecutionPolicy.from_mapping(execution)
    assert resolved is not None
    assert policy.native_bulk_transport == resolved
    assert policy.to_dict()["native_chunks"]["transport"] == resolved.to_dict()
    with pytest.raises(ValueError, match=f"transport_backend_unavailable:{backend}$"):
        validate_native_config(value)


@pytest.mark.parametrize("backend", ["mssql_python", "mssql_sqlclient"])
def test_unwired_transport_rejected_before_any_runtime_effect(tmp_path, backend):
    value, events, _ = runtime(tmp_path)
    authored = {"backend": backend, "input": "rows", "max_worker_address_space_bytes": 8 << 30}
    with pytest.raises(ValueError, match=f"transport_backend_unavailable:{backend}$"):
        value.run(config(transport=authored), owner="test")
    assert events == []


def test_transport_marker_cannot_fall_back_when_route_markers_missing():
    value = SimpleNamespace(options={"native_transfer": {"execution": {"native_chunks": {"transport": None}}}})
    assert native_requested(value)
    with pytest.raises(ValueError, match="transport_invalid"):
        native_transport_policy(value)


def test_omitted_transport_remains_absent_and_bcp_admissible():
    value = config()
    assert native_transport_policy(value) is None
    policy = NativeTransferExecutionPolicy.from_mapping(value.options["native_transfer"]["execution"])
    assert "transport" not in policy.to_dict()["native_chunks"]
    validate_native_config(value)


@pytest.mark.parametrize("backend", ["mssql_python", "mssql_sqlclient"])
def test_offline_plan_reports_requested_backend_unavailable(backend):
    value = config(transport={"backend": backend, "input": "rows", "max_worker_address_space_bytes": 8 << 30})
    plan = {"source": {"type": "clickhouse"}, "sink": {"type": "mssql"}, "type_matrix": {}, "physical_design": {}}
    project_mssql_native(plan, value)
    assert plan["mssql_native"]["bulk_transport"]["backend"] == backend
    assert plan["mssql_native"]["status"] == "backend_runtime_unavailable"
    assert plan["bulk_path"] == "clickhouse_bounded_mssql_native_tds"
    assert plan["native_transfer_route_decision"]["release_gate"] == "backend_runtime_unavailable"


def test_bound_transport_cannot_override_omitted_bcp_config(tmp_path):
    from dpone.contracts.mssql_native_chunks import NativeBulkTransportPolicy

    value, events, _ = runtime(tmp_path)
    original = value.bindings

    def wrong_binding(*args):
        binding = original(*args)
        binding.stage_context.plan.transport = NativeBulkTransportPolicy("mssql_python", "rows", 8 << 30)
        return binding

    value.bindings = wrong_binding
    with pytest.raises(WindowContractError, match="transport_policy_mismatch"):
        value.run(config(), owner="test")
    assert events == ["preflight"]


@pytest.mark.parametrize("backend", ["mssql_python", "mssql_sqlclient"])
@pytest.mark.parametrize("input_mode", ["rows", "arrow"])
@pytest.mark.parametrize("output_format", ["text", "json", "md"])
def test_real_cli_plan_preserves_requested_transport(tmp_path, monkeypatch, capsys, backend, input_mode, output_format):
    from dpone.cli.main import main
    from tests.test_mssql_native_plan_diagnostics import _manifest

    authored = {"backend": backend, "input": input_mode, "max_worker_address_space_bytes": 8 << 30}
    path = _manifest(tmp_path, "transport", authored)
    monkeypatch.chdir(tmp_path)
    with pytest.raises(SystemExit) as error:
        main(["plan", str(path), "--format", output_format])
    assert error.value.code == 0
    output = capsys.readouterr()
    for expected in (backend, input_mode, "backend_runtime_unavailable"):
        assert expected in output.out
    assert not output.err
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("schema_name", ["etl-config.schema.json", "etl-batch-manifest.schema.json"])
def test_transport_schemas_preserve_backend_bounds_and_raw_removal(schema_name):
    import json
    from pathlib import Path

    import jsonschema

    schema = json.loads((Path(__file__).parents[1] / "src/dpone/schema" / schema_name).read_text())
    jsonschema.Draft7Validator.check_schema(schema)
    assert "source_read" not in schema["definitions"]["native_transfer"]["properties"]
    transport = schema["definitions"]["native_transfer_execution"]["properties"]["native_chunks"]["properties"][
        "transport"
    ]
    validator = jsonschema.Draft7Validator(transport)
    for backend in ("mssql_python", "mssql_sqlclient"):
        valid = {"backend": backend, "input": "rows", "max_worker_address_space_bytes": 8 << 30}
        validator.validate(valid)
        for invalid in (None, {**valid, "backend": "bcp"}, {**valid, "batch_rows": True}, {**valid, "unknown": 1}):
            assert list(validator.iter_errors(invalid))
        if backend == "mssql_python":
            assert list(validator.iter_errors({**valid, "max_input_batch_bytes": 64 << 20}))
        else:
            assert list(validator.iter_errors({**valid, "max_worker_address_space_bytes": 64 << 20}))


@pytest.mark.parametrize("backend", ["mssql_python", "mssql_sqlclient"])
@pytest.mark.parametrize("operation", ["import_file", "inspect", "settle"])
def test_bcp_importer_rejects_explicit_transport_before_effects(tmp_path, backend, operation):
    from dataclasses import replace

    from dpone.contracts.mssql_native_chunks import NativeBulkTransportPolicy
    from tests.test_mssql_native_staged_import import importer

    value, plan, _ = importer(tmp_path)
    plan = replace(plan, transport=NativeBulkTransportPolicy(backend, "rows", 8 << 30))

    def unexpected_effect(*args):
        raise AssertionError("BCP effect reached for explicit TDS policy")

    value._assert_lease = unexpected_effect
    args = {
        "import_file": (plan, None, "attempt", None),
        "inspect": (plan, None, None),
        "settle": (plan, "attempt", None),
    }
    with pytest.raises(ValueError, match=f"transport_backend_unavailable:{backend}$"):
        getattr(value, operation)(*args[operation])


@pytest.mark.parametrize(
    "authored",
    [
        None,
        {},
        {"backend": "mssql_python"},
        {"backend": "bcp", "input": "rows", "max_worker_address_space_bytes": 8 << 30},
    ],
)
@pytest.mark.parametrize("output_format", ["text", "json", "md"])
def test_invalid_transport_cli_is_actionable_configuration_error(
    tmp_path, monkeypatch, capsys, caplog, authored, output_format
):
    from dpone.cli.main import main
    from tests.test_mssql_native_plan_diagnostics import _manifest

    path = _manifest(tmp_path, "transport", authored)
    monkeypatch.chdir(tmp_path)
    with pytest.raises(SystemExit) as error:
        main(["plan", str(path), "--format", output_format])
    assert error.value.code == 2
    assert "native_chunks.transport" in caplog.text
    assert "omit transport" in caplog.text
    assert "Traceback" not in caplog.text
    assert capsys.readouterr().out == ""
    assert list(tmp_path.iterdir()) == [path]
