"""Distinct SqlClient policy cannot silently select the Python SDK."""

from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.adapters.mssql_tds_supervisor_process import PythonTdsWorkerLauncher
from dpone.app.mssql_tds_worker_bootstrap import _copy_job
from dpone.app.mssql_tds_worker_request import TdsWorkerJob
from dpone.contracts.mssql_native_chunks import NativeBulkTransportPolicy
from dpone.contracts.strict_json import canonical_json_bytes
from dpone.manifest.mssql_native_policy import validate_native_config
from tests.test_mssql_tds_worker_request import job


def sqlclient(**changes):
    return dict(backend="mssql_sqlclient", input="rows", max_worker_address_space_bytes=8 << 30) | changes


@pytest.mark.parametrize("mode", ["rows", "arrow"])
def test_sqlclient_resolved_batch_identity_roundtrips(mode):
    policy = NativeBulkTransportPolicy.from_mapping(sqlclient(input=mode))
    assert policy.max_input_batch_bytes == 64 << 20
    assert policy.to_dict()["max_input_batch_bytes"] == 64 << 20
    assert NativeBulkTransportPolicy.from_mapping(policy.to_dict()) == policy
    assert replace(policy, max_input_batch_bytes=32 << 20).to_dict() != policy.to_dict()


@pytest.mark.parametrize("value", [None, True, False, "67108864", 1.0, (1 << 20) - 1, (256 << 20) + 1])
def test_sqlclient_batch_bytes_reject_invalid_values(value):
    with pytest.raises(ValueError, match="transport_invalid:max_input_batch_bytes"):
        NativeBulkTransportPolicy.from_mapping(sqlclient(max_input_batch_bytes=value))


@pytest.mark.parametrize("value", [1 << 20, 256 << 20])
def test_sqlclient_batch_bytes_admit_boundaries(value):
    assert NativeBulkTransportPolicy.from_mapping(sqlclient(max_input_batch_bytes=value)).max_input_batch_bytes == value


@pytest.mark.parametrize("value", [64 << 20, (8 << 30) - 1, (16 << 30) + 1])
def test_sqlclient_has_its_own_address_space_admission(value):
    with pytest.raises(ValueError, match="transport_invalid:max_worker_address_space_bytes"):
        NativeBulkTransportPolicy.from_mapping(sqlclient(max_worker_address_space_bytes=value))


@pytest.mark.parametrize("value", [None, 64 << 20])
def test_python_policy_never_accepts_sqlclient_only_authored_field(value):
    with pytest.raises(ValueError, match="transport_invalid"):
        NativeBulkTransportPolicy.from_mapping(sqlclient(backend="mssql_python", max_input_batch_bytes=value))


def test_python_policy_serialization_is_unchanged():
    assert NativeBulkTransportPolicy("mssql_python", "rows", 64 << 20).to_dict() == {
        "backend": "mssql_python",
        "input": "rows",
        "max_worker_address_space_bytes": 64 << 20,
        "batch_rows": 65536,
        "startup_timeout_seconds": 30,
        "operation_timeout_seconds": 300,
        "terminate_timeout_seconds": 10,
        "drop_timeout_seconds": 30,
    }


def test_python_worker_rejects_sqlclient_even_with_matching_identity():
    original = job()
    policy = NativeBulkTransportPolicy.from_mapping(sqlclient())
    identity = replace(original.identity, policy_sha256=sha256(canonical_json_bytes(policy.to_dict())).hexdigest())
    with pytest.raises(ValueError, match="tds_worker_request_invalid"):
        TdsWorkerJob(identity, policy, original.file, original.wire, original.connection_string, original.max_row_bytes)
    with pytest.raises(ValueError, match="tds_backend_mismatch"):
        PythonTdsWorkerLauncher(
            policy=policy, identity=identity, python_executable=Path("/absent"), package_root=Path("/absent")
        )


def test_python_copy_rejects_before_sdk_or_input_access():
    with pytest.raises(ValueError, match="tds_backend_mismatch"):
        _copy_job(SimpleNamespace(policy=NativeBulkTransportPolicy.from_mapping(sqlclient())))


def test_unwired_sqlclient_rejected_before_source_configuration_or_io():
    cfg = SimpleNamespace(options={"native_transfer": {"execution": {"native_chunks": {"transport": sqlclient()}}}})
    with pytest.raises(ValueError, match="transport_backend_unavailable:mssql_sqlclient"):
        validate_native_config(cfg)


@pytest.mark.parametrize("mode", ["rows", "arrow"])
@pytest.mark.parametrize("output_format", ["text", "json", "md"])
def test_offline_plan_reports_sqlclient_dependencies_and_unavailable_runtime(
    tmp_path, monkeypatch, capsys, mode, output_format
):
    from dpone.cli.main import main
    from tests.test_mssql_tds_plan_contract import manifest

    path = manifest(tmp_path, sqlclient(input=mode))
    monkeypatch.chdir(tmp_path)
    with pytest.raises(SystemExit) as error:
        main(["plan", str(path), "--format", output_format])
    assert error.value.code == 0
    output = capsys.readouterr()
    for expected in ("mssql_sqlclient", "max_input_batch_bytes", "backend_runtime_unavailable", ".NET==8.0.31"):
        assert expected in output.out
    assert "mssql-python==" not in output.out
    assert "pyarrow==" not in output.out
    assert not output.err
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("schema_name", ["etl-config.schema.json", "etl-batch-manifest.schema.json"])
def test_schema_sqlclient_limits_and_python_field_exclusion(schema_name):
    import json

    import jsonschema

    schema = json.loads((Path(__file__).parents[1] / "src/dpone/schema" / schema_name).read_text())
    transport = schema["definitions"]["native_transfer_execution"]["properties"]["native_chunks"]["properties"][
        "transport"
    ]
    validator = jsonschema.Draft7Validator(transport)
    for value in (sqlclient(), sqlclient(max_input_batch_bytes=1 << 20), sqlclient(max_input_batch_bytes=256 << 20)):
        validator.validate(value)
    for value in (
        sqlclient(max_worker_address_space_bytes=64 << 20),
        sqlclient(max_input_batch_bytes=None),
        sqlclient(max_input_batch_bytes=False),
        sqlclient(max_input_batch_bytes=(256 << 20) + 1),
        sqlclient(backend="mssql_python", max_input_batch_bytes=64 << 20),
    ):
        assert list(validator.iter_errors(value)), value


def test_journal_binds_sqlclient_engine_and_resolved_byte_budget(tmp_path):
    from dpone.adapters.mssql_native_chunks_journal import NativeChunkJournal
    from dpone.contracts.bounded_window import WindowContractError
    from tests.test_mssql_tds_plan_contract import setup_journal

    policy = NativeBulkTransportPolicy.from_mapping(sqlclient())
    store, lease, plan, journal = setup_journal(tmp_path, transport=policy)
    original = store.load(journal.key)
    assert NativeChunkJournal(store, lease, plan).data == journal.data
    for changed in (
        NativeBulkTransportPolicy("mssql_python", "rows", 8 << 30),
        replace(policy, max_input_batch_bytes=32 << 20),
    ):
        with pytest.raises(WindowContractError, match="journal_identity_changed"):
            NativeChunkJournal(store, lease, replace(plan, transport=changed))
    assert store.load(journal.key) == original
