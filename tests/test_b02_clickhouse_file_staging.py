"""Public sink admission and lifecycle contracts using constructor-owned runners."""

import logging

import pytest

from dpone.config.load_config import LoadConfig
from dpone.readiness.schema_contracts import SchemaContract
from dpone.runtime.connectors.bulk_text_codec import BulkTextCodec
from dpone.runtime.etl.contract_artifacts import ContractValidatedFileArtifact
from dpone.runtime.etl.file_contract_validation import validate_mssql_delimited_file_contract
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sinks.clickhouse_sink import ClickHouseSink
from dpone.runtime.sinks.clickhouse_validated_file_models import ClickHouseValidatedFilePolicy, FileConsumptionError
from dpone.runtime.sinks.load_payload import LoadPayload


class NoTargetIO:
    def execute_query(self, *_args, **_kwargs):
        raise AssertionError("Unexpected mutation")

    def get_records(self, *_args, **_kwargs):
        raise AssertionError("Unexpected target read")


@pytest.fixture
def source(tmp_path):
    path = tmp_path / "source.bcp"
    path.write_bytes(b"1\n")
    raw = FileExportArtifact(
        str(path), ["id"], format="mssql-delimited", bulk_text_codec=BulkTextCodec(), rows_exported=1, estimated_rows=9
    )
    schema = (("id", "int"),)
    contract = SchemaContract.from_config({"enforcement": "strict", "columns": {"id": {"type": "integer"}}})
    validate_mssql_delimited_file_contract(raw, schema=schema, contract=contract)
    wrapped = ContractValidatedFileArtifact(raw, contract=contract, schema=schema, run_id="test", load_id="test")
    return raw, wrapped, LoadPayload(wrapped, schema)


@pytest.mark.parametrize("mode", ["auto", "python", "native_tcp", "driver"])
def test_mode_denial_precedes_runner_construction_and_target_io(tmp_path, source, mode):
    raw, wrapped, payload = source

    def forbidden_factory(*_args):
        raise AssertionError("Runner constructed before admission")

    sink = ClickHouseSink(
        NoTargetIO(), logger=logging.getLogger("test"), validated_file_runner_factory=forbidden_factory
    )
    config = LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="sample",
        source_table="rows",
        target_schema="sample",
        target_table="rows",
        options={"clickhouse_bulk": {"mode": mode}},
    )
    policy = ClickHouseValidatedFilePolicy(work_directory=tmp_path / "work", max_spool_bytes=1_048_576)
    with pytest.raises(FileConsumptionError):
        sink.stage_validated_file(config, payload, policy=policy)
    assert wrapped.validation_summary.accepted_rows == 0
    assert raw.contract_validation_receipt.rows_validated == 1


@pytest.mark.parametrize(
    ("dtype", "target"),
    [
        ("float", "Float64"),
        ("datetime", "DateTime64(7)"),
        ("uniqueidentifier", "UUID"),
        ("binary nullable", "Nullable(String)"),
        ("decimal(77,2)", "Decimal(77,2)"),
        ("int", "Int64"),
        ("nvarchar(max)", "FixedString(10)"),
    ],
)
def test_unsupported_type_pairs_reject_even_empty_export_before_runner(tmp_path, dtype, target):
    path = tmp_path / "empty.bcp"
    path.write_bytes(b"")
    schema = (("value", dtype),)
    raw = FileExportArtifact(
        str(path), ["value"], format="mssql-delimited", bulk_text_codec=BulkTextCodec(), rows_exported=0
    )
    contract = SchemaContract.from_config({"enforcement": "strict", "columns": {}})
    validate_mssql_delimited_file_contract(raw, schema=schema, contract=contract)
    wrapper = ContractValidatedFileArtifact(raw, contract=contract, schema=schema, run_id="test", load_id="test")

    def forbidden(*_args):
        raise AssertionError("unsupported type reached runner")

    sink = ClickHouseSink(NoTargetIO(), logger=logging.getLogger("test"), validated_file_runner_factory=forbidden)
    config = LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="sample",
        source_table="rows",
        target_schema="sample",
        target_table="rows",
        options={
            "clickhouse_bulk": {"mode": "http"},
            "physical_design": {"columns": {"value": {"target_type": {"clickhouse": target}}}},
        },
    )
    with pytest.raises(FileConsumptionError):
        sink.stage_validated_file(
            config,
            LoadPayload(wrapper, schema),
            policy=ClickHouseValidatedFilePolicy(work_directory=tmp_path / "work", max_spool_bytes=1_048_576),
        )
    assert not (tmp_path / "work").exists()
    assert wrapper.validation_summary.accepted_rows == 0
