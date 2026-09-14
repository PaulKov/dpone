"""Prepared bytes are checked independently from the source receipt and encoder."""

from time import monotonic

import pytest

from dpone.config.load_config import LoadConfig
from dpone.readiness.schema_contracts import SchemaContract
from dpone.runtime.connectors.bulk_text_codec import BulkTextCodec
from dpone.runtime.etl.contract_artifacts import ContractValidatedFileArtifact
from dpone.runtime.etl.file_contract_validation import validate_mssql_delimited_file_contract
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sinks.clickhouse_physical_types import ClickHousePhysicalColumnTypeResolver
from dpone.runtime.sinks.clickhouse_validated_file_journal import ClickHouseFileAttemptJournal
from dpone.runtime.sinks.clickhouse_validated_file_models import ClickHouseValidatedFilePolicy, FileConsumptionError
from dpone.runtime.sinks.clickhouse_validated_file_preparation import ClickHouseValidatedFilePreparer, make_plan
from dpone.runtime.storage_policy import StoragePreflightService


def test_prepared_wire_preserves_three_distinct_rows_and_original_receipt(tmp_path):
    schema = (("id", "int"), ("text", "nvarchar(max) nullable"), ("binary", "varbinary(max) nullable"))
    wire = b'1001\t\x1dE\t\x1dE\n1002\t\t\n1003\t"quoted"\\N\t00ff\n'
    path = tmp_path / "source.bcp"
    path.write_bytes(wire)
    raw = FileExportArtifact(
        str(path),
        [n for n, _ in schema],
        format="mssql-delimited",
        bulk_text_codec=BulkTextCodec(),
        rows_exported=3,
        estimated_rows=19,
    )
    contract = SchemaContract.from_config({"enforcement": "strict", "columns": {}})
    receipt = validate_mssql_delimited_file_contract(raw, schema=schema, contract=contract)
    wrapper = ContractValidatedFileArtifact(raw, contract=contract, schema=schema, run_id="test", load_id="test")
    config = LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="sample",
        source_table="rows",
        target_schema="sample",
        target_table="rows",
        options={
            "clickhouse_bulk": {"mode": "http"},
            "physical_design": {
                "columns": {
                    name: {"target_type": {"clickhouse": dtype}}
                    for name, dtype in (("id", "Int32"), ("text", "Nullable(String)"), ("binary", "Nullable(String)"))
                }
            },
        },
    )
    policy = ClickHouseValidatedFilePolicy(work_directory=tmp_path / "attempts", max_spool_bytes=1_048_576)
    plan = make_plan(config, schema, resolver=ClickHousePhysicalColumnTypeResolver())
    with wrapper.file_validation_attempt("a" * 32) as attempt:
        with ClickHouseFileAttemptJournal(policy, "a" * 32, storage=StoragePreflightService()) as journal:
            prepared = ClickHouseValidatedFilePreparer(clock=monotonic).prepare(attempt, plan, policy, journal)
            try:
                assert prepared.stream.read() == (
                    b'\xe9\x03\x00\x00\x00\x00\x00\x00\xea\x03\x00\x00\x01\x01\xeb\x03\x00\x00\x00\x0a"quoted"\\N\x00\x02\x00\xff'
                )
                assert prepared.rows == 3
                assert raw.contract_validation_receipt is receipt
                assert path.read_bytes() == wire
                assert wrapper.validation_summary.accepted_rows == 0
            finally:
                prepared.stream.close()
                journal.release_spool()


@pytest.mark.parametrize(
    ("dtype", "target", "wire"),
    [
        ("tinyint", "UInt8", b"256\n"),
        ("tinyint", "UInt8", b"-1\n"),
        ("smallint", "Int16", b"32768\n"),
        ("smallint", "Int16", b"-32769\n"),
        ("int", "Int32", b"2147483648\n"),
        ("bigint", "Int64", b"9223372036854775808\n"),
        ("bit", "Bool", b"2\n"),
        ("bit", "Bool", b"true\n"),
        ("decimal(3,2)", "Decimal(3,2)", b"1.001\n"),
        ("decimal(3,2)", "Decimal(3,2)", b"10.00\n"),
        ("decimal(3,2)", "Decimal(3,2)", b"NaN\n"),
        ("int", "Int32", b"1.5\n"),
        ("int", "Int32", b"\n"),
    ],
)
def test_value_rejection_locates_cell_without_exposing_value(tmp_path, dtype, target, wire):
    schema = (("value", dtype),)
    path = tmp_path / "source.bcp"
    path.write_bytes(wire)
    raw = FileExportArtifact(
        str(path), ["value"], format="mssql-delimited", bulk_text_codec=BulkTextCodec(), rows_exported=1
    )
    contract = SchemaContract.from_config({"enforcement": "strict", "columns": {}})
    validate_mssql_delimited_file_contract(raw, schema=schema, contract=contract)
    wrapper = ContractValidatedFileArtifact(raw, contract=contract, schema=schema, run_id="test", load_id="test")
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
    policy = ClickHouseValidatedFilePolicy(work_directory=tmp_path / "attempts", max_spool_bytes=1_048_576)
    plan = make_plan(config, schema, resolver=ClickHousePhysicalColumnTypeResolver())
    with wrapper.file_validation_attempt("b" * 32) as attempt:
        with ClickHouseFileAttemptJournal(policy, "b" * 32, storage=StoragePreflightService()) as journal:
            with pytest.raises(FileConsumptionError) as error:
                ClickHouseValidatedFilePreparer(clock=monotonic).prepare(attempt, plan, policy, journal)
            assert error.value.blocker == "value_not_representable"
            assert error.value.details == {"column": "value", "row_ordinal": 1}
    assert wrapper.validation_summary.accepted_rows == 0
