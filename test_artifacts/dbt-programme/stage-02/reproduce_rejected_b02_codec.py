"""Public-DI reproduction of the rejected B02 candidate's codec blocker."""

import json
import logging
import tempfile
from pathlib import Path

from dpone.config import LoadConfig
from dpone.readiness.schema_contracts import SchemaContract
from dpone.runtime.connectors.bulk_text_codec import BulkTextCodec
from dpone.runtime.etl.contract_artifacts import ContractValidatedFileArtifact
from dpone.runtime.etl.file_contract_validation import validate_mssql_delimited_file_contract
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sinks.clickhouse import ClickHouseSink
from dpone.runtime.sinks.load_payload import LoadPayload


class RecordingConnection:
    def __init__(self):
        self.rows = []

    def execute(self, query, rows, **kwargs):
        self.rows.extend(rows)


class RecordingConnector:
    def __init__(self):
        self.connection = RecordingConnection()
        self.statements = []

    def execute_query(self, query, params=None):
        self.statements.append(query)
        return 0

    def get_records(self, query):
        raise AssertionError("No catalog I/O expected")


codec = BulkTextCodec()
schema = (("id", "int"), ("value", "nvarchar(max) nullable"))
values = ["", None, "tab\ttext", "line\ntext", "carriage\rtext", "marker\x1dtext", '"quoted"', "Юникод"]
contract = SchemaContract.from_config(
    {
        "enforcement": "strict",
        "columns": {
            "id": {"type": "integer", "nullable": False},
            "value": {"type": "string", "nullable": True},
        },
    }
)
config = LoadConfig(
    source_conn_id="synthetic",
    target_conn_id="synthetic",
    source_schema="sample",
    source_table="rows",
    target_schema="sample",
    target_table="rows",
    options={"clickhouse_bulk": {"mode": "python"}},
)
with tempfile.TemporaryDirectory(prefix="dpone-codec-review-") as directory:
    path = Path(directory) / "synthetic.bcp"
    path.write_bytes(
        "".join(
            f"{index}\t{'' if value is None else codec.encode(value)}\n" for index, value in enumerate(values)
        ).encode()
    )
    raw = FileExportArtifact(
        str(path), ["id", "value"], format="mssql-delimited", bulk_text_codec=codec, rows_exported=len(values)
    )
    validate_mssql_delimited_file_contract(raw, schema=schema, contract=contract)
    wrapped = ContractValidatedFileArtifact(
        raw, contract=contract, schema=schema, run_id="synthetic", load_id="synthetic"
    )
    connector = RecordingConnector()
    sink = ClickHouseSink(connector, logger=logging.getLogger("synthetic"))
    handle = sink.stage_payload(config, LoadPayload(wrapped, schema))
    result = {
        "assessed_commit": "69013d51dbdaff578767cee54ae49a2119cebc40",
        "entrypoint": "ClickHouseSink.stage_payload",
        "mode": "python",
        "source_receipt_rows": raw.contract_validation_receipt.rows_validated,
        "accepted_rows": wrapped.validation_summary.accepted_rows,
        "decoded_staging": handle.decoded_config is not None,
        "expected_rows": list(enumerate(values)),
        "driver_rows": connector.connection.rows,
        "logical_fidelity": connector.connection.rows == list(enumerate(values)),
        "database_io": False,
    }
    sink.abort_staged_load(handle)
    print(json.dumps(result, indent=2, ensure_ascii=False))
