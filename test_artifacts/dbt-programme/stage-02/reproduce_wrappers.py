"""Reproduce baseline wrapper observations through public calls and constructor DI.

The real ClickHouse sink executes its staging, ingestion and cleanup code.
Recording connector methods consume synthetic rows without database I/O. No SDK,
module, instance method or private entrypoint is replaced.
"""

from __future__ import annotations

import json
from argparse import ArgumentParser
from logging import getLogger
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from dpone.config import LoadConfig
from dpone.readiness.schema_contracts import SchemaContract
from dpone.runtime.connectors.bulk_text_codec import BulkTextCodec
from dpone.runtime.etl.contract_artifacts import ContractValidatedFileArtifact, PartitionedContractValidationArtifact
from dpone.runtime.etl.file_contract_validation import (
    FileContractValidationError,
    validate_mssql_delimited_file_contract,
)
from dpone.runtime.file_artifacts import FileExportArtifact, PartitionedFileExportArtifact
from dpone.runtime.sinks.clickhouse import ClickHouseSink
from dpone.runtime.sinks.load_payload import LoadPayload

SCHEMA = (("id", "int"),)
EXPECTED_ROWS = [(16909060,), (286397204,)]


class RecordingConnection:
    """Capture rows passed to the public connector connection port."""

    def __init__(self) -> None:
        self.rows: list[tuple[object, ...]] = []

    def execute(self, query: str, rows: list[tuple[object, ...]], **kwargs: object) -> None:
        del query, kwargs
        self.rows.extend(rows)


class RecordingConnector:
    """Provide the constructor-injected subset used by this staging scenario."""

    def __init__(self) -> None:
        self.connection = RecordingConnection()
        self.statements: list[str] = []

    def execute_query(self, query: str, params: object = None) -> int:
        del params
        self.statements.append(query)
        return 0

    def get_records(self, query: str) -> list[tuple[object, ...]]:
        del query
        raise AssertionError("Unexpected catalog query in the bounded staging scenario")


class RecordingStagingManager:
    """Record the public materializer callbacks used by partition admission."""

    def __init__(self) -> None:
        self.creates = 0
        self.loaded: list[FileExportArtifact] = []

    def create(self, config: object, schema: object) -> SimpleNamespace:
        del config, schema
        self.creates += 1
        return SimpleNamespace(row_count=0)

    def load_from_file(self, handle: object, artifact: FileExportArtifact) -> int:
        del handle
        self.loaded.append(artifact)
        return len(EXPECTED_ROWS)


def reproduce(output_dir: Path) -> dict[str, Any]:
    """Return observed failures and positive controls without altering runtime code."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "wrapper-sentinel.bcp"
    path.write_bytes(b"16909060\n286397204\n")
    contract = SchemaContract.from_config(
        {"enforcement": "strict", "columns": {"id": {"type": "integer", "nullable": False}}}
    )
    raw = FileExportArtifact(
        str(path), ["id"], format="mssql-delimited", bulk_text_codec=BulkTextCodec(), rows_exported=2
    )
    receipt = validate_mssql_delimited_file_contract(raw, schema=SCHEMA, contract=contract)
    wrapped = ContractValidatedFileArtifact(
        raw, contract=contract, schema=SCHEMA, run_id="synthetic-run", load_id="synthetic-load"
    )
    config = LoadConfig(
        source_conn_id="synthetic_source",
        target_conn_id="synthetic_target",
        source_schema="sample",
        source_table="rows",
        target_schema="sample",
        target_table="rows",
        options={"clickhouse_bulk": {"mode": "python"}},
    )
    connector = RecordingConnector()
    sink = ClickHouseSink(connector, logger=getLogger("synthetic"))
    handle = sink.stage_payload(config, LoadPayload(raw, SCHEMA))
    assert handle.staged_rows == 2 and connector.connection.rows == EXPECTED_ROWS
    sink.abort_staged_load(handle)
    report: dict[str, Any] = {
        "real_sink_io": "SKIP",
        "entrypoint": "ClickHouseSink.stage_payload",
        "dependency_injection": "ClickHouseSink(connector=RecordingConnector), explicit python bulk",
        "method_replacements": False,
        "raw_file_control": {"status": "PASS", "rows": connector.connection.rows, "staged_rows": handle.staged_rows},
    }
    before = len(connector.connection.rows)
    try:
        sink.stage_payload(config, LoadPayload(wrapped, SCHEMA))
    except AttributeError as exc:
        assert str(exc) == "'ClickHouseSink' object has no attribute 'create'"
        assert len(connector.connection.rows) == before
        assert raw.contract_validation_receipt is receipt
        assert wrapped.terminal_receipt is None and raw.terminal_receipt is None
        report["wrapped_file_dispatch"] = {
            "status": "REPRODUCED",
            "error": str(exc),
            "additional_rows": 0,
            "receipt_identity_preserved": True,
            "terminal_outcome": None,
        }
    else:
        raise AssertionError("Expected baseline wrapper dispatch failure")

    manager = RecordingStagingManager()
    rebound = wrapped.rebind_columns(("target_id",))
    try:
        rebound.materialize(manager, config, (("target_id", "int"),))
    except FileContractValidationError as exc:
        assert exc.blocker == "file_contract_receipt.wire_identity_mismatch" and manager.creates == 0
        report["renamed_wrapper"] = {"status": "REPRODUCED_SAFE_FAILURE", "blocker": exc.blocker}
    else:
        raise AssertionError("Expected baseline source-receipt mismatch after rename")

    unvalidated = FileExportArtifact(
        str(path), ["id"], format="mssql-delimited", bulk_text_codec=BulkTextCodec(), rows_exported=2
    )
    single_hint = ContractValidatedFileArtifact(
        unvalidated,
        contract=contract,
        schema=SCHEMA,
        run_id="synthetic-run",
        load_id="synthetic-load",
        prevalidated=True,
    )
    try:
        single_hint.materialize(manager, config, SCHEMA)
    except FileContractValidationError as exc:
        assert exc.blocker == "file_contract_receipt.required" and manager.creates == 0
        report["single_file_boolean_rejected"] = {"status": "PASS", "blocker": exc.blocker}
    else:
        raise AssertionError("Single-file boolean admitted missing receipt")

    partitions = PartitionedFileExportArtifact([unvalidated], ["id"])
    unchecked = PartitionedContractValidationArtifact(partitions, prevalidated=True)
    assert unchecked.validated_file_contract_artifact is partitions
    result = unchecked.materialize(manager, config, SCHEMA)
    assert result.row_count == 2 and manager.creates == 1
    assert manager.loaded == [unvalidated] and unvalidated.contract_validation_receipt is None
    report["partition_boolean_admission"] = {
        "status": "REPRODUCED",
        "child_receipt": None,
        "rows": result.row_count,
        "default_runtime_reachability": "UNVERIFIED; no production construction site found",
    }
    return report


if __name__ == "__main__":
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    result = reproduce(args.output_dir)
    (args.output_dir / "wrapper-observations.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print("Public-DI wrapper observations reproduced; see wrapper-observations.json. Live certification: SKIP.")
