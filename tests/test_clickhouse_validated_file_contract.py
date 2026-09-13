"""Receipted files through the real public ClickHouse staging boundary and DI."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from logging import getLogger
from pathlib import Path
from typing import Any

import pytest

from dpone.config import LoadConfig
from dpone.readiness.schema_contracts import SchemaContract
from dpone.runtime.artifact_integrity import ArtifactIntegrityError
from dpone.runtime.connectors.bulk_text_codec import BulkTextCodec
from dpone.runtime.etl.contract_artifacts import ContractValidatedFileArtifact
from dpone.runtime.etl.file_contract_validation import (
    FileContractValidationError,
    validate_mssql_delimited_file_contract,
)
from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sinks.clickhouse import ClickHouseSink
from dpone.runtime.sinks.load_payload import LoadPayload

SCHEMA = (("id", "int"),)
ROWS = [(16909060,), (286397204,)]


class RecordingConnection:
    def __init__(self, on_insert: Callable[[], None] | None = None) -> None:
        self.calls: list[list[tuple[object, ...]]] = []
        self.on_insert = on_insert

    def execute(self, query: str, rows: list[tuple[object, ...]], **kwargs: object) -> None:
        del query, kwargs
        self.calls.append(list(rows))
        if self.on_insert is not None:
            self.on_insert()


class RecordingConnector:
    """No network: only the public connector port used by the real sink is faked."""

    host, port, database, user, password, secure = "synthetic.invalid", 9000, "sample", "synthetic", "", False

    def __init__(self, on_insert: Callable[[], None] | None = None) -> None:
        self.connection = RecordingConnection(on_insert)
        self.statements: list[str] = []

    def execute_query(self, query: str, params: object = None) -> int:
        del params
        self.statements.append(query)
        return 0

    def get_records(self, query: str) -> list[tuple[object, ...]]:
        del query
        raise AssertionError("Unexpected catalog request in this bounded staging test")


def _config(*, mode: str = "python") -> LoadConfig:
    return LoadConfig(
        source_conn_id="synthetic_source",
        target_conn_id="synthetic_target",
        source_schema="sample",
        source_table="rows",
        target_schema="sample",
        target_table="rows",
        options={"clickhouse_bulk": {"mode": mode, "client": {"command": "synthetic-client"}}},
    )


def _file(tmp_path: Path, *, empty: bool = False, validated: bool = True, estimate: int | None = 999):
    path = tmp_path / "synthetic.bcp"
    path.write_bytes(b"" if empty else b"16909060\n286397204\n")
    raw = FileExportArtifact(
        str(path),
        ["id"],
        format="mssql-delimited",
        bulk_text_codec=BulkTextCodec(),
        rows_exported=0 if empty else 2,
        estimated_rows=estimate,
    )
    contract = SchemaContract.from_config(
        {"enforcement": "strict", "columns": {"id": {"type": "integer", "nullable": False}}}
    )
    if validated:
        validate_mssql_delimited_file_contract(raw, schema=SCHEMA, contract=contract)
    wrapped = ContractValidatedFileArtifact(
        raw,
        contract=contract,
        schema=SCHEMA,
        run_id="synthetic-run",
        load_id="synthetic-load",
        prevalidated=True,
    )
    return path, raw, wrapped, contract


@pytest.mark.parametrize("empty", [False, True])
def test_public_staging_preserves_validated_file_rows_receipt_and_ownership(tmp_path: Path, empty: bool) -> None:
    path, raw, wrapped, _ = _file(tmp_path, empty=empty)
    receipt = raw.contract_validation_receipt
    original_bytes = path.read_bytes()
    expected = [] if empty else ROWS
    raw_connector, wrapped_connector = RecordingConnector(), RecordingConnector()
    raw_sink = ClickHouseSink(raw_connector, logger=getLogger("synthetic"))
    sink = ClickHouseSink(wrapped_connector, logger=getLogger("synthetic"))
    raw_handle = raw_sink.stage_payload(_config(), LoadPayload(raw, SCHEMA))

    handle = sink.stage_payload(_config(), LoadPayload(wrapped, SCHEMA))

    assert handle.staged_rows == raw_handle.staged_rows == len(expected)
    assert [row for call in raw_connector.connection.calls for row in call] == expected
    assert [row for call in wrapped_connector.connection.calls for row in call] == expected
    assert len(wrapped_connector.connection.calls) == (0 if empty else 1)
    assert wrapped.validation_summary.accepted_rows == len(expected)
    assert wrapped.validation_summary.validation_mode == "opaque_file_prevalidated"
    assert wrapped.validated_file_contract_artifact is raw
    assert raw.contract_validation_receipt is receipt
    assert wrapped.terminal_authority is raw.terminal_authority
    assert wrapped.terminal_receipt is None and path.read_bytes() == original_bytes
    raw_sink.abort_staged_load(raw_handle)
    sink.abort_staged_load(handle)
    wrapped.terminate(ArtifactTerminalOutcome.RETAIN_COMMIT_UNKNOWN)
    assert path.read_bytes() == original_bytes


@pytest.mark.parametrize("issued_after_construction", [False, True])
def test_missing_receipt_and_cached_denial_cannot_be_bypassed(tmp_path: Path, issued_after_construction: bool) -> None:
    _, raw, wrapped, contract = _file(tmp_path, validated=False)
    if issued_after_construction:
        validate_mssql_delimited_file_contract(raw, schema=SCHEMA, contract=contract)
    connector = RecordingConnector()
    sink = ClickHouseSink(connector, logger=getLogger("synthetic"))

    with pytest.raises(FileContractValidationError) as caught:
        sink.stage_payload(_config(), LoadPayload(wrapped, SCHEMA))

    assert caught.value.blocker == "file_contract_receipt.required"
    assert connector.connection.calls == []
    assert wrapped.validation_summary.accepted_rows == 0


def _mutate(kind: str, path: Path, raw: FileExportArtifact, contract: SchemaContract) -> None:
    if kind == "bytes":
        path.write_bytes(b"16909061\n286397204\n")
    elif kind == "receipt_removed":
        raw.contract_validation_receipt = None
    elif kind == "receipt_replaced":
        raw.contract_validation_receipt = replace(raw.contract_validation_receipt, validator_version=99)
    elif kind == "columns":
        raw.columns = ["different_id"]
    elif kind == "contract":
        contract.columns.clear()
    else:
        raise AssertionError("Unknown synthetic mutation")


@pytest.mark.parametrize("kind", ["bytes", "receipt_removed", "receipt_replaced", "columns", "contract"])
@pytest.mark.parametrize("during_insert", [False, True])
def test_mutation_blocks_success_before_or_after_exactly_one_insert(
    tmp_path: Path, kind: str, during_insert: bool
) -> None:
    path, raw, wrapped, contract = _file(tmp_path)

    def mutate() -> None:
        _mutate(kind, path, raw, contract)

    connector = RecordingConnector(mutate if during_insert else None)
    sink = ClickHouseSink(connector, logger=getLogger("synthetic"))
    if not during_insert:
        mutate()

    with pytest.raises((ArtifactIntegrityError, FileContractValidationError)):
        sink.stage_payload(_config(), LoadPayload(wrapped, SCHEMA))

    assert len(connector.connection.calls) == int(during_insert)
    assert wrapped.validation_summary.accepted_rows == 0
    assert wrapped.validation_summary.validation_mode == "opaque_file"
    assert any(sql.startswith("DROP TABLE IF EXISTS") for sql in connector.statements)
    assert raw.terminal_receipt is None and wrapped.terminal_receipt is None


def test_connector_failure_is_preserved_without_success_or_retry(tmp_path: Path) -> None:
    _, _, wrapped, _ = _file(tmp_path)
    failure = ConnectionError("synthetic insert failure")

    def fail() -> None:
        raise failure

    connector = RecordingConnector(fail)
    sink = ClickHouseSink(connector, logger=getLogger("synthetic"))
    with pytest.raises(ConnectionError) as caught:
        sink.stage_payload(_config(), LoadPayload(wrapped, SCHEMA))

    assert caught.value is failure
    assert len(connector.connection.calls) == 1
    assert wrapped.validation_summary.accepted_rows == 0
    assert any(sql.startswith("DROP TABLE IF EXISTS") for sql in connector.statements)

    # A later explicit staging attempt can consume the unchanged source receipt.
    next_connector = RecordingConnector()
    next_sink = ClickHouseSink(next_connector, logger=getLogger("synthetic"))
    next_handle = next_sink.stage_payload(_config(), LoadPayload(wrapped, SCHEMA))
    assert next_connector.connection.calls == [ROWS]
    assert next_handle.staged_rows == wrapped.validation_summary.accepted_rows == 2
    next_sink.abort_staged_load(next_handle)


def test_failed_later_consumption_cannot_reuse_a_previous_success_summary(tmp_path: Path) -> None:
    path, _, wrapped, _ = _file(tmp_path)
    connector = RecordingConnector()
    sink = ClickHouseSink(connector, logger=getLogger("synthetic"))
    handle = sink.stage_payload(_config(), LoadPayload(wrapped, SCHEMA))
    assert wrapped.validation_summary.accepted_rows == 2
    sink.abort_staged_load(handle)
    path.write_bytes(b"16909061\n286397204\n")

    with pytest.raises(ArtifactIntegrityError):
        sink.stage_payload(_config(), LoadPayload(wrapped, SCHEMA))

    assert len(connector.connection.calls) == 1
    assert wrapped.validation_summary.accepted_rows == 0
    assert wrapped.validation_summary.validation_mode == "opaque_file"


@pytest.mark.parametrize("estimate", [1, 0, -1])
def test_loader_count_cannot_contradict_validated_file(tmp_path: Path, estimate: int) -> None:
    path, _, wrapped, _ = _file(tmp_path, estimate=estimate)
    loaded: list[bytes] = []

    class RecordingClientRunner:
        def __init__(self, credentials: Any, options: Any) -> None:
            del credentials, options

        def insert_file(self, table: str, columns: list[str], input_path: str) -> None:
            del table, columns
            assert input_path == str(path)
            loaded.append(Path(input_path).read_bytes())

    connector = RecordingConnector()
    sink = ClickHouseSink(connector, logger=getLogger("synthetic"), client_runner_cls=RecordingClientRunner)
    expected = FileContractValidationError if estimate >= 0 else RuntimeError
    with pytest.raises(expected):
        sink.stage_payload(_config(mode="client"), LoadPayload(wrapped, SCHEMA))

    assert loaded == [path.read_bytes()]
    assert wrapped.validation_summary.accepted_rows == 0
    assert any(sql.startswith("DROP TABLE IF EXISTS") for sql in connector.statements)
