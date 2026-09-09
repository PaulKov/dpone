from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from threading import Thread
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome
from dpone.runtime.physical_chunking import (
    PhysicalChunkedFileExportArtifact,
    PhysicalChunkLimitExceeded,
    PhysicalChunkPolicy,
    RowBoundaryChunkWriter,
)
from dpone.runtime.sinks.clickhouse_staged_load import ClickHouseStagedLoadService
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sources.strategies.mssql.mssql_queryout_artifacts import MSSQLQueryoutArtifactFactory
from dpone.runtime.sources.strategies.mssql.mssql_single_scan_chunks import BcpSingleScanChunkExporter


class CapturingLogger:
    def log_etl_progress(self, _event: str, _payload: dict[str, object]) -> None:
        return None


def test_clickhouse_stage_is_dropped_when_later_physical_row_exceeds_max(tmp_path: Path) -> None:
    writer = RowBoundaryChunkWriter(
        policy=PhysicalChunkPolicy(target_chunk_bytes=4, max_chunk_bytes=8),
        columns=("value",),
        directory=tmp_path,
        format="mssql-delimited",
    )
    artifact = PhysicalChunkedFileExportArtifact(
        chunk_generator=lambda: writer.write([b"a\nb\nSECRET-ROW\n"]),
        columns=("value",),
        evidence_path=tmp_path / "physical_chunks.json",
    )
    sink = _FailingPhysicalStagedSink()

    with pytest.raises(PhysicalChunkLimitExceeded):
        ClickHouseStagedLoadService(sink).stage(
            _load_config(tmp_path),
            LoadPayload(artifact=artifact, schema=[("value", "String")]),
        )

    assert sink.loaded_chunks == [b"a\nb\n"]
    assert sink.dropped_tables == ["DWH_Raw.orders"]


def test_clickhouse_raw_staging_plan_is_cleaned_after_ambiguous_create_timeout(tmp_path: Path) -> None:
    class Sink:
        def __init__(self) -> None:
            self.dropped_tables: list[str] = []

        @staticmethod
        def _table(config: LoadConfig) -> str:
            return f"{config.target_schema}.{config.target_table}"

        def _drop_table(self, table: str, _config: LoadConfig) -> None:
            self.dropped_tables.append(table)

    sink = Sink()

    def create_then_timeout(_load_config: LoadConfig, _staging_config: LoadConfig, _payload: LoadPayload) -> None:
        raise TimeoutError("ambiguous raw create timeout")

    service = ClickHouseStagedLoadService(
        sink,
        plan_staging_table=lambda load_config: replace(load_config, target_table="orders__staging_planned"),
        create_planned_staging_table=create_then_timeout,
    )

    with pytest.raises(TimeoutError, match="ambiguous raw create timeout"):
        service.stage(_load_config(tmp_path), LoadPayload(artifact=SimpleNamespace(), schema=[("value", "String")]))

    assert sink.dropped_tables == ["DWH_Raw.orders__staging_planned"]


def test_clickhouse_cleanup_attempts_every_staging_table_after_one_drop_fails(
    tmp_path: Path,
) -> None:
    class Sink:
        def __init__(self) -> None:
            self.dropped_tables: list[str] = []

        @staticmethod
        def _table(config: LoadConfig) -> str:
            return f"{config.target_schema}.{config.target_table}"

        def _drop_table(self, table: str, _config: LoadConfig) -> None:
            self.dropped_tables.append(table)
            if len(self.dropped_tables) == 1:
                raise RuntimeError("first staging drop failed")

    sink = Sink()
    service = ClickHouseStagedLoadService(sink)
    config = _load_config(tmp_path)
    handle = SimpleNamespace(
        finalization_config=replace(config, target_table="orders__finalization"),
        decoded_config=replace(config, target_table="orders__decoded"),
        staging_config=replace(config, target_table="orders__raw"),
    )

    with pytest.raises(RuntimeError, match="first staging drop failed"):
        service.cleanup(handle)

    assert sink.dropped_tables == [
        "DWH_Raw.orders__finalization",
        "DWH_Raw.orders__decoded",
        "DWH_Raw.orders__raw",
    ]


def test_single_scan_exporter_terminates_bcp_and_removes_partial_resources_on_oversized_row(
    tmp_path: Path,
) -> None:
    connector = _OversizedBcpConnector(b"safe\n12345678\n")
    exporter = BcpSingleScanChunkExporter(connector, CapturingLogger())

    with pytest.raises(PhysicalChunkLimitExceeded):
        list(
            exporter._export_chunks(
                query="SELECT value FROM source",
                columns=("value",),
                directory=tmp_path,
                bcp_options=SimpleNamespace(row_terminator="\n"),
                policy=PhysicalChunkPolicy(target_chunk_bytes=8, max_chunk_bytes=8),
                source_table="source",
                artifact_format="mssql-delimited",
            )
        )

    assert connector.process is not None
    assert connector.process.terminated is True
    assert not list(tmp_path.glob("*.fifo"))
    assert not list(tmp_path.glob("dpone_physical_chunk_*.bcp"))


def test_loader_failure_remains_primary_when_single_scan_abort_also_fails(tmp_path: Path) -> None:
    connector = _AbortFailingBcpConnector(b"a\nb\n")
    exporter = BcpSingleScanChunkExporter(connector, CapturingLogger())
    artifact = exporter.artifact(
        query="SELECT value FROM source",
        columns=("value",),
        directory=tmp_path,
        bcp_options=SimpleNamespace(row_terminator="\n"),
        policy=PhysicalChunkPolicy(target_chunk_bytes=2, max_chunk_bytes=8),
        source_table="source",
        artifact_format="mssql-delimited",
        bulk_text_codec=None,
        bulk_wire_contract=None,
        source_scan_decision=None,
    )

    with pytest.raises(RuntimeError, match="primary loader failure") as caught:
        artifact.load_with(lambda _artifact: (_ for _ in ()).throw(RuntimeError("primary loader failure")))

    notes = getattr(caught.value, "__notes__", [])
    assert notes == ["physical chunk iterator cleanup failed: process_cleanup_failed"]
    assert "SECRET-ABORT" not in " ".join(notes)
    assert not list(tmp_path.glob("*.fifo"))
    artifact.terminate(ArtifactTerminalOutcome.ABORT)
    assert not list(tmp_path.glob("dpone_physical_chunk_*.bcp"))


def test_mssql_queryout_validates_physical_limits_before_source_io(tmp_path: Path) -> None:
    connector = _ShapeConnector()
    config = _load_config(
        tmp_path,
        physical_chunking={
            "mode": "required",
            "target_chunk_bytes": "9MiB",
            "max_chunk_bytes": "8MiB",
        },
    )

    with pytest.raises(ValueError, match="physical_chunk_target_exceeds_max_bytes"):
        MSSQLQueryoutArtifactFactory(connector, CapturingLogger(), sink_connector=object()).artifact_for_query(
            config,
            "SELECT [doc_date], [amount] FROM [reporting].[orders]",
            [("doc_date", "date"), ("amount", "decimal(18,2)")],
        )

    assert connector.queries == []


class _OversizedBcpConnector:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.process: _OversizedBcpProcess | None = None

    def _bcp_runner(self, options=None):
        del options
        return self

    def queryout_process(self, query: str, output_path: str):
        del query

        def write_payload() -> None:
            with open(output_path, "wb", buffering=0) as handle:
                handle.write(self.payload)

        thread = Thread(target=write_payload, daemon=True)
        thread.start()
        self.process = _OversizedBcpProcess(thread)
        return self.process


class _AbortFailingBcpConnector(_OversizedBcpConnector):
    def queryout_process(self, query: str, output_path: str):
        process = super().queryout_process(query, output_path)
        process.abort_error = RuntimeError("SECRET-ABORT")
        return process


class _OversizedBcpProcess:
    def __init__(self, thread: Thread) -> None:
        self.thread = thread
        self.terminated = False
        self.abort_error: Exception | None = None

    def wait(self):
        raise AssertionError("oversized row must fail before BCP completion")

    def poll(self):
        return None if self.thread.is_alive() else 0

    def abort(self) -> None:
        self.terminated = True
        self.thread.join(timeout=5)
        if self.abort_error is not None:
            raise self.abort_error


class _FailingPhysicalStagedSink:
    def __init__(self) -> None:
        self._staging_decoder = SimpleNamespace(prepare=lambda *_args: (_args[1], None))
        self.loaded_chunks: list[bytes] = []
        self.dropped_tables: list[str] = []

    def _create_payload_staging_table(self, load_config: LoadConfig, payload: LoadPayload) -> LoadConfig:
        del payload
        return load_config

    def _insert_payload(self, load_config: LoadConfig, payload: LoadPayload) -> int:
        del load_config
        return payload.artifact.load_with(
            lambda file_artifact: self.loaded_chunks.append(Path(file_artifact.file_path).read_bytes()) or 1
        )

    @staticmethod
    def _table(load_config: LoadConfig) -> str:
        return f"{load_config.target_schema}.{load_config.target_table}"

    def _drop_table(self, table: str, load_config: LoadConfig) -> None:
        del load_config
        self.dropped_tables.append(table)


class _ShapeConnector:
    bcp_path = "bcp"
    trust_server_certificate = "yes"

    def __init__(self) -> None:
        self.queries: list[str] = []

    def quote_identifier(self, name: str) -> str:
        return f"[{name}]"

    def get_records(self, query: str, params=None, as_dict: bool = False):
        del params, as_dict
        self.queries.append(query)
        raise AssertionError(f"source I/O must not start: {query}")


def _load_config(
    tmp_path: Path,
    *,
    physical_chunking: dict[str, object] | None = None,
) -> LoadConfig:
    return LoadConfig(
        source_conn_id="mssql_source",
        target_conn_id="clickhouse_sink",
        source_schema="reporting",
        source_table="orders",
        target_schema="DWH_Raw",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            "partition_tmp_dir": str(tmp_path),
            "native_transfer": {
                "snapshot": {
                    "scan": {"mode": "single_scan"},
                    "physical_chunking": physical_chunking or {},
                }
            },
        },
    )
