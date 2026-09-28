from __future__ import annotations

from pathlib import Path
from threading import Event, Thread
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome
from dpone.runtime.physical_chunking import (
    PHYSICAL_CHUNKS_SCHEMA_VERSION,
    PhysicalChunkedFileExportArtifact,
    PhysicalChunkPolicy,
    RowBoundaryChunkWriter,
)
from dpone.runtime.source_scan import SourceScanPlanner, SourceShape
from dpone.runtime.sources.strategies.mssql.mssql_queryout_artifacts import MSSQLQueryoutArtifactFactory
from dpone.runtime.sources.strategies.mssql.mssql_single_scan_chunks import BcpSingleScanChunkExporter
from dpone.runtime.sources.strategies.mssql.mssql_source_shape import MSSQLSourceShapeInspector


class CapturingLogger:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, object]]] = []

    def log_etl_progress(self, event: str, payload: dict[str, object]) -> None:
        self.events.append((event, payload))


@pytest.mark.parametrize(
    ("database", "schema", "catalog", "qualified"),
    [
        ("analytics_staging", "clickhouse", "[analytics_staging].", "[analytics_staging].[clickhouse].[orders]"),
        (
            "analytics_staging",
            "analytics_staging.clickhouse",
            "[analytics_staging].",
            "[analytics_staging].[clickhouse].[orders]",
        ),
        (None, "analytics_staging.clickhouse", "[analytics_staging].", "[analytics_staging].[clickhouse].[orders]"),
        (None, "dbo", "", "[dbo].[orders]"),
        ("Business]Store", "o'ps", "[Business]]Store].", "[Business]]Store].[o''ps].[orders]"),
    ],
)
def test_source_shape_queries_the_source_catalog_not_the_work_database(
    database: str | None,
    schema: str,
    catalog: str,
    qualified: str,
) -> None:
    queries: list[str] = []

    def records(query: str, **kwargs):
        queries.append(query)
        return [{"table_kind": "view", "has_seekable_boundary": False, "stats_confidence": "low"}]

    shape = MSSQLSourceShapeInspector(SimpleNamespace(get_records=records)).inspect(
        SimpleNamespace(source_database=database, source_schema=schema, source_table="orders"),
        boundary_column="id",
    )

    assert shape.table_kind == "view"
    assert f"OBJECT_ID(N'{qualified}')" in queries[0]
    for table in ("views", "indexes", "index_columns", "columns", "stats"):
        assert f"{catalog}sys.{table}" in queries[0]
    assert "OBJECTPROPERTY" not in queries[0]
    assert "USE " not in queries[0]


def test_source_scan_auto_selects_single_scan_chunks_for_heap_partitioning() -> None:
    decision = SourceScanPlanner().plan(
        source_options={
            "partitioning": {"column": "doc_date", "bounds": "auto", "num_partitions": 4},
            "native_transfer": {
                "snapshot": {
                    "scan": {"mode": "auto", "heap_policy": "single_scan_chunks", "require_index_for_range": True},
                    "physical_chunking": {"mode": "auto", "target_chunk_bytes": "64MiB"},
                }
            },
        },
        source_shape=SourceShape(table_kind="heap", has_seekable_boundary=False, stats_confidence="low"),
    )

    evidence = decision.to_evidence()

    assert decision.selected_scan == "single_scan_chunks"
    assert "source_heap_range_parallelism_blocked" in decision.warnings
    assert "physical_chunking_enabled" in decision.reasons
    assert evidence["schema_version"] == "dpone.native_transfer.source_scan_decision.v1"


def test_source_scan_auto_keeps_range_partitioning_for_indexed_boundary() -> None:
    decision = SourceScanPlanner().plan(
        source_options={
            "partitioning": {"column": "id", "bounds": "auto", "num_partitions": 8},
            "native_transfer": {"snapshot": {"scan": {"mode": "auto", "require_index_for_range": True}}},
        },
        source_shape=SourceShape(table_kind="clustered", has_seekable_boundary=True, stats_confidence="high"),
    )

    assert decision.selected_scan == "range_partitioned"
    assert decision.blockers == ()
    assert decision.warnings == ()


def test_row_boundary_chunk_writer_never_splits_character_rows(tmp_path: Path) -> None:
    policy = PhysicalChunkPolicy(target_chunk_bytes=5, max_chunk_bytes=32)
    writer = RowBoundaryChunkWriter(
        policy=policy,
        columns=("value",),
        directory=tmp_path,
        format="mssql-delimited",
        row_terminator=b"\n",
    )

    chunks = list(writer.write([b"aa\nbbb", b"\ncccc\n", b"dd\n"]))

    payloads = [Path(chunk.file_path).read_bytes() for chunk in chunks]
    assert payloads == [b"aa\nbbb\n", b"cccc\n", b"dd\n"]
    assert [chunk.row_count for chunk in chunks] == [2, 1, 1]
    assert all(payload.endswith(b"\n") for payload in payloads)


def test_physical_chunked_artifact_loads_and_cleans_each_chunk(tmp_path: Path) -> None:
    policy = PhysicalChunkPolicy(target_chunk_bytes=4, max_chunk_bytes=32)
    writer = RowBoundaryChunkWriter(
        policy=policy,
        columns=("value",),
        directory=tmp_path,
        format="mssql-delimited",
        row_terminator=b"\n",
    )

    artifact = PhysicalChunkedFileExportArtifact(
        chunk_generator=lambda: writer.write([b"a\nb\ncc\n"]),
        columns=("value",),
        evidence_path=tmp_path / "physical_chunks.json",
    )
    loaded_payloads: list[bytes] = []

    inserted = artifact.load_with(
        lambda file_artifact: loaded_payloads.append(Path(file_artifact.file_path).read_bytes()) or 1
    )

    assert inserted == 3
    assert loaded_payloads == [b"a\nb\n", b"cc\n"]
    assert len(list(tmp_path.glob("dpone_physical_chunk_*.bcp"))) == 2
    artifact.terminate(ArtifactTerminalOutcome.SUCCESS)
    assert not list(tmp_path.glob("dpone_physical_chunk_*.bcp"))
    evidence = (tmp_path / "physical_chunks.json").read_text(encoding="utf-8")
    assert PHYSICAL_CHUNKS_SCHEMA_VERSION in evidence
    assert "loaded_to_staging" in evidence
    assert "retained_until_terminal" in evidence


def test_physical_chunked_artifact_propagates_bulk_wire_contract_to_each_chunk(tmp_path: Path) -> None:
    policy = PhysicalChunkPolicy(target_chunk_bytes=4, max_chunk_bytes=32)
    writer = RowBoundaryChunkWriter(
        policy=policy,
        columns=("value",),
        directory=tmp_path,
        format="mssql-delimited",
        row_terminator=b"\n",
    )
    contract = SimpleNamespace(selected_route="typed_raw_direct")
    artifact = PhysicalChunkedFileExportArtifact(
        chunk_generator=lambda: writer.write([b"a\nb\n"]),
        columns=("value",),
        evidence_path=tmp_path / "physical_chunks.json",
        bulk_wire_contract=contract,
    )
    seen_contracts: list[object] = []

    artifact.load_with(lambda file_artifact: seen_contracts.append(file_artifact.bulk_wire_contract) or 1)

    assert seen_contracts == [contract]


def test_mssql_queryout_auto_returns_lazy_chunks_for_heap_before_bounds_scan(tmp_path: Path) -> None:
    connector = _FakeMssqlConnector(table_kind="heap", has_seekable_boundary=False)
    config = _load_config(
        tmp_path,
        scan={"mode": "auto", "heap_policy": "single_scan_chunks", "require_index_for_range": True},
        physical_chunking={"mode": "auto", "target_chunk_bytes": "8MiB"},
    )

    artifact = MSSQLQueryoutArtifactFactory(connector, CapturingLogger(), sink_connector=object()).artifact_for_query(
        config,
        "SELECT [doc_date], [amount] FROM [reporting].[orders]",
        [("doc_date", "date"), ("amount", "decimal(18,2)")],
    )

    assert isinstance(artifact, PhysicalChunkedFileExportArtifact)
    assert artifact.cleanup_policy == "eager"
    assert artifact.source_scan_decision.selected_scan == "single_scan_chunks"
    assert not any("dpone_bounds" in query for query in connector.queries)


def test_mssql_queryout_single_scan_chunks_keep_typed_raw_bulk_wire_contract(tmp_path: Path) -> None:
    connector = _FakeMssqlConnector(table_kind="heap", has_seekable_boundary=False)
    config = _load_config(
        tmp_path,
        scan={"mode": "single_scan", "heap_policy": "single_scan_chunks", "require_index_for_range": True},
        physical_chunking={"mode": "required", "target_chunk_bytes": "8MiB"},
    )
    config.options["native_transfer"]["wire"] = {"mode": "typed_raw", "delimiter_profile": "ascii_control"}
    config.options["clickhouse_bulk"] = {"mode": "http", "ingest_contract": "typed_staging"}

    artifact = MSSQLQueryoutArtifactFactory(
        connector,
        CapturingLogger(),
        sink_connector=ClickHouseConnector(),
    ).artifact_for_query(
        config,
        "SELECT [doc_date], [amount] FROM [reporting].[orders]",
        [("doc_date", "date"), ("amount", "decimal(18,2)")],
    )

    assert isinstance(artifact, PhysicalChunkedFileExportArtifact)
    assert getattr(artifact, "bulk_wire_contract").selected_route == "typed_raw_direct"


def test_mssql_queryout_forced_range_blocks_heap_before_source_io(tmp_path: Path) -> None:
    connector = _FakeMssqlConnector(table_kind="heap", has_seekable_boundary=False)
    config = _load_config(
        tmp_path,
        scan={"mode": "range_partitioned", "require_index_for_range": True},
        physical_chunking={"mode": "off"},
    )

    with pytest.raises(ValueError, match="source_heap_range_parallelism_blocked"):
        MSSQLQueryoutArtifactFactory(connector, CapturingLogger(), sink_connector=object()).artifact_for_query(
            config,
            "SELECT [doc_date], [amount] FROM [reporting].[orders]",
            [("doc_date", "date"), ("amount", "decimal(18,2)")],
        )

    assert not any("dpone_bounds" in query for query in connector.queries)


@pytest.mark.parametrize("strategy", [LoadStrategy.SNAPSHOT_DIFF, LoadStrategy.SCD2])
def test_mssql_native_chunks_reject_strategy_metadata_before_source_io(
    tmp_path: Path,
    strategy: LoadStrategy,
) -> None:
    connector = _FakeMssqlConnector(table_kind="heap", has_seekable_boundary=False)
    config = _native_chunk_config(tmp_path)
    config.load_strategy = strategy

    with pytest.raises(ValueError, match="mssql_bcp_native_strategy_metadata_unsupported"):
        MSSQLQueryoutArtifactFactory(
            connector,
            CapturingLogger(),
            sink_connector=ClickHouseConnector(),
        ).artifact_for_query(config, "SELECT [id] FROM [reporting].[orders]", [("id", "int")])

    assert connector.queries == []
    assert connector.bcp_runner_calls == 0


@pytest.mark.parametrize(
    "schema_contract",
    [
        {"enforcement": "strict"},
        {"columns": {}},
        {"columns": {"id": {"nullable": False}}},
    ],
)
def test_mssql_native_chunks_reject_schema_contract_before_source_io(
    tmp_path: Path,
    schema_contract: dict[str, object],
) -> None:
    connector = _FakeMssqlConnector(table_kind="heap", has_seekable_boundary=False)
    config = _native_chunk_config(tmp_path)
    config.options["schema_contract"] = schema_contract

    with pytest.raises(ValueError, match="mssql_bcp_native_physical_chunks_schema_contract_unsupported"):
        MSSQLQueryoutArtifactFactory(
            connector,
            CapturingLogger(),
            sink_connector=ClickHouseConnector(),
        ).artifact_for_query(config, "SELECT [id] FROM [reporting].[orders]", [("id", "int")])

    assert connector.queries == []
    assert connector.bcp_runner_calls == 0


def test_late_bcp_failure_drops_stage_and_cleans_fifo_and_chunks(tmp_path: Path) -> None:
    connector = _LateFailingBcpConnector(payload=b"1\n2\n")
    artifact = BcpSingleScanChunkExporter(connector, CapturingLogger()).artifact(
        query="SELECT [id] FROM [reporting].[orders]",
        columns=("id",),
        directory=tmp_path,
        bcp_options=SimpleNamespace(row_terminator="\n"),
        policy=PhysicalChunkPolicy(
            mode="required",
            target_chunk_bytes=2,
            max_chunk_bytes=16,
            cleanup_policy="eager",
        ),
        source_table="reporting.orders",
        artifact_format="mssql-delimited",
        bulk_text_codec=None,
        bulk_wire_contract=None,
        source_scan_decision=None,
    )
    staging = _RecordingStagingManager()

    with pytest.raises(RuntimeError, match="late_bcp_failure"):
        artifact.materialize(staging, object(), [("id", "int")])

    assert staging.loaded_payloads == [b"1\n", b"2\n"]
    assert staging.cleanup_calls == 1
    assert connector.process.reaped is True
    assert connector.process.aborted is True
    assert not list(tmp_path.glob("dpone_mssql_single_scan_*.fifo"))
    assert not list(tmp_path.glob("dpone_physical_chunk_*.bcp"))


class _FakeMssqlConnector:
    bcp_path = "bcp"
    trust_server_certificate = "yes"

    def __init__(self, *, table_kind: str, has_seekable_boundary: bool) -> None:
        self.table_kind = table_kind
        self.has_seekable_boundary = has_seekable_boundary
        self.queries: list[str] = []
        self.bcp_runner_calls = 0

    def quote_identifier(self, name: str) -> str:
        return f"[{name}]"

    def get_records(self, query: str, params=None, as_dict: bool = False):
        del params, as_dict
        self.queries.append(query)
        if "dpone_source_shape" in query:
            return [
                {
                    "table_kind": self.table_kind,
                    "has_seekable_boundary": int(self.has_seekable_boundary),
                    "stats_confidence": "low" if self.table_kind == "heap" else "high",
                }
            ]
        raise AssertionError(f"unexpected MSSQL query: {query}")

    def _bcp_runner(self, options=None):
        del options
        self.bcp_runner_calls += 1
        return SimpleNamespace()


class _LateFailingBcpProcess:
    def __init__(self, fifo_path: str, payload: bytes) -> None:
        self._done = Event()
        self.reaped = False
        self.aborted = False
        self._thread = Thread(target=self._write, args=(fifo_path, payload), daemon=True)
        self._thread.start()

    def _write(self, fifo_path: str, payload: bytes) -> None:
        with Path(fifo_path).open("wb", buffering=0) as stream:
            stream.write(payload)
        self._done.set()

    def poll(self) -> int | None:
        return 1 if self._done.is_set() else None

    def wait(self):
        self._thread.join(timeout=2)
        self.reaped = True
        raise RuntimeError("late_bcp_failure")

    def abort(self) -> None:
        self._thread.join(timeout=2)
        self.aborted = True
        self.reaped = True


class _LateFailingBcpConnector:
    def __init__(self, *, payload: bytes) -> None:
        self.payload = payload
        self.process: _LateFailingBcpProcess

    def _bcp_runner(self, options):
        del options
        connector = self

        class _Runner:
            @staticmethod
            def queryout_process(query: str, fifo_path: str) -> _LateFailingBcpProcess:
                del query
                connector.process = _LateFailingBcpProcess(fifo_path, connector.payload)
                return connector.process

        return _Runner()


class _StagingHandle:
    def __init__(self, owner: _RecordingStagingManager) -> None:
        self.owner = owner
        self.row_count = 0

    def cleanup(self) -> None:
        self.owner.cleanup_calls += 1


class _RecordingStagingManager:
    def __init__(self) -> None:
        self.loaded_payloads: list[bytes] = []
        self.cleanup_calls = 0

    def create(self, load_config, schema):
        del load_config, schema
        return _StagingHandle(self)

    def load_from_file(self, handle, artifact) -> int:
        del handle
        payload = Path(artifact.file_path).read_bytes()
        self.loaded_payloads.append(payload)
        return payload.count(b"\n")


class ClickHouseConnector:
    pass


def _load_config(tmp_path: Path, *, scan: dict[str, object], physical_chunking: dict[str, object]) -> LoadConfig:
    return LoadConfig(
        source_conn_id="mssql_source",
        target_conn_id="clickhouse_sink",
        source_schema="reporting",
        source_table="orders",
        target_schema="DWH_Raw",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            "partitioning": {
                "strategy": "auto",
                "column": "doc_date",
                "bounds": "auto",
                "num_partitions": 4,
            },
            "partition_tmp_dir": str(tmp_path),
            "native_transfer": {"snapshot": {"scan": scan, "physical_chunking": physical_chunking}},
        },
    )


def _native_chunk_config(tmp_path: Path) -> LoadConfig:
    config = _load_config(
        tmp_path,
        scan={"mode": "single_scan", "heap_policy": "single_scan_chunks", "require_index_for_range": True},
        physical_chunking={"mode": "required", "target_chunk_bytes": "8MiB"},
    )
    config.options["native_transfer"]["wire"] = {
        "mode": "typed_binary",
        "source_native_format": "bcp_native",
        "binary_format": "native",
    }
    config.options["clickhouse_bulk"] = {"mode": "http", "ingest_contract": "typed_binary_staging"}
    return config


@pytest.mark.parametrize("binary_format,target_format", [("native", "Native"), ("rowbinary", "RowBinary")])
def test_native_required_chunks_preserve_selected_binary_format(tmp_path, binary_format, target_format):
    connector = _FakeMssqlConnector(table_kind="heap", has_seekable_boundary=False)
    config = _load_config(
        tmp_path,
        scan={"mode": "single_scan", "heap_policy": "single_scan_chunks", "require_index_for_range": True},
        physical_chunking={"mode": "required", "target_chunk_bytes": "8MiB"},
    )
    config.options["native_transfer"]["wire"] = {
        "mode": "typed_binary",
        "source_native_format": "bcp_native",
        "binary_format": binary_format,
    }
    config.options["clickhouse_bulk"] = {"mode": "http", "ingest_contract": "typed_binary_staging"}
    artifact = MSSQLQueryoutArtifactFactory(
        connector,
        CapturingLogger(),
        sink_connector=ClickHouseConnector(),
    ).artifact_for_query(config, "SELECT [id] FROM [reporting].[orders]", [("id", "int")])
    assert isinstance(artifact, PhysicalChunkedFileExportArtifact)
    assert artifact.native_wire_contract.target_format == target_format
    assert artifact.bulk_wire_contract.input_format == target_format
    assert artifact.format == "mssql-bcp-native"
