from __future__ import annotations

from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.contracts.run_context import RunContext
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.bootstrap_runner import DefaultProcessRunner
from dpone.runtime.columnar_parquet_writer import ParquetChunkWriter
from dpone.runtime.route_runtime import RouteCapabilityBlocked
from dpone.runtime.route_runtime_factory import (
    LoadStepAuditStorageFactory,
    RouteCapabilityRuntimeFactory,
)
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.storage import LocalObjectStorageClient


def test_runtime_factory_returns_none_when_columnar_and_capabilities_are_disabled() -> None:
    assembly = _AssemblySpy()
    factory = RouteCapabilityRuntimeFactory(columnar_assembly=assembly)

    orchestrator = factory.build(load_config=_cfg(options={}), source=object(), sink=object(), logger=_Logger())

    assert orchestrator is None
    assert assembly.calls == 0


def test_runtime_factory_builds_columnar_orchestrator_for_enabled_fast_path(tmp_path) -> None:
    sink = _Sink()
    source = _MssqlSource()
    factory = RouteCapabilityRuntimeFactory(
        columnar_assembly=_real_columnar_assembly(tmp_path, sink_connector=sink.connector)
    )

    orchestrator = factory.build(load_config=_cfg(), source=source, sink=sink, logger=_Logger())

    assert orchestrator is not None
    context = orchestrator.prepare(
        load_config=_cfg(),
        source=source,
        sink=sink,
        load_record=SimpleNamespace(run_id="run-1", load_id="load-1"),
    )
    result = context.extract(
        load_config=_cfg(),
        source=source,
        sink=sink,
        state=None,
        load_record=SimpleNamespace(run_id="run-1", load_id="load-1"),
    )

    assert source.extract_calls == 0
    assert result.artifact.__class__.__name__ == "ObjectStorageColumnarChunkedArtifact"
    assert source.connector.stream_calls == []
    assert [window.row_count for window in result.artifact.iter_windows()] == [2]
    assert source.connector.stream_calls == [("SELECT id, name FROM dbo.orders", 10000)]
    assert context.decision.selected_route_id == "object_storage_pull_s3"
    assert context.evidence["details"]["execution_mode"] == "chunked"


def test_runtime_factory_auto_falls_back_to_single_node_s3_when_s3cluster_probe_fails(tmp_path) -> None:
    sink = _Sink(fail_s3cluster=True)
    source = _MssqlSource()
    factory = RouteCapabilityRuntimeFactory(
        columnar_assembly=_real_columnar_assembly(tmp_path, sink_connector=sink.connector)
    )
    config = _cfg(columnar_pull={"use_cluster_function": "auto", "cluster": "dwh"})

    orchestrator = factory.build(load_config=config, source=source, sink=sink, logger=_Logger())
    assert orchestrator is not None

    context = orchestrator.prepare(
        load_config=config,
        source=source,
        sink=sink,
        load_record=SimpleNamespace(run_id="run-1", load_id="load-1"),
    )

    assert context.decision.selected_route_id == "object_storage_pull_s3"
    assert context.decision.fallback_reason == "sink.cluster_pull_unsupported"


def test_runtime_factory_builds_direct_push_without_object_storage_options(tmp_path) -> None:
    sink = _Sink()
    source = _MssqlSource()
    factory = RouteCapabilityRuntimeFactory(
        columnar_assembly=_real_columnar_assembly(tmp_path, sink_connector=sink.connector)
    )
    config = _cfg(
        mode="required",
        options={
            "run_id": "run-1",
            "runtime": {
                "capabilities": {
                    "mode": "required",
                    "requested_route_id": "direct_push_columnar",
                }
            },
            "native_transfer": {
                "snapshot": {
                    "columnar_fast_path": {
                        "mode": "required",
                        "provider": "direct_push_columnar",
                    }
                }
            },
            "clickhouse_bulk": {"mode": "client"},
        },
    )

    orchestrator = factory.build(load_config=config, source=source, sink=sink, logger=_Logger())
    assert orchestrator is not None
    context = orchestrator.prepare(
        load_config=config,
        source=source,
        sink=sink,
        load_record=SimpleNamespace(run_id="run-1", load_id="load-1"),
    )
    result = context.extract(
        load_config=config,
        source=source,
        sink=sink,
        state=None,
        load_record=SimpleNamespace(run_id="run-1", load_id="load-1"),
    )

    assert context.decision.selected_route_id == "direct_push_columnar"
    assert result.artifact.__class__.__name__ == "LocalColumnarChunkedArtifact"
    assert source.connector.stream_calls == []
    assert [chunk.row_count for chunk in result.artifact.iter_chunks()] == [2]
    assert source.connector.stream_calls == [("SELECT id, name FROM dbo.orders", 10000)]


def test_runtime_factory_keeps_explicit_direct_push_file_mode(tmp_path) -> None:
    sink = _Sink()
    source = _MssqlSource()
    factory = RouteCapabilityRuntimeFactory(
        columnar_assembly=_real_columnar_assembly(tmp_path, sink_connector=sink.connector)
    )
    config = _cfg(
        mode="required",
        options={
            "run_id": "run-1",
            "runtime": {
                "capabilities": {
                    "mode": "required",
                    "requested_route_id": "direct_push_columnar",
                }
            },
            "native_transfer": {
                "snapshot": {
                    "columnar_fast_path": {
                        "mode": "required",
                        "provider": "direct_push_columnar",
                        "direct_push": {"mode": "file"},
                    }
                }
            },
            "clickhouse_bulk": {"mode": "client"},
        },
    )

    orchestrator = factory.build(load_config=config, source=source, sink=sink, logger=_Logger())
    assert orchestrator is not None
    context = orchestrator.prepare(
        load_config=config,
        source=source,
        sink=sink,
        load_record=SimpleNamespace(run_id="run-1", load_id="load-1"),
    )
    result = context.extract(
        load_config=config,
        source=source,
        sink=sink,
        state=None,
        load_record=SimpleNamespace(run_id="run-1", load_id="load-1"),
    )

    assert result.artifact.__class__.__name__ == "LocalColumnarStagingManifest"
    assert source.connector.stream_calls == [("SELECT id, name FROM dbo.orders", 10000)]


def test_runtime_factory_required_blocks_before_source_io_when_named_collection_missing(tmp_path) -> None:
    sink = _Sink(fail_named_collection=True)
    source = _MssqlSource()
    factory = RouteCapabilityRuntimeFactory(
        columnar_assembly=_real_columnar_assembly(tmp_path, sink_connector=sink.connector)
    )
    config = _cfg(mode="required")
    orchestrator = factory.build(load_config=config, source=source, sink=sink, logger=_Logger())
    assert orchestrator is not None

    with pytest.raises(RouteCapabilityBlocked, match="sink.auth.named_collection_missing"):
        orchestrator.prepare(
            load_config=config,
            source=source,
            sink=sink,
            load_record=SimpleNamespace(run_id="run-1", load_id="load-1"),
        )

    assert source.extract_calls == 0
    assert source.connector.stream_calls == []


def test_runtime_factory_auto_falls_back_to_default_extract_when_parquet_writer_missing(tmp_path) -> None:
    sink = _Sink()
    source = _MssqlSource()
    assembly = _real_columnar_assembly(tmp_path, sink_connector=sink.connector, writer=_FakeParquetWriter(False))
    factory = RouteCapabilityRuntimeFactory(columnar_assembly=assembly)
    processor_orchestrator = factory.build(load_config=_cfg(), source=source, sink=sink, logger=_Logger())
    assert processor_orchestrator is not None

    context = processor_orchestrator.prepare(
        load_config=_cfg(),
        source=source,
        sink=sink,
        load_record=SimpleNamespace(run_id="run-1", load_id="load-1"),
    )
    result = context.extract(
        load_config=_cfg(),
        source=source,
        sink=sink,
        state=None,
        load_record=SimpleNamespace(run_id="run-1", load_id="load-1"),
    )

    assert context.decision.selected_route_id == "typed_binary_streaming"
    assert context.decision.fallback_reason == "source.columnar_writer_missing"
    assert source.extract_calls == 1
    assert result.artifact.estimated_rows == 1


def test_default_process_runner_wires_factory_orchestrator_into_processor(tmp_path) -> None:
    sink = _Sink()
    source = _MssqlSource()
    factory = RouteCapabilityRuntimeFactory(
        columnar_assembly=_real_columnar_assembly(tmp_path, sink_connector=sink.connector)
    )
    process = SimpleNamespace(
        config=SimpleNamespace(
            name="factory-run",
            source_obj=source,
            sink_obj=sink,
            load_config=_cfg(),
            etl_logger=_Logger(),
            run_state_storage=None,
            partition_checkpoint_store=None,
            ensure_runtime_bindings=lambda: None,
        ),
        current_state=None,
    )

    result = DefaultProcessRunner(route_capability_factory=factory).run(
        process,
        context=RunContext(run_id="run-1"),
    )

    assert result.status == "success"
    assert result.inserted_rows == 2
    assert result.final_rows == 2
    assert source.extract_calls == 0


def test_load_step_audit_storage_factory_selects_clickhouse_postgres_mssql_and_respects_disabled() -> None:
    factory = LoadStepAuditStorageFactory()

    assert factory.from_sink(_Sink(), _cfg()).__class__.__name__ == "ClickHouseLoadStepAuditStorage"
    assert factory.from_sink(_Sink(name="PostgresSink"), _cfg()).__class__.__name__ == "PostgresLoadStepAuditStorage"
    assert factory.from_sink(_Sink(name="MSSQLSink"), _cfg()).__class__.__name__ == "MSSQLLoadStepAuditStorage"
    assert factory.from_sink(_Sink(), _cfg(audit_enabled=False)) is None


def _real_columnar_assembly(tmp_path, *, sink_connector, writer: ParquetChunkWriter | None = None):
    from dpone.runtime.columnar_runtime_assembly import ColumnarRuntimeAssembly

    return ColumnarRuntimeAssembly(
        object_client=LocalObjectStorageClient(tmp_path / "store"),
        parquet_writer=writer or _FakeParquetWriter(True),
        clickhouse_probe_connector=sink_connector,
    )


def _cfg(
    *,
    mode: str = "auto",
    options: dict | None = None,
    columnar_pull: dict | None = None,
    audit_enabled: bool = True,
) -> LoadConfig:
    if options is not None:
        return LoadConfig(
            source_conn_id="mssql",
            target_conn_id="clickhouse",
            source_schema="dbo",
            source_table="orders",
            target_schema="raw",
            target_table="orders",
            load_strategy=LoadStrategy.FULL_REFRESH,
            options=options,
        )
    return LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="raw",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            "run_id": "run-1",
            "runtime": {"capabilities": {"mode": mode}},
            "load_governance": {"audit": {"enabled": audit_enabled, "state_schema": "etl_state"}},
            "native_transfer": {
                "snapshot": {
                    "columnar_fast_path": {
                        "mode": mode,
                        "provider": "auto",
                        "execution": {
                            "target_chunk_bytes": "64MiB",
                            "max_chunk_bytes": "128MiB",
                        },
                        "object_storage": {
                            "uri_prefix": "s3://dpone-stage/msql/{run_id}/",
                            "format": "parquet",
                            "compression": "zstd",
                            "runtime_access": {
                                "connection_type": "env",
                                "connection_id": "S3_WRITER",
                            },
                            "clickhouse_read_access": {
                                "mode": "named_collection",
                                "named_collection": "dpone_stage",
                            },
                            "preflight": {
                                "require_runtime_write": True,
                                "require_clickhouse_read": True,
                                "require_cluster_read": False,
                            },
                        },
                    }
                }
            },
            "clickhouse_bulk": {"columnar_pull": columnar_pull or {"use_cluster_function": "s3"}},
        },
    )


class _MssqlSource:
    def __init__(self) -> None:
        self.connector = _MssqlConnector()
        self.extract_calls = 0

    def get_incremental_state(self, load_config):
        del load_config
        return None

    def extract(self, load_config, state):
        del load_config, state
        self.extract_calls += 1
        return ExtractResult(artifact=InMemoryRowsArtifact([{"id": 1}]), schema=[("id", "int")])


class _MssqlConnector:
    def __init__(self) -> None:
        self.stream_calls: list[tuple[str, int]] = []

    def fetch_schema(self, schema, table, *, database=None):
        del schema, table, database
        return [("id", "int"), ("name", "nvarchar(50)")]

    def build_select_query(self, schema, table, columns, *, database=None):
        del database
        return f"SELECT {', '.join(columns)} FROM {schema}.{table}"

    def quote_identifier(self, value):
        return f"[{value}]"

    def get_records_streaming(self, query, *, batch_size, as_dict=False):
        assert as_dict is False
        self.stream_calls.append((query, batch_size))
        yield [(1, "alpha"), (2, "beta")]


class _Sink:
    def __init__(
        self,
        *,
        name: str = "ClickHouseSink",
        fail_s3cluster: bool = False,
        fail_named_collection: bool = False,
    ) -> None:
        self.connector = _ClickHouseConnector(
            fail_s3cluster, fail_named_collection, name=name.replace("Sink", "Connector")
        )
        self._name = name

    @property
    def __class__(self):
        return type(self._name, (), {})

    def load(self, load_config, payload):
        del load_config
        if hasattr(payload.artifact, "iter_windows"):
            rows = sum(window.row_count for window in payload.artifact.iter_windows())
            return LoadResult(inserted_rows=rows, updated_rows=0, total_rows=rows)
        rows = getattr(payload.artifact, "estimated_rows", None) or 0
        return LoadResult(inserted_rows=rows, updated_rows=0, total_rows=rows)


class _ClickHouseConnector:
    def __init__(self, fail_s3cluster: bool, fail_named_collection: bool, *, name: str) -> None:
        self.fail_s3cluster = fail_s3cluster
        self.fail_named_collection = fail_named_collection
        self.queries: list[str] = []
        self._name = name

    @property
    def __class__(self):
        return type(self._name, (), {})

    def execute(self, sql: str) -> None:
        self._execute(sql)

    def execute_query(self, sql: str, params=None):
        del params
        self._execute(sql)
        return 2

    def get_records(self, sql: str):
        self.queries.append(sql)
        return [("25.3.1",)]

    def _execute(self, sql: str) -> None:
        self.queries.append(sql)
        if self.fail_s3cluster and "s3Cluster" in sql:
            raise RuntimeError("s3Cluster unavailable")
        if self.fail_named_collection and "dpone_stage" in sql:
            raise RuntimeError("named collection missing")


class _FakeParquetWriter:
    def __init__(self, available: bool) -> None:
        self.available = available

    def is_available(self) -> bool:
        return self.available

    def write_chunk(self, *, local_path, schema, rows, compression: str) -> int:
        del schema, compression
        materialized = list(rows)
        local_path.write_bytes(b"PAR1 fake parquet PAR1")
        return len(materialized)


class _AssemblySpy:
    def __init__(self) -> None:
        self.calls = 0

    def build(self, *, load_config, source, sink):
        del load_config, source, sink
        self.calls += 1
        raise AssertionError("disabled runtime must not build columnar assembly")


class _Logger:
    def log_etl_start(self, payload):
        del payload

    def log_etl_progress(self, event, payload):
        del event, payload

    def log_etl_error(self, message, payload):
        del message, payload

    def log_etl_end(self, payload):
        del payload

    def info(self, message):
        del message

    def warning(self, message):
        del message
