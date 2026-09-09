from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.byte_stream_artifacts import ByteStreamArtifact
from dpone.runtime.connectors.mssql_bulk import BcpOptions
from dpone.runtime.export_benchmark import (
    ExportProbeRequest,
    SourceExportBenchmarkService,
    SourceExportProbeRunner,
)
from dpone.runtime.export_optimizer_models import ExportOptimizerPolicy, ExportProviderProbe
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.source_export_optimizer import SourceExportOptimizer
from dpone.runtime.sources.strategies.mssql.mssql_export_benchmark import (
    MssqlBcpProbeRunner,
    MssqlOdbcArrayProbeRunner,
)
from dpone.runtime.sources.strategies.mssql.mssql_queryout_artifacts import MSSQLQueryoutArtifactFactory


def test_export_benchmark_service_runs_candidates_and_selects_fastest_safe_provider(tmp_path: Path) -> None:
    service = SourceExportBenchmarkService(
        runners=[
            _StaticProbeRunner(ExportProviderProbe(provider_id="mssql_bcp_native", rows_per_second=100_000)),
            _StaticProbeRunner(ExportProviderProbe(provider_id="mssql_odbc_array", rows_per_second=160_000)),
            _StaticProbeRunner(
                ExportProviderProbe(
                    provider_id="range_partitioned",
                    rows_per_second=240_000,
                    blockers=("source_heap_range_parallelism_blocked",),
                )
            ),
        ],
        optimizer=SourceExportOptimizer(),
    )

    decision = service.benchmark(
        ExportOptimizerPolicy(candidates=("mssql_bcp_native", "mssql_odbc_array", "range_partitioned")),
        current_default="mssql_bcp_native",
        request=_probe_request(tmp_path),
    )

    assert decision.selected_provider == "mssql_odbc_array"
    assert decision.recommended_provider == "mssql_odbc_array"
    assert decision.rejected["range_partitioned"] == "source_heap_range_parallelism_blocked"
    assert decision.measured_speedup_pct == 60.0


def test_mssql_bcp_probe_runner_tests_packet_sizes_and_cleans_temp_files(tmp_path: Path) -> None:
    clock = _FakeClock()
    connector = _FakeBcpConnector(tmp_path, clock)
    runner = MssqlBcpProbeRunner(
        provider_id="mssql_bcp_native",
        connector=connector,
        file_format="native",
        packet_sizes=(4096, 65535),
        clock=clock,
    )

    probe = runner.probe(_probe_request(tmp_path, probe_rows=5000))

    assert probe.provider_id == "mssql_bcp_native"
    assert probe.rows_per_second == 250_000.0
    assert probe.bytes_per_second == 25_000_000.0
    assert "bcp_packet_size=65535" in probe.reasons
    assert all("SELECT TOP (5000)" in query for query, _options in connector.calls)
    assert [options.packet_size for _query, options in connector.calls] == [4096, 65535]
    assert not list(tmp_path.glob("dpone_export_probe_*.bcp"))


def test_mssql_odbc_array_probe_runner_counts_batches_without_materializing_all_rows(tmp_path: Path) -> None:
    connector = _FakeOdbcConnector(
        batches=[
            [(1, "abc"), (2, "def")],
            [(3, "ghi")],
        ]
    )
    clock = _FakeClock(step=0.5)
    runner = MssqlOdbcArrayProbeRunner(connector=connector, fetch_size=2, clock=clock)

    probe = runner.probe(_probe_request(tmp_path, probe_rows=3))

    assert probe.provider_id == "mssql_odbc_array"
    assert probe.rows_per_second == 3.0
    assert probe.bytes_per_second and probe.bytes_per_second > 0
    assert connector.requests == [("SELECT TOP (3) * FROM (SELECT id, value FROM dbo.t) AS dpone_export_probe", 2)]


def test_mssql_queryout_factory_returns_odbc_array_rowbinary_artifact_when_optimizer_selects_it(tmp_path: Path) -> None:
    connector = _FakeOdbcConnector(batches=[[{"id": 1}, {"id": 2}]])
    config = LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        batch_size=2,
        options={
            "partition_tmp_dir": str(tmp_path),
            "native_transfer": {
                "wire": {"mode": "typed_binary", "source_native_format": "bcp_native", "binary_format": "rowbinary"},
                "snapshot": {
                    "export_optimizer": {"mode": "auto", "min_speedup_pct": 15},
                    "export_optimizer_probes": [
                        {"provider_id": "mssql_bcp_native", "rows_per_second": 100000},
                        {"provider_id": "mssql_odbc_array", "rows_per_second": 180000},
                    ],
                },
            },
            "clickhouse_bulk": {"ingest_contract": "typed_binary_staging"},
        },
    )

    artifact = MSSQLQueryoutArtifactFactory(
        connector,
        SimpleNamespace(log_etl_progress=lambda *_args, **_kwargs: None),
        sink_connector=ClickHouseConnector(),
    ).artifact_for_query(config, "SELECT id FROM dbo.orders", [("id", "int")])

    assert isinstance(artifact, ByteStreamArtifact)
    assert artifact.format == "clickhouse-rowbinary"
    assert getattr(artifact, "source_export_provider") == "mssql_odbc_array"
    assert getattr(artifact, "bulk_wire_contract").selected_route == "typed_binary_bcp_native"
    assert not connector.bcp_calls


def test_mssql_queryout_factory_treats_yaml_false_export_optimizer_mode_as_disabled(tmp_path: Path) -> None:
    connector = _FakeOdbcConnector(batches=[[{"id": 1}, {"id": 2}]])
    config = LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        batch_size=2,
        options={
            "runtime_storage": {"work_dir": str(tmp_path)},
            "native_transfer": {
                "wire": {"mode": "typed_raw", "delimiter_profile": "ascii_control"},
                "snapshot": {
                    "export_optimizer": {"mode": False, "min_speedup_pct": 0},
                    "export_optimizer_probes": [
                        {"provider_id": "mssql_bcp_character_raw", "rows_per_second": 100000},
                        {"provider_id": "mssql_odbc_array", "rows_per_second": 999999},
                    ],
                },
            },
            "clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_staging"},
        },
    )

    artifact = MSSQLQueryoutArtifactFactory(
        connector,
        SimpleNamespace(log_etl_progress=lambda *_args, **_kwargs: None),
        sink_connector=ClickHouseConnector(),
    ).artifact_for_query(config, "SELECT id FROM dbo.orders", [("id", "int")])

    assert isinstance(artifact, FileExportArtifact)
    assert artifact.format == "mssql-delimited"
    assert getattr(artifact, "bulk_wire_contract").selected_route == "typed_raw_direct"
    assert connector.requests == []
    assert connector.bcp_calls == ["SELECT id FROM dbo.orders"]


def test_export_optimizer_schema_exposes_probe_runtime_knobs() -> None:
    for path in ("src/dpone/schema/etl-config.schema.json", "src/dpone/schema/etl-batch-manifest.schema.json"):
        schema = json.loads(Path(path).read_text(encoding="utf-8"))
        optimizer = schema["definitions"]["native_transfer_export_optimizer_policy"]["properties"]

        assert optimizer["bcp_probe_packets"]["default"] == [16384, 32768, 65535]
        assert optimizer["odbc_fetch_size"]["default"] == 50000


class _StaticProbeRunner(SourceExportProbeRunner):
    def __init__(self, probe: ExportProviderProbe) -> None:
        self.provider_id = probe.provider_id
        self._probe = probe

    def probe(self, request: ExportProbeRequest) -> ExportProviderProbe:
        return self._probe


class _FakeClock:
    def __init__(self, *, step: float = 0.0) -> None:
        self.value = 0.0
        self.step = step

    def __call__(self) -> float:
        current = self.value
        self.value += self.step
        return current

    def advance(self, seconds: float) -> None:
        self.value += seconds


class _FakeBcpConnector:
    bcp_path = "bcp"
    trust_server_certificate = "yes"

    def __init__(self, directory: Path, clock: _FakeClock) -> None:
        self.directory = directory
        self.clock = clock
        self.calls: list[tuple[str, BcpOptions]] = []

    def bcp_queryout(self, query: str, output_path: str, *, options: BcpOptions | None = None) -> int:
        assert options is not None
        self.calls.append((query, options))
        if options.packet_size == 4096:
            self.clock.advance(0.10)
            Path(output_path).write_bytes(b"x" * 1_000_000)
        else:
            self.clock.advance(0.02)
            Path(output_path).write_bytes(b"x" * 500_000)
        return 5000


class _FakeOdbcConnector:
    bcp_path = "bcp"
    trust_server_certificate = "yes"

    def __init__(self, batches: list[list[object]]) -> None:
        self.batches = batches
        self.requests: list[tuple[str, int]] = []
        self.bcp_calls: list[str] = []

    def get_records_streaming(self, query: str, params=None, batch_size: int = 10000, as_dict: bool = False):
        del params, as_dict
        self.requests.append((query, batch_size))
        yield from self.batches

    def bcp_queryout(self, query: str, output_path: str, *, options=None) -> int:
        del output_path, options
        self.bcp_calls.append(query)
        return 0


class ClickHouseConnector:
    pass


def _probe_request(tmp_path: Path, *, probe_rows: int = 100_000) -> ExportProbeRequest:
    return ExportProbeRequest(
        source_type="mssql",
        query="SELECT id, value FROM dbo.t",
        schema=(("id", "int"), ("value", "varchar(20)")),
        work_dir=tmp_path,
        probe_rows=probe_rows,
        max_probe_seconds=30,
        batch_size=10_000,
    )
