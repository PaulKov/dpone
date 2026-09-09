from __future__ import annotations

import importlib.util
import subprocess
import sys
from argparse import Namespace
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from tools.mssql_stress_governance_mssql import (
    ensure_target_identity_registry,
    mssql_connector_for_database,
)

from dpone.runtime.file_artifacts import FileExportArtifact, PartitionedFileExportArtifact


def _load_mssql_stress_module():
    path = Path("tools/mssql_stress.py")
    tools_path = str(path.parent.resolve())
    if tools_path not in sys.path:
        sys.path.insert(0, tools_path)
    spec = importlib.util.spec_from_file_location("dpone_tools_mssql_stress", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_mssql_stress_direct_cli_has_no_test_package_dependency() -> None:
    """Match the release workflow's direct-script import semantics."""

    completed = subprocess.run(
        [sys.executable, "tools/mssql_stress.py", "--help"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert "dpone MSSQL/Postgres/ClickHouse stress harness" in completed.stdout
    assert "from tests." not in Path("tools/mssql_stress.py").read_text(encoding="utf-8")


def test_mssql_stress_logger_implements_complete_etl_lifecycle() -> None:
    module = _load_mssql_stress_module()
    logger = module.StressLogger()

    logger.log_etl_start(
        {
            "source_schema": "public",
            "source_table": "bench_orders",
            "target_schema": "dbo",
            "target_table": "bench_orders",
            "load_strategy": "full_refresh",
            "batch_size": 50_000,
        }
    )
    logger.log_etl_progress("SOURCE_READY", {"Rows": 25_000})
    logger.log_etl_error("synthetic failure", {"phase": "test"})
    logger.log_etl_end({"status": "error", "errors": ["synthetic failure"]})

    assert logger.metrics is not None
    assert logger.metrics.error_count == 1


def test_governance_fixture_clones_reviewed_mssql_coordinates() -> None:
    template = SimpleNamespace(
        host="sql.example.test",
        port=1433,
        user="operator",
        password="secret",
        driver="ODBC Driver 18 for SQL Server",
        encrypt="yes",
        trust_server_certificate="no",
        connect_timeout=7,
        query_timeout=11,
        autocommit=True,
        application_name="dpone-certification",
        bcp_path="/opt/mssql-tools18/bin/bcp",
        odbc_options={"ApplicationIntent": "ReadWrite"},
    )

    connector = mssql_connector_for_database(template, "disposable_state")

    assert connector.database == "disposable_state"
    assert connector.host == template.host
    assert connector.port == template.port
    assert connector.user == template.user
    assert connector.password == template.password
    assert connector.application_name == template.application_name
    assert connector.odbc_options == template.odbc_options


def test_governance_fixture_provisions_exact_target_identity_boundary() -> None:
    statements: list[str] = []

    class RecordingConnector:
        def get_records(self, query: str) -> list[tuple[object]]:
            if "DB_NAME" in query:
                return [("dpone_it",)]
            if "OBJECT_ID" in query:
                return [(None,)]
            raise AssertionError(f"unexpected query: {query}")

        def execute_query(self, query: str) -> int:
            statements.append(" ".join(query.split()))
            return 0

    ensure_target_identity_registry(RecordingConnector(), database="dpone_it")

    assert len(statements) == 2
    assert "CREATE TABLE [dpone_it].[dbo].[dpone_target_identity]" in statements[0]
    assert "UNIQUE NONCLUSTERED ([schema_name], [table_name])" in statements[0]
    assert "CREATE TRIGGER [dbo].[trg_dpone_target_identity_immutable]" in statements[1]
    assert "DPONE_TARGET_IDENTITY_IMMUTABLE" in statements[1]


def test_governance_fixture_rejects_wrong_target_database() -> None:
    connector = SimpleNamespace(get_records=lambda _query: [("other_database",)])

    with pytest.raises(AssertionError, match="opened the wrong database"):
        ensure_target_identity_registry(connector, database="dpone_it")


def test_mssql_stress_uses_canonical_partitioning_options() -> None:
    module = _load_mssql_stress_module()
    module.CURRENT_ARGS = Namespace(
        rows=1000,
        partition_column="id",
        num_partitions=4,
        lower_bound=1,
        upper_bound=None,
        export_workers=3,
        load_workers=2,
        partition_workers=None,
        parallel_load_workers=None,
    )

    options = module.partition_options()

    assert options == {
        "partitioning": {
            "strategy": "range",
            "column": "id",
            "bounds": {"lower": 1, "upper": 1000},
            "num_partitions": 4,
            "export_workers": 3,
            "load_workers": 2,
        }
    }


def test_mssql_stress_uses_canonical_bulk_and_clickhouse_options() -> None:
    module = _load_mssql_stress_module()
    module.CURRENT_ARGS = Namespace(
        bcp_path="/opt/mssql-tools18/bin/bcp",
        batch_size=250000,
        optimizer_profile=None,
        clickhouse_bulk_mode="http",
        clickhouse_client_command=None,
        clickhouse_client_host=None,
        clickhouse_client_port=None,
        clickhouse_http_host="127.0.0.1",
        clickhouse_http_port=8123,
    )

    assert module.mssql_bulk_options() == {
        "bulk": {
            "mode": "bcp",
            "bcp": {
                "bcp_path": "/opt/mssql-tools18/bin/bcp",
                "batch_size": 250000,
            },
        }
    }
    assert module.clickhouse_bulk_options() == {
        "clickhouse_bulk": {
            "mode": "http",
            "http": {
                "host": "127.0.0.1",
                "port": 8123,
            },
        }
    }


def test_mssql_stress_passes_native_transfer_optimizer_profile() -> None:
    module = _load_mssql_stress_module()
    module.CURRENT_ARGS = Namespace(
        optimizer_profile="high_throughput_safe",
        bcp_path="/opt/mssql-tools18/bin/bcp",
        batch_size=250000,
        clickhouse_bulk_mode="http",
        clickhouse_client_command=None,
        clickhouse_client_host=None,
        clickhouse_client_port=None,
        clickhouse_http_host=None,
        clickhouse_http_port=None,
    )

    assert module.native_transfer_options() == {"native_transfer": {"optimizer_profile": "high_throughput_safe"}}
    assert module.mssql_bulk_options()["native_transfer"]["optimizer_profile"] == "high_throughput_safe"
    assert module.clickhouse_bulk_options()["native_transfer"]["optimizer_profile"] == "high_throughput_safe"


def test_mssql_stress_does_not_hardcode_clickhouse_target_database() -> None:
    text = Path("tools/mssql_stress.py").read_text(encoding="utf-8")

    assert 'target_schema="dpone"' not in text
    assert "target_schema=clickhouse.database" in text


def test_mssql_stress_creates_clustered_boundary() -> None:
    module = _load_mssql_stress_module()
    queries: list[str] = []

    class RecordingConnector:
        def execute_query(self, query: str) -> int:
            queries.append(" ".join(query.split()))
            return 0

    module.create_mssql_benchmark_boundary(RecordingConnector())

    assert queries == ["CREATE UNIQUE CLUSTERED INDEX [IX_dpone_bench_orders_id] ON [dbo].[bench_orders] ([id])"]


@pytest.mark.parametrize("load_fails", [False, True])
def test_run_postgres_to_mssql_uses_governed_load_before_boundary(
    monkeypatch: pytest.MonkeyPatch,
    load_fails: bool,
) -> None:
    module = _load_mssql_stress_module()
    events: list[tuple[str, object]] = []
    module.CURRENT_ARGS = Namespace(
        rows=4,
        partition_column=None,
        num_partitions=1,
        bcp_path="bcp",
        batch_size=4,
        optimizer_profile=None,
    )

    class RecordingCampaign:
        def route(self, *, target_schema: str, target_table: str) -> object:
            events.append(("route", (target_schema, target_table)))
            return object()

    @contextmanager
    def recording_campaign(connector: object, *, target_database: str):
        events.append(("campaign", (connector, target_database)))
        yield RecordingCampaign()

    class RecordingRunner:
        def __init__(self, route: object, pg: object, *, logger: object, source_type: type[object]) -> None:
            events.append(("runner", (route, pg, logger, source_type)))
            self.source = SimpleNamespace(
                export_metric=module.StageMetric.from_elapsed("postgres_to_mssql.source_export", 4, 0.001),
                artifact={"partition_count": 1},
            )

        def run(self, config: object, *, label: str) -> dict[str, int]:
            assert config.export_format == "csv"
            events.append(("load", (config, label)))
            if load_fails:
                raise RuntimeError("load failed")
            return {"loaded_rows": 4}

    class RecordingConnector:
        database = "dpone_it"

        def execute_query(self, query: str) -> int:
            events.append(("execute", " ".join(query.split())))
            return 0

    connector = RecordingConnector()
    postgres = SimpleNamespace(database="dpone_it")
    monkeypatch.setattr(module, "governed_mssql_campaign", recording_campaign)
    monkeypatch.setattr(module, "GovernedStandardEtlRunner", RecordingRunner)

    if load_fails:
        with pytest.raises(RuntimeError, match="load failed"):
            module.run_postgres_to_mssql(postgres, connector, module.StressLogger(), batch_size=4)
        assert "execute" not in [event for event, _ in events]
        return

    result = module.run_postgres_to_mssql(postgres, connector, module.StressLogger(), batch_size=4)

    assert result.metric.rows == 4
    assert [event for event, _ in events] == ["campaign", "route", "runner", "load", "execute"]
    assert events[-1][1] == ("CREATE UNIQUE CLUSTERED INDEX [IX_dpone_bench_orders_id] ON [dbo].[bench_orders] ([id])")


def test_artifact_diagnostics_reports_partition_bytes_and_skew(tmp_path: Path) -> None:
    module = _load_mssql_stress_module()
    first = tmp_path / "part-000.tsv"
    second = tmp_path / "part-001.tsv"
    first.write_text("a\tb\n1\t2\n", encoding="utf-8")
    second.write_text("a\tb\n1\t2\n3\t4\n", encoding="utf-8")

    artifact = PartitionedFileExportArtifact(
        [
            FileExportArtifact(str(first), ["a", "b"], estimated_rows=2),
            FileExportArtifact(str(second), ["a", "b"], estimated_rows=3),
        ],
        ["a", "b"],
        max_workers=2,
        estimated_rows=5,
    )

    diagnostics = module.artifact_diagnostics(artifact, phase_seconds=2.0)

    assert diagnostics["partition_count"] == 2
    assert diagnostics["total_bytes"] == first.stat().st_size + second.stat().st_size
    assert diagnostics["estimated_rows"] == 5
    assert diagnostics["mb_per_second"] > 0
    assert diagnostics["slowest_partition"]["estimated_rows"] == 3
    assert diagnostics["partition_row_skew"] == 1
    assert [item["index"] for item in diagnostics["partitions"]] == [0, 1]
