from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpone.ops.routes.models import RouteKey
from dpone.ops.routes.refresh_execution_executor import RouteRefreshChunkExecutionRequest
from dpone.ops.routes.refresh_executors import (
    MssqlClickHouseRefreshConfig,
    MssqlClickHouseRouteRefreshExecutor,
    RouteRefreshExecutorRegistry,
)


class _RecordingExporter:
    def __init__(self, *, rows_read: int = 2) -> None:
        self.rows_read = rows_read
        self.calls: list[tuple[str, Path]] = []

    def export_chunk(self, *, query: str, output_path: Path) -> object:
        self.calls.append((query, output_path))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("1\talpha\n2\tbeta\n", encoding="utf-8")
        return {
            "rows_read": self.rows_read,
            "redacted_command": ("bcp", "***"),
            "stdout_tail": "2 rows copied",
            "stderr_tail": "",
        }


class _RecordingLoader:
    def __init__(self, *, rows_written: int = 2) -> None:
        self.rows_written = rows_written
        self.prepare_calls: list[tuple[str, str, str, str]] = []
        self.calls: list[tuple[str, tuple[str, ...], Path]] = []

    def prepare_chunk(
        self,
        *,
        table: str,
        boundary_column: str,
        start: str,
        end: str,
        idempotency_key: str,
    ) -> object:
        del idempotency_key
        self.prepare_calls.append((table, boundary_column, start, end))
        return {"rows_deleted": 0}

    def load_chunk(self, *, table: str, columns: tuple[str, ...], input_path: Path, idempotency_key: str) -> object:
        del idempotency_key
        self.calls.append((table, columns, input_path))
        return {
            "rows_written": self.rows_written,
            "redacted_command": ("clickhouse-client", "***"),
            "stdout_tail": "",
            "stderr_tail": "",
        }


class _RecordingPostgresExporter:
    def __init__(self, *, rows_read: int = 2) -> None:
        self.rows_read = rows_read
        self.calls: list[tuple[str, Path]] = []

    def export_chunk(self, *, query: str, output_path: Path) -> object:
        self.calls.append((query, output_path))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("1\talpha\n2\tbeta\n", encoding="utf-8")
        return {
            "rows_read": self.rows_read,
            "redacted_command": ("postgres-copy",),
            "stdout_tail": "COPY 2",
            "stderr_tail": "",
        }


class _RecordingMssqlLoader:
    def __init__(self, *, rows_written: int = 2) -> None:
        self.rows_written = rows_written
        self.prepare_calls: list[tuple[str, str, str, str]] = []
        self.calls: list[tuple[str, tuple[str, ...], Path]] = []

    def prepare_chunk(
        self,
        *,
        table: str,
        boundary_column: str,
        start: str,
        end: str,
        idempotency_key: str,
    ) -> object:
        del idempotency_key
        self.prepare_calls.append((table, boundary_column, start, end))
        return {"rows_deleted": 0, "redacted_command": ("sqlcmd", "***")}

    def load_chunk(self, *, table: str, columns: tuple[str, ...], input_path: Path, idempotency_key: str) -> object:
        del idempotency_key
        self.calls.append((table, columns, input_path))
        return {
            "rows_written": self.rows_written,
            "redacted_command": ("bcp", "***"),
            "stdout_tail": "2 rows copied",
            "stderr_tail": "",
        }


def _request(tmp_path: Path, *, route: RouteKey | None = None) -> RouteRefreshChunkExecutionRequest:
    return RouteRefreshChunkExecutionRequest(
        route=route or RouteKey.of("mssql", "clickhouse", "incremental_merge"),
        dataset="analytics.orders",
        ordinal=1,
        start="1",
        end="10",
        partition="",
        source_boundary="1..10",
        sink_boundary="1..10",
        idempotency_key="mssql_to_clickhouse__incremental_merge:analytics.orders:1:1..10",
        runner_id="operator-a",
        execute=True,
        output_dir=str(tmp_path),
        plan_path=str(tmp_path / "route_refresh_plan.json"),
    )


def _postgres_mssql_request(tmp_path: Path, *, route: RouteKey | None = None) -> RouteRefreshChunkExecutionRequest:
    return RouteRefreshChunkExecutionRequest(
        route=route or RouteKey.of("postgres", "mssql", "incremental_merge"),
        dataset="dbo.orders",
        ordinal=1,
        start="1",
        end="10",
        partition="",
        source_boundary="1..10",
        sink_boundary="1..10",
        idempotency_key="postgres_to_mssql__incremental_merge:dbo.orders:1:1..10",
        runner_id="operator-a",
        execute=True,
        output_dir=str(tmp_path),
        plan_path=str(tmp_path / "route_refresh_plan.json"),
    )


def _config() -> MssqlClickHouseRefreshConfig:
    return MssqlClickHouseRefreshConfig(
        source_dataset="dbo.orders",
        target_dataset="analytics.orders",
        boundary_column="id",
        columns=("id", "name"),
    )


def test_postgres_mssql_executor_exports_loads_and_writes_chunk_artifact(tmp_path: Path) -> None:
    from dpone.ops.routes.refresh_executors.postgres_mssql import (
        PostgresMssqlRefreshConfig,
        PostgresMssqlRouteRefreshExecutor,
    )

    exporter = _RecordingPostgresExporter()
    loader = _RecordingMssqlLoader()
    executor = PostgresMssqlRouteRefreshExecutor(
        config=PostgresMssqlRefreshConfig(
            source_dataset="public.orders",
            target_dataset="dbo.orders",
            boundary_column="id",
            columns=("id", "name"),
        ),
        exporter=exporter,
        loader=loader,
    )

    result = executor.execute_chunk(_postgres_mssql_request(tmp_path))

    assert result.passed is True
    assert result.status == "succeeded"
    assert result.rows_read == 2
    assert result.rows_written == 2
    assert Path(result.artifact_path).is_file()
    assert '"public"."orders"' in exporter.calls[0][0]
    assert '"id" BETWEEN 1 AND 10' in exporter.calls[0][0]
    assert loader.prepare_calls == [("[dbo].[orders]", "id", "1", "10")]
    assert loader.calls == [("[dbo].[orders]", ("id", "name"), exporter.calls[0][1])]

    payload = json.loads(Path(result.artifact_path).read_text(encoding="utf-8"))
    assert payload["schema_version"] == "dpone.postgres_mssql.route_refresh_chunk.v1"
    assert payload["route"]["case_id"] == "postgres_to_mssql__incremental_merge"
    assert payload["chunk"]["ordinal"] == 1
    assert payload["export"]["rows_read"] == 2
    assert payload["prepare"]["rows_deleted"] == 0
    assert payload["load"]["rows_written"] == 2
    assert payload["artifact_sha256"] != "0" * 64


def test_postgres_mssql_executor_blocks_unsupported_route_before_side_effects(tmp_path: Path) -> None:
    from dpone.ops.routes.refresh_executors.postgres_mssql import (
        PostgresMssqlRefreshConfig,
        PostgresMssqlRouteRefreshExecutor,
    )

    exporter = _RecordingPostgresExporter()
    loader = _RecordingMssqlLoader()
    executor = PostgresMssqlRouteRefreshExecutor(
        config=PostgresMssqlRefreshConfig(
            source_dataset="public.orders",
            target_dataset="dbo.orders",
            boundary_column="id",
            columns=("id", "name"),
        ),
        exporter=exporter,
        loader=loader,
    )

    result = executor.execute_chunk(
        _postgres_mssql_request(tmp_path, route=RouteKey.of("mssql", "clickhouse", "incremental_merge"))
    )

    assert result.passed is False
    assert result.status == "failed"
    assert "postgres_mssql_refresh_executor.route_unsupported" in result.blockers
    assert exporter.calls == []
    assert loader.prepare_calls == []
    assert loader.calls == []


def test_mssql_clickhouse_executor_exports_loads_and_writes_chunk_artifact(tmp_path: Path) -> None:
    exporter = _RecordingExporter()
    loader = _RecordingLoader()
    executor = MssqlClickHouseRouteRefreshExecutor(config=_config(), exporter=exporter, loader=loader)

    result = executor.execute_chunk(_request(tmp_path))

    assert result.passed is True
    assert result.status == "succeeded"
    assert result.rows_read == 2
    assert result.rows_written == 2
    assert Path(result.artifact_path).is_file()
    assert "[dbo].[orders]" in exporter.calls[0][0]
    assert "[id] BETWEEN 1 AND 10" in exporter.calls[0][0]
    assert loader.prepare_calls == [("`analytics`.`orders`", "id", "1", "10")]
    assert loader.calls == [("`analytics`.`orders`", ("id", "name"), exporter.calls[0][1])]

    payload = json.loads(Path(result.artifact_path).read_text(encoding="utf-8"))
    assert payload["schema_version"] == "dpone.mssql_clickhouse.route_refresh_chunk.v1"
    assert payload["route"]["case_id"] == "mssql_to_clickhouse__incremental_merge"
    assert payload["chunk"]["ordinal"] == 1
    assert payload["export"]["rows_read"] == 2
    assert payload["prepare"]["rows_deleted"] == 0
    assert payload["load"]["rows_written"] == 2
    assert payload["artifact_sha256"] != "0" * 64


def test_mssql_clickhouse_executor_blocks_unsupported_route_before_side_effects(tmp_path: Path) -> None:
    exporter = _RecordingExporter()
    loader = _RecordingLoader()
    executor = MssqlClickHouseRouteRefreshExecutor(config=_config(), exporter=exporter, loader=loader)

    result = executor.execute_chunk(_request(tmp_path, route=RouteKey.of("postgres", "mssql", "incremental_merge")))

    assert result.passed is False
    assert result.status == "failed"
    assert "mssql_clickhouse_refresh_executor.route_unsupported" in result.blockers
    assert exporter.calls == []
    assert loader.prepare_calls == []
    assert loader.calls == []


def test_route_refresh_executor_registry_builds_mssql_clickhouse_executor_from_config(tmp_path: Path) -> None:
    config_path = tmp_path / "mssql_clickhouse_executor.json"
    config_path.write_text(
        json.dumps(
            {
                "source_dataset": "dbo.orders",
                "target_dataset": "analytics.orders",
                "boundary_column": "id",
                "columns": ["id", "name"],
                "mssql": {"host": "127.0.0.1", "port": 1433, "database": "src", "user": "sa", "password": "secret"},
                "clickhouse": {
                    "host": "127.0.0.1",
                    "port": 9000,
                    "database": "analytics",
                    "user": "default",
                    "password": "secret",
                },
            }
        ),
        encoding="utf-8",
    )

    executor = RouteRefreshExecutorRegistry.default().build(
        "mssql_clickhouse",
        config_json=config_path,
    )

    assert isinstance(executor, MssqlClickHouseRouteRefreshExecutor)


def test_route_refresh_executor_registry_builds_postgres_mssql_executor_from_config(tmp_path: Path) -> None:
    config_path = tmp_path / "postgres_mssql_executor.json"
    config_path.write_text(
        json.dumps(
            {
                "source_dataset": "public.orders",
                "target_dataset": "dbo.orders",
                "boundary_column": "id",
                "columns": ["id", "name"],
                "postgres": {
                    "host": "127.0.0.1",
                    "port": 5432,
                    "database": "src",
                    "user": "postgres",
                    "password": "secret",
                },
                "mssql": {"host": "127.0.0.1", "port": 1433, "database": "dst", "user": "sa", "password": "secret"},
            }
        ),
        encoding="utf-8",
    )

    executor = RouteRefreshExecutorRegistry.default().build(
        "postgres_mssql",
        config_json=config_path,
    )

    assert executor.__class__.__name__ == "PostgresMssqlRouteRefreshExecutor"


def test_route_refresh_executor_registry_returns_none_for_default_backend() -> None:
    assert RouteRefreshExecutorRegistry.default().build(None, config_json=None) is None
    assert RouteRefreshExecutorRegistry.default().build("none", config_json=None) is None


def test_postgres_mssql_refresh_bcp_runner_has_default_deadline() -> None:
    from dpone.ops.routes.refresh_executors.postgres_mssql_adapters import build_bcp_runner

    runner = build_bcp_runner({"host": "sql.example.com", "database": "dwh", "trusted_connection": True})

    assert runner.options.timeout_seconds == 3600


def test_mssql_clickhouse_refresh_bcp_runner_has_default_deadline() -> None:
    from dpone.ops.routes.refresh_executors.mssql_clickhouse_adapters import build_bcp_runner

    runner = build_bcp_runner({"host": "sql.example.com", "database": "dwh", "trusted_connection": True})

    assert runner.options.timeout_seconds == 3600


@pytest.mark.parametrize("value", [0, -1, True, False])
def test_refresh_bcp_adapters_reject_invalid_explicit_deadline(value: object) -> None:
    from dpone.ops.routes.refresh_executors.mssql_clickhouse_adapters import (
        build_bcp_runner as build_mssql_clickhouse_runner,
    )
    from dpone.ops.routes.refresh_executors.postgres_mssql_adapters import (
        build_bcp_runner as build_postgres_mssql_runner,
    )

    payload = {
        "host": "sql.example.com",
        "database": "dwh",
        "trusted_connection": True,
        "options": {"timeout_seconds": value},
    }
    for builder in (build_postgres_mssql_runner, build_mssql_clickhouse_runner):
        with pytest.raises(ValueError, match="MSSQL BCP timeout_seconds must be a positive integer"):
            builder(payload)


def test_route_refresh_execution_service_stays_executor_agnostic() -> None:
    service_text = Path("src/dpone/ops/route_refresh_execute.py").read_text(encoding="utf-8")

    forbidden_fragments = {
        "refresh_executors",
        "MssqlClickHouseRouteRefreshExecutor",
        "BcpRunner",
        "ClickHouseClientRunner",
    }

    assert [fragment for fragment in forbidden_fragments if fragment in service_text] == []


def test_mssql_clickhouse_executor_modules_stay_focused() -> None:
    module_limits = {
        Path("src/dpone/ops/routes/refresh_executors/mssql_clickhouse.py"): 80,
        Path("src/dpone/ops/routes/refresh_executors/mssql_clickhouse_adapters.py"): 220,
        Path("src/dpone/ops/routes/refresh_executors/mssql_clickhouse_artifacts.py"): 180,
        Path("src/dpone/ops/routes/refresh_executors/mssql_clickhouse_config.py"): 170,
        Path("src/dpone/ops/routes/refresh_executors/mssql_clickhouse_executor.py"): 260,
        Path("src/dpone/ops/routes/refresh_executors/native_pipeline.py"): 220,
        Path("src/dpone/ops/routes/refresh_executors/postgres_mssql.py"): 90,
        Path("src/dpone/ops/routes/refresh_executors/postgres_mssql_adapters.py"): 240,
        Path("src/dpone/ops/routes/refresh_executors/postgres_mssql_artifacts.py"): 190,
        Path("src/dpone/ops/routes/refresh_executors/postgres_mssql_config.py"): 170,
        Path("src/dpone/ops/routes/refresh_executors/postgres_mssql_executor.py"): 260,
        Path("src/dpone/ops/routes/refresh_executors/registry.py"): 120,
    }

    offenders = [
        f"{path}:{len(path.read_text(encoding='utf-8').splitlines())}>{limit}"
        for path, limit in module_limits.items()
        if path.is_file() and len(path.read_text(encoding="utf-8").splitlines()) > limit
    ]

    assert offenders == []
