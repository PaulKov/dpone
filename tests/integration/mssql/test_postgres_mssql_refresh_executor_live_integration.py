from __future__ import annotations

import json
import os
import uuid
from collections.abc import Sequence
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pytest
from route_refresh_wide_fixtures import (
    add_postgres_mssql_evolved_column,
    build_postgres_mssql_columns,
    column_names,
    create_mssql_wide_target,
    create_postgres_wide_source,
    postgres_mssql_query_template,
    type_hints,
)

from dpone.ops.route_refresh_execute import RouteRefreshExecutionService
from dpone.ops.route_refresh_plan import RouteRefreshPlanService
from dpone.ops.route_refresh_snapshot_capture import RouteRefreshSnapshotCaptureService
from dpone.ops.route_refresh_verify import RouteRefreshVerificationService
from dpone.ops.routes.refresh_executors import RouteRefreshExecutorRegistry
from dpone.ops.routes.refresh_snapshot_capture_adapters import (
    MssqlRouteRefreshRowsReader,
    PostgresRouteRefreshRowsReader,
)
from dpone.ops.routes.refresh_verification_verifier import JsonRouteRefreshSnapshotReader
from dpone.runtime.connectors.mssql import MSSQLConnector

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_postgres,
    pytest.mark.integration_mssql,
]

_TRUTHY = {"1", "true", "yes", "on"}
_LIVE_ROWS = 10_000
_LIVE_COLUMNS = 200


def _truthy_env(name: str) -> bool:
    return str(os.getenv(name, "0")).strip().lower() in _TRUTHY


pytestmark = [
    *pytestmark,
    pytest.mark.skipif(
        not _truthy_env("DPONE_RUN_INTEGRATION"),
        reason="Integration tests are disabled. Set DPONE_RUN_INTEGRATION=1 to enable them.",
    ),
    pytest.mark.skipif(
        not _truthy_env("DPONE_RUN_REFRESH_EXECUTOR_LIVE"),
        reason="Postgres -> MSSQL refresh executor live certification is disabled. "
        "Set DPONE_RUN_REFRESH_EXECUTOR_LIVE=1 to enable it.",
    ),
]


def _mssql_connector() -> MSSQLConnector:
    pytest.importorskip("pyodbc")
    connector = MSSQLConnector(
        host=os.getenv("DPONE_IT_MSSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("DPONE_IT_MSSQL_PORT", os.getenv("DPONE_IT_MSSQL_PORT_FORWARD", "51433"))),
        database=os.getenv("DPONE_IT_MSSQL_DATABASE", "master"),
        user=os.getenv("DPONE_IT_MSSQL_USER", "sa"),
        password=os.getenv("DPONE_IT_MSSQL_PASSWORD", "Dp0ne.Strong.Pw.2026!"),
        driver=os.getenv("DPONE_IT_MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
        trust_server_certificate=os.getenv("DPONE_IT_MSSQL_TRUST_SERVER_CERTIFICATE", "yes"),
        bcp_path=os.getenv("DPONE_IT_MSSQL_BCP_PATH", "bcp"),
    )
    try:
        connector.execute_query("SELECT 1")
    except Exception as exc:
        connector.close()
        pytest.skip(f"MSSQL integration endpoint is unavailable: {exc}")
    return connector


def test_postgres_mssql_refresh_executor_replays_wide_chunks_and_verifies_exactly(
    postgres_connector: Any,
    postgres_settings: Any,
    tmp_path: Path,
) -> None:
    mssql = _mssql_connector()
    artifact_root = Path(os.getenv("DPONE_REFRESH_EXECUTOR_LIVE_ARTIFACT_DIR", str(tmp_path)))
    artifact_root.mkdir(parents=True, exist_ok=True)
    schema = f"rr_{uuid.uuid4().hex[:8]}"
    source_table = "orders"
    target_table = "orders"
    dataset = f"{schema}.{target_table}"
    columns = build_postgres_mssql_columns(_LIVE_COLUMNS)
    runtime_config_dir = TemporaryDirectory(prefix="dpone-live-executor-")
    runtime_config_root = Path(runtime_config_dir.name)
    try:
        create_postgres_wide_source(
            postgres_connector,
            schema=schema,
            table=source_table,
            rows=_LIVE_ROWS,
            columns=columns,
        )
        create_mssql_wide_target(mssql, schema=schema, table=target_table, columns=columns)

        plan_json = _write_plan(output_dir=artifact_root / "plan", dataset=dataset, rows=_LIVE_ROWS)
        config_json = _write_executor_config(
            path=runtime_config_root / "executor-config.json",
            source_dataset=f"{schema}.{source_table}",
            target_dataset=dataset,
            postgres_settings=postgres_settings,
            columns=columns,
        )

        first_report = _execute(
            config_json=config_json, plan_json=plan_json, output_dir=artifact_root / "execute-first"
        )
        replay_report = _execute(
            config_json=config_json,
            plan_json=plan_json,
            output_dir=artifact_root / "execute-replay",
            runner_id="refresh-live-certification-replay",
        )
        capture_report = _capture(
            postgres=postgres_connector,
            mssql=mssql,
            execution_json=artifact_root / "execute-replay" / "route_refresh_execution.json",
            output_dir=artifact_root / "capture",
            source_dataset=f"{schema}.{source_table}",
            target_dataset=dataset,
            columns=columns,
        )
        verification_report = _verify(
            execution_json=artifact_root / "execute-replay" / "route_refresh_execution.json",
            capture_report=capture_report,
            output_dir=artifact_root / "verify",
            columns=columns,
        )

        assert first_report.passed is True
        assert first_report.ready_for_state_promotion is True
        assert first_report.summary["chunks_succeeded"] == 2
        assert first_report.summary["rows_written"] == _LIVE_ROWS
        assert replay_report.passed is True
        assert replay_report.summary["rows_written"] == _LIVE_ROWS
        assert _mssql_count(mssql, schema=schema, table=target_table) == _LIVE_ROWS
        assert _mssql_column_count(mssql, schema=schema, table=target_table) == _LIVE_COLUMNS
        assert capture_report.passed is True
        assert capture_report.status == "captured"
        assert capture_report.summary["chunks_captured"] == 2
        assert capture_report.summary["source_rows"] == _LIVE_ROWS
        assert capture_report.summary["sink_rows"] == _LIVE_ROWS
        assert capture_report.summary["column_count"] == _LIVE_COLUMNS
        assert verification_report.passed is True
        assert verification_report.status == "verified"
        assert verification_report.ready_for_state_promotion is True
        assert verification_report.summary["chunks_verified"] == 2
        assert verification_report.summary["source_rows"] == _LIVE_ROWS
        assert verification_report.summary["sink_rows"] == _LIVE_ROWS
        assert verification_report.blockers == ()

        evolved = add_postgres_mssql_evolved_column(
            postgres_connector,
            mssql,
            schema=schema,
            source_table=source_table,
            target_table=target_table,
        )
        evolved_columns = [*columns, evolved]
        evolved_config_json = _write_executor_config(
            path=runtime_config_root / "executor-config-evolved.json",
            source_dataset=f"{schema}.{source_table}",
            target_dataset=dataset,
            postgres_settings=postgres_settings,
            columns=evolved_columns,
        )
        evolved_report = _execute(
            config_json=evolved_config_json,
            plan_json=plan_json,
            output_dir=artifact_root / "execute-schema-evolution",
            runner_id="refresh-live-certification-schema-evolution",
        )
        evolved_capture = _capture(
            postgres=postgres_connector,
            mssql=mssql,
            execution_json=artifact_root / "execute-schema-evolution" / "route_refresh_execution.json",
            output_dir=artifact_root / "capture-schema-evolution",
            source_dataset=f"{schema}.{source_table}",
            target_dataset=dataset,
            columns=evolved_columns,
        )
        evolved_verification = _verify(
            execution_json=artifact_root / "execute-schema-evolution" / "route_refresh_execution.json",
            capture_report=evolved_capture,
            output_dir=artifact_root / "verify-schema-evolution",
            columns=evolved_columns,
        )

        assert evolved_report.passed is True
        assert evolved_capture.passed is True
        assert evolved_capture.summary["column_count"] == _LIVE_COLUMNS + 1
        assert evolved_verification.passed is True
        assert evolved_verification.blockers == ()
        assert (artifact_root / "capture" / "route_refresh_snapshot_capture.json").is_file()
        assert (artifact_root / "capture" / "source_route_refresh_snapshot.json").is_file()
        assert (artifact_root / "capture" / "sink_route_refresh_snapshot.json").is_file()
        assert (artifact_root / "verify" / "route_refresh_verification.json").is_file()

        for chunk in replay_report.chunks:
            artifact = json.loads(Path(chunk.artifact_path).read_text(encoding="utf-8"))
            assert artifact["schema_version"] == "dpone.postgres_mssql.route_refresh_chunk.v1"
            assert artifact["route"]["case_id"] == "postgres_to_mssql__incremental_merge"
            assert artifact["prepare"]["redacted_command"]
            assert artifact["artifact_sha256"] != "0" * 64
    finally:
        runtime_config_dir.cleanup()
        mssql.execute_query(f"DROP TABLE IF EXISTS [{schema}].[{target_table}]")
        mssql.execute_query(f"DROP SCHEMA IF EXISTS [{schema}]")
        postgres_connector.execute_query(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        mssql.close()


def _execute(
    *,
    config_json: Path,
    plan_json: Path,
    output_dir: Path,
    runner_id: str = "refresh-live-certification",
) -> Any:
    return RouteRefreshExecutionService(
        executor=RouteRefreshExecutorRegistry.default().build("postgres_mssql", config_json=config_json)
    ).execute(
        route_refresh_plan_json=plan_json,
        output_dir=output_dir,
        runner_id=runner_id,
        execute=True,
    )


def _capture(
    *,
    postgres: Any,
    mssql: MSSQLConnector,
    execution_json: Path,
    output_dir: Path,
    source_dataset: str,
    target_dataset: str,
    columns: Sequence[Any],
) -> Any:
    return RouteRefreshSnapshotCaptureService(
        source_reader=PostgresRouteRefreshRowsReader(
            connector=postgres,
            dataset=source_dataset,
            columns=tuple(column_names(columns)),
            boundary_column="order_id",
            query_template=postgres_mssql_query_template(columns),
        ),
        sink_reader=MssqlRouteRefreshRowsReader(
            connector=mssql,
            dataset=target_dataset,
            columns=tuple(column_names(columns)),
            boundary_column="order_id",
        ),
    ).capture(
        route_refresh_execution_json=execution_json,
        output_dir=output_dir,
        runner_id="refresh-live-certification-capture",
        key_columns=("order_id",),
        boundary_column="order_id",
        columns=tuple(column_names(columns)),
        type_hints=type_hints(columns),
    )


def _verify(*, execution_json: Path, capture_report: Any, output_dir: Path, columns: Sequence[Any]) -> Any:
    return RouteRefreshVerificationService(
        source_reader=JsonRouteRefreshSnapshotReader(capture_report.source_snapshot_json),
        sink_reader=JsonRouteRefreshSnapshotReader(capture_report.sink_snapshot_json),
    ).verify(
        route_refresh_execution_json=execution_json,
        output_dir=output_dir,
        runner_id="refresh-live-certification-verify",
        key_columns=("order_id",),
        boundary_column="order_id",
        columns=tuple(column_names(columns)),
        type_hints=type_hints(columns),
    )


def _write_plan(*, output_dir: Path, dataset: str, rows: int) -> Path:
    report = RouteRefreshPlanService().plan(
        output_dir=output_dir,
        source="postgres",
        sink="mssql",
        strategy="incremental_merge",
        dataset=dataset,
        reason="live_certification",
        window_kind="integer",
        start="1",
        end=str(rows),
        chunk_size=max(1, rows // 2),
    )
    return Path(report.json_path)


def _write_executor_config(
    *,
    path: Path,
    source_dataset: str,
    target_dataset: str,
    postgres_settings: Any,
    columns: Sequence[Any],
) -> Path:
    payload = {
        "source_dataset": source_dataset,
        "target_dataset": target_dataset,
        "boundary_column": "order_id",
        "columns": column_names(columns),
        "query_template": postgres_mssql_query_template(columns),
        "postgres": {
            "host": os.getenv("DPONE_IT_POSTGRES_HOST", getattr(postgres_settings, "host")),
            "port": int(os.getenv("DPONE_IT_POSTGRES_PORT", str(getattr(postgres_settings, "port")))),
            "database": getattr(postgres_settings, "database"),
            "user": getattr(postgres_settings, "user"),
            "password": getattr(postgres_settings, "password"),
        },
        "mssql": {
            "host": os.getenv("DPONE_IT_MSSQL_HOST", "127.0.0.1"),
            "port": int(os.getenv("DPONE_IT_MSSQL_PORT", os.getenv("DPONE_IT_MSSQL_PORT_FORWARD", "51433"))),
            "database": os.getenv("DPONE_IT_MSSQL_DATABASE", "master"),
            "user": os.getenv("DPONE_IT_MSSQL_USER", "sa"),
            "password": os.getenv("DPONE_IT_MSSQL_PASSWORD", "Dp0ne.Strong.Pw.2026!"),
            "trust_server_certificate": True,
            "options": {
                "bcp_path": os.getenv("DPONE_IT_MSSQL_BCP_PATH", "bcp"),
                "batch_size": 100000,
                "packet_size": 16384,
                "timeout_seconds": 3600,
                "trust_server_certificate": True,
            },
        },
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _mssql_count(connector: MSSQLConnector, *, schema: str, table: str) -> int:
    return int(
        connector.get_records(
            f"SELECT COUNT_BIG(*) AS row_count FROM [{schema}].[{table}]",
            as_dict=True,
        )[0]["row_count"]
    )


def _mssql_column_count(connector: MSSQLConnector, *, schema: str, table: str) -> int:
    return int(
        connector.get_records(
            f"""
            SELECT COUNT(*) AS column_count
            FROM sys.columns AS c
            INNER JOIN sys.tables AS t
                ON t.object_id = c.object_id
            INNER JOIN sys.schemas AS s
                ON s.schema_id = t.schema_id
            WHERE s.name = '{schema}' AND t.name = '{table}'
            """,
            as_dict=True,
        )[0]["column_count"]
    )
