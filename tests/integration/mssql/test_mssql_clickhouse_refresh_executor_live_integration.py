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
    add_mssql_clickhouse_evolved_column,
    build_mssql_clickhouse_columns,
    column_names,
    create_clickhouse_wide_target,
    create_mssql_wide_source,
    mssql_clickhouse_query_template,
    type_hints,
)

from dpone.ops.route_refresh_execute import RouteRefreshExecutionService
from dpone.ops.route_refresh_plan import RouteRefreshPlanService
from dpone.ops.route_refresh_snapshot_capture import RouteRefreshSnapshotCaptureService
from dpone.ops.route_refresh_verify import RouteRefreshVerificationService
from dpone.ops.routes.refresh_executors import RouteRefreshExecutorRegistry
from dpone.ops.routes.refresh_snapshot_capture_adapters import (
    ClickHouseRouteRefreshRowsReader,
    MssqlRouteRefreshRowsReader,
)
from dpone.ops.routes.refresh_verification_verifier import JsonRouteRefreshSnapshotReader
from dpone.runtime.connectors.mssql import MSSQLConnector

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_mssql,
    pytest.mark.integration_clickhouse,
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
        reason="MSSQL -> ClickHouse refresh executor live certification is disabled. "
        "Set DPONE_RUN_REFRESH_EXECUTOR_LIVE=1 to enable it.",
    ),
]


def _mssql_connector() -> MSSQLConnector:
    pytest.importorskip("pyodbc")
    host = os.getenv("DPONE_IT_MSSQL_HOST")
    if not host:
        pytest.skip("DPONE_IT_MSSQL_HOST is not configured")
    connector = MSSQLConnector(
        host=host,
        port=int(os.getenv("DPONE_IT_MSSQL_PORT", "1433")),
        database=os.getenv("DPONE_IT_MSSQL_DATABASE", "master"),
        user=os.getenv("DPONE_IT_MSSQL_USER", "sa"),
        password=os.getenv("DPONE_IT_MSSQL_PASSWORD", ""),
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


def test_mssql_clickhouse_refresh_executor_replays_wide_chunks_and_verifies_exactly(
    clickhouse_connector: Any,
    clickhouse_settings: Any,
    tmp_path: Path,
) -> None:
    mssql = _mssql_connector()
    artifact_root = Path(os.getenv("DPONE_REFRESH_EXECUTOR_LIVE_ARTIFACT_DIR", str(tmp_path)))
    artifact_root.mkdir(parents=True, exist_ok=True)
    schema = f"rr_{uuid.uuid4().hex[:8]}"
    source_table = "orders"
    target_table = f"rr_orders_{uuid.uuid4().hex[:8]}"
    dataset = f"{clickhouse_settings.database}.{target_table}"
    columns = build_mssql_clickhouse_columns(_LIVE_COLUMNS)
    runtime_config_dir = TemporaryDirectory(prefix="dpone-live-executor-")
    runtime_config_root = Path(runtime_config_dir.name)
    try:
        create_mssql_wide_source(mssql, schema=schema, table=source_table, rows=_LIVE_ROWS, columns=columns)
        create_clickhouse_wide_target(
            clickhouse_connector,
            database=clickhouse_settings.database,
            table=target_table,
            columns=columns,
        )

        plan_json = _write_plan(output_dir=artifact_root / "plan", dataset=dataset, rows=_LIVE_ROWS)
        config_json = _write_executor_config(
            path=runtime_config_root / "executor-config.json",
            source_dataset=f"{schema}.{source_table}",
            target_dataset=dataset,
            clickhouse_settings=clickhouse_settings,
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
            mssql=mssql,
            clickhouse=clickhouse_connector,
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
        assert (
            _clickhouse_count(clickhouse_connector, database=clickhouse_settings.database, table=target_table)
            == _LIVE_ROWS
        )
        assert (
            _clickhouse_column_count(clickhouse_connector, database=clickhouse_settings.database, table=target_table)
            == _LIVE_COLUMNS
        )
        assert capture_report.passed is True
        assert capture_report.status == "captured"
        assert capture_report.summary["chunks_captured"] == 2
        assert capture_report.summary["source_rows"] == _LIVE_ROWS
        assert capture_report.summary["sink_rows"] == _LIVE_ROWS
        assert capture_report.summary["column_count"] == _LIVE_COLUMNS
        assert verification_report.passed is True
        assert verification_report.status == "verified"
        assert verification_report.summary["chunks_verified"] == 2
        assert verification_report.summary["source_rows"] == _LIVE_ROWS
        assert verification_report.summary["sink_rows"] == _LIVE_ROWS
        assert verification_report.blockers == ()

        evolved = add_mssql_clickhouse_evolved_column(
            mssql,
            clickhouse_connector,
            schema=schema,
            table=source_table,
            database=clickhouse_settings.database,
            target_table=target_table,
        )
        evolved_columns = [*columns, evolved]
        evolved_config_json = _write_executor_config(
            path=runtime_config_root / "executor-config-evolved.json",
            source_dataset=f"{schema}.{source_table}",
            target_dataset=dataset,
            clickhouse_settings=clickhouse_settings,
            columns=evolved_columns,
        )
        evolved_report = _execute(
            config_json=evolved_config_json,
            plan_json=plan_json,
            output_dir=artifact_root / "execute-schema-evolution",
            runner_id="refresh-live-certification-schema-evolution",
        )
        evolved_capture = _capture(
            mssql=mssql,
            clickhouse=clickhouse_connector,
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
            assert artifact["schema_version"] == "dpone.mssql_clickhouse.route_refresh_chunk.v1"
            assert artifact["route"]["case_id"] == "mssql_to_clickhouse__incremental_merge"
            assert artifact["prepare"]["redacted_command"]
            assert artifact["artifact_sha256"] != "0" * 64
    finally:
        runtime_config_dir.cleanup()
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{clickhouse_settings.database}`.`{target_table}`")
        mssql.execute_query(f"DROP TABLE IF EXISTS [{schema}].[{source_table}]")
        mssql.execute_query(f"DROP SCHEMA IF EXISTS [{schema}]")
        mssql.close()


def _execute(
    *,
    config_json: Path,
    plan_json: Path,
    output_dir: Path,
    runner_id: str = "refresh-live-certification",
) -> Any:
    return RouteRefreshExecutionService(
        executor=RouteRefreshExecutorRegistry.default().build("mssql_clickhouse", config_json=config_json)
    ).execute(
        route_refresh_plan_json=plan_json,
        output_dir=output_dir,
        runner_id=runner_id,
        execute=True,
    )


def _capture(
    *,
    mssql: MSSQLConnector,
    clickhouse: Any,
    execution_json: Path,
    output_dir: Path,
    source_dataset: str,
    target_dataset: str,
    columns: Sequence[Any],
) -> Any:
    return RouteRefreshSnapshotCaptureService(
        source_reader=MssqlRouteRefreshRowsReader(
            connector=mssql,
            dataset=source_dataset,
            columns=tuple(column_names(columns)),
            boundary_column="order_id",
            query_template=mssql_clickhouse_query_template(columns),
        ),
        sink_reader=ClickHouseRouteRefreshRowsReader(
            connector=clickhouse,
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
        source="mssql",
        sink="clickhouse",
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
    clickhouse_settings: Any,
    columns: Sequence[Any],
) -> Path:
    payload = {
        "source_dataset": source_dataset,
        "target_dataset": target_dataset,
        "boundary_column": "order_id",
        "columns": column_names(columns),
        "query_template": mssql_clickhouse_query_template(columns),
        "mssql": {
            "host": os.getenv("DPONE_IT_MSSQL_HOST", "127.0.0.1"),
            "port": int(os.getenv("DPONE_IT_MSSQL_PORT", "1433")),
            "database": os.getenv("DPONE_IT_MSSQL_DATABASE", "master"),
            "user": os.getenv("DPONE_IT_MSSQL_USER", "sa"),
            "password": os.getenv("DPONE_IT_MSSQL_PASSWORD", ""),
            "options": {
                "bcp_path": os.getenv("DPONE_IT_MSSQL_BCP_PATH", "bcp"),
                "batch_size": 100000,
                "packet_size": 16384,
                "timeout_seconds": 3600,
                "trust_server_certificate": True,
            },
        },
        "clickhouse": {
            "host": os.getenv("DPONE_IT_CH_CLIENT_HOST", getattr(clickhouse_settings, "host")),
            "port": int(os.getenv("DPONE_IT_CH_CLIENT_PORT", str(getattr(clickhouse_settings, "port")))),
            "database": getattr(clickhouse_settings, "database"),
            "user": getattr(clickhouse_settings, "user"),
            "password": getattr(clickhouse_settings, "password"),
            "options": {
                "client_command": os.getenv("DPONE_IT_CH_CLIENT_COMMAND", "clickhouse-client"),
                "input_format": "TabSeparated",
                "timeout_seconds": 3600,
                "settings": {"mutations_sync": 2},
            },
        },
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _clickhouse_count(connector: Any, *, database: str, table: str) -> int:
    return int(
        connector.get_records(
            f"SELECT count() AS row_count FROM `{database}`.`{table}`",
            as_dict=True,
        )[0]["row_count"]
    )


def _clickhouse_column_count(connector: Any, *, database: str, table: str) -> int:
    return int(
        connector.get_records(
            f"""
            SELECT count() AS column_count
            FROM system.columns
            WHERE database = '{database}' AND table = '{table}'
            """,
            as_dict=True,
        )[0]["column_count"]
    )
