"""Airflow-driven backfill integration: interval contract end-to-end.

Simulates exactly what an Airflow task instance does with a dpone pack:

1. the pack templates ``DPONE_INTERVAL_START/END`` etc. into KPO ``env_vars``;
2. Airflow renders them per DAG run;
3. the pod executes ``dpone run <manifest>`` with those variables set.

The test renders two different data intervals, runs the real CLI against the
docker stack, and asserts each run loads exactly its own interval window
(idempotent per-interval semantics), that campaign identities differ per
interval, and that the XCom summary carries the ``interval``/``backfill``
sections.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_backfill,
    pytest.mark.integration_postgres,
    pytest.mark.integration_clickhouse,
]
if str(os.getenv("DPONE_RUN_INTEGRATION", "0")).strip().lower() not in {"1", "true", "yes", "on"}:
    pytest.skip("Integration tests are disabled", allow_module_level=True)

_ROOT = Path(__file__).resolve().parents[3]

_MANIFEST_TEMPLATE = """
name: {process}

source:
  type: postgres
  connection_id: {source_conn}
  connection_type: env
  table:
    schema: {source_schema}
    name: {table}
  options:
    batch_commit_mode: whole

sink:
  type: clickhouse
  connection_id: {sink_conn}
  connection_type: env
  table:
    schema: {target_schema}
    name: {table}
  strategy:
    mode: backfill
    unique_key: [id]
    backfill:
      inner_mode: incremental_merge
      state_dir: {state_dir}
      chunk:
        column: business_date
        from: "{{{{ data_interval_start }}}}"
        to: "{{{{ data_interval_end }}}}"
        step: 1d
        kind: timestamp
  options:
    load_governance:
      enabled: false
    lineage:
      enabled: false

state:
  type: disabled

runtime:
  compatibility:
    legacy_runtime_connections: explicit_only
"""


def _airflow_rendered_env(interval_start: str, interval_end: str, *, dag_id: str, run_id: str) -> dict[str, str]:
    """Values exactly as Airflow renders the pack env_vars templates."""

    return {
        "DPONE_DAG_ID": dag_id,
        "DPONE_DAG_RUN_ID": run_id,
        "DPONE_TRY_NUMBER": "1",
        "DPONE_LOGICAL_DATE": interval_start,
        "DPONE_INTERVAL_START": interval_start,
        "DPONE_INTERVAL_END": interval_end,
    }


def _connection_env(postgres_settings, clickhouse_settings, *, source_conn: str, sink_conn: str) -> dict[str, str]:
    pg = source_conn.upper()
    ch = sink_conn.upper()
    return {
        f"{pg}_HOST": postgres_settings.host,
        f"{pg}_PORT": str(postgres_settings.port),
        f"{pg}_DATABASE": postgres_settings.database,
        f"{pg}_USERNAME": postgres_settings.user,
        f"{pg}_PASSWORD": postgres_settings.password,
        f"{ch}_HOST": clickhouse_settings.host,
        f"{ch}_PORT": str(clickhouse_settings.port),
        f"{ch}_DATABASE": clickhouse_settings.database,
        f"{ch}_USERNAME": clickhouse_settings.user,
        f"{ch}_PASSWORD": clickhouse_settings.password,
    }


def _run_cli(manifest: Path, env: dict[str, str], evidence_path: Path) -> dict:
    process_env = {**os.environ, **env}
    result = subprocess.run(
        [sys.executable, "-m", "dpone.cli.main", "run", str(manifest), "--format", "json"],
        capture_output=True,
        text=True,
        cwd=_ROOT,
        env=process_env,
        timeout=300,
        check=False,
    )
    assert result.returncode == 0, f"dpone run failed:\nstdout={result.stdout[-4000:]}\nstderr={result.stderr[-2000:]}"
    payload = json.loads(result.stdout[result.stdout.index("{") :])
    evidence_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def test_airflow_interval_driven_backfill_is_idempotent_per_interval(
    postgres_connector,
    postgres_schema,
    postgres_settings,
    clickhouse_connector,
    clickhouse_settings,
    tmp_path,
) -> None:
    sys.path.insert(0, str(_ROOT / "tests" / "integration" / "backfill"))
    try:
        from backfill_toolkit import SeedSpec
        from endpoints import ClickHouseEndpoint, PostgresEndpoint
    finally:
        sys.path.pop(0)

    spec = SeedSpec()
    table = f"bf_af_{uuid.uuid4().hex[:8]}"
    source = PostgresEndpoint(postgres_connector)
    target = ClickHouseEndpoint(clickhouse_connector, database=clickhouse_settings.database)
    source.seed(postgres_schema, table, spec)
    target.drop(clickhouse_settings.database, table)

    source_conn = f"pg_af_{uuid.uuid4().hex[:6]}"
    sink_conn = f"ch_af_{uuid.uuid4().hex[:6]}"
    manifest = tmp_path / "orders_backfill.yml"
    manifest.write_text(
        _MANIFEST_TEMPLATE.format(
            process=f"orders_backfill_{table}",
            source_conn=source_conn,
            sink_conn=sink_conn,
            source_schema=postgres_schema,
            target_schema=clickhouse_settings.database,
            table=table,
            state_dir=tmp_path / "ledger",
        ).lstrip(),
        encoding="utf-8",
    )
    base_env = _connection_env(postgres_settings, clickhouse_settings, source_conn=source_conn, sink_conn=sink_conn)

    try:
        # --- DAG run 1: interval 2025-01-01 -> 2025-01-03 (2 daily chunks)
        first = _run_cli(
            manifest,
            {
                **base_env,
                **_airflow_rendered_env(
                    "2025-01-01T00:00:00+00:00",
                    "2025-01-03T00:00:00+00:00",
                    dag_id="orders_backfill",
                    run_id="scheduled__2025-01-01",
                ),
            },
            tmp_path / "evidence-1.json",
        )
        backfill_first = first["result"]["details"]["backfill"]
        assert backfill_first["chunks_total"] == 2
        assert backfill_first["chunks_committed"] == 2
        first_window = source.checksum(
            postgres_schema, table, predicate="business_date >= '2025-01-01' AND business_date < '2025-01-03'"
        )
        assert target.checksum(target.database, table) == first_window

        # --- Re-run of the SAME interval (Airflow task clear): no duplicates.
        rerun = _run_cli(
            manifest,
            {
                **base_env,
                **_airflow_rendered_env(
                    "2025-01-01T00:00:00+00:00",
                    "2025-01-03T00:00:00+00:00",
                    dag_id="orders_backfill",
                    run_id="scheduled__2025-01-01_try2",
                ),
            },
            tmp_path / "evidence-1-rerun.json",
        )
        backfill_rerun = rerun["result"]["details"]["backfill"]
        assert backfill_rerun["run_key"] == backfill_first["run_key"], "same interval -> same campaign identity"
        assert backfill_rerun["chunks_skipped_resume"] == 2, "committed interval chunks must not re-run"
        assert target.checksum(target.database, table) == first_window
        assert target.duplicate_id_count(target.database, table) == 0

        # --- DAG run 2: next interval 2025-01-03 -> 2025-01-05.
        second = _run_cli(
            manifest,
            {
                **base_env,
                **_airflow_rendered_env(
                    "2025-01-03T00:00:00+00:00",
                    "2025-01-05T00:00:00+00:00",
                    dag_id="orders_backfill",
                    run_id="scheduled__2025-01-03",
                ),
            },
            tmp_path / "evidence-2.json",
        )
        backfill_second = second["result"]["details"]["backfill"]
        assert backfill_second["run_key"] != backfill_first["run_key"], "each interval owns its campaign identity"
        assert backfill_second["chunks_committed"] == 2
        both_windows = source.checksum(
            postgres_schema, table, predicate="business_date >= '2025-01-01' AND business_date < '2025-01-05'"
        )
        assert target.checksum(target.database, table) == both_windows

        _assert_xcom_summary_contract(tmp_path)
    finally:
        target.drop(clickhouse_settings.database, table)


def _assert_xcom_summary_contract(tmp_path: Path) -> None:
    """The XCom builder must surface interval and backfill progress sections."""

    xcom_path = tmp_path / "xcom-return.json"
    env = {
        **os.environ,
        "DPONE_INTERVAL_START": "2025-01-01T00:00:00+00:00",
        "DPONE_INTERVAL_END": "2025-01-03T00:00:00+00:00",
        "DPONE_LOGICAL_DATE": "2025-01-01T00:00:00+00:00",
        "DPONE_DAG_ID": "orders_backfill",
    }
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "dpone.cli.main",
            "gitops",
            "airflow",
            "xcom-from-evidence",
            "--evidence-path",
            str(tmp_path / "evidence-1.json"),
            "--xcom-output",
            str(xcom_path),
            "--status",
            "passed",
        ],
        capture_output=True,
        text=True,
        cwd=_ROOT,
        env=env,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    summary = json.loads(xcom_path.read_text(encoding="utf-8"))
    assert summary["kind"] == "gitops.airflow_xcom_summary"
    assert summary["interval"]["interval_start"] == "2025-01-01T00:00:00+00:00"
    assert summary["interval"]["dag_id"] == "orders_backfill"
    assert summary["backfill"]["chunks_total"] == 2
    assert "chunks" not in summary["backfill"], "XCom must stay bounded (no per-chunk payloads)"
