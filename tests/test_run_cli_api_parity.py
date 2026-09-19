from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.backfill.mapping import AIRFLOW_MAPPING_ITEM_ENV
from dpone.config.load_config import LoadConfig
from dpone.contracts.clickhouse_external_replication import ExternalContractError
from dpone.contracts.process_types import ProcessResult
from dpone.contracts.run_interval import RUN_INTERVAL_ENV_NAMES
from dpone.ports.runtime_hydrator import RuntimeBindings
from dpone.services.interval_context import IntervalContextService
from dpone.services.run_invocation_context import RunInvocationContextService
from dpone.services.run_manifest import RunManifestService

ROOT = Path(__file__).resolve().parents[1]
EXTERNAL_REPLICATION_MANIFEST = ROOT / "examples/batch/clickhouse-external-replication-full-refresh.batch.yaml"


def test_cli_and_python_api_resolve_equivalent_environment_invocation(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    from dpone import api
    from dpone.commands import run_cmd

    mapping_payload = json.dumps(
        {
            "schema": "dpone.airflow-mapping-item.v1",
            "mode": "summary",
            "mapping_plan_fingerprint": f"sha256:{'a' * 64}",
            "backfill_plan_hash": f"sha256:{'b' * 64}",
            "item_index": 0,
            "first_chunk_index": 1,
            "last_chunk_index": 2,
            "chunks_count": 2,
        }
    )
    environment = {
        "DPONE_DAG_ID": "orders_daily",
        "DPONE_DAG_RUN_ID": "scheduled__2026-07-29",
        "DPONE_LOGICAL_DATE": "2026-07-29T00:00:00+00:00",
        "DPONE_INTERVAL_START": "2026-07-29T00:00:00+00:00",
        "DPONE_INTERVAL_END": "2026-07-30T00:00:00+00:00",
        AIRFLOW_MAPPING_ITEM_ENV: mapping_payload,
    }
    for name in (*RUN_INTERVAL_ENV_NAMES, AIRFLOW_MAPPING_ITEM_ENV):
        monkeypatch.delenv(name, raising=False)
    for name, value in environment.items():
        monkeypatch.setenv(name, value)

    calls: list[dict[str, object]] = []

    class Service:
        def run(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(
                passed=True,
                to_dict=lambda: {"passed": True},
                to_markdown=lambda: "# dpone run\n",
                to_text=lambda: "dpone run\n",
            )

    monkeypatch.setattr(run_cmd, "RunManifestService", Service)
    monkeypatch.setattr(run_cmd, "build_manifest_context", lambda args, ctx: object())
    monkeypatch.setattr(api, "RunManifestService", Service)

    assert (
        run_cmd.cmd_run(
            _run_args(tmp_path / "manifest.yaml"),
            ctx=object(),
            logger=logging.getLogger("test"),
        )
        == 0
    )
    capsys.readouterr()
    api.run(tmp_path / "manifest.yaml")

    cli_call, api_call = calls
    assert cli_call["dag_id"] == api_call["dag_id"] == "orders_daily"
    assert (
        cli_call["execution_date"] == api_call["execution_date"] == datetime.fromisoformat("2026-07-29T00:00:00+00:00")
    )
    assert cli_call["run_context_config"] == api_call["run_context_config"]
    assert _mutated_options(cli_call) == _mutated_options(api_call)


def test_python_api_explicit_identity_overrides_environment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from dpone import api

    for name in RUN_INTERVAL_ENV_NAMES:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DPONE_DAG_ID", "environment_dag")
    monkeypatch.setenv("DPONE_LOGICAL_DATE", "2026-07-29T00:00:00+00:00")
    calls: list[dict[str, object]] = []

    class Service:
        def run(self, **kwargs):
            calls.append(kwargs)
            return "result"

    monkeypatch.setattr(api, "RunManifestService", Service)
    explicit_date = datetime.fromisoformat("2026-07-30T12:34:56+00:00")

    assert (
        api.run(
            tmp_path / "manifest.yaml",
            dag_id="explicit_dag",
            execution_date=explicit_date,
        )
        == "result"
    )

    assert calls[0]["dag_id"] == "explicit_dag"
    assert calls[0]["execution_date"] == explicit_date


def test_invocation_context_uses_one_environment_snapshot() -> None:
    source_environment = {
        "DPONE_DAG_ID": "snapshot_dag",
        "DPONE_LOGICAL_DATE": "2026-07-29T00:00:00+00:00",
    }

    class MutatingMappingContext:
        def from_environ(self, environ):
            source_environment["DPONE_DAG_ID"] = "mutated_dag"
            return {"observed_dag_id": environ["DPONE_DAG_ID"]}

    invocation = RunInvocationContextService(
        mapping_context_service=MutatingMappingContext(),
        interval_context_factory=IntervalContextService,
    ).resolve(environ=source_environment)

    assert invocation.dag_id == "snapshot_dag"
    assert invocation.run_context_config == {"observed_dag_id": "snapshot_dag"}


def test_external_replication_manifest_has_cli_python_execution_parity(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The documented external route has one normalized plan and result contract."""

    from dpone import api
    from dpone.commands import run_cmd
    from dpone.ports import runtime_hydrator

    normalized_routes: list[dict[str, object]] = []

    class Hydrator:
        def build(self, *, config: dict[str, Any], load_config: LoadConfig) -> RuntimeBindings:
            del config
            return RuntimeBindings(source_obj=object(), sink_obj=object())

    class Process:
        def __init__(self, *, config: Any, config_path: str) -> None:
            del config_path
            load = config.load_config
            cluster = load.options["physical_design"]["storage"]["clickhouse"]["cluster"]
            normalized_routes.append(
                {
                    "source": load.options["source_type"],
                    "sink": load.options["sink_type"],
                    "strategy": load.load_strategy.value,
                    "source_connection": load.source_conn_id,
                    "sink_connection": load.target_conn_id,
                    "target": f"{load.target_schema}.{load.target_table}",
                    "cluster": dict(cluster),
                }
            )

        def run(self, *, context: Any, dag_id: str | None, execution_date: Any) -> ProcessResult:
            del dag_id, execution_date
            return ProcessResult(
                status="success",
                inserted_rows=2,
                updated_rows=0,
                final_rows=2,
                extracted_rows=2,
                duration_seconds=0.25,
                errors=[],
                details={
                    "identity": {"run_id": context.run_id, "operation_id": "opaque-operation"},
                    "state": {"checkpoint": "committed"},
                    "evidence": {"phase": "COMMITTED", "cleanup": "complete"},
                },
            )

    def service() -> RunManifestService:
        return RunManifestService(process_factory=Process)

    monkeypatch.setattr(runtime_hydrator, "_RUNTIME_HYDRATOR", Hydrator())
    monkeypatch.setattr(run_cmd, "RunManifestService", service)
    monkeypatch.setattr(api, "RunManifestService", service)
    args = _run_args(EXTERNAL_REPLICATION_MANIFEST)
    args.run_id = "external-cluster-full-refresh-parity"

    assert run_cmd.cmd_run(args, ctx=object(), logger=logging.getLogger("test")) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    python_payload = api.run(
        EXTERNAL_REPLICATION_MANIFEST,
        run_id="external-cluster-full-refresh-parity",
    ).to_dict()

    assert normalized_routes == [normalized_routes[0], normalized_routes[0]]
    assert normalized_routes[0] == {
        "source": "mssql",
        "sink": "clickhouse",
        "strategy": "full_refresh",
        "source_connection": "source_connection",
        "sink_connection": "target_connection",
        "target": "analytics.target_table",
        "cluster": {
            "name": "analytics_cluster",
            "ddl_scope": "cluster",
            "replication_mode": "external",
            "external_content_row_budget": 100000,
        },
    }
    assert cli_payload.pop("manifest") == "$ABSOLUTE_PATH"
    assert Path(str(python_payload.pop("manifest"))).resolve() == EXTERNAL_REPLICATION_MANIFEST
    assert cli_payload == python_payload
    assert python_payload["result"]["details"] == {
        "identity": {
            "run_id": "external-cluster-full-refresh-parity",
            "operation_id": "opaque-operation",
        },
        "state": {"checkpoint": "committed"},
        "evidence": {"phase": "COMMITTED", "cleanup": "complete"},
    }


def test_external_replication_typed_failure_matches_cli_and_python_api(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Both public entrypoints retain the external-route error classification."""

    from dpone import api
    from dpone.commands import run_cmd
    from dpone.ports import runtime_hydrator

    class Hydrator:
        def build(self, *, config: dict[str, Any], load_config: LoadConfig) -> RuntimeBindings:
            del config, load_config
            return RuntimeBindings(source_obj=object(), sink_obj=object())

    class FailingProcess:
        def __init__(self, *, config: Any, config_path: str) -> None:
            del config, config_path

        def run(self, *, context: Any, dag_id: str | None, execution_date: Any) -> ProcessResult:
            del context, dag_id, execution_date
            raise ExternalContractError("MODE_MISMATCH", "external topology is inconsistent")

    def service() -> RunManifestService:
        return RunManifestService(process_factory=FailingProcess)

    monkeypatch.setattr(runtime_hydrator, "_RUNTIME_HYDRATOR", Hydrator())
    monkeypatch.setattr(run_cmd, "RunManifestService", service)
    monkeypatch.setattr(api, "RunManifestService", service)
    args = _run_args(EXTERNAL_REPLICATION_MANIFEST)
    args.run_id = "external-cluster-full-refresh-failure"

    assert run_cmd.cmd_run(args, ctx=object(), logger=logging.getLogger("test")) == 1
    cli_error = json.loads(capsys.readouterr().err)
    with pytest.raises(ExternalContractError) as raised:
        api.run(
            EXTERNAL_REPLICATION_MANIFEST,
            run_id="external-cluster-full-refresh-failure",
        )

    assert raised.value.code == "DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_MODE_MISMATCH"
    assert cli_error["result"]["error_code"] == raised.value.code
    assert cli_error["result"]["errors"] == [str(raised.value)]


def _mutated_options(call: dict[str, object]) -> dict[str, object]:
    mutator = call["load_config_mutator"]
    assert callable(mutator)
    config = LoadConfig(
        source_conn_id="src",
        target_conn_id="tgt",
        source_schema="dbo",
        source_table="orders",
        target_schema="analytics",
        target_table="orders",
        options={"predicate": "{{ data_interval_start }}"},
    )
    return mutator(config).options


def _run_args(path: Path) -> SimpleNamespace:
    return SimpleNamespace(
        path=path,
        selector=None,
        run_id=None,
        dag_id=None,
        execution_date=None,
        interval_start=None,
        interval_end=None,
        retry_attempts=0,
        retry_backoff_seconds=0.0,
        format="json",
        registry=[],
        sample=None,
        target=None,
        select=[],
        exclude=[],
        state=None,
        selectors="selectors.yaml",
        max_selected=10,
    )
