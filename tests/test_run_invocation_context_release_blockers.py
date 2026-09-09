"""Release-blocker regressions for scheduler-aware run invocation."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from dpone import api
from dpone.backfill.mapping import AIRFLOW_MAPPING_ITEM_ENV
from dpone.commands import run_cmd
from dpone.contracts import ETLConfigurationError
from dpone.contracts.process_types import ProcessResult
from dpone.contracts.run_interval import RUN_INTERVAL_ENV_NAMES
from dpone.services.airflow_mapping_context import AirflowMappingContextService
from dpone.services.interval_context import IntervalContextService
from dpone.services.manifest import ManifestCommandContext
from dpone.services.run_invocation_context import RunInvocationContextService
from dpone.services.run_manifest import RunManifestService


@pytest.mark.parametrize(
    ("environment", "error_code"),
    (
        (
            {AIRFLOW_MAPPING_ITEM_ENV: '{"predicate":"not a mapping item"}'},
            "DPONE_AIRFLOW_MAPPING_ITEM_INVALID",
        ),
        (
            {
                "DPONE_PARTITION_DIMENSION": "business_date",
                "DPONE_PARTITION_MODE": "native",
            },
            "DPONE_AIRFLOW_PARTITION_KEY_MISSING",
        ),
        (
            {"DPONE_LOGICAL_DATE": "not-an-iso-date"},
            "DPONE_LOGICAL_DATE_INVALID",
        ),
    ),
)
def test_scheduler_configuration_failure_exits_two_before_manifest_loading(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    environment: dict[str, str],
    error_code: str,
) -> None:
    _replace_scheduler_environment(monkeypatch, environment)

    def fail_manifest_loading(*_args: object, **_kwargs: object) -> object:
        pytest.fail("scheduler configuration must fail before manifest loading")

    monkeypatch.setattr(run_cmd, "build_manifest_context", fail_manifest_loading)

    exit_code = run_cmd.cmd_run(
        _run_args(Path("manifest.yaml")),
        ctx=object(),
        logger=logging.getLogger("test"),
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert payload["result"]["error_code"] == error_code


def test_executed_value_error_remains_runtime_exit_one(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _replace_scheduler_environment(monkeypatch, {})

    class RuntimeFailureService:
        def run(self, **kwargs: Any) -> object:
            kwargs["on_execution_started"]()
            raise ValueError("runtime token=secret")

    monkeypatch.setattr(run_cmd, "RunManifestService", RuntimeFailureService)
    monkeypatch.setattr(run_cmd, "build_manifest_context", lambda args, ctx: object())

    exit_code = run_cmd.cmd_run(
        _run_args(Path("manifest.yaml")),
        ctx=object(),
        logger=logging.getLogger("test"),
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert payload["passed"] is False


def test_empty_explicit_values_preserve_environment_scheduler_identity() -> None:
    environment = {
        "DPONE_DAG_ID": "environment_dag",
        "DPONE_LOGICAL_DATE": "2026-07-29T00:00:00+00:00",
        "DPONE_INTERVAL_START": "2026-07-29T00:00:00+00:00",
        "DPONE_INTERVAL_END": "2026-07-30T00:00:00+00:00",
    }

    invocation = _invocation_service().resolve(
        environ=environment,
        dag_id="",
        execution_date="",
        interval_start="",
        interval_end="",
        normalize_explicit_execution_date=True,
    )

    assert invocation.dag_id == "environment_dag"
    assert invocation.execution_date == datetime.fromisoformat("2026-07-29T00:00:00+00:00")
    assert _effective_interval(invocation.load_config_mutator) == {
        "dag_id": "environment_dag",
        "interval_end": "2026-07-30T00:00:00+00:00",
        "interval_start": "2026-07-29T00:00:00+00:00",
        "logical_date": "2026-07-29T00:00:00+00:00",
    }


@pytest.mark.parametrize(
    ("environment_name", "error_code"),
    (
        ("DPONE_LOGICAL_DATE", "DPONE_LOGICAL_DATE_INVALID"),
        ("DPONE_INTERVAL_START", "DPONE_INTERVAL_START_INVALID"),
        ("DPONE_INTERVAL_END", "DPONE_INTERVAL_END_INVALID"),
    ),
)
def test_malformed_effective_iso_environment_value_is_configuration_error(
    environment_name: str,
    error_code: str,
) -> None:
    with pytest.raises(ETLConfigurationError) as caught:
        _invocation_service().resolve(
            environ={
                "DPONE_DAG_ID": "orders_daily",
                environment_name: "not-an-iso-date",
            }
        )

    assert getattr(caught.value, "code", None) == error_code


def test_cli_and_python_execute_equivalent_real_manifest_scheduler_context(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    manifest = tmp_path / "manifest.yaml"
    manifest.write_text(
        """
name: scheduler_context
source:
  type: postgres
  connection_id: source
  table:
    schema: public
    name: orders
  options:
    custom_predicate: "created_at >= '{{ data_interval_start }}'"
sink:
  type: postgres
  connection_id: target
  table:
    schema: analytics
    name: orders
  strategy:
    mode: full_refresh
state:
  type: disabled
""".strip()
        + "\n",
        encoding="utf-8",
    )
    environment = {
        "DPONE_DAG_ID": "orders_daily",
        "DPONE_DAG_RUN_ID": "scheduled__2026-07-29",
        "DPONE_LOGICAL_DATE": "2026-07-29T00:00:00+00:00",
        "DPONE_INTERVAL_START": "2026-07-29T00:00:00+00:00",
        "DPONE_INTERVAL_END": "2026-07-30T00:00:00+00:00",
        AIRFLOW_MAPPING_ITEM_ENV: _mapping_item_json(),
    }
    _replace_scheduler_environment(monkeypatch, environment)
    observations: list[dict[str, object]] = []

    class CapturingProcess:
        def __init__(self, config: object, config_path: str) -> None:
            self._config = config
            self._config_path = config_path

        def run(
            self,
            *,
            context: object,
            dag_id: str | None,
            execution_date: object | None,
        ) -> ProcessResult:
            observations.append(
                {
                    "config": self._config,
                    "config_path": self._config_path,
                    "context": context,
                    "dag_id": dag_id,
                    "execution_date": execution_date,
                }
            )
            return ProcessResult(
                status="success",
                inserted_rows=1,
                updated_rows=0,
                final_rows=1,
                extracted_rows=1,
                duration_seconds=0.01,
                errors=[],
            )

    class MetadataOnlyLoader:
        def __init__(self, delegate: object) -> None:
            self._delegate = delegate

        def load(self, path: Path, *, metadata_only: bool = True) -> object:
            del metadata_only
            return self._delegate.load(path, metadata_only=True)  # type: ignore[attr-defined,no-any-return]

    class ExecutingService:
        def run(self, **kwargs: Any) -> object:
            manifest_ctx = kwargs["manifest_ctx"]
            kwargs["manifest_ctx"] = ManifestCommandContext(
                registry_paths=manifest_ctx.registry_paths,
                loader=MetadataOnlyLoader(manifest_ctx.loader),  # type: ignore[arg-type]
            )
            return RunManifestService(process_factory=CapturingProcess).run(**kwargs)

    monkeypatch.setattr(run_cmd, "RunManifestService", ExecutingService)
    monkeypatch.setattr(api, "RunManifestService", ExecutingService)

    cli_args = _run_args(manifest)
    cli_args.dag_id = ""
    cli_args.execution_date = ""
    cli_args.interval_start = ""
    cli_args.interval_end = ""
    assert run_cmd.cmd_run(cli_args, ctx=object(), logger=logging.getLogger("test")) == 0
    assert json.loads(capsys.readouterr().out)["passed"] is True
    assert api.run(manifest, dag_id="", execution_date="").passed is True

    assert len(observations) == 2
    cli_observation, python_observation = observations
    for observation in observations:
        assert observation["dag_id"] == "orders_daily"
        assert observation["execution_date"] == datetime.fromisoformat("2026-07-29T00:00:00+00:00")
        context = observation["context"]
        assert context.run_id == "scheduled__2026-07-29"  # type: ignore[attr-defined]
        selection = context.config["airflow_mapping_selection"]  # type: ignore[attr-defined]
        assert selection.chunk_indexes == (1, 2)
        load_config = observation["config"].load_config  # type: ignore[attr-defined]
        assert load_config.options["interval"]["interval_start"] == "2026-07-29T00:00:00+00:00"
        assert load_config.options["source_custom_predicate"] == ("created_at >= '2026-07-29T00:00:00+00:00'")
    assert cli_observation["dag_id"] == python_observation["dag_id"]
    assert cli_observation["execution_date"] == python_observation["execution_date"]


def _invocation_service() -> RunInvocationContextService:
    return RunInvocationContextService(
        mapping_context_service=AirflowMappingContextService(),
        interval_context_factory=IntervalContextService,
    )


def _run_args(path: Path) -> argparse.Namespace:
    return argparse.Namespace(
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


def _replace_scheduler_environment(
    monkeypatch: pytest.MonkeyPatch,
    environment: dict[str, str],
) -> None:
    for name in (*RUN_INTERVAL_ENV_NAMES, AIRFLOW_MAPPING_ITEM_ENV):
        monkeypatch.delenv(name, raising=False)
    for name, value in environment.items():
        monkeypatch.setenv(name, value)


def _mapping_item_json() -> str:
    return json.dumps(
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


def _effective_interval(mutator: object) -> dict[str, str | None]:
    from dpone.config.load_config import LoadConfig

    load_config = LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="public",
        source_table="orders",
        target_schema="analytics",
        target_table="orders",
    )
    assert callable(mutator)
    interval = mutator(load_config).options["interval"]
    return {
        "dag_id": interval["dag_id"],
        "interval_end": interval["interval_end"],
        "interval_start": interval["interval_start"],
        "logical_date": interval["logical_date"],
    }
