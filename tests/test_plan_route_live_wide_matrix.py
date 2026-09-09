"""Unit tests for tools/ci/plan_route_live_wide_matrix.py."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "tools" / "ci" / "plan_route_live_wide_matrix.py"


def _load_helper():
    spec = importlib.util.spec_from_file_location("plan_route_live_wide_matrix", HELPER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


helper = _load_helper()


def test_schedule_plans_all_docker_legs_and_no_bq() -> None:
    plan = helper.plan_route_live_wide_matrix(event_name="schedule")
    assert plan.run_docker is True
    assert plan.run_bq is False
    assert len(plan.docker_include) == 12
    assert plan.bq_include == []
    assert {"source": "mssql", "sink": "clickhouse"} in plan.docker_include


def test_dispatch_filters_source_and_sink() -> None:
    plan = helper.plan_route_live_wide_matrix(
        event_name="workflow_dispatch",
        source="postgres",
        sink="mssql",
    )
    assert plan.docker_include == [{"source": "postgres", "sink": "mssql"}]
    assert plan.run_bq is False


def test_dispatch_bigquery_requires_flag() -> None:
    with pytest.raises(ValueError, match="include_bigquery"):
        helper.plan_route_live_wide_matrix(
            event_name="workflow_dispatch",
            sink="bigquery",
            include_bigquery=False,
        )

    plan = helper.plan_route_live_wide_matrix(
        event_name="workflow_dispatch",
        source="mysql",
        sink="bigquery",
        include_bigquery=True,
    )
    assert plan.docker_include == []
    assert plan.bq_include == [{"source": "mysql"}]


def test_cli_json_and_github_output(capsys: pytest.CaptureFixture[str]) -> None:
    assert helper.main(["--event-name", "schedule"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["run_docker"] is True
    assert payload["run_bq"] is False

    assert (
        helper.main(
            [
                "--event-name",
                "workflow_dispatch",
                "--source",
                "postgres",
                "--sink",
                "kafka",
                "--github-output",
            ]
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "docker_matrix<<EOF" in out
    assert "run_docker=true" in out
    assert "run_bq=false" in out


def test_cli_rejects_invalid_bigquery_combo(capsys: pytest.CaptureFixture[str]) -> None:
    assert (
        helper.main(
            [
                "--event-name",
                "workflow_dispatch",
                "--sink",
                "bigquery",
            ]
        )
        == 1
    )
    err = capsys.readouterr().err
    assert "include_bigquery" in err
    # Ensure JSON helper still produces parseable docker matrix shape.
    plan = helper.plan_route_live_wide_matrix(event_name="schedule")
    assert json.loads(json.dumps(plan.docker_matrix()))["include"]
