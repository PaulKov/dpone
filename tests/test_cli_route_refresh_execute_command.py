from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.ops.route_refresh_plan import RouteRefreshPlanService


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _plan(tmp_path: Path, *, invalid_window: bool = False) -> Path:
    report = RouteRefreshPlanService().plan(
        output_dir=tmp_path / "plan",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        dataset="analytics.orders",
        reason="range_replay",
        window_kind="integer",
        start="100" if invalid_window else "1",
        end="10" if invalid_window else "20",
        chunk_size=10,
    )
    return Path(report.json_path)


def test_ops_route_refresh_execute_cli_outputs_dry_run_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    plan_json = _plan(tmp_path)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-refresh-execute",
                "--route-refresh-plan-json",
                str(plan_json),
                "--runner-id",
                "operator-a",
                "--output-dir",
                str(tmp_path / "execute"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "dpone.route_refresh_execution.v1"
    assert payload["mode"] == "dry_run"
    assert payload["executed"] is False
    assert payload["status"] == "dry_run"
    assert Path(payload["json_path"]).exists()


def test_ops_route_refresh_execute_cli_requires_backend_for_execute(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    plan_json = _plan(tmp_path)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-refresh-execute",
                "--route-refresh-plan-json",
                str(plan_json),
                "--runner-id",
                "operator-a",
                "--execute",
                "--output-dir",
                str(tmp_path / "execute"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert "route_refresh_execution.executor_unavailable" in payload["blockers"]


def test_ops_route_refresh_execute_cli_reports_missing_mssql_clickhouse_config(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    plan_json = _plan(tmp_path)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-refresh-execute",
                "--route-refresh-plan-json",
                str(plan_json),
                "--runner-id",
                "operator-a",
                "--execute",
                "--executor",
                "mssql_clickhouse",
                "--output-dir",
                str(tmp_path / "execute"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "partial_failure"
    assert "mssql_clickhouse_refresh_executor.config_missing" in payload["blockers"]
    assert payload["chunks"][0]["status"] == "failed"


def test_ops_route_refresh_execute_cli_reports_missing_postgres_mssql_config(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    plan_json = (
        RouteRefreshPlanService()
        .plan(
            output_dir=tmp_path / "plan",
            source="postgres",
            sink="mssql",
            strategy="incremental_merge",
            dataset="dbo.orders",
            reason="range_replay",
            window_kind="integer",
            start="1",
            end="20",
            chunk_size=10,
        )
        .json_path
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-refresh-execute",
                "--route-refresh-plan-json",
                str(plan_json),
                "--runner-id",
                "operator-a",
                "--execute",
                "--executor",
                "postgres_mssql",
                "--output-dir",
                str(tmp_path / "execute"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "partial_failure"
    assert "postgres_mssql_refresh_executor.config_missing" in payload["blockers"]
    assert payload["chunks"][0]["status"] == "failed"


def test_ops_route_refresh_execute_cli_returns_nonzero_for_blocked_plan(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    plan_json = _plan(tmp_path, invalid_window=True)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-refresh-execute",
                "--route-refresh-plan-json",
                str(plan_json),
                "--runner-id",
                "operator-a",
                "--output-dir",
                str(tmp_path / "execute"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert "route_refresh_execution.plan_blocked" in payload["blockers"]
