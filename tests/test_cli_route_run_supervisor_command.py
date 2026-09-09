from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_ops_route_run_supervisor_cli_outputs_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    route = {"source": "mssql", "sink": "clickhouse", "strategy": "incremental_merge"}
    artifacts = {
        name: _write_json(tmp_path / f"{name}.json", {"route": route, "passed": True, "blockers": []})
        for name in ("route_readiness", "route_execution_ledger", "state_promotion")
    }

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-run-supervisor",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--strategy",
                "incremental_merge",
                "--run-id",
                "orders-run-001",
                "--dataset",
                "analytics.orders",
                "--manifest",
                "manifests/orders.yml",
                "--artifact",
                f"route_readiness={artifacts['route_readiness']}",
                "--artifact",
                f"route_execution_ledger={artifacts['route_execution_ledger']}",
                "--artifact",
                f"state_promotion={artifacts['state_promotion']}",
                "--output-dir",
                str(tmp_path / "route-run"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["decision"]["status"] == "ready"
    assert payload["run"]["manifest"] == "manifests/orders.yml"
    assert Path(payload["json_path"]).exists()


def test_ops_route_run_supervisor_cli_returns_nonzero_for_blocked_run(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    readiness = _write_json(
        tmp_path / "readiness.json",
        {"route": {"source": "mssql", "sink": "clickhouse", "strategy": "incremental_merge"}, "passed": True},
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-run-supervisor",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--strategy",
                "incremental_merge",
                "--run-id",
                "orders-run-001",
                "--dataset",
                "analytics.orders",
                "--artifact",
                f"route_readiness={readiness}",
                "--output-dir",
                str(tmp_path / "route-run"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["decision"]["status"] == "blocked"
    assert "route_execution_ledger.missing" in payload["blockers"]


def test_ops_route_run_supervisor_cli_accepts_route_refresh_mode(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    readiness = _write_json(
        tmp_path / "readiness.json",
        {"route": {"source": "mssql", "sink": "clickhouse", "strategy": "incremental_merge"}, "passed": True},
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-run-supervisor",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--strategy",
                "incremental_merge",
                "--run-id",
                "orders-run-001",
                "--dataset",
                "analytics.orders",
                "--run-mode",
                "route_refresh",
                "--artifact",
                f"route_readiness={readiness}",
                "--output-dir",
                str(tmp_path / "route-run"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["run"]["mode"] == "route_refresh"
    assert payload["execution_contract"]["mode"] == "route_refresh"
    assert "route_refresh_execution.missing" in payload["blockers"]
    assert any("route-refresh-execute" in command for command in payload["execution_contract"]["next_commands"])


def test_ops_route_run_supervisor_cli_accepts_hyphenated_run_mode_alias(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-run-supervisor",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--strategy",
                "incremental_merge",
                "--run-id",
                "orders-run-001",
                "--dataset",
                "analytics.orders",
                "--run-mode",
                "route-refresh",
                "--output-dir",
                str(tmp_path / "route-run"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["run"]["mode"] == "route_refresh"
    assert payload["execution_contract"]["mode"] == "route_refresh"
