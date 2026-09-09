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
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def test_ops_route_refresh_plan_cli_outputs_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    quality = _write_json(
        tmp_path / "route_data_quality.json",
        {
            "route": {"source": "mssql", "sink": "clickhouse", "strategy": "incremental_merge"},
            "passed": True,
            "summary": "route DQ passed",
        },
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-refresh-plan",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--strategy",
                "incremental_merge",
                "--dataset",
                "analytics.orders",
                "--reason",
                "dq_repair",
                "--window-kind",
                "integer",
                "--start",
                "1",
                "--end",
                "250",
                "--chunk-size",
                "100",
                "--artifact",
                f"route_data_quality={quality}",
                "--output-dir",
                str(tmp_path / "refresh"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "dpone.route_refresh_plan.v1"
    assert payload["status"] == "ready"
    assert len(payload["chunks"]) == 3
    assert Path(payload["json_path"]).exists()


def test_ops_route_refresh_plan_cli_returns_nonzero_for_blocked_plan(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-refresh-plan",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--strategy",
                "incremental_merge",
                "--dataset",
                "analytics.orders",
                "--reason",
                "range_replay",
                "--window-kind",
                "integer",
                "--start",
                "100",
                "--end",
                "10",
                "--chunk-size",
                "10",
                "--output-dir",
                str(tmp_path / "refresh"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert "route_refresh.window_invalid" in payload["blockers"]
