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


def _artifact(tmp_path: Path, name: str, *, passed: bool = True) -> Path:
    return _write_json(
        tmp_path / f"{name}.json",
        {
            "route": {"source": "mssql", "sink": "clickhouse", "strategy": "incremental_merge"},
            "passed": passed,
            "summary": f"{name} ok",
            "quality": {"score": 100.0, "dimensions": {name: {"score": 100.0, "passed": passed}}},
            "exceptions": {"count": 0, "ratio": 0.0, "max_age_hours": 0.0},
            "blockers": [] if passed else [f"{name}.failed"],
        },
    )


def test_ops_route_data_quality_cli_outputs_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    artifacts = {name: _artifact(tmp_path, name) for name in ("data_contract", "quarantine", "reconciliation")}

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-data-quality",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--strategy",
                "incremental_merge",
                "--artifact",
                f"data_contract={artifacts['data_contract']}",
                "--artifact",
                f"quarantine={artifacts['quarantine']}",
                "--artifact",
                f"reconciliation={artifacts['reconciliation']}",
                "--output-dir",
                str(tmp_path / "route-dq"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "dpone.route_data_quality.v1"
    assert payload["status"] == "passed"
    assert payload["score"] == 100.0
    assert Path(payload["json_path"]).exists()


def test_ops_route_data_quality_cli_returns_nonzero_for_blocked_scorecard(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    data_contract = _artifact(tmp_path, "data_contract")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-data-quality",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--strategy",
                "incremental_merge",
                "--artifact",
                f"data_contract={data_contract}",
                "--output-dir",
                str(tmp_path / "route-dq"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert "quarantine.missing" in payload["blockers"]
