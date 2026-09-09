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


def test_ops_industrial_readiness_cli_outputs_json_and_writes_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    matrix = _write_json(
        tmp_path / "matrix.json",
        {
            "passed": True,
            "cases": [{"source": "postgres", "sink": "mssql", "strategy": "incremental_merge", "row_count": 10000}],
        },
    )
    correctness = _write_json(tmp_path / "correctness.json", {"passed": True})
    reliability = _write_json(tmp_path / "reliability.json", {"passed": True})
    performance = _write_json(tmp_path / "performance.json", {"passed": True})
    ux = _write_json(tmp_path / "ux.json", {"passed": True})
    governance = _write_json(tmp_path / "governance.json", {"passed": True})
    schema_evolution = _write_json(tmp_path / "schema_evolution.json", {"passed": True})

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "industrial-readiness",
                "--output-dir",
                str(tmp_path / "industrial"),
                "--release",
                "v0.5.1",
                "--artifact",
                f"local_matrix={matrix}",
                "--artifact",
                f"correctness={correctness}",
                "--artifact",
                f"reliability={reliability}",
                "--artifact",
                f"performance_lab={performance}",
                "--artifact",
                f"ux={ux}",
                "--artifact",
                f"governance={governance}",
                "--artifact",
                f"schema_evolution={schema_evolution}",
                "--case",
                "postgres:mssql:incremental_merge",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["level"] == "industrial_ready"
    assert payload["matrix"]["total_cases"] == 1
    assert Path(payload["json_path"]).exists()
    assert Path(payload["markdown_path"]).exists()
