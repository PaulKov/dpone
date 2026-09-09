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


def _write_json(path: Path, payload: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_ops_route_reconciliation_repair_cli_outputs_json_and_nonzero_for_repairs(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    source_rows = _write_json(tmp_path / "source.json", [{"id": 1, "status": "paid"}])
    target_rows = _write_json(tmp_path / "target.json", [{"id": 1, "status": "pending"}])

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-reconciliation-repair",
                "--source",
                "postgres",
                "--sink",
                "mssql",
                "--strategy",
                "incremental_merge",
                "--source-rows-json",
                str(source_rows),
                "--target-rows-json",
                str(target_rows),
                "--key",
                "id",
                "--compare-column",
                "status",
                "--output-dir",
                str(tmp_path / "route-repair"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["route"]["case_id"] == "postgres_to_mssql__incremental_merge"
    assert payload["repair_plan"]["actions"][0]["action"] == "replay_source_row"
    assert Path(payload["json_path"]).exists()
