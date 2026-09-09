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


def test_ops_production_maturity_cli_outputs_json_and_blocks_red_evidence(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    certification = tmp_path / "certification.json"
    certification.write_text(json.dumps({"passed": False, "blockers": ["matrix.red"]}), encoding="utf-8")
    security = tmp_path / "security.json"
    security.write_text(json.dumps({"passed": True}), encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "production-maturity",
                "--output-dir",
                str(tmp_path / "maturity"),
                "--release",
                "v0.5.1",
                "--artifact",
                f"certification={certification}",
                "--artifact",
                f"security={security}",
                "--require",
                "certification",
                "--require",
                "security",
                "--require",
                "supply_chain",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is False
    assert payload["level"] == "blocked"
    assert payload["blockers"] == ["certification.not_passed", "supply_chain.missing"]
    assert Path(payload["json_path"]).exists()
    assert Path(payload["markdown_path"]).exists()
