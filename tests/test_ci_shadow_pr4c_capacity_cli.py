from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / "tools" / "ci" / "calibrate_pr_gate_shadow_capacity.py"


def _module():
    spec = importlib.util.spec_from_file_location("capacity_calibration_tool", TOOL)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_cli_refuses_to_start_without_the_authenticated_workflow_token(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    event = tmp_path / "event.json"
    event.write_text(json.dumps({"repository": {"id": 1, "full_name": "PaulKov/dpone"}}), encoding="utf-8")
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)

    exit_code = _module().main(["--event", str(event), "--output", str(tmp_path / "evidence.json")])

    assert exit_code == 70
    assert "GITHUB_TOKEN" in capsys.readouterr().err
    assert not (tmp_path / "evidence.json").exists()
