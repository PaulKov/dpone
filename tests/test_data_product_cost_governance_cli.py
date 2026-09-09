from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from dpone.cli import main as cli_main


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def test_data_product_cost_cli_end_to_end(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest = _write_yaml(tmp_path / "manifest.yaml", _manifest())
    bundle = _write_json(tmp_path / "bundle.json", {"bundle_id": "sha256:bundle", "pack_id": "sha256:pack"})
    runtime = _write_json(
        tmp_path / "runtime.json",
        {
            "duration_ms": 120000,
            "bytes_written": 107374182,
            "staging_bytes": 107374182,
            "query_bytes_read": 10737418,
            "table_bytes_before": 1073741824,
            "table_bytes_after": 1181116006,
        },
    )

    for args in (
        ["data", "product", "cost", "--help"],
        ["data", "product", "cost", "plan", "--help"],
        ["data", "product", "cost", "evaluate", "--help"],
        ["data", "product", "cost", "gate", "--help"],
        ["data", "product", "cost", "forecast", "--help"],
        ["data", "product", "cost", "report", "--help"],
    ):
        with pytest.raises(SystemExit) as help_exit:
            cli_main.main(args)
        assert help_exit.value.code == 0
    capsys.readouterr()

    plan_path = tmp_path / "cost-plan.json"
    with pytest.raises(SystemExit) as plan_exit:
        cli_main.main(
            [
                "data",
                "product",
                "cost",
                "plan",
                "--manifest",
                str(manifest),
                "--bundle",
                str(bundle),
                "--format",
                "json",
                "--output",
                str(plan_path),
            ]
        )
    assert plan_exit.value.code == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["schema_version"] == "dpone.data_product_cost_plan.v1"
    assert _read_json(plan_path)["cost_plan_id"] == plan["cost_plan_id"]

    evaluation_path = tmp_path / "cost-evaluation.json"
    with pytest.raises(SystemExit) as evaluate_exit:
        cli_main.main(
            [
                "data",
                "product",
                "cost",
                "evaluate",
                "--plan",
                str(plan_path),
                "--runtime-artifact",
                str(runtime),
                "--format",
                "json",
                "--output",
                str(evaluation_path),
            ]
        )
    assert evaluate_exit.value.code == 0
    evaluation = json.loads(capsys.readouterr().out)
    assert evaluation["schema_version"] == "dpone.data_product_cost_evaluation.v1"

    gate_path = tmp_path / "cost-gate.json"
    with pytest.raises(SystemExit) as gate_exit:
        cli_main.main(
            [
                "data",
                "product",
                "cost",
                "gate",
                "--evaluation",
                str(evaluation_path),
                "--profile",
                "prod_strict",
                "--format",
                "json",
                "--output",
                str(gate_path),
            ]
        )
    assert gate_exit.value.code == 0
    gate = json.loads(capsys.readouterr().out)
    assert gate["status"] == "allowed"

    forecast_path = tmp_path / "cost-forecast.md"
    with pytest.raises(SystemExit) as forecast_exit:
        cli_main.main(
            [
                "data",
                "product",
                "cost",
                "forecast",
                "--evaluation",
                str(evaluation_path),
                "--history",
                str(_write_json(tmp_path / "history.json", {"evaluations": [evaluation]})),
                "--format",
                "md",
                "--output",
                str(forecast_path),
            ]
        )
    assert forecast_exit.value.code == 0
    forecast_md = capsys.readouterr().out
    assert "# Data Product Cost Forecast" in forecast_md
    assert forecast_path.read_text(encoding="utf-8") == forecast_md

    report_path = tmp_path / "cost-report.md"
    with pytest.raises(SystemExit) as report_exit:
        cli_main.main(
            [
                "data",
                "product",
                "cost",
                "report",
                "--gate",
                str(gate_path),
                "--format",
                "md",
                "--output",
                str(report_path),
            ]
        )
    assert report_exit.value.code == 0
    assert "# Data Product Cost Governance Report" in capsys.readouterr().out


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def _write_yaml(path: Path, payload: dict) -> Path:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _manifest() -> dict:
    from tests.test_data_product_cost_governance_contracts import _manifest

    return _manifest()
