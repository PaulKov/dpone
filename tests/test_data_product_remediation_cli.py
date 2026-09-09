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


def test_data_product_remediation_cli_end_to_end(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest = _write_yaml(tmp_path / "manifest.yaml", _manifest())
    trust_gate = _write_json(tmp_path / "trust-gate.json", _trust_gate())
    trust_snapshot = _write_json(tmp_path / "trust-snapshot.json", _trust_snapshot())
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    _write_json(evidence_dir / "assertion-gate.json", _assertion_gate(status="blocked"))

    for args in (
        ["data", "product", "remediation", "--help"],
        ["data", "product", "remediation", "plan", "--help"],
        ["data", "product", "remediation", "runbook", "render", "--help"],
        ["data", "product", "remediation", "gate", "--help"],
        ["data", "product", "remediation", "closeout", "--help"],
        ["data", "product", "remediation", "report", "--help"],
    ):
        with pytest.raises(SystemExit) as help_exit:
            cli_main.main(args)
        assert help_exit.value.code == 0
    capsys.readouterr()
    with pytest.raises(SystemExit) as registry_help_exit:
        cli_main.main(["schema", "migration", "registry", "record", "--help"])
    assert registry_help_exit.value.code == 0
    registry_help = capsys.readouterr().out
    assert "--data-product-remediation-gate" in registry_help
    assert "--data-product-remediation-closeout" in registry_help
    assert "data_product_remediation_closed" in registry_help

    plan_path = tmp_path / "remediation-plan.json"
    with pytest.raises(SystemExit) as plan_exit:
        cli_main.main(
            [
                "data",
                "product",
                "remediation",
                "plan",
                "--manifest",
                str(manifest),
                "--trust-snapshot",
                str(trust_snapshot),
                "--trust-gate",
                str(trust_gate),
                "--evidence-dir",
                str(evidence_dir),
                "--format",
                "json",
                "--output",
                str(plan_path),
            ]
        )
    assert plan_exit.value.code == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["schema_version"] == "dpone.data_product_remediation_plan.v1"
    assert _read_json(plan_path)["remediation_plan_id"] == plan["remediation_plan_id"]

    runbook_path = tmp_path / "runbook.md"
    with pytest.raises(SystemExit) as runbook_exit:
        cli_main.main(
            [
                "data",
                "product",
                "remediation",
                "runbook",
                "render",
                "--plan",
                str(plan_path),
                "--format",
                "md",
                "--output",
                str(runbook_path),
            ]
        )
    assert runbook_exit.value.code == 0
    runbook = capsys.readouterr().out
    assert "# Data Product Remediation Runbook" in runbook
    assert runbook_path.read_text(encoding="utf-8") == runbook

    gate_path = tmp_path / "remediation-gate.json"
    with pytest.raises(SystemExit) as gate_exit:
        cli_main.main(
            [
                "data",
                "product",
                "remediation",
                "gate",
                "--plan",
                str(plan_path),
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

    fresh_dir = tmp_path / "fresh"
    fresh_dir.mkdir()
    _write_json(fresh_dir / "assertion-gate.json", _assertion_gate(status="allowed", evidence_id="sha256:fresh"))
    closeout_path = tmp_path / "closeout.json"
    with pytest.raises(SystemExit) as closeout_exit:
        cli_main.main(
            [
                "data",
                "product",
                "remediation",
                "closeout",
                "--plan",
                str(plan_path),
                "--evidence-dir",
                str(fresh_dir),
                "--format",
                "json",
                "--output",
                str(closeout_path),
            ]
        )
    assert closeout_exit.value.code == 0
    closeout = json.loads(capsys.readouterr().out)
    assert closeout["status"] == "allowed"

    report_path = tmp_path / "report.md"
    with pytest.raises(SystemExit) as report_exit:
        cli_main.main(
            [
                "data",
                "product",
                "remediation",
                "report",
                "--gate",
                str(gate_path),
                "--closeout",
                str(closeout_path),
                "--format",
                "md",
                "--output",
                str(report_path),
            ]
        )
    assert report_exit.value.code == 0
    assert "# Data Product Remediation Report" in capsys.readouterr().out


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
    from tests.test_data_product_remediation_contracts import _manifest

    return _manifest()


def _trust_gate() -> dict:
    from tests.test_data_product_remediation_contracts import _trust_gate

    return _trust_gate(status="blocked")


def _trust_snapshot() -> dict:
    from tests.test_data_product_remediation_contracts import _trust_snapshot

    return _trust_snapshot()


def _assertion_gate(*, status: str, evidence_id: str = "sha256:assertion-old") -> dict:
    from tests.test_data_product_remediation_contracts import _assertion_gate

    return _assertion_gate(status=status, evidence_id=evidence_id)
