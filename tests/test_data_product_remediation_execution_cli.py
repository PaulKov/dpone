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


def test_data_product_remediation_execution_cli_help_and_output_parity(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest = _write_yaml(tmp_path / "manifest.yaml", _manifest())
    remediation_plan = _write_json(tmp_path / "remediation-plan.json", _remediation_plan())
    remediation_gate = _write_json(tmp_path / "remediation-gate.json", _remediation_gate(_read_json(remediation_plan)))
    authority_gate = _write_json(tmp_path / "authority-gate.json", _authority_gate())
    params = _write_json(tmp_path / "params.json", {"assertion-plan": "orders.assertion-plan.json"})

    for args in (
        ["data", "product", "remediation", "execution", "--help"],
        ["data", "product", "remediation", "execution", "plan", "--help"],
        ["data", "product", "remediation", "execution", "run", "--help"],
        ["data", "product", "remediation", "execution", "certify", "--help"],
        ["data", "product", "remediation", "execution", "report", "--help"],
    ):
        with pytest.raises(SystemExit) as help_exit:
            cli_main.main(args)
        assert help_exit.value.code == 0
    capsys.readouterr()

    execution_plan_path = tmp_path / "execution-plan.json"
    with pytest.raises(SystemExit) as plan_exit:
        cli_main.main(
            [
                "data",
                "product",
                "remediation",
                "execution",
                "plan",
                "--manifest",
                str(manifest),
                "--remediation-plan",
                str(remediation_plan),
                "--remediation-gate",
                str(remediation_gate),
                "--authority-gate",
                str(authority_gate),
                "--parameters",
                str(params),
                "--format",
                "json",
                "--output",
                str(execution_plan_path),
            ]
        )
    assert plan_exit.value.code == 0
    execution_plan = json.loads(capsys.readouterr().out)
    assert execution_plan["schema_version"] == "dpone.data_product_remediation_execution_plan.v1"
    assert _read_json(execution_plan_path) == execution_plan

    run_path = tmp_path / "execution-run.json"
    with pytest.raises(SystemExit) as run_exit:
        cli_main.main(
            [
                "data",
                "product",
                "remediation",
                "execution",
                "run",
                "--plan",
                str(execution_plan_path),
                "--idempotency-key",
                "orders-remediation-1",
                "--format",
                "json",
                "--output",
                str(run_path),
            ]
        )
    assert run_exit.value.code == 0
    run = json.loads(capsys.readouterr().out)
    assert run["status"] == "dry_run"
    assert _read_json(run_path) == run

    fresh_dir = tmp_path / "fresh"
    fresh_dir.mkdir()
    _write_json(fresh_dir / "assertion-gate.json", _assertion_gate(status="allowed", evidence_id="sha256:fresh"))
    certificate_path = tmp_path / "certificate.json"
    with pytest.raises(SystemExit) as certificate_exit:
        cli_main.main(
            [
                "data",
                "product",
                "remediation",
                "execution",
                "certify",
                "--run",
                str(run_path),
                "--evidence-dir",
                str(fresh_dir),
                "--profile",
                "advisory",
                "--format",
                "json",
                "--output",
                str(certificate_path),
            ]
        )
    assert certificate_exit.value.code == 0
    certificate = json.loads(capsys.readouterr().out)
    assert certificate["status"] == "warning"
    assert _read_json(certificate_path) == certificate

    report_path = tmp_path / "report.md"
    with pytest.raises(SystemExit) as report_exit:
        cli_main.main(
            [
                "data",
                "product",
                "remediation",
                "execution",
                "report",
                "--certificate",
                str(certificate_path),
                "--format",
                "md",
                "--output",
                str(report_path),
            ]
        )
    assert report_exit.value.code == 0
    report = capsys.readouterr().out
    assert "# Data Product Remediation Execution Report" in report
    assert report_path.read_text(encoding="utf-8") == report


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
    from tests.test_data_product_remediation_execution_contracts import _manifest

    return _manifest()


def _remediation_plan() -> dict:
    from tests.test_data_product_remediation_execution_contracts import _remediation_plan

    return _remediation_plan()


def _remediation_gate(plan: dict) -> dict:
    from tests.test_data_product_remediation_execution_contracts import _remediation_gate

    return _remediation_gate(plan)


def _authority_gate() -> dict:
    from tests.test_data_product_remediation_execution_contracts import _authority_gate

    return _authority_gate()


def _assertion_gate(*, status: str, evidence_id: str = "sha256:assertion-old") -> dict:
    from tests.test_data_product_remediation_execution_contracts import _assertion_gate

    return _assertion_gate(status=status, evidence_id=evidence_id)
