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


def test_data_product_access_enforcement_cli_end_to_end(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest = _write_yaml(tmp_path / "manifest.yaml", _manifest())
    classification = _write_json(tmp_path / "classification.json", _classification())
    entitlement = _write_json(tmp_path / "entitlement.json", _entitlement_plan())
    privacy = _write_json(tmp_path / "privacy.json", _privacy_impact())
    access_gate = _write_json(tmp_path / "access-gate.json", _access_gate())
    authority_gate = _write_json(tmp_path / "authority-gate.json", _authority_gate())
    target_connection = _write_json(tmp_path / "clickhouse.json", _target_connection())

    for args in (
        ["data", "product", "access", "enforcement", "--help"],
        ["data", "product", "access", "enforcement", "plan", "--help"],
        ["data", "product", "access", "enforcement", "apply", "--help"],
        ["data", "product", "access", "drift", "--help"],
        ["data", "product", "access", "drift", "inspect", "--help"],
        ["data", "product", "access", "enforcement", "certify", "--help"],
        ["data", "product", "access", "enforcement", "report", "--help"],
    ):
        with pytest.raises(SystemExit) as help_exit:
            cli_main.main(args)
        assert help_exit.value.code == 0
    capsys.readouterr()

    plan_path = tmp_path / "plan.json"
    with pytest.raises(SystemExit) as plan_exit:
        cli_main.main(
            [
                "data",
                "product",
                "access",
                "enforcement",
                "plan",
                "--manifest",
                str(manifest),
                "--classification",
                str(classification),
                "--entitlement-plan",
                str(entitlement),
                "--privacy-impact",
                str(privacy),
                "--access-gate",
                str(access_gate),
                "--authority-gate",
                str(authority_gate),
                "--target-connection",
                str(target_connection),
                "--environment",
                "prod",
                "--format",
                "json",
                "--output",
                str(plan_path),
            ]
        )
    assert plan_exit.value.code == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["status"] == "ready"
    assert _read_json(plan_path)["access_enforcement_plan_id"] == plan["access_enforcement_plan_id"]

    run_path = tmp_path / "run.json"
    with pytest.raises(SystemExit) as apply_exit:
        cli_main.main(
            [
                "data",
                "product",
                "access",
                "enforcement",
                "apply",
                "--plan",
                str(plan_path),
                "--format",
                "json",
                "--output",
                str(run_path),
            ]
        )
    assert apply_exit.value.code == 0
    run = json.loads(capsys.readouterr().out)
    assert run["status"] == "dry_run"

    drift_path = tmp_path / "drift.json"
    _write_json(
        tmp_path / "clickhouse-with-state.json", {**_target_connection(), "access_state": plan["desired_state"]}
    )
    with pytest.raises(SystemExit) as drift_exit:
        cli_main.main(
            [
                "data",
                "product",
                "access",
                "drift",
                "inspect",
                "--plan",
                str(plan_path),
                "--target-connection",
                str(tmp_path / "clickhouse-with-state.json"),
                "--format",
                "json",
                "--output",
                str(drift_path),
            ]
        )
    assert drift_exit.value.code == 0
    drift = json.loads(capsys.readouterr().out)
    assert drift["status"] == "clean"

    certificate_path = tmp_path / "certificate.json"
    with pytest.raises(SystemExit) as certify_exit:
        cli_main.main(
            [
                "data",
                "product",
                "access",
                "enforcement",
                "certify",
                "--run",
                str(run_path),
                "--drift-report",
                str(drift_path),
                "--target-connection",
                str(target_connection),
                "--profile",
                "advisory",
                "--format",
                "json",
                "--output",
                str(certificate_path),
            ]
        )
    assert certify_exit.value.code == 0
    certificate = json.loads(capsys.readouterr().out)
    assert certificate["schema_version"] == "dpone.data_product_access_enforcement_certificate.v1"

    report_path = tmp_path / "report.md"
    with pytest.raises(SystemExit) as report_exit:
        cli_main.main(
            [
                "data",
                "product",
                "access",
                "enforcement",
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
    markdown = capsys.readouterr().out
    assert "# Data Product Access Enforcement Report" in markdown
    assert report_path.read_text(encoding="utf-8") == markdown


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
    from tests.test_data_product_access_enforcement_contracts import _manifest as _base_manifest

    return _base_manifest()


def _classification() -> dict:
    from tests.test_data_product_access_enforcement_contracts import _classification as _base_classification

    return _base_classification()


def _entitlement_plan() -> dict:
    from tests.test_data_product_access_enforcement_contracts import _entitlement_plan as _base_entitlement_plan

    return _base_entitlement_plan()


def _privacy_impact() -> dict:
    from tests.test_data_product_access_enforcement_contracts import _privacy_impact as _base_privacy_impact

    return _base_privacy_impact()


def _access_gate() -> dict:
    from tests.test_data_product_access_enforcement_contracts import _access_gate as _base_access_gate

    return _base_access_gate()


def _authority_gate() -> dict:
    from tests.test_data_product_access_enforcement_contracts import _authority_gate as _base_authority_gate

    return _base_authority_gate()


def _target_connection() -> dict:
    from tests.test_data_product_access_enforcement_contracts import _target_connection as _base_target_connection

    return _base_target_connection()
