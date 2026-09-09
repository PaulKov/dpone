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


def test_data_product_governance_cli_plan_render_publish_and_verify(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest = _write_yaml(tmp_path / "manifest.yaml", _manifest())
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    _write_json(evidence_dir / "policy-gate.json", _artifact("data_product_policy_gate", "allowed"))

    for args in (
        ["data", "product", "governance", "--help"],
        ["data", "product", "governance", "export", "--help"],
        ["data", "product", "governance", "export", "plan", "--help"],
        ["data", "product", "governance", "export", "render", "--help"],
        ["data", "product", "governance", "publish", "--help"],
        ["data", "product", "governance", "verify", "--help"],
    ):
        with pytest.raises(SystemExit) as help_exit:
            cli_main.main(args)
        assert help_exit.value.code == 0
    capsys.readouterr()

    plan_path = tmp_path / "governance-plan.json"
    with pytest.raises(SystemExit) as plan_exit:
        cli_main.main(
            [
                "data",
                "product",
                "governance",
                "export",
                "plan",
                "--manifest",
                str(manifest),
                "--evidence-dir",
                str(evidence_dir),
                "--targets",
                "datahub,json",
                "--format",
                "json",
                "--output",
                str(plan_path),
            ]
        )
    assert plan_exit.value.code == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["schema_version"] == "dpone.data_product_governance_export_plan.v1"
    assert plan["status"] == "ready"
    assert (
        json.loads(plan_path.read_text(encoding="utf-8"))["governance_export_plan_id"]
        == plan["governance_export_plan_id"]
    )

    payload_path = tmp_path / "datahub-payload.json"
    with pytest.raises(SystemExit) as render_exit:
        cli_main.main(
            [
                "data",
                "product",
                "governance",
                "export",
                "render",
                "--plan",
                str(plan_path),
                "--target",
                "datahub",
                "--format",
                "json",
                "--output",
                str(payload_path),
            ]
        )
    assert render_exit.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "rendered"
    assert payload_path.read_text(encoding="utf-8") == json.dumps(payload, ensure_ascii=False, indent=2) + "\n"

    receipt_path = tmp_path / "receipt.json"
    with pytest.raises(SystemExit) as publish_exit:
        cli_main.main(
            [
                "data",
                "product",
                "governance",
                "publish",
                "--payload",
                str(payload_path),
                "--provider",
                "datahub",
                "--format",
                "json",
                "--output",
                str(receipt_path),
            ]
        )
    assert publish_exit.value.code == 0
    receipt = json.loads(capsys.readouterr().out)
    assert receipt["status"] == "dry_run"
    assert receipt["network_writes"] == []

    verification_path = tmp_path / "verification.json"
    with pytest.raises(SystemExit) as verify_exit:
        cli_main.main(
            [
                "data",
                "product",
                "governance",
                "verify",
                "--receipt",
                str(receipt_path),
                "--format",
                "json",
                "--output",
                str(verification_path),
            ]
        )
    assert verify_exit.value.code == 0
    verification = json.loads(capsys.readouterr().out)
    assert verification["status"] == "verified"
    assert (
        verification_path.read_text(encoding="utf-8") == json.dumps(verification, ensure_ascii=False, indent=2) + "\n"
    )


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def _write_yaml(path: Path, payload: dict) -> Path:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _manifest() -> dict:
    return {
        "sink": {
            "options": {
                "data_product": {
                    "id": "analytics.orders",
                    "owner": "data-platform",
                    "governance_export": {
                        "enabled": True,
                        "mode": "gate",
                        "profile": "prod_strict",
                        "targets": [{"provider": "datahub", "mode": "render"}],
                    },
                }
            }
        }
    }


def _artifact(kind: str, status: str) -> dict:
    return {
        "schema_version": f"dpone.{kind}.v1",
        "status": status,
        "product_id": "analytics.orders",
        "policy_gate_id": "sha256:" + "1" * 64,
        "blockers": [],
        "warnings": [],
    }
