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


def test_data_product_policy_cli_evaluate_waiver_gate_and_report(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest_path = _write_yaml(tmp_path / "manifest.yaml", _manifest())
    assertion_gate_path = _write_json(
        tmp_path / "assertion-gate.json", _artifact("data_product_assertion_gate", "blocked")
    )
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    (evidence_dir / "data-product-assertion-gate.json").write_text(
        assertion_gate_path.read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    for args in (
        ["data", "product", "policy", "--help"],
        ["data", "product", "policy", "evaluate", "--help"],
        ["data", "product", "policy", "waiver", "--help"],
        ["data", "product", "policy", "waiver", "request", "--help"],
        ["data", "product", "policy", "waiver", "approve", "--help"],
        ["data", "product", "policy", "gate", "--help"],
        ["data", "product", "policy", "report", "--help"],
    ):
        with pytest.raises(SystemExit) as help_exit:
            cli_main.main(args)
        assert help_exit.value.code == 0
    capsys.readouterr()

    evaluation_path = tmp_path / "policy-evaluation.json"
    with pytest.raises(SystemExit) as evaluate_exit:
        cli_main.main(
            [
                "data",
                "product",
                "policy",
                "evaluate",
                "--manifest",
                str(manifest_path),
                "--evidence-dir",
                str(evidence_dir),
                "--format",
                "json",
                "--output",
                str(evaluation_path),
            ]
        )
    assert evaluate_exit.value.code == 2
    evaluation = json.loads(capsys.readouterr().out)
    assert evaluation["schema_version"] == "dpone.data_product_policy_evaluation.v1"
    assert evaluation["status"] == "blocked"
    assert (
        json.loads(evaluation_path.read_text(encoding="utf-8"))["policy_evaluation_id"]
        == evaluation["policy_evaluation_id"]
    )

    request_path = tmp_path / "waiver-request.json"
    with pytest.raises(SystemExit) as request_exit:
        cli_main.main(
            [
                "data",
                "product",
                "policy",
                "waiver",
                "request",
                "--evaluation",
                str(evaluation_path),
                "--rule-id",
                "require_assertion_gate",
                "--reason",
                "approved migration window",
                "--expires-at",
                "2026-07-12T00:00:00Z",
                "--format",
                "json",
                "--output",
                str(request_path),
            ]
        )
    assert request_exit.value.code == 0
    request = json.loads(capsys.readouterr().out)
    assert request["status"] == "requested"

    approval_path = _write_yaml(tmp_path / "approval.yaml", {"ticket": "GOV-123", "approved_by": "data-governance"})
    waiver_path = tmp_path / "waiver.json"
    with pytest.raises(SystemExit) as approve_exit:
        cli_main.main(
            [
                "data",
                "product",
                "policy",
                "waiver",
                "approve",
                "--request",
                str(request_path),
                "--actor",
                "data-governance",
                "--approval",
                str(approval_path),
                "--approved-at",
                "2026-06-28T12:00:00Z",
                "--format",
                "json",
                "--output",
                str(waiver_path),
            ]
        )
    assert approve_exit.value.code == 0
    waiver = json.loads(capsys.readouterr().out)
    assert waiver["status"] == "approved"

    gate_path = tmp_path / "policy-gate.json"
    with pytest.raises(SystemExit) as gate_exit:
        cli_main.main(
            [
                "data",
                "product",
                "policy",
                "gate",
                "--evaluation",
                str(evaluation_path),
                "--waiver",
                str(waiver_path),
                "--profile",
                "prod_strict",
                "--observed-at",
                "2026-06-28T12:00:00Z",
                "--format",
                "json",
                "--output",
                str(gate_path),
            ]
        )
    assert gate_exit.value.code == 0
    gate = json.loads(capsys.readouterr().out)
    assert gate["status"] == "waived"

    report_path = tmp_path / "policy-report.md"
    with pytest.raises(SystemExit) as report_exit:
        cli_main.main(
            [
                "data",
                "product",
                "policy",
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
    markdown = capsys.readouterr().out
    assert "# Data Product Policy Report" in markdown
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


def _manifest() -> dict:
    return {
        "sink": {
            "options": {
                "data_product": {
                    "id": "analytics.orders",
                    "owner": "data-platform",
                    "tier": "gold",
                    "criticality": "high",
                    "policy": {
                        "enabled": True,
                        "mode": "gate",
                        "profile": "prod_strict",
                        "packs": [
                            {
                                "id": "production_release_guardrails",
                                "version": "1.0.0",
                                "owner": "data-governance",
                                "rules": [
                                    {
                                        "id": "require_assertion_gate",
                                        "severity": "critical",
                                        "require_artifacts": ["data_product_assertion_gate"],
                                    }
                                ],
                            }
                        ],
                        "waivers": {
                            "enabled": True,
                            "max_duration_days": 14,
                            "require_owner": True,
                            "require_approval_evidence": True,
                            "expired_policy": "block",
                        },
                    },
                }
            }
        }
    }


def _artifact(kind: str, status: str) -> dict:
    return {
        "schema_version": f"dpone.{kind}.v1",
        "status": status,
        "blockers": [f"{kind}.blocked"] if status == "blocked" else [],
        "warnings": [],
    }
