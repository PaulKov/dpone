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


def test_data_product_compliance_cli_plan_evaluate_gate_and_package(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest_path = _write_yaml(tmp_path / "manifest.yaml", _manifest())
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    for kind, status in {
        "data_product_policy_gate": "waived",
        "data_product_authority_gate": "allowed",
        "data_product_evidence_signature": "signed",
        "data_product_release_closeout_gate": "allowed",
        "data_product_assertion_gate": "allowed",
        "data_product_slo_gate": "allowed",
        "data_product_error_budget_gate": "allowed",
    }.items():
        _write_json(evidence_dir / f"{kind}.json", _artifact(kind, status))

    for args in (
        ["data", "product", "compliance", "--help"],
        ["data", "product", "compliance", "controls", "--help"],
        ["data", "product", "compliance", "controls", "plan", "--help"],
        ["data", "product", "compliance", "controls", "evaluate", "--help"],
        ["data", "product", "compliance", "controls", "gate", "--help"],
        ["data", "product", "compliance", "audit", "--help"],
        ["data", "product", "compliance", "audit", "package", "--help"],
    ):
        with pytest.raises(SystemExit) as help_exit:
            cli_main.main(args)
        assert help_exit.value.code == 0
    capsys.readouterr()

    plan_path = tmp_path / "compliance-plan.json"
    with pytest.raises(SystemExit) as plan_exit:
        cli_main.main(
            [
                "data",
                "product",
                "compliance",
                "controls",
                "plan",
                "--manifest",
                str(manifest_path),
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
    assert plan["schema_version"] == "dpone.data_product_compliance_control_plan.v1"
    assert json.loads(plan_path.read_text(encoding="utf-8"))["compliance_plan_id"] == plan["compliance_plan_id"]

    evaluation_path = tmp_path / "compliance-evaluation.json"
    with pytest.raises(SystemExit) as evaluate_exit:
        cli_main.main(
            [
                "data",
                "product",
                "compliance",
                "controls",
                "evaluate",
                "--plan",
                str(plan_path),
                "--format",
                "json",
                "--output",
                str(evaluation_path),
            ]
        )
    assert evaluate_exit.value.code == 0
    evaluation = json.loads(capsys.readouterr().out)
    assert evaluation["status"] == "passed"

    gate_path = tmp_path / "compliance-gate.json"
    with pytest.raises(SystemExit) as gate_exit:
        cli_main.main(
            [
                "data",
                "product",
                "compliance",
                "controls",
                "gate",
                "--evaluation",
                str(evaluation_path),
                "--profile",
                "regulated",
                "--format",
                "json",
                "--output",
                str(gate_path),
            ]
        )
    assert gate_exit.value.code == 0
    gate = json.loads(capsys.readouterr().out)
    assert gate["status"] == "allowed"

    package_path = tmp_path / "audit-package.md"
    with pytest.raises(SystemExit) as package_exit:
        cli_main.main(
            [
                "data",
                "product",
                "compliance",
                "audit",
                "package",
                "--gate",
                str(gate_path),
                "--format",
                "md",
                "--output",
                str(package_path),
            ]
        )
    assert package_exit.value.code == 0
    markdown = capsys.readouterr().out
    assert "# Data Product Audit Evidence Package" in markdown
    assert package_path.read_text(encoding="utf-8") == markdown


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
                    "compliance": {
                        "enabled": True,
                        "mode": "gate",
                        "profile": "regulated",
                        "frameworks": [
                            {
                                "id": "soc2",
                                "version": "2026.1",
                                "owner": "security-governance",
                                "controls": [
                                    {
                                        "id": "CC7.2",
                                        "title": "Release evidence",
                                        "severity": "critical",
                                        "require_artifacts": [
                                            "data_product_policy_gate",
                                            "data_product_authority_gate",
                                            "data_product_evidence_signature",
                                            "data_product_release_closeout_gate",
                                        ],
                                        "allowed_statuses": {
                                            "data_product_policy_gate": ["allowed", "warning", "waived"],
                                            "data_product_evidence_signature": ["signed", "verified"],
                                        },
                                    },
                                    {
                                        "id": "CC9.2",
                                        "title": "Quality monitoring",
                                        "severity": "high",
                                        "require_artifacts": [
                                            "data_product_assertion_gate",
                                            "data_product_slo_gate",
                                            "data_product_error_budget_gate",
                                        ],
                                    },
                                ],
                            }
                        ],
                    },
                }
            }
        }
    }


def _artifact(kind: str, status: str) -> dict:
    return {
        "schema_version": f"dpone.{kind}.v1",
        f"{kind.removeprefix('data_product_')}_id": "sha256:" + kind[:1] * 64,
        "status": status,
        "product_id": "analytics.orders",
        "recorded_at": "2026-06-28T12:00:00Z",
        "blockers": [],
        "warnings": [],
    }
