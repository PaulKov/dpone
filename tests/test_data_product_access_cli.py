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


def test_data_product_access_cli_end_to_end(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest = _write_yaml(tmp_path / "manifest.yaml", _manifest())
    contract = _write_json(tmp_path / "contract.json", _schema_contract())
    matrix = _write_json(tmp_path / "consumer-matrix.json", _consumer_matrix())
    authority = _write_json(tmp_path / "authority-gate.json", _authority_gate())

    for args in (
        ["data", "product", "access", "--help"],
        ["data", "product", "access", "classify", "--help"],
        ["data", "product", "access", "entitlements", "--help"],
        ["data", "product", "access", "entitlements", "plan", "--help"],
        ["data", "product", "privacy", "--help"],
        ["data", "product", "privacy", "impact", "--help"],
        ["data", "product", "privacy", "impact", "assess", "--help"],
        ["data", "product", "access", "gate", "--help"],
        ["data", "product", "access", "report", "--help"],
    ):
        with pytest.raises(SystemExit) as help_exit:
            cli_main.main(args)
        assert help_exit.value.code == 0
    capsys.readouterr()

    classification_path = tmp_path / "access-classification.json"
    with pytest.raises(SystemExit) as classify_exit:
        cli_main.main(
            [
                "data",
                "product",
                "access",
                "classify",
                "--manifest",
                str(manifest),
                "--schema-contract",
                str(contract),
                "--format",
                "json",
                "--output",
                str(classification_path),
            ]
        )
    assert classify_exit.value.code == 0
    classification = json.loads(capsys.readouterr().out)
    assert classification["schema_version"] == "dpone.data_product_access_classification.v1"
    assert _read_json(classification_path)["access_classification_id"] == classification["access_classification_id"]

    entitlement_path = tmp_path / "entitlement-plan.json"
    with pytest.raises(SystemExit) as entitlement_exit:
        cli_main.main(
            [
                "data",
                "product",
                "access",
                "entitlements",
                "plan",
                "--manifest",
                str(manifest),
                "--classification",
                str(classification_path),
                "--consumer-matrix",
                str(matrix),
                "--format",
                "json",
                "--output",
                str(entitlement_path),
            ]
        )
    assert entitlement_exit.value.code == 0
    entitlement = json.loads(capsys.readouterr().out)
    assert entitlement["status"] == "ready"

    privacy_path = tmp_path / "privacy-impact.json"
    with pytest.raises(SystemExit) as privacy_exit:
        cli_main.main(
            [
                "data",
                "product",
                "privacy",
                "impact",
                "assess",
                "--manifest",
                str(manifest),
                "--entitlement-plan",
                str(entitlement_path),
                "--authority-gate",
                str(authority),
                "--format",
                "json",
                "--output",
                str(privacy_path),
            ]
        )
    assert privacy_exit.value.code == 0
    privacy = json.loads(capsys.readouterr().out)
    assert privacy["schema_version"] == "dpone.data_product_privacy_impact_assessment.v1"
    assert privacy["status"] == "assessed"

    gate_path = tmp_path / "access-gate.json"
    with pytest.raises(SystemExit) as gate_exit:
        cli_main.main(
            [
                "data",
                "product",
                "access",
                "gate",
                "--entitlement-plan",
                str(entitlement_path),
                "--privacy-impact",
                str(privacy_path),
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
    assert _read_json(gate_path)["access_gate_id"] == gate["access_gate_id"]

    report_path = tmp_path / "access-report.md"
    with pytest.raises(SystemExit) as report_exit:
        cli_main.main(
            [
                "data",
                "product",
                "access",
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
    assert "# Data Product Access Governance Report" in markdown
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
    return {
        "sink": {
            "options": {
                "data_product": {
                    "id": "analytics.orders",
                    "owner": "data-platform",
                    "tier": "gold",
                    "criticality": "high",
                    "access_governance": {
                        "enabled": True,
                        "mode": "gate",
                        "profile": "regulated",
                        "unknown_consumer": "block",
                        "classification": {
                            "default": "internal",
                            "columns": {
                                "customer_email": {
                                    "class": "pii",
                                    "glossary_terms": ["EMAIL_PLAINTEXT"],
                                    "masking": "hash",
                                    "lawful_basis_required": True,
                                },
                                "amount": {"class": "financial", "masking": "none"},
                            },
                        },
                        "entitlements": [
                            {
                                "subject": "finance.daily_margin",
                                "type": "consumer",
                                "owner": "finance-analytics",
                                "purpose": "finance_close",
                                "actions": ["read"],
                                "columns": ["amount", "customer_id"],
                            },
                            {
                                "subject": "support.ops_debug",
                                "type": "group",
                                "owner": "support",
                                "purpose": "support_debug",
                                "lawful_basis": "support_contract",
                                "actions": ["read"],
                                "columns": ["customer_email"],
                                "masking_required": True,
                                "masking": "hash",
                            },
                        ],
                        "privacy": {
                            "require_lawful_basis_for": ["pii", "regulated"],
                            "block_sensitive_export_without_approval": True,
                            "require_authority_gate_for": ["pii", "regulated"],
                        },
                    },
                }
            }
        }
    }


def _schema_contract() -> dict:
    return {
        "schema_version": "dpone.schema_contract_version.v1",
        "contract_id": "analytics.orders",
        "columns": [
            {"name": "customer_id"},
            {"name": "customer_email"},
            {"name": "amount"},
        ],
    }


def _consumer_matrix() -> dict:
    return {
        "schema_version": "dpone.schema_contract_consumer_matrix.v1",
        "status": "compatible",
        "consumer_matrix_id": "sha256:" + "c" * 64,
        "consumers": [
            {"id": "finance.daily_margin", "owner": "finance-analytics", "reads": {"columns": ["amount"]}},
            {"id": "support.ops_debug", "owner": "support", "reads": {"columns": ["customer_email"]}},
        ],
        "summary": {"consumers_count": 2, "blocked_consumers": 0},
    }


def _authority_gate() -> dict:
    return {
        "schema_version": "dpone.data_product_authority_gate.v1",
        "status": "allowed",
        "authority_gate_id": "sha256:" + "a" * 64,
        "blockers": [],
        "warnings": [],
    }
