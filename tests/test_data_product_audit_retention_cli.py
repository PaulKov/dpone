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


def test_data_product_audit_cli_archive_retention_and_legal_hold(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest_path = _write_yaml(tmp_path / "manifest.yaml", _manifest(tmp_path / "archive"))
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    for kind, status in {
        "data_product_audit_package": "allowed",
        "data_product_compliance_gate": "allowed",
        "data_product_policy_gate": "waived",
        "data_product_authority_gate": "allowed",
    }.items():
        _write_json(evidence_dir / f"{kind}.json", _artifact(kind, status))
    authority_gate = _write_json(
        evidence_dir / "authority-gate.json", _artifact("data_product_authority_gate", "allowed")
    )

    for args in (
        ["data", "product", "audit", "--help"],
        ["data", "product", "audit", "archive", "--help"],
        ["data", "product", "audit", "archive", "plan", "--help"],
        ["data", "product", "audit", "archive", "run", "--help"],
        ["data", "product", "audit", "archive", "verify", "--help"],
        ["data", "product", "audit", "retention", "plan", "--help"],
        ["data", "product", "audit", "legal-hold", "apply", "--help"],
    ):
        with pytest.raises(SystemExit) as help_exit:
            cli_main.main(args)
        assert help_exit.value.code == 0
    capsys.readouterr()

    plan_path = tmp_path / "audit-archive-plan.json"
    with pytest.raises(SystemExit) as plan_exit:
        cli_main.main(
            [
                "data",
                "product",
                "audit",
                "archive",
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
    assert plan["schema_version"] == "dpone.data_product_audit_archive_plan.v1"
    assert json.loads(plan_path.read_text(encoding="utf-8"))["audit_archive_plan_id"] == plan["audit_archive_plan_id"]

    dry_run_path = tmp_path / "audit-archive-dry-run.json"
    with pytest.raises(SystemExit) as dry_run_exit:
        cli_main.main(
            [
                "data",
                "product",
                "audit",
                "archive",
                "run",
                "--plan",
                str(plan_path),
                "--format",
                "json",
                "--output",
                str(dry_run_path),
            ]
        )
    assert dry_run_exit.value.code == 0
    dry_run = json.loads(capsys.readouterr().out)
    assert dry_run["status"] == "dry_run"
    assert not Path(dry_run["archive_uri"]).exists()

    run_path = tmp_path / "audit-archive-run.json"
    with pytest.raises(SystemExit) as run_exit:
        cli_main.main(
            [
                "data",
                "product",
                "audit",
                "archive",
                "run",
                "--plan",
                str(plan_path),
                "--execute",
                "--format",
                "json",
                "--output",
                str(run_path),
            ]
        )
    assert run_exit.value.code == 0
    run = json.loads(capsys.readouterr().out)
    assert run["status"] == "archived"

    verify_path = tmp_path / "audit-archive-verification.json"
    with pytest.raises(SystemExit) as verify_exit:
        cli_main.main(
            [
                "data",
                "product",
                "audit",
                "archive",
                "verify",
                "--archive-run",
                str(run_path),
                "--format",
                "json",
                "--output",
                str(verify_path),
            ]
        )
    assert verify_exit.value.code == 0
    verification = json.loads(capsys.readouterr().out)
    assert verification["status"] == "verified"

    hold_path = tmp_path / "legal-hold.json"
    with pytest.raises(SystemExit) as hold_exit:
        cli_main.main(
            [
                "data",
                "product",
                "audit",
                "legal-hold",
                "apply",
                "--archive-run",
                str(run_path),
                "--reason",
                "SOC2 evidence hold",
                "--authority-gate",
                str(authority_gate),
                "--format",
                "json",
                "--output",
                str(hold_path),
            ]
        )
    assert hold_exit.value.code == 0
    hold = json.loads(capsys.readouterr().out)
    assert hold["status"] == "held"

    retention_path = tmp_path / "retention-plan.md"
    with pytest.raises(SystemExit) as retention_exit:
        cli_main.main(
            [
                "data",
                "product",
                "audit",
                "retention",
                "plan",
                "--manifest",
                str(manifest_path),
                "--archive-verification",
                str(verify_path),
                "--legal-hold",
                str(hold_path),
                "--format",
                "md",
                "--output",
                str(retention_path),
            ]
        )
    assert retention_exit.value.code == 2
    markdown = capsys.readouterr().out
    assert "# Data Product Audit Retention Plan" in markdown
    assert retention_path.read_text(encoding="utf-8") == markdown


def test_audit_retention_bundle_and_registry_cli_flags(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_cli(monkeypatch)

    for args, expected in (
        (
            ["schema", "migration", "bundle", "build", "--help"],
            "--data-product-audit-archive-verification",
        ),
        (
            ["schema", "migration", "registry", "record", "--help"],
            "audit_archive_verified",
        ),
    ):
        with pytest.raises(SystemExit) as help_exit:
            cli_main.main(args)
        assert help_exit.value.code == 0
        assert expected in capsys.readouterr().out


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def _write_yaml(path: Path, payload: dict) -> Path:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _manifest(store_uri: Path) -> dict:
    return {
        "sink": {
            "options": {
                "data_product": {
                    "id": "analytics.orders",
                    "owner": "data-platform",
                    "tier": "gold",
                    "criticality": "high",
                    "audit_retention": {
                        "enabled": True,
                        "mode": "gate",
                        "profile": "regulated",
                        "archive": {
                            "store_backend": "local_fs",
                            "store_uri": str(store_uri),
                            "layout": "{product_id}/{yyyy}/{mm}/{audit_archive_id}",
                            "include_artifacts": [
                                "data_product_audit_package",
                                "data_product_compliance_gate",
                                "data_product_policy_gate",
                                "data_product_authority_gate",
                            ],
                            "manifest_hash_chain": True,
                            "merkle_root": True,
                        },
                        "retention": {"min_days": 2555, "delete_after_days": 3650, "expired_policy": "block"},
                        "legal_hold": {
                            "enabled": True,
                            "policy": "block_delete",
                            "require_reason": True,
                            "require_authority_gate": True,
                        },
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
