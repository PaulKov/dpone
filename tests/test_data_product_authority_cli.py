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


def test_data_product_authority_cli_end_to_end(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    monkeypatch.setenv("DPONE_AUTHORITY_SIGNING_KEY", "super-secret-test-key")
    manifest_path = _write_yaml(tmp_path / "manifest.yaml", _manifest())
    request_path = _write_json(tmp_path / "waiver-request.json", _waiver_request(requested_by="alice"))
    artifact_path = _write_json(tmp_path / "waiver.json", _waiver())
    owner_approval = _write_yaml(
        tmp_path / "owner.yaml",
        {"actor": "bob", "waiver_request_id": "sha256:" + "1" * 64, "ticket": "GOV-OWNER"},
    )
    governance_approval = _write_yaml(
        tmp_path / "governance.yaml",
        {"actor": "data-governance", "waiver_request_id": "sha256:" + "1" * 64, "ticket": "GOV-PLATFORM"},
    )

    for args in (
        ["data", "product", "authority", "--help"],
        ["data", "product", "authority", "registry", "build", "--help"],
        ["data", "product", "authority", "check", "--help"],
        ["data", "product", "authority", "quorum", "verify", "--help"],
        ["data", "product", "authority", "signature", "sign", "--help"],
        ["data", "product", "authority", "signature", "verify", "--help"],
        ["data", "product", "authority", "gate", "--help"],
        ["data", "product", "authority", "report", "--help"],
    ):
        with pytest.raises(SystemExit) as help_exit:
            cli_main.main(args)
        assert help_exit.value.code == 0
    capsys.readouterr()

    registry_path = tmp_path / "authority-registry.json"
    with pytest.raises(SystemExit) as registry_exit:
        cli_main.main(
            [
                "data",
                "product",
                "authority",
                "registry",
                "build",
                "--manifest",
                str(manifest_path),
                "--format",
                "json",
                "--output",
                str(registry_path),
            ]
        )
    assert registry_exit.value.code == 0
    registry = json.loads(capsys.readouterr().out)
    assert registry["status"] == "ready"
    assert (
        json.loads(registry_path.read_text(encoding="utf-8"))["authority_registry_id"]
        == registry["authority_registry_id"]
    )

    check_path = tmp_path / "authority-check.json"
    with pytest.raises(SystemExit) as check_exit:
        cli_main.main(
            [
                "data",
                "product",
                "authority",
                "check",
                "--registry",
                str(registry_path),
                "--actor",
                "data-governance",
                "--action",
                "policy_waiver.approve",
                "--subject",
                str(request_path),
                "--format",
                "json",
                "--output",
                str(check_path),
            ]
        )
    assert check_exit.value.code == 0
    check = json.loads(capsys.readouterr().out)
    assert check["status"] == "allowed"

    quorum_path = tmp_path / "approval-quorum.json"
    with pytest.raises(SystemExit) as quorum_exit:
        cli_main.main(
            [
                "data",
                "product",
                "authority",
                "quorum",
                "verify",
                "--registry",
                str(registry_path),
                "--request",
                str(request_path),
                "--approval",
                str(owner_approval),
                "--approval",
                str(governance_approval),
                "--format",
                "json",
                "--output",
                str(quorum_path),
            ]
        )
    assert quorum_exit.value.code == 0
    quorum = json.loads(capsys.readouterr().out)
    assert quorum["status"] == "allowed"

    waiver_with_authority_path = tmp_path / "waiver-with-authority.json"
    with pytest.raises(SystemExit) as waiver_approve_exit:
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
                str(governance_approval),
                "--authority-check",
                str(check_path),
                "--approval-quorum",
                str(quorum_path),
                "--approved-at",
                "2026-06-28T12:00:00Z",
                "--format",
                "json",
                "--output",
                str(waiver_with_authority_path),
            ]
        )
    assert waiver_approve_exit.value.code == 0
    waiver_with_authority = json.loads(capsys.readouterr().out)
    assert waiver_with_authority["authority_evidence"]["authority_check_id"] == check["authority_check_id"]
    assert waiver_with_authority["authority_evidence"]["approval_quorum_id"] == quorum["approval_quorum_id"]

    signature_path = tmp_path / "waiver.signature.json"
    with pytest.raises(SystemExit) as sign_exit:
        cli_main.main(
            [
                "data",
                "product",
                "authority",
                "signature",
                "sign",
                "--registry",
                str(registry_path),
                "--artifact",
                str(artifact_path),
                "--actor",
                "data-governance",
                "--format",
                "json",
                "--output",
                str(signature_path),
            ]
        )
    assert sign_exit.value.code == 0
    signature = json.loads(capsys.readouterr().out)
    assert signature["status"] == "signed"

    verification_path = tmp_path / "signature-verification.json"
    with pytest.raises(SystemExit) as verify_exit:
        cli_main.main(
            [
                "data",
                "product",
                "authority",
                "signature",
                "verify",
                "--registry",
                str(registry_path),
                "--artifact",
                str(artifact_path),
                "--signature",
                str(signature_path),
                "--format",
                "json",
                "--output",
                str(verification_path),
            ]
        )
    assert verify_exit.value.code == 0
    verification = json.loads(capsys.readouterr().out)
    assert verification["status"] == "verified"

    gate_path = tmp_path / "authority-gate.json"
    with pytest.raises(SystemExit) as gate_exit:
        cli_main.main(
            [
                "data",
                "product",
                "authority",
                "gate",
                "--authority-check",
                str(check_path),
                "--approval-quorum",
                str(quorum_path),
                "--signature",
                str(signature_path),
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

    report_path = tmp_path / "authority-report.md"
    with pytest.raises(SystemExit) as report_exit:
        cli_main.main(
            [
                "data",
                "product",
                "authority",
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
    assert "# Data Product Authority Report" in markdown
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
                    "authority": {
                        "enabled": True,
                        "mode": "gate",
                        "profile": "prod_strict",
                        "unknown_actor": "block",
                        "identities": [
                            {
                                "id": "alice",
                                "type": "user",
                                "owner": "data-platform",
                                "roles": ["change_author"],
                            },
                            {"id": "bob", "type": "user", "owner": "data-platform", "roles": ["data_owner"]},
                            {
                                "id": "data-governance",
                                "type": "group",
                                "owner": "governance",
                                "roles": ["governance_approver"],
                            },
                        ],
                        "roles": [
                            {"id": "data_owner", "grants": ["policy_waiver.approve"]},
                            {"id": "governance_approver", "grants": ["policy_waiver.approve"]},
                            {"id": "change_author", "grants": ["change.author"]},
                        ],
                        "approval": {
                            "quorum": {
                                "policy_waiver": {
                                    "min_approvals": 2,
                                    "required_roles": ["data_owner", "governance_approver"],
                                }
                            }
                        },
                        "separation_of_duties": {
                            "prevent_self_approval": True,
                            "disallow_same_actor_for": ["requested_by", "implemented_by", "approved_by"],
                        },
                        "signing": {
                            "enabled": True,
                            "algorithm": "hmac_sha256",
                            "key_env": "DPONE_AUTHORITY_SIGNING_KEY",
                            "required_for": ["dpone.data_product_waiver.v1"],
                        },
                    },
                }
            }
        }
    }


def _waiver_request(*, requested_by: str) -> dict:
    return {
        "schema_version": "dpone.data_product_waiver_request.v1",
        "status": "requested",
        "waiver_request_id": "sha256:" + "1" * 64,
        "policy_evaluation_id": "sha256:" + "2" * 64,
        "policy_pack_id": "sha256:" + "3" * 64,
        "product_id": "analytics.orders",
        "rule_id": "require_assertion_gate",
        "severity": "critical",
        "requested_by": requested_by,
        "expires_at": "2026-07-12T00:00:00Z",
        "blockers": [],
        "warnings": [],
    }


def _waiver() -> dict:
    return {
        "schema_version": "dpone.data_product_waiver.v1",
        "waiver_id": "sha256:" + "4" * 64,
        "status": "approved",
        "waiver_request_id": "sha256:" + "1" * 64,
        "policy_evaluation_id": "sha256:" + "2" * 64,
        "policy_pack_id": "sha256:" + "3" * 64,
        "product_id": "analytics.orders",
        "rule_id": "require_assertion_gate",
        "actor": "data-governance",
        "blockers": [],
        "warnings": [],
    }
