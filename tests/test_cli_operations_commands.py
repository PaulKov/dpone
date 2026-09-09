from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.commands.registry import get_commands
from dpone.ops.checksums import sha256_file


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _write_bound_artifact_index(
    path: Path,
    *,
    release: str,
    artifacts: tuple[Path, ...],
) -> Path:
    path.write_text(
        json.dumps(
            {
                "release": release,
                "passed": True,
                "items": [
                    {
                        "name": artifact.name,
                        "path": str(artifact),
                        "sha256": sha256_file(artifact),
                    }
                    for artifact in artifacts
                ],
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def test_ops_command_group_is_registered() -> None:
    assert "ops" in {command.name for command in get_commands()}


def test_ops_contract_check_outputs_json(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "contract-check",
                "--rows-json",
                '[{"id": 1, "email": null}]',
                "--contract-json",
                '{"required_columns":["id","email"],"not_null":["email"]}',
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1


def test_ops_marketplace_outputs_json(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(["ops", "marketplace", "--format", "json"])

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert "connectors" in payload
    assert "postgres" in payload["connectors"]


def test_ops_certification_automation_plan_outputs_json(
    monkeypatch: pytest.MonkeyPatch, capsys, tmp_path: Path
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "certification-automation-plan",
                "--output-dir",
                str(tmp_path / "automation"),
                "--profile",
                "mock_contract",
                "--row-count",
                "10000",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["profile"] == "mock_contract"
    assert payload["passed"] is True
    assert payload["step_count"] == 11
    assert "certification_suite.json" in payload["required_artifacts"]


def test_ops_evidence_bundle_outputs_json(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "evidence-bundle",
                "--artifact-dir",
                str(tmp_path),
                "--run-id",
                "run_01",
                "--source",
                "postgres",
                "--sink",
                "mssql",
                "--strategy",
                "snapshot_diff",
                "--rows-json",
                '[{"id": 1}]',
                "--contract-json",
                '{"required_columns":["id"]}',
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is False
    certification = next(item for item in payload["items"] if item["name"] == "certification")
    assert certification["passed"] is False
    assert {item["name"] for item in payload["items"]} == {"certification", "data_contract", "marketplace"}


def test_ops_go_live_gate_outputs_blockers(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path) -> None:
    _patch_cli(monkeypatch)
    bundle_path = tmp_path / "bundle.json"
    bundle_path.write_text(
        json.dumps(
            {
                "run_id": "run_02",
                "passed": False,
                "items": [
                    {
                        "name": "data_contract",
                        "path": "contract.json",
                        "sha256": "0" * 64,
                        "required": True,
                        "passed": False,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(["ops", "go-live-gate", "--bundle-json", str(bundle_path), "--format", "json"])

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is False
    assert payload["blockers"] == ["data_contract"]


def test_ops_policy_evaluate_outputs_violations(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path) -> None:
    _patch_cli(monkeypatch)
    bundle_path = tmp_path / "bundle.json"
    bundle_path.write_text(
        json.dumps(
            {
                "run_id": "run_03",
                "passed": True,
                "items": [
                    {
                        "name": "certification",
                        "path": str(tmp_path / "certification_report.json"),
                        "sha256": "1" * 64,
                        "required": True,
                        "passed": True,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "policy-evaluate",
                "--bundle-json",
                str(bundle_path),
                "--policy-json",
                '{"required_evidence":["certification","data_contract"]}',
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is False
    assert "missing_required_evidence.data_contract" in payload["violations"]


def test_ops_diff_outputs_reconciliation_samples(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "diff",
                "--source-rows-json",
                '[{"id": 1, "amount": 100}, {"id": 2, "amount": 200}]',
                "--target-rows-json",
                '[{"id": 1, "amount": 100}, {"id": 2, "amount": 250}]',
                "--key",
                "id",
                "--compare-columns",
                "amount",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is False
    assert payload["mismatch_count"] == 1
    assert payload["samples"][0]["kind"] == "mismatch"


def test_ops_security_audit_outputs_findings(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "security-audit",
                "--manifest-json",
                '{"source":{"connection_type":"params","credentials":{"password":"plain-text-password"}}}',
                "--log-text",
                "token=ghp_example1234567890abcdef",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is False
    assert "manifest.inline_secret.source.credentials.password" in {item["code"] for item in payload["findings"]}


def test_ops_slo_evaluate_outputs_breaches(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "slo-evaluate",
                "--metrics-json",
                '{"freshness_lag_seconds":900,"throughput_rows_per_second":250}',
                "--objectives-json",
                '{"freshness_lag_seconds":{"max":300},"throughput_rows_per_second":{"min":1000}}',
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is False
    assert {item["code"] for item in payload["results"]} == {
        "slo.freshness_lag_seconds.max",
        "slo.throughput_rows_per_second.min",
    }


def test_ops_incident_pack_outputs_blocking_items(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path) -> None:
    _patch_cli(monkeypatch)
    evidence_path = tmp_path / "evidence.json"
    policy_path = tmp_path / "policy.json"
    evidence_path.write_text(json.dumps({"passed": True}), encoding="utf-8")
    policy_path.write_text(json.dumps({"passed": False, "violations": ["policy.red"]}), encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "incident-pack",
                "--artifact-dir",
                str(tmp_path / "pack"),
                "--incident-id",
                "inc_01",
                "--title",
                "Release gate review",
                "--severity",
                "release",
                "--artifact",
                f"evidence={evidence_path}",
                "--artifact",
                f"policy={policy_path}",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is False
    assert {item["name"] for item in payload["items"]} == {"evidence", "policy"}


def test_ops_rollback_execute_outputs_dry_run(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "rollback-execute",
                "--sink",
                "mssql",
                "--target",
                "landing.orders",
                "--load-id",
                "01JLOAD0000000000000000000",
                "--strategy",
                "shadow_swap",
                "--require-backup",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert payload["applied"] is False
    assert payload["backup_validated"] is True


def test_ops_certification_history_outputs_regressions(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path) -> None:
    _patch_cli(monkeypatch)
    previous = tmp_path / "previous.json"
    current = tmp_path / "current.json"
    previous.write_text(
        json.dumps({"passed": True, "results": [{"case_id": "postgres_to_mssql__snapshot_diff", "passed": True}]}),
        encoding="utf-8",
    )
    current.write_text(
        json.dumps({"passed": False, "results": [{"case_id": "postgres_to_mssql__snapshot_diff", "passed": False}]}),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "certification-history",
                "--history-dir",
                str(tmp_path / "history"),
                "--release",
                "v0.2.9",
                "--current-report",
                str(current),
                "--previous-report",
                str(previous),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "regression"
    assert payload["new_failures"] == ["postgres_to_mssql__snapshot_diff"]


def test_ops_connector_badges_outputs_regression_badges(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path) -> None:
    _patch_cli(monkeypatch)
    history_index = tmp_path / "certification_history_index.json"
    history_index.write_text(
        json.dumps(
            {
                "releases": [
                    {
                        "release": "v0.2.9",
                        "status": "regression",
                        "badge": "regression",
                        "new_failures": ["postgres_to_mssql__snapshot_diff"],
                        "fixed_failures": [],
                        "unchanged_failures": [],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "connector-badges",
                "--output-dir",
                str(tmp_path / "badges"),
                "--history-index",
                str(history_index),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    entries = {item["connector"]: item for item in payload["entries"]}
    assert entries["postgres"]["badge"] == "regression"
    assert entries["mssql"]["badge"] == "regression"


def test_ops_release_gate_outputs_blockers(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path) -> None:
    _patch_cli(monkeypatch)
    evidence_path = tmp_path / "evidence.json"
    security_path = tmp_path / "security.json"
    evidence_path.write_text(json.dumps({"passed": True}), encoding="utf-8")
    security_path.write_text(json.dumps({"passed": False, "findings": [{"code": "secret"}]}), encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "release-gate",
                "--artifact-dir",
                str(tmp_path / "release_gate"),
                "--release",
                "v0.2.9",
                "--artifact",
                f"evidence={evidence_path}",
                "--artifact",
                f"security={security_path}",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is False
    assert payload["blockers"] == ["security"]


def test_ops_artifact_index_outputs_index(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path) -> None:
    _patch_cli(monkeypatch)
    root = tmp_path / "test_artifacts"
    root.mkdir()
    (root / "release_gate.json").write_text(json.dumps({"release": "v0.2.9", "passed": True}), encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "artifact-index",
                "--output-dir",
                str(tmp_path / "index"),
                "--root",
                str(root),
                "--release",
                "v0.2.9",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["release"] == "v0.2.9"
    assert payload["total_artifacts"] == 1
    assert payload["items"][0]["artifact_type"] == "release_gate"


def test_ops_evidence_chain_outputs_chain_entry(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path) -> None:
    _patch_cli(monkeypatch)
    artifact_index = tmp_path / "artifact_index.json"
    artifact_index.write_text(json.dumps({"release": "v0.2.9", "passed": True}), encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "evidence-chain",
                "--chain-dir",
                str(tmp_path / "chain"),
                "--release",
                "v0.2.9",
                "--artifact-index",
                str(artifact_index),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["release"] == "v0.2.9"
    assert payload["verified"] is True
    assert len(payload["chain_hash"]) == 64


def test_ops_evidence_chain_verify_outputs_verification_report(
    monkeypatch: pytest.MonkeyPatch, capsys, tmp_path
) -> None:
    _patch_cli(monkeypatch)
    artifact_index = tmp_path / "artifact_index.json"
    artifact_index.write_text(json.dumps({"release": "v0.2.9", "passed": True}), encoding="utf-8")
    chain_dir = tmp_path / "chain"

    with pytest.raises(SystemExit) as append_exc:
        cli_main.main(
            [
                "ops",
                "evidence-chain",
                "--chain-dir",
                str(chain_dir),
                "--release",
                "v0.2.9",
                "--artifact-index",
                str(artifact_index),
                "--format",
                "json",
            ]
        )
    assert append_exc.value.code == 0
    capsys.readouterr()

    with pytest.raises(SystemExit) as verify_exc:
        cli_main.main(
            [
                "ops",
                "evidence-chain-verify",
                "--chain-dir",
                str(chain_dir),
                "--format",
                "json",
            ]
        )

    assert verify_exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["verified"] is True
    assert payload["entry_count"] == 1
    assert payload["broken_entries"] == []


def test_ops_release_summary_outputs_go_no_go_report(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path) -> None:
    _patch_cli(monkeypatch)
    release = "oss_rc_1"
    replay = tmp_path / "replay.json"
    replay.write_text('{"passed": true}\n', encoding="utf-8")
    matrix_suite = tmp_path / "matrix_certification_suite.json"
    connector_suite = tmp_path / "connector_certification_suite.json"
    for suite, suite_id in ((matrix_suite, "matrix"), (connector_suite, "connectors")):
        suite.write_text(
            json.dumps(
                {
                    "schema_version": "dpone.certification_suite.v1",
                    "release_id": release,
                    "suite_id": suite_id,
                    "passed": True,
                    "evidence_status": "PASS",
                    "blockers": [],
                }
            ),
            encoding="utf-8",
        )
    indexes = {
        "replay-chain": _write_bound_artifact_index(
            tmp_path / "replay_artifact_index.json",
            release=release,
            artifacts=(replay,),
        ),
        "matrix-chain": _write_bound_artifact_index(
            tmp_path / "matrix_artifact_index.json",
            release=release,
            artifacts=(matrix_suite,),
        ),
        "connector-chain": _write_bound_artifact_index(
            tmp_path / "connector_artifact_index.json",
            release=release,
            artifacts=(connector_suite,),
        ),
    }
    for chain_name, artifact_index in indexes.items():
        with pytest.raises(SystemExit) as append_exc:
            cli_main.main(
                [
                    "ops",
                    "evidence-chain",
                    "--chain-dir",
                    str(tmp_path / chain_name),
                    "--release",
                    release,
                    "--artifact-index",
                    str(artifact_index),
                    "--format",
                    "json",
                ]
            )
        assert append_exc.value.code == 0
        capsys.readouterr()

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "release-summary",
                "--output-dir",
                str(tmp_path / "summary"),
                "--release-id",
                release,
                "--replay-chain-dir",
                str(tmp_path / "replay-chain"),
                "--matrix-suite",
                str(matrix_suite),
                "--matrix-chain-dir",
                str(tmp_path / "matrix-chain"),
                "--connector-suite",
                str(connector_suite),
                "--connector-chain-dir",
                str(tmp_path / "connector-chain"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["release_id"] == "oss_rc_1"
    assert len(payload["items"]) == 5
    assert (tmp_path / "summary" / "release_summary.json").exists()


def test_ops_release_orchestrator_outputs_release_pack(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path) -> None:
    _patch_cli(monkeypatch)
    root = tmp_path / "ops-artifacts"
    root.mkdir()
    (root / "security_audit.json").write_text(json.dumps({"passed": True, "findings": []}), encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "release-orchestrator",
                "--output-dir",
                str(tmp_path / "release"),
                "--release",
                "v0.2.9",
                "--root",
                str(root),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["release"] == "v0.2.9"
    assert payload["passed"] is True
    assert {step["name"] for step in payload["steps"]} == {
        "artifact_index",
        "evidence_chain",
        "release_gate",
        "docs_publish_pack",
        "runbook_pack",
    }


def test_ops_release_promote_outputs_promotion_manifest(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path) -> None:
    _patch_cli(monkeypatch)
    orchestration = tmp_path / "release_orchestration.json"
    orchestration.write_text(json.dumps({"release": "v0.2.9", "passed": True}), encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "release-promote",
                "--output-dir",
                str(tmp_path / "promotion"),
                "--release",
                "v0.2.9",
                "--from-env",
                "staging",
                "--to-env",
                "production",
                "--artifact",
                f"release_orchestration={orchestration}",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["release"] == "v0.2.9"
    assert payload["from_environment"] == "staging"
    assert payload["to_environment"] == "production"
    assert payload["passed"] is True
    assert payload["items"][0]["name"] == "release_orchestration"


def test_ops_env_drift_outputs_allowed_drift(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path) -> None:
    _patch_cli(monkeypatch)
    source = tmp_path / "staging.json"
    target = tmp_path / "production.json"
    source.write_text(
        json.dumps({"connection": {"host": "staging.db"}, "strategy": {"mode": "append"}}), encoding="utf-8"
    )
    target.write_text(json.dumps({"connection": {"host": "prod.db"}, "strategy": {"mode": "append"}}), encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "env-drift",
                "--output-dir",
                str(tmp_path / "drift"),
                "--source-env",
                "staging",
                "--target-env",
                "production",
                "--source",
                str(source),
                "--target",
                str(target),
                "--allowlist-path",
                "connection.host",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["allowed_drift_count"] == 1
    assert payload["blocking_drift_count"] == 0


def test_ops_change_request_outputs_approval_artifact(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path) -> None:
    _patch_cli(monkeypatch)
    orchestration = tmp_path / "release_orchestration.json"
    orchestration.write_text(json.dumps({"release": "v0.2.9", "passed": True}), encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "change-request",
                "--output-dir",
                str(tmp_path / "change-request"),
                "--change-id",
                "CR-2026-0001",
                "--release",
                "v0.2.9",
                "--target-env",
                "production",
                "--risk-level",
                "medium",
                "--requested-by",
                "release-manager@example.com",
                "--approver",
                "data-architect@example.com",
                "--artifact",
                f"release_orchestration={orchestration}",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["change_id"] == "CR-2026-0001"
    assert payload["passed"] is True
    assert payload["approvers"] == ["data-architect@example.com"]
    assert payload["items"][0]["name"] == "release_orchestration"


def test_ops_approval_record_outputs_approval_audit(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path) -> None:
    _patch_cli(monkeypatch)
    change_request = tmp_path / "change_request.json"
    change_request.write_text(
        json.dumps(
            {
                "change_id": "CR-2026-0001",
                "release": "v0.2.9",
                "target_environment": "production",
                "passed": True,
                "approvers": ["data-architect@example.com"],
                "expires_at": None,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "approval-record",
                "--output-dir",
                str(tmp_path / "approval"),
                "--change-request",
                str(change_request),
                "--actor",
                "data-architect@example.com",
                "--decision",
                "approved",
                "--comment",
                "Release evidence reviewed.",
                "--quorum-required",
                "1",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["change_id"] == "CR-2026-0001"
    assert payload["status"] == "approved"
    assert payload["passed"] is True
    assert payload["approvals_count"] == 1


def test_ops_deployment_record_outputs_deployment_audit(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path) -> None:
    _patch_cli(monkeypatch)
    approval_record = tmp_path / "approval_record.json"
    approval_record.write_text(
        json.dumps(
            {
                "change_id": "CR-2026-0001",
                "release": "v0.2.9",
                "target_environment": "production",
                "status": "approved",
                "passed": True,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "deployment-record",
                "--output-dir",
                str(tmp_path / "deployment"),
                "--deployment-id",
                "DEP-2026-0001",
                "--environment",
                "production",
                "--actor",
                "release-manager@example.com",
                "--approval-record",
                str(approval_record),
                "--status",
                "succeeded",
                "--post-check",
                "smoke=true",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["deployment_id"] == "DEP-2026-0001"
    assert payload["status"] == "succeeded"
    assert payload["passed"] is True
    assert payload["post_checks_count"] == 1


def test_ops_post_deploy_verify_outputs_release_closed_gate(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path) -> None:
    _patch_cli(monkeypatch)
    deployment_record = tmp_path / "deployment_record.json"
    slo = tmp_path / "slo_report.json"
    deployment_record.write_text(
        json.dumps(
            {
                "deployment_id": "DEP-2026-0001",
                "release": "v0.2.9",
                "environment": "production",
                "passed": True,
            }
        ),
        encoding="utf-8",
    )
    slo.write_text(json.dumps({"passed": True, "results": []}), encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "post-deploy-verify",
                "--output-dir",
                str(tmp_path / "post-deploy"),
                "--deployment-record",
                str(deployment_record),
                "--artifact",
                f"slo={slo}",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["deployment_id"] == "DEP-2026-0001"
    assert payload["status"] == "release_closed"
    assert payload["passed"] is True


def test_ops_release_close_outputs_closure_artifact(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path) -> None:
    _patch_cli(monkeypatch)
    post_deploy = tmp_path / "post_deploy_verify.json"
    post_deploy.write_text(
        json.dumps(
            {
                "deployment_id": "DEP-2026-0001",
                "release": "v0.2.9",
                "environment": "production",
                "status": "release_closed",
                "passed": True,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "release-close",
                "--output-dir",
                str(tmp_path / "release-close"),
                "--release",
                "v0.2.9",
                "--closed-by",
                "release-manager@example.com",
                "--post-deploy-verify",
                str(post_deploy),
                "--notes",
                "Release closed.",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["release"] == "v0.2.9"
    assert payload["status"] == "closed"
    assert payload["passed"] is True


def test_ops_docs_publish_pack_outputs_pages_manifest(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path) -> None:
    _patch_cli(monkeypatch)
    release_gate = tmp_path / "release_gate.json"
    connector_badges = tmp_path / "connector_badges.json"
    release_gate.write_text(json.dumps({"passed": True, "blockers": []}), encoding="utf-8")
    connector_badges.write_text(
        json.dumps({"passed": True, "entries": [{"connector": "postgres", "badge": "certified"}]}),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "docs-publish-pack",
                "--output-dir",
                str(tmp_path / "publish"),
                "--release",
                "v0.2.9",
                "--artifact",
                f"release_gate={release_gate}",
                "--artifact",
                f"connector_badges={connector_badges}",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["readme_snippet_path"].endswith("README_SNIPPET.md")


def test_ops_manifest_bundle_outputs_redacted_bundle(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path) -> None:
    _patch_cli(monkeypatch)
    manifest = tmp_path / "manifest.yml"
    manifest.write_text("credentials:\n  password: plain-text-password\n", encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "manifest-bundle",
                "--output-dir",
                str(tmp_path / "bundle"),
                "--bundle-id",
                "bundle_01",
                "--file",
                f"manifest={manifest}",
                "--redact",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["items"][0]["name"] == "manifest"
    copied_path = payload["items"][0]["copied_path"]
    assert "plain-text-password" not in Path(copied_path).read_text(encoding="utf-8")


def test_ops_runbook_pack_outputs_operator_runbook(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path) -> None:
    _patch_cli(monkeypatch)
    release_gate = tmp_path / "release_gate.json"
    release_gate.write_text(json.dumps({"passed": True, "blockers": []}), encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "runbook-pack",
                "--output-dir",
                str(tmp_path / "runbook"),
                "--runbook-id",
                "runbook_01",
                "--title",
                "Release go-live",
                "--artifact",
                f"release_gate={release_gate}",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["operator_runbook_path"].endswith("OPERATOR_RUNBOOK.md")


def test_ops_run_registry_outputs_auditable_run_entry(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path) -> None:
    _patch_cli(monkeypatch)
    run_result = tmp_path / "run_result.json"
    quality = tmp_path / "quality.json"
    run_result.write_text(
        json.dumps(
            {
                "run_id": "run_03",
                "process": "orders",
                "manifest": "examples/postgres_to_mssql.yml",
                "passed": True,
                "result": {"status": "success"},
            }
        ),
        encoding="utf-8",
    )
    quality.write_text(json.dumps({"passed": True}), encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "run-registry",
                "--output-dir",
                str(tmp_path / "registry"),
                "--run-result",
                str(run_result),
                "--artifact",
                f"quality={quality}",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["run_id"] == "run_03"
    assert payload["artifact_count"] == 1
    assert payload["index_path"].endswith("run_registry_index.json")


def test_ops_lineage_export_outputs_openlineage_event(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path) -> None:
    _patch_cli(monkeypatch)
    run_registry = tmp_path / "run_04__run_registry.json"
    run_registry.write_text(
        json.dumps(
            {
                "run_id": "run_04",
                "process": "orders_daily",
                "manifest": "manifests/orders.yml",
                "status": "success",
                "passed": True,
                "run_result_sha256": "a" * 64,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "lineage-export",
                "--output-dir",
                str(tmp_path / "lineage"),
                "--run-registry-entry",
                str(run_registry),
                "--namespace",
                "dpone.local",
                "--input",
                "postgres=public.orders",
                "--output",
                "mssql=landing.orders",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["event_type"] == "COMPLETE"
    assert payload["event_path"].endswith("run_04__openlineage.json")


def test_ops_benchmark_baseline_outputs_regression_gate(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "benchmark-baseline",
                "--output-dir",
                str(tmp_path / "benchmark"),
                "--metrics-json",
                '{"throughput_rows_per_second":85000}',
                "--baseline-json",
                '{"throughput_rows_per_second":{"value":100000,"direction":"higher"}}',
                "--allowed-regression-ratio",
                "0.10",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is False
    assert payload["items"][0]["metric"] == "throughput_rows_per_second"
    assert payload["items"][0]["allowed_threshold"] == 90000


def test_ops_dbt_lineage_outputs_graph_artifacts(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path) -> None:
    _patch_cli(monkeypatch)
    manifest = tmp_path / "manifest.json"
    run_results = tmp_path / "run_results.json"
    manifest.write_text(
        json.dumps(
            {
                "nodes": {
                    "model.demo.fct_orders": {
                        "unique_id": "model.demo.fct_orders",
                        "name": "fct_orders",
                        "resource_type": "model",
                        "relation_name": "analytics.fct_orders",
                        "depends_on": {"nodes": []},
                    }
                },
                "sources": {},
            }
        ),
        encoding="utf-8",
    )
    run_results.write_text(
        json.dumps({"metadata": {"invocation_id": "dbt_invocation_01"}, "results": []}),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "dbt-lineage",
                "--output-dir",
                str(tmp_path / "dbt_lineage"),
                "--manifest",
                str(manifest),
                "--run-results",
                str(run_results),
                "--namespace",
                "dbt.local",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["model_count"] == 1
    assert payload["openlineage_path"].endswith("dbt_openlineage.json")


def test_ops_certification_suite_outputs_full_gate(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path) -> None:
    _patch_cli(monkeypatch)
    certification = tmp_path / "certification_report.json"
    benchmark = tmp_path / "benchmark_baseline.json"
    strategy_bundle = tmp_path / "strategy_certification_bundle.json"
    certification.write_text(
        json.dumps(
            {
                "passed": True,
                "evidence_status": "PASS",
                "production_certification": "VERIFIED",
                "results": [{"case_id": "postgres_to_mssql__replace", "passed": True}],
            }
        ),
        encoding="utf-8",
    )
    benchmark.write_text(json.dumps({"passed": True, "items": []}), encoding="utf-8")
    strategy_bundle.write_text(
        json.dumps(
            {
                "passed": True,
                "evidence_status": "PASS",
                "schema_version": "dpone.strategy.certification_bundle.v1",
                "items": [],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "certification-suite",
                "--output-dir",
                str(tmp_path / "suite"),
                "--suite-id",
                "manual_matrix",
                "--certification-report",
                str(certification),
                "--benchmark-baseline",
                str(benchmark),
                "--require-benchmark",
                "--strategy-certification-bundle",
                str(strategy_bundle),
                "--require-strategy-certification",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["suite_id"] == "manual_matrix"
    assert payload["case_count"] == 1
    assert payload["items"][1]["name"] == "benchmark_baseline"
    assert payload["items"][-1]["name"] == "strategy_certification_bundle"


def test_ops_integration_matrix_report_outputs_certification_report(
    monkeypatch: pytest.MonkeyPatch, capsys, tmp_path
) -> None:
    _patch_cli(monkeypatch)
    artifact_dir = tmp_path / "matrix"
    artifact_dir.mkdir()
    (artifact_dir / "postgres_to_mssql__replace.json").write_text(
        json.dumps(
            {
                "case_id": "postgres_to_mssql__replace",
                "source": "postgres",
                "sink": "mssql",
                "strategy": "replace",
            }
        ),
        encoding="utf-8",
    )
    (artifact_dir / "postgres_to_mssql__replace__behavior.json").write_text(
        json.dumps(
            {
                "case_id": "postgres_to_mssql__replace",
                "source": "postgres",
                "sink": "mssql",
                "strategy": "replace",
                "passed": True,
                "expected_row_count": 10000,
                "actual_row_count": 10000,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "integration-matrix-report",
                "--artifact-dir",
                str(artifact_dir),
                "--output-dir",
                str(tmp_path / "report"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["total_cases"] == 1
    assert (tmp_path / "report" / "certification_report.json").exists()


def test_ops_release_summary_outputs_json(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path: Path) -> None:
    from dpone.ops.evidence_chain import EvidenceChainService

    _patch_cli(monkeypatch)
    release = "oss_rc"

    def write_chain(name: str, artifact: Path) -> Path:
        artifact_index = tmp_path / f"{name}_artifact_index.json"
        _write_bound_artifact_index(
            artifact_index,
            release=release,
            artifacts=(artifact,),
        )
        chain_dir = tmp_path / name / "chain"
        EvidenceChainService().append(
            chain_dir=chain_dir,
            release=release,
            artifact_index_path=artifact_index,
        )
        return chain_dir

    def write_suite(name: str) -> Path:
        suite_path = tmp_path / f"{name}_certification_suite.json"
        suite_path.write_text(
            json.dumps(
                {
                    "schema_version": "dpone.certification_suite.v1",
                    "release_id": release,
                    "suite_id": name,
                    "passed": True,
                    "evidence_status": "PASS",
                    "blockers": [],
                    "case_count": 2,
                    "evidence_count": 6,
                    "items": [],
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        return suite_path

    replay = tmp_path / "replay.json"
    replay.write_text('{"passed": true}\n', encoding="utf-8")
    matrix_suite = write_suite("matrix")
    connector_suite = write_suite("connectors")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "release-summary",
                "--output-dir",
                str(tmp_path / "summary"),
                "--release-id",
                release,
                "--replay-chain-dir",
                str(write_chain("replay", replay)),
                "--matrix-suite",
                str(matrix_suite),
                "--matrix-chain-dir",
                str(write_chain("matrix", matrix_suite)),
                "--connector-suite",
                str(connector_suite),
                "--connector-chain-dir",
                str(write_chain("connectors", connector_suite)),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["release_id"] == "oss_rc"
    assert payload["passed"] is True
    assert len(payload["items"]) == 5
