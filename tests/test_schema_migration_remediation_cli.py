from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.readiness.migration_control import MigrationPack, MigrationTarget


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def test_schema_migration_remediation_cli_help_is_registered(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _patch_cli(monkeypatch)
    for args in (
        ["schema", "migration", "remediation", "--help"],
        ["schema", "migration", "remediation", "plan", "--help"],
        ["schema", "migration", "remediation", "apply", "--help"],
        ["schema", "migration", "remediation", "certify", "--help"],
        ["schema", "migration", "remediation", "report", "--help"],
    ):
        with pytest.raises(SystemExit) as exc:
            cli_main.main(args)
        assert exc.value.code == 0
    assert "remediation" in capsys.readouterr().out


def test_schema_migration_remediation_plan_apply_certify_report_outputs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _patch_cli(monkeypatch)
    pack = _pack()
    pack_path = _write_json(tmp_path / "pack.json", pack.to_dict(command="plan"))
    watch_path = _write_json(tmp_path / "watch.json", _watch_certificate(pack))
    ledger_path = _write_json(tmp_path / "ledger.json", _ledger(pack))
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest())
    connection_path = _write_json(tmp_path / "clickhouse-prod.json", {"type": "clickhouse", "environment": "prod"})
    approval_path = _write_json(tmp_path / "approval.json", _approval(pack))

    plan_output = tmp_path / "remediation-plan.json"
    with pytest.raises(SystemExit) as plan_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "remediation",
                "plan",
                "--pack",
                str(pack_path),
                "--watch-certificate",
                str(watch_path),
                "--ledger",
                str(ledger_path),
                "--manifest",
                str(manifest_path),
                "--target-connection",
                str(connection_path),
                "--environment",
                "prod",
                "--format",
                "json",
                "--output",
                str(plan_output),
            ]
        )
    assert plan_exit.value.code == 0
    plan = json.loads(plan_output.read_text(encoding="utf-8"))
    assert plan["schema_version"] == "dpone.schema_migration_remediation_plan.v1"

    run_output = tmp_path / "remediation-run.json"
    with pytest.raises(SystemExit) as apply_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "remediation",
                "apply",
                "--plan",
                str(plan_output),
                "--approval",
                str(approval_path),
                "--format",
                "json",
                "--output",
                str(run_output),
            ]
        )
    assert apply_exit.value.code == 0
    run = json.loads(run_output.read_text(encoding="utf-8"))
    assert run["status"] == "dry_run"

    certificate_output = tmp_path / "remediation-certificate.json"
    with pytest.raises(SystemExit) as certify_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "remediation",
                "certify",
                "--run",
                str(run_output),
                "--profile",
                "advisory",
                "--format",
                "json",
                "--output",
                str(certificate_output),
            ]
        )
    assert certify_exit.value.code == 0
    certificate = json.loads(certificate_output.read_text(encoding="utf-8"))
    assert certificate["schema_version"] == "dpone.schema_migration_remediation_certificate.v1"

    report_output = tmp_path / "remediation-report.md"
    with pytest.raises(SystemExit) as report_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "remediation",
                "report",
                "--certificate",
                str(certificate_output),
                "--format",
                "md",
                "--output",
                str(report_output),
            ]
        )
    assert report_exit.value.code == 0
    assert "Schema Migration Remediation" in report_output.read_text(encoding="utf-8")
    assert "dpone.schema_migration_remediation" in capsys.readouterr().out


def test_bundle_and_registry_cli_accept_remediation_certificate(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _patch_cli(monkeypatch)
    pack = _pack()
    pack_path = _write_json(tmp_path / "pack.json", pack.to_dict(command="plan"))
    certificate_path = _write_json(tmp_path / "remediation-certificate.json", _remediation_certificate(pack))
    bundle_dir = tmp_path / "bundle"

    with pytest.raises(SystemExit) as build_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "bundle",
                "build",
                "--pack",
                str(pack_path),
                "--remediation-certificate",
                str(certificate_path),
                "--output-dir",
                str(bundle_dir),
                "--format",
                "json",
            ]
        )
    assert build_exit.value.code == 0
    bundle = json.loads((bundle_dir / "bundle.json").read_text(encoding="utf-8"))
    assert "remediation_certificate" in {artifact["kind"] for artifact in bundle["artifacts"]}

    store_uri = tmp_path / "registry.json"
    with pytest.raises(SystemExit) as record_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "registry",
                "record",
                "--bundle",
                str(bundle_dir / "bundle.json"),
                "--remediation-certificate",
                str(certificate_path),
                "--environment",
                "prod",
                "--stage",
                "remediated",
                "--store-uri",
                str(store_uri),
                "--format",
                "json",
            ]
        )
    assert record_exit.value.code == 0
    registry = json.loads(store_uri.read_text(encoding="utf-8"))
    assert registry["records"][0]["stage"] == "remediated"


def _pack() -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders", "table_settings": {"index_granularity": 8192}},
        actual={"sink_type": "clickhouse", "table": "analytics.orders", "table_settings": {}},
        changes=({"change_type": "table_setting", "path": "table_settings.index_granularity"},),
        ddl=("ALTER TABLE `analytics`.`orders` MODIFY SETTING index_granularity = 8192",),
        strategy="online_safe",
        rollback={"supported": True, "ddl": ["ALTER TABLE `analytics`.`orders` RESET SETTING index_granularity"]},
    )


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _manifest() -> dict[str, object]:
    return {
        "sink": {
            "options": {
                "physical_design": {
                    "migration": {
                        "remediation": {
                            "enabled": True,
                            "mode": "gate",
                            "profile": "prod_strict",
                            "execution": {"require_approval": True},
                        }
                    }
                }
            }
        }
    }


def _watch_certificate(pack: MigrationPack) -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_migration_watch_certificate.v1",
        "certificate_id": "sha256:" + "9" * 64,
        "pack_id": pack.pack_id,
        "environment": "prod",
        "target": pack.target.to_dict(),
        "status": "blocked",
        "profile": "prod_strict",
        "samples": {"planned": 2, "executed": 2, "passed": 0, "failed": 1},
        "remediation": {"decision": "rollback_required", "commands": []},
        "checks": [],
        "blockers": ["schema_migration_watch.physical_drift"],
        "warnings": [],
        "metrics": {},
    }


def _ledger(pack: MigrationPack) -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_migration_ledger.v1",
        "records": [
            {
                "schema_version": "dpone.schema_migration_ledger_record.v1",
                "pack_id": pack.pack_id,
                "status": "applied",
                "target": pack.target.to_dict(),
                "desired_fingerprint": pack.desired_fingerprint,
                "actual_fingerprint": pack.actual_fingerprint,
                "environment": "prod",
                "blockers": [],
                "warnings": [],
            }
        ],
    }


def _approval(pack: MigrationPack) -> dict[str, object]:
    return {"pack_id": pack.pack_id, "approved_by": "owner", "approved_risks": ["controlled_rollback"]}


def _remediation_certificate(pack: MigrationPack) -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_migration_remediation_certificate.v1",
        "certificate_id": "sha256:" + "a" * 64,
        "pack_id": pack.pack_id,
        "watch_certificate_id": "sha256:" + "9" * 64,
        "environment": "prod",
        "target": pack.target.to_dict(),
        "status": "certified",
        "profile": "prod_strict",
        "checks": [],
        "blockers": [],
        "warnings": [],
        "metrics": {},
    }


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
