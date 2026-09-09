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


def test_schema_migration_backup_cli_help_is_registered(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _patch_cli(monkeypatch)
    for args in (
        ["schema", "migration", "backup", "--help"],
        ["schema", "migration", "backup", "plan", "--help"],
        ["schema", "migration", "backup", "create", "--help"],
        ["schema", "migration", "backup", "restore", "--help"],
        ["schema", "migration", "backup", "restore", "plan", "--help"],
        ["schema", "migration", "backup", "restore", "run", "--help"],
        ["schema", "migration", "backup", "certify", "--help"],
        ["schema", "migration", "backup", "report", "--help"],
    ):
        with pytest.raises(SystemExit) as exc:
            cli_main.main(args)
        assert exc.value.code == 0
    assert "backup" in capsys.readouterr().out


def test_schema_migration_backup_plan_create_restore_certify_report_outputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_cli(monkeypatch)
    pack = _pack()
    pack_path = _write_json(tmp_path / "pack.json", pack.to_dict(command="plan"))
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest())
    prod_connection = _write_json(tmp_path / "clickhouse-prod.json", {"type": "clickhouse", "environment": "prod"})
    stage_connection = _write_json(
        tmp_path / "clickhouse-stage.json",
        {"type": "clickhouse", "environment": "stage"},
    )
    approval_path = _write_json(tmp_path / "approval.json", _approval(pack))

    plan_output = tmp_path / "backup-plan.json"
    with pytest.raises(SystemExit) as plan_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "backup",
                "plan",
                "--pack",
                str(pack_path),
                "--manifest",
                str(manifest_path),
                "--target-connection",
                str(prod_connection),
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
    assert plan["schema_version"] == "dpone.schema_migration_backup_plan.v1"

    run_output = tmp_path / "backup-run.json"
    with pytest.raises(SystemExit) as create_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "backup",
                "create",
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
    assert create_exit.value.code == 0
    backup_run = json.loads(run_output.read_text(encoding="utf-8"))
    assert backup_run["status"] == "dry_run"

    restore_plan_output = tmp_path / "restore-plan.json"
    with pytest.raises(SystemExit) as restore_plan_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "backup",
                "restore",
                "plan",
                "--backup-run",
                str(run_output),
                "--target-connection",
                str(stage_connection),
                "--environment",
                "stage",
                "--format",
                "json",
                "--output",
                str(restore_plan_output),
            ]
        )
    assert restore_plan_exit.value.code == 0
    restore_plan = json.loads(restore_plan_output.read_text(encoding="utf-8"))
    assert restore_plan["schema_version"] == "dpone.schema_migration_restore_plan.v1"

    restore_run_output = tmp_path / "restore-run.json"
    with pytest.raises(SystemExit) as restore_run_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "backup",
                "restore",
                "run",
                "--plan",
                str(restore_plan_output),
                "--format",
                "json",
                "--output",
                str(restore_run_output),
            ]
        )
    assert restore_run_exit.value.code == 0
    restore_run = json.loads(restore_run_output.read_text(encoding="utf-8"))
    assert restore_run["status"] == "dry_run"

    certificate_output = tmp_path / "backup-certificate.json"
    with pytest.raises(SystemExit) as certify_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "backup",
                "certify",
                "--backup-run",
                str(run_output),
                "--restore-run",
                str(restore_run_output),
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
    assert certificate["schema_version"] == "dpone.schema_migration_backup_certificate.v1"

    report_output = tmp_path / "backup-report.md"
    with pytest.raises(SystemExit) as report_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "backup",
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
    assert "Schema Migration Backup" in report_output.read_text(encoding="utf-8")
    assert "dpone.schema_migration_backup" in capsys.readouterr().out


def test_bundle_and_registry_cli_accept_backup_certificate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    pack = _pack()
    pack_path = _write_json(tmp_path / "pack.json", pack.to_dict(command="plan"))
    certificate_path = _write_json(tmp_path / "backup-certificate.json", _backup_certificate(pack))
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
                "--backup-certificate",
                str(certificate_path),
                "--output-dir",
                str(bundle_dir),
                "--format",
                "json",
            ]
        )
    assert build_exit.value.code == 0
    bundle = json.loads((bundle_dir / "bundle.json").read_text(encoding="utf-8"))
    assert "backup_certificate" in {artifact["kind"] for artifact in bundle["artifacts"]}

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
                "--backup-certificate",
                str(certificate_path),
                "--environment",
                "prod",
                "--stage",
                "backup_certified",
                "--store-uri",
                str(store_uri),
                "--format",
                "json",
            ]
        )
    assert record_exit.value.code == 0
    registry = json.loads(store_uri.read_text(encoding="utf-8"))
    assert registry["records"][0]["stage"] == "backup_certified"


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _pack() -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders", "engine": "MergeTree", "order_by": ["id"]},
        actual={"sink_type": "clickhouse", "table": "analytics.orders", "engine": "MergeTree", "order_by": []},
        changes=({"change_type": "order_by", "path": "order_by"},),
        strategy="shadow",
        phases=({"name": "contract", "operations": [{"name": "contract", "sql": "DROP TABLE old"}]},),
        rollback={"supported": True, "ddl": ["EXCHANGE TABLES a AND b"]},
    )


def _manifest() -> dict[str, object]:
    return {
        "sink": {
            "options": {
                "physical_design": {
                    "migration": {
                        "backup": {
                            "enabled": True,
                            "mode": "gate",
                            "profile": "prod_strict",
                            "strategy": "target_native",
                            "restore_rehearsal": {"enabled": True, "environment": "stage"},
                            "clickhouse": {
                                "destination": "Disk('backups', 'dpone/{pack_id}/{table}.zip')",
                            },
                        }
                    }
                }
            }
        }
    }


def _approval(pack: MigrationPack) -> dict[str, object]:
    return {"pack_id": pack.pack_id, "approved_by": "owner", "approved_risks": ["backup"]}


def _backup_certificate(pack: MigrationPack) -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_migration_backup_certificate.v1",
        "certificate_id": "sha256:" + "b" * 64,
        "pack_id": pack.pack_id,
        "environment": "prod",
        "target": pack.target.to_dict(),
        "status": "certified",
        "profile": "prod_strict",
        "backup_run_id": "sha256:" + "c" * 64,
        "restore_run_id": "sha256:" + "d" * 64,
        "checks": [],
        "blockers": [],
        "warnings": [],
        "metrics": {},
    }


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
