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


def test_schema_migration_recovery_cli_help_is_registered(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _patch_cli(monkeypatch)
    for args in (
        ["schema", "migration", "recovery", "--help"],
        ["schema", "migration", "recovery", "point", "--help"],
        ["schema", "migration", "recovery", "point", "record", "--help"],
        ["schema", "migration", "recovery", "point", "latest", "--help"],
        ["schema", "migration", "recovery", "chain", "verify", "--help"],
        ["schema", "migration", "recovery", "restore", "plan", "--help"],
        ["schema", "migration", "recovery", "restore", "run", "--help"],
        ["schema", "migration", "recovery", "restore", "certify", "--help"],
        ["schema", "migration", "recovery", "retention", "plan", "--help"],
    ):
        with pytest.raises(SystemExit) as exc:
            cli_main.main(args)
        assert exc.value.code == 0
    assert "recovery" in capsys.readouterr().out


def test_schema_migration_recovery_cli_record_verify_restore_and_retention(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_cli(monkeypatch)
    pack = _pack()
    certificate_path = _write_json(tmp_path / "backup-certificate.json", _backup_certificate(pack))
    store_uri = tmp_path / "recovery.json"

    point_output = tmp_path / "recovery-point.json"
    with pytest.raises(SystemExit) as record_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "recovery",
                "point",
                "record",
                "--backup-certificate",
                str(certificate_path),
                "--environment",
                "prod",
                "--store-uri",
                str(store_uri),
                "--format",
                "json",
                "--output",
                str(point_output),
            ]
        )
    assert record_exit.value.code == 0
    point = json.loads(point_output.read_text(encoding="utf-8"))
    assert point["schema_version"] == "dpone.schema_migration_recovery_point.v1"

    latest_output = tmp_path / "latest.json"
    with pytest.raises(SystemExit) as latest_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "recovery",
                "point",
                "latest",
                "--target",
                "clickhouse.analytics.orders",
                "--environment",
                "prod",
                "--store-uri",
                str(store_uri),
                "--format",
                "json",
                "--output",
                str(latest_output),
            ]
        )
    assert latest_exit.value.code == 0
    assert json.loads(latest_output.read_text(encoding="utf-8"))["restore_point_id"] == point["restore_point_id"]

    chain_output = tmp_path / "chain.json"
    with pytest.raises(SystemExit) as chain_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "recovery",
                "chain",
                "verify",
                "--restore-point-id",
                point["restore_point_id"],
                "--require-restore-rehearsal",
                "--store-uri",
                str(store_uri),
                "--format",
                "json",
                "--output",
                str(chain_output),
            ]
        )
    assert chain_exit.value.code == 0
    assert json.loads(chain_output.read_text(encoding="utf-8"))["status"] == "verified"

    connection_path = _write_json(tmp_path / "clickhouse-stage.json", {"type": "clickhouse", "environment": "stage"})
    restore_plan_output = tmp_path / "restore-plan.json"
    with pytest.raises(SystemExit) as restore_plan_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "recovery",
                "restore",
                "plan",
                "--restore-point-id",
                point["restore_point_id"],
                "--target-connection",
                str(connection_path),
                "--environment",
                "stage",
                "--store-uri",
                str(store_uri),
                "--format",
                "json",
                "--output",
                str(restore_plan_output),
            ]
        )
    assert restore_plan_exit.value.code == 0
    assert json.loads(restore_plan_output.read_text(encoding="utf-8"))["schema_version"].endswith("restore_plan.v1")

    restore_run_output = tmp_path / "restore-run.json"
    with pytest.raises(SystemExit) as restore_run_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "recovery",
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

    certificate_output = tmp_path / "recovery-restore-certificate.json"
    with pytest.raises(SystemExit) as certify_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "recovery",
                "restore",
                "certify",
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
    assert json.loads(certificate_output.read_text(encoding="utf-8"))["schema_version"].endswith("certificate.v1")

    retention_output = tmp_path / "retention.md"
    with pytest.raises(SystemExit) as retention_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "recovery",
                "retention",
                "plan",
                "--target",
                "clickhouse.analytics.orders",
                "--environment",
                "prod",
                "--store-uri",
                str(store_uri),
                "--format",
                "md",
                "--output",
                str(retention_output),
            ]
        )
    assert retention_exit.value.code == 0
    assert "Schema Migration Recovery Retention Plan" in retention_output.read_text(encoding="utf-8")
    assert "dpone.schema_migration_recovery" in capsys.readouterr().out


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _pack() -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders"},
        actual={"sink_type": "clickhouse", "table": "analytics.orders"},
        changes=({"change_type": "drop_column", "path": "columns.legacy"},),
        strategy="block",
    )


def _backup_certificate(pack: MigrationPack) -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_migration_backup_certificate.v1",
        "certificate_id": "sha256:" + "a" * 64,
        "pack_id": pack.pack_id,
        "environment": "prod",
        "target": pack.target.to_dict(),
        "status": "certified",
        "profile": "prod_strict",
        "backup_run_id": "sha256:" + "b" * 64,
        "restore_run_id": "sha256:" + "c" * 64,
        "backup_destination": "Disk('backups', 'orders.zip')",
        "backup_kind": "full",
        "created_at": "2026-06-22T12:00:00Z",
        "valid_until": "2027-07-22T12:00:00Z",
        "restore_rehearsal": {"status": "passed", "restore_run_id": "sha256:" + "c" * 64},
        "checks": [],
        "blockers": [],
        "warnings": [],
        "metrics": {},
    }


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
