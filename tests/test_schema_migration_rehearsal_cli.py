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


def test_schema_migration_rehearse_cli_help_is_registered(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(["schema", "migration", "rehearse", "--help"])

    assert exc.value.code == 0
    assert "{plan,run,certify,report,fixture}" in capsys.readouterr().out


def test_rehearsal_cli_plan_run_certify_and_report_outputs_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_cli(monkeypatch)
    pack = _write_json(tmp_path / "pack.json", _pack().to_dict(command="plan"))
    connection = _write_json(
        tmp_path / "connection.json",
        {"type": "clickhouse", "environment": "stage", "database": "analytics"},
    )
    output_dir = tmp_path / "rehearsal"
    with pytest.raises(SystemExit) as plan_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "rehearse",
                "plan",
                "--pack",
                str(pack),
                "--environment",
                "stage",
                "--target-connection",
                str(connection),
                "--output-dir",
                str(output_dir),
                "--format",
                "json",
            ]
        )

    assert plan_exit.value.code == 0
    plan_payload = json.loads((output_dir / "rehearsal-plan.json").read_text(encoding="utf-8"))
    assert plan_payload["schema_version"] == "dpone.schema_migration_rehearsal_plan.v1"
    assert plan_payload["status"] == "planned"

    run_output = tmp_path / "run.json"
    with pytest.raises(SystemExit) as run_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "rehearse",
                "run",
                "--plan",
                str(output_dir / "rehearsal-plan.json"),
                "--format",
                "json",
                "--output",
                str(run_output),
            ]
        )
    assert run_exit.value.code == 0
    run_payload = json.loads(run_output.read_text(encoding="utf-8"))
    assert run_payload["status"] == "dry_run"
    assert run_payload["executed"] is False

    cert_output = tmp_path / "certificate.json"
    with pytest.raises(SystemExit) as certify_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "rehearse",
                "certify",
                "--run",
                str(run_output),
                "--profile",
                "advisory",
                "--format",
                "json",
                "--output",
                str(cert_output),
            ]
        )
    assert certify_exit.value.code == 0
    certificate = json.loads(cert_output.read_text(encoding="utf-8"))
    assert certificate["schema_version"] == "dpone.schema_migration_rehearsal_certificate.v1"
    assert certificate["status"] == "warning"

    report_output = tmp_path / "report.md"
    with pytest.raises(SystemExit) as report_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "rehearse",
                "report",
                "--certificate",
                str(cert_output),
                "--format",
                "md",
                "--output",
                str(report_output),
            ]
        )
    assert report_exit.value.code == 0
    assert "# Schema Migration Rehearsal Certificate" in report_output.read_text(encoding="utf-8")
    assert capsys.readouterr().out


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _pack() -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders"},
        actual={"sink_type": "clickhouse", "table": "analytics.orders"},
        ddl=("ALTER TABLE analytics.orders MODIFY SETTING index_granularity = 8192",),
        strategy="online_safe",
    )


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
