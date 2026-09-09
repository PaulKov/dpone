from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def test_schema_migration_watch_cli_help_is_registered(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as group_exit:
        cli_main.main(["schema", "migration", "watch", "--help"])
    with pytest.raises(SystemExit) as plan_exit:
        cli_main.main(["schema", "migration", "watch", "plan", "--help"])

    assert group_exit.value.code == 0
    assert plan_exit.value.code == 0
    assert "{plan,run,status,certify,report}" in capsys.readouterr().out


def test_watch_cli_plan_run_certify_report_bundle_and_registry_outputs(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    pack = _pack()
    pack_path = _write_json(tmp_path / "pack.json", pack.to_dict(command="plan"))
    post_apply_path = _write_json(tmp_path / "post-apply-certificate.json", _post_apply_certificate(pack))
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest())
    connection_path = _write_json(tmp_path / "connection.json", {"type": "clickhouse", "environment": "prod"})
    plan_output = tmp_path / "watch-plan.json"

    with pytest.raises(SystemExit) as plan_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "watch",
                "plan",
                "--pack",
                str(pack_path),
                "--post-apply-certificate",
                str(post_apply_path),
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
    assert plan["schema_version"] == "dpone.schema_migration_watch_plan.v1"
    assert plan["status"] == "planned"
    assert json.loads(capsys.readouterr().out)["watch_plan_id"] == plan["watch_plan_id"]

    run_output = tmp_path / "watch-run.json"
    with pytest.raises(SystemExit) as run_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "watch",
                "run",
                "--plan",
                str(plan_output),
                "--format",
                "json",
                "--output",
                str(run_output),
            ]
        )

    assert run_exit.value.code == 0
    run = json.loads(run_output.read_text(encoding="utf-8"))
    assert run["schema_version"] == "dpone.schema_migration_watch_run.v1"
    assert run["status"] == "dry_run"

    with pytest.raises(SystemExit) as status_exit:
        cli_main.main(["schema", "migration", "watch", "status", "--run", str(run_output), "--format", "table"])

    assert status_exit.value.code == 0
    assert "schema migration watch" in capsys.readouterr().out

    certificate_output = tmp_path / "watch-certificate.json"
    with pytest.raises(SystemExit) as certify_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "watch",
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
    assert certificate["schema_version"] == "dpone.schema_migration_watch_certificate.v1"
    assert certificate["status"] == "warning"

    report_output = tmp_path / "watch-report.md"
    with pytest.raises(SystemExit) as report_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "watch",
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
    assert "# Schema Migration Watch Certificate" in report_output.read_text(encoding="utf-8")
    capsys.readouterr()

    output_dir = tmp_path / "review"
    with pytest.raises(SystemExit) as build_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "bundle",
                "build",
                "--pack",
                str(pack_path),
                "--watch-certificate",
                str(certificate_output),
                "--attest",
                "--output-dir",
                str(output_dir),
                "--format",
                "json",
            ]
        )

    assert build_exit.value.code == 0
    bundle = json.loads(capsys.readouterr().out)
    assert "watch_certificate" in {artifact["kind"] for artifact in bundle["artifacts"]}

    with pytest.raises(SystemExit) as record_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "registry",
                "record",
                "--bundle",
                str(output_dir / "bundle.json"),
                "--watch-certificate",
                str(certificate_output),
                "--environment",
                "prod",
                "--stage",
                "watched",
                "--store-uri",
                str(tmp_path / "registry.json"),
                "--format",
                "json",
            ]
        )

    assert record_exit.value.code == 0
    record = json.loads(capsys.readouterr().out)
    assert record["stage"] == "watched"


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _pack() -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders", "engine": "MergeTree", "order_by": ["id"]},
        actual={"sink_type": "clickhouse", "table": "analytics.orders", "engine": "MergeTree", "order_by": ["id"]},
        changes=({"change_type": "table_setting", "path": "table_settings.index_granularity"},),
        ddl=("ALTER TABLE analytics.orders MODIFY SETTING index_granularity = 8192",),
        strategy="online_safe",
    )


def _manifest() -> dict[str, object]:
    return {
        "sink": {
            "options": {
                "physical_design": {
                    "migration": {
                        "watch": {
                            "enabled": True,
                            "mode": "gate",
                            "profile": "prod_strict",
                            "window": {"duration": "0s", "interval": "0s", "min_successful_samples": 1},
                            "verification": {"physical_design": True, "canary_queries": True, "query_health": False},
                            "canaries": [
                                {
                                    "id": "orders_count_positive",
                                    "type": "sql",
                                    "owner": "data-platform",
                                    "severity": "critical",
                                    "query": "SELECT 1 AS ok",
                                    "expect": {"column": "ok", "equals": 1},
                                }
                            ],
                        }
                    }
                }
            }
        }
    }


def _post_apply_certificate(pack: MigrationPack) -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_migration_post_apply_certificate.v1",
        "certificate_id": "sha256:" + "6" * 64,
        "pack_id": pack.pack_id,
        "bundle_id": None,
        "environment": "prod",
        "target": pack.target.to_dict(),
        "status": "verified",
        "profile": "prod_strict",
        "checks": [],
        "rollback_window": {"status": "open", "supported_until_phase": "contract"},
        "blockers": [],
        "warnings": [],
        "metrics": {"row_count": 10},
    }


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def _artifact(kind: str, path: str, payload: dict[str, object], *, required: bool) -> MigrationEvidenceArtifact:
    content = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return MigrationEvidenceArtifact.from_bytes(kind=kind, path=path, content=content, required=required)


def _bundle(pack: MigrationPack) -> dict[str, object]:
    return MigrationBundleBuilder().build(
        artifacts=(_artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),),
        attest=True,
    )
