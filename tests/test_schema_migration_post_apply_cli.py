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


def test_schema_migration_post_apply_cli_help_is_registered(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as group_exit:
        cli_main.main(["schema", "migration", "post-apply", "--help"])
    with pytest.raises(SystemExit) as plan_exit:
        cli_main.main(["schema", "migration", "post-apply", "plan", "--help"])

    assert group_exit.value.code == 0
    assert plan_exit.value.code == 0
    assert "{plan,run,certify,report}" in capsys.readouterr().out


def test_post_apply_cli_plan_run_certify_and_report_outputs_files(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    pack = _pack()
    pack_path = _write_json(tmp_path / "pack.json", pack.to_dict(command="plan"))
    bundle_path = _write_json(tmp_path / "bundle.json", _bundle(pack))
    ledger_path = _write_json(tmp_path / "ledger.json", _ledger(pack))
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest())
    connection_path = _write_json(tmp_path / "connection.json", {"type": "clickhouse", "environment": "prod"})
    plan_output = tmp_path / "post-apply-plan.json"

    with pytest.raises(SystemExit) as plan_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "post-apply",
                "plan",
                "--pack",
                str(pack_path),
                "--bundle",
                str(bundle_path),
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
    plan_payload = json.loads(plan_output.read_text(encoding="utf-8"))
    assert plan_payload["schema_version"] == "dpone.schema_migration_post_apply_plan.v1"
    assert plan_payload["status"] == "planned"
    assert json.loads(capsys.readouterr().out)["post_apply_plan_id"] == plan_payload["post_apply_plan_id"]

    run_output = tmp_path / "post-apply-run.json"
    with pytest.raises(SystemExit) as run_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "post-apply",
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
    run_payload = json.loads(run_output.read_text(encoding="utf-8"))
    assert run_payload["schema_version"] == "dpone.schema_migration_post_apply_run.v1"
    assert run_payload["status"] == "dry_run"

    certificate_output = tmp_path / "post-apply-certificate.json"
    with pytest.raises(SystemExit) as certify_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "post-apply",
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
    assert certificate["schema_version"] == "dpone.schema_migration_post_apply_certificate.v1"
    assert certificate["status"] == "warning"

    report_output = tmp_path / "post-apply-report.md"
    with pytest.raises(SystemExit) as report_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "post-apply",
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
    assert "# Schema Migration Post-Apply Certificate" in report_output.read_text(encoding="utf-8")


def test_bundle_build_cli_accepts_post_apply_certificate(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    pack = _pack()
    pack_path = _write_json(tmp_path / "pack.json", pack.to_dict(command="plan"))
    certificate_path = _write_json(
        tmp_path / "post-apply-certificate.json",
        {
            "schema_version": "dpone.schema_migration_post_apply_certificate.v1",
            "certificate_id": "sha256:" + "6" * 64,
            "pack_id": pack.pack_id,
            "bundle_id": None,
            "environment": "prod",
            "target": pack.target.to_dict(),
            "status": "verified",
            "profile": "prod_strict",
            "checks": [],
            "blockers": [],
            "warnings": [],
            "metrics": {"duration_ms": 10, "checks_executed": 4, "canaries_executed": 1},
        },
    )
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
                "--post-apply-certificate",
                str(certificate_path),
                "--attest",
                "--output-dir",
                str(output_dir),
                "--format",
                "json",
            ]
        )

    assert build_exit.value.code == 0
    bundle = json.loads(capsys.readouterr().out)
    assert bundle["status"] == "ready"
    assert bundle["summary"]["post_apply_certificate_id"] == "sha256:" + "6" * 64
    assert "post_apply_certificate" in {artifact["kind"] for artifact in bundle["artifacts"]}


def test_registry_record_cli_accepts_verified_post_apply_certificate(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    pack = _pack()
    (tmp_path / "pack.json").write_bytes(
        json.dumps(pack.to_dict(command="plan"), ensure_ascii=False, sort_keys=True).encode("utf-8")
    )
    bundle_path = _write_json(tmp_path / "bundle.json", _bundle(pack))
    certificate_path = _write_json(
        tmp_path / "post-apply-certificate.json",
        {
            "schema_version": "dpone.schema_migration_post_apply_certificate.v1",
            "certificate_id": "sha256:" + "7" * 64,
            "pack_id": pack.pack_id,
            "bundle_id": None,
            "environment": "prod",
            "target": pack.target.to_dict(),
            "status": "verified",
            "profile": "prod_strict",
            "checks": [],
            "blockers": [],
            "warnings": [],
            "metrics": {"duration_ms": 10, "checks_executed": 4, "canaries_executed": 1},
        },
    )

    with pytest.raises(SystemExit) as record_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "registry",
                "record",
                "--bundle",
                str(bundle_path),
                "--post-apply-certificate",
                str(certificate_path),
                "--environment",
                "prod",
                "--stage",
                "verified",
                "--store-uri",
                str(tmp_path / "registry.json"),
                "--format",
                "json",
            ]
        )

    assert record_exit.value.code == 0
    record = json.loads(capsys.readouterr().out)
    assert record["stage"] == "verified"
    assert "post_apply_certificate" in {artifact["kind"] for artifact in record["artifact_refs"]}


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


def _bundle(pack: MigrationPack) -> dict[str, object]:
    artifact = _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True)
    return MigrationBundleBuilder().build(artifacts=(artifact,), attest=True)


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


def _manifest() -> dict[str, object]:
    return {
        "sink": {
            "options": {
                "physical_design": {
                    "migration": {
                        "post_apply": {
                            "enabled": True,
                            "mode": "gate",
                            "profile": "prod_strict",
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


def _artifact(kind: str, path: str, payload: dict[str, object], *, required: bool) -> MigrationEvidenceArtifact:
    content = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return MigrationEvidenceArtifact.from_bytes(kind=kind, path=path, content=content, required=required)


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
