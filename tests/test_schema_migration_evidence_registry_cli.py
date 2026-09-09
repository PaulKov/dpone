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


def test_schema_migration_registry_help_surfaces(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_cli(monkeypatch)

    commands = (
        ["schema", "migration", "registry", "--help"],
        ["schema", "migration", "registry", "record", "--help"],
        ["schema", "migration", "registry", "history", "--help"],
        ["schema", "migration", "registry", "latest", "--help"],
        ["schema", "migration", "registry", "audit-report", "--help"],
    )

    for command in commands:
        with pytest.raises(SystemExit) as exc:
            cli_main.main(command)
        assert exc.value.code == 0


def test_registry_record_history_latest_and_audit_report_local_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    bundle_path, gate_path, trust_path, diff_path = _write_evidence(tmp_path)
    store_uri = tmp_path / "registry.json"

    with pytest.raises(SystemExit) as record_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "registry",
                "record",
                "--bundle",
                str(bundle_path),
                "--gate",
                str(gate_path),
                "--trust",
                str(trust_path),
                "--diff",
                str(diff_path),
                "--environment",
                "prod",
                "--stage",
                "approved",
                "--actor",
                "ci",
                "--store-uri",
                str(store_uri),
                "--format",
                "json",
            ]
        )

    assert record_exit.value.code == 0
    record = json.loads(capsys.readouterr().out)
    assert record["schema_version"] == "dpone.schema_migration_evidence_registry_record.v1"
    assert record["stage"] == "approved"
    assert store_uri.exists()

    with pytest.raises(SystemExit) as history_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "registry",
                "history",
                "--target",
                "clickhouse.analytics.orders",
                "--environment",
                "prod",
                "--store-uri",
                str(store_uri),
                "--format",
                "table",
            ]
        )

    assert history_exit.value.code == 0
    table = capsys.readouterr().out
    assert "schema migration evidence registry" in table
    assert "approved" in table
    assert "analytics.orders" in table

    with pytest.raises(SystemExit) as latest_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "registry",
                "latest",
                "--target",
                "clickhouse.analytics.orders",
                "--environment",
                "prod",
                "--stage",
                "approved",
                "--store-uri",
                str(store_uri),
                "--format",
                "json",
            ]
        )

    assert latest_exit.value.code == 0
    latest = json.loads(capsys.readouterr().out)
    assert latest["record"]["record_id"] == record["record_id"]

    report_path = tmp_path / "audit.md"
    with pytest.raises(SystemExit) as report_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "registry",
                "audit-report",
                "--target",
                "clickhouse.analytics.orders",
                "--from",
                "2026-06-22",
                "--to",
                "2026-06-22",
                "--store-uri",
                str(store_uri),
                "--format",
                "md",
                "--output",
                str(report_path),
            ]
        )

    assert report_exit.value.code == 0
    markdown = capsys.readouterr().out
    assert "# Schema Migration Evidence Audit" in markdown
    assert report_path.read_text(encoding="utf-8") == markdown


def test_registry_record_sqlite_backend_and_conflict_exit_code(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    bundle_path, gate_path, trust_path, diff_path = _write_evidence(tmp_path)
    db_path = tmp_path / "registry.sqlite3"

    base_command = [
        "schema",
        "migration",
        "registry",
        "record",
        "--bundle",
        str(bundle_path),
        "--gate",
        str(gate_path),
        "--trust",
        str(trust_path),
        "--diff",
        str(diff_path),
        "--environment",
        "prod",
        "--stage",
        "approved",
        "--store-backend",
        "sqlite",
        "--store-uri",
        str(db_path),
        "--format",
        "json",
    ]

    with pytest.raises(SystemExit) as first_exit:
        cli_main.main(base_command)
    assert first_exit.value.code == 0
    capsys.readouterr()

    gate = json.loads(gate_path.read_text(encoding="utf-8"))
    gate["gate_id"] = "sha256:" + "9" * 64
    gate_path.write_text(json.dumps(gate, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    with pytest.raises(SystemExit) as conflict_exit:
        cli_main.main(base_command)

    payload = json.loads(capsys.readouterr().out)
    assert conflict_exit.value.code == 2
    assert payload["status"] == "blocked"
    assert "evidence_registry.record_conflict" in payload["blockers"]
    assert db_path.exists()


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _write_evidence(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    pack = MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders", "order_by": ["id"]},
        actual={"sink_type": "clickhouse", "table": "analytics.orders", "order_by": ["id"]},
        changes=({"change_type": "table_setting", "path": "table_settings.index_granularity"},),
        ddl=("ALTER TABLE analytics.orders MODIFY SETTING index_granularity = 8192",),
        strategy="online_safe",
    )
    pack_path = _write_json(tmp_path / "pack.json", pack.to_dict(command="plan"))
    artifact = MigrationEvidenceArtifact.from_bytes(
        kind="migration_pack",
        path=str(pack_path),
        content=pack_path.read_bytes(),
        required=True,
    )
    bundle = MigrationBundleBuilder().build(artifacts=(artifact,), attest=True)
    bundle_path = _write_json(tmp_path / "bundle.json", bundle)
    gate_path = _write_json(
        tmp_path / "gate.json",
        {
            "schema_version": "dpone.schema_migration_bundle_gate.v1",
            "status": "allowed",
            "gate_id": "sha256:" + "3" * 64,
            "bundle_id": bundle["bundle_id"],
            "pack_id": pack.pack_id,
            "target": {"sink_type": "clickhouse", "table": "analytics.orders"},
            "blockers": [],
            "warnings": [],
        },
    )
    trust_path = _write_json(
        tmp_path / "trust.json",
        {
            "schema_version": "dpone.schema_migration_trust_verification.v1",
            "status": "trusted",
            "trust_verification_id": "sha256:" + "4" * 64,
            "bundle_id": bundle["bundle_id"],
            "pack_id": pack.pack_id,
            "provenance_id": "sha256:" + "5" * 64,
            "scm": {"repository": "https://github.com/acme/data-platform", "commit_sha": "a" * 40},
            "blockers": [],
            "warnings": [],
        },
    )
    diff_path = _write_json(
        tmp_path / "diff.json",
        {
            "schema_version": "dpone.schema_migration_bundle_diff.v1",
            "status": "same",
            "diff_id": "sha256:" + "6" * 64,
            "head_bundle_id": bundle["bundle_id"],
            "head_pack_id": pack.pack_id,
            "blockers": [],
            "warnings": [],
            "changes": [],
        },
    )
    return bundle_path, gate_path, trust_path, diff_path


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path
