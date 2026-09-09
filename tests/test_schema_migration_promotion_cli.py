from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from dpone.cli import main as cli_main
from dpone.readiness.migration_control import (
    ArtifactMigrationLedgerStore,
    MigrationLedgerRecord,
    MigrationPack,
    MigrationTarget,
)


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def test_schema_migration_promotion_help_surfaces(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as verify_exit:
        cli_main.main(["schema", "migration", "verify-env", "--help"])
    with pytest.raises(SystemExit) as certify_exit:
        cli_main.main(["schema", "migration", "certify", "--help"])
    with pytest.raises(SystemExit) as promote_exit:
        cli_main.main(["schema", "migration", "promote", "--help"])

    assert verify_exit.value.code == 0
    assert certify_exit.value.code == 0
    assert promote_exit.value.code == 0


def test_verify_env_certify_promote_output_json_and_files(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    pack = _pack()
    pack_path = _write_pack(tmp_path, pack)
    actual_path = _write_json(tmp_path / "actual-stage.json", pack.actual or {})
    ledger_path = tmp_path / "stage-ledger.json"
    ArtifactMigrationLedgerStore(ledger_path).append(
        MigrationLedgerRecord(pack_id=pack.pack_id, status="applied", target=pack.target)
    )
    contract = _write_environment_contract(tmp_path, stage_actual=actual_path, stage_ledger=ledger_path)
    cert_path = tmp_path / "stage.cert.json"
    promotion_path = tmp_path / "stage-to-prod.promotion.json"
    approval = _write_approval(tmp_path, pack_id=pack.pack_id)

    with pytest.raises(SystemExit) as verify_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "verify-env",
                "--pack",
                str(pack_path),
                "--environment",
                "stage",
                "--environment-contract",
                str(contract),
                "--format",
                "json",
            ]
        )

    assert verify_exit.value.code == 0
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "ok"
    assert report["environment"] == "stage"

    with pytest.raises(SystemExit) as cert_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "certify",
                "--pack",
                str(pack_path),
                "--environment",
                "stage",
                "--environment-contract",
                str(contract),
                "--output",
                str(cert_path),
                "--format",
                "json",
            ]
        )

    assert cert_exit.value.code == 0
    cert = json.loads(capsys.readouterr().out)
    assert cert["status"] == "certified"
    assert json.loads(cert_path.read_text(encoding="utf-8"))["certification_id"] == cert["certification_id"]

    with pytest.raises(SystemExit) as promote_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "promote",
                "--pack",
                str(pack_path),
                "--from",
                "stage",
                "--to",
                "prod",
                "--certificate",
                str(cert_path),
                "--approval",
                str(approval),
                "--environment-contract",
                str(contract),
                "--output",
                str(promotion_path),
                "--format",
                "json",
            ]
        )

    assert promote_exit.value.code == 0
    promotion = json.loads(capsys.readouterr().out)
    assert promotion["status"] == "promoted"
    assert promotion["certification_id"] == cert["certification_id"]
    assert json.loads(promotion_path.read_text(encoding="utf-8"))["promotion_id"] == promotion["promotion_id"]


def test_apply_with_environment_contract_blocks_without_promotion(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    pack = _pack()
    pack_path = _write_pack(tmp_path, pack)
    contract = _write_environment_contract(tmp_path)

    with pytest.raises(SystemExit) as apply_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "apply",
                "--plan",
                str(pack_path),
                "--environment",
                "prod",
                "--environment-contract",
                str(contract),
                "--format",
                "json",
            ]
        )

    assert apply_exit.value.code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert "migration_promotion.promotion_required" in payload["blockers"]


def test_apply_with_valid_promotion_uses_environment_ledger(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    pack = _pack()
    pack_path = _write_pack(tmp_path, pack)
    actual_path = _write_json(tmp_path / "actual-stage.json", pack.actual or {})
    stage_ledger = tmp_path / "stage-ledger.json"
    prod_ledger = tmp_path / "prod-ledger.json"
    ArtifactMigrationLedgerStore(stage_ledger).append(
        MigrationLedgerRecord(pack_id=pack.pack_id, status="applied", target=pack.target)
    )
    contract = _write_environment_contract(
        tmp_path,
        stage_actual=actual_path,
        stage_ledger=stage_ledger,
        prod_ledger=prod_ledger,
    )
    cert = tmp_path / "stage.cert.json"
    promotion = tmp_path / "promotion.json"
    approval = _write_approval(tmp_path, pack_id=pack.pack_id)
    with pytest.raises(SystemExit) as cert_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "certify",
                "--pack",
                str(pack_path),
                "--environment",
                "stage",
                "--environment-contract",
                str(contract),
                "--output",
                str(cert),
                "--format",
                "json",
            ]
        )
    assert cert_exit.value.code == 0
    capsys.readouterr()
    with pytest.raises(SystemExit) as promote_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "promote",
                "--pack",
                str(pack_path),
                "--from",
                "stage",
                "--to",
                "prod",
                "--certificate",
                str(cert),
                "--approval",
                str(approval),
                "--environment-contract",
                str(contract),
                "--output",
                str(promotion),
                "--format",
                "json",
            ]
        )
    assert promote_exit.value.code == 0
    capsys.readouterr()

    with pytest.raises(SystemExit) as apply_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "apply",
                "--plan",
                str(pack_path),
                "--environment",
                "prod",
                "--environment-contract",
                str(contract),
                "--promotion",
                str(promotion),
                "--format",
                "json",
            ]
        )

    assert apply_exit.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "applied"
    assert payload["environment"] == "prod"
    assert payload["promotion_id"].startswith("sha256:")
    saved = json.loads(prod_ledger.read_text(encoding="utf-8"))
    assert saved["records"][0]["environment"] == "prod"
    assert saved["records"][0]["promotion_id"] == payload["promotion_id"]


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _pack() -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders", "order_by": ["id"]},
        actual={"sink_type": "clickhouse", "table": "analytics.orders", "order_by": ["id"]},
        ddl=("ALTER TABLE analytics.orders MODIFY SETTING index_granularity = 8192",),
    )


def _write_pack(tmp_path: Path, pack: MigrationPack) -> Path:
    path = tmp_path / "pack.json"
    path.write_text(json.dumps(pack.to_dict(command="plan")), encoding="utf-8")
    return path


def _write_approval(tmp_path: Path, *, pack_id: str) -> Path:
    path = tmp_path / "approval.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "schema_version": "dpone.schema_migration_promotion_approval.v1",
                "pack_id": pack_id,
                "from_environment": "stage",
                "to_environment": "prod",
                "approved_by": "data-platform-owner",
                "approved_at": "2026-06-21T12:00:00Z",
                "expires_at": "2026-06-28T12:00:00Z",
                "approved_risks": ["prod_promotion"],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


def _write_environment_contract(
    tmp_path: Path,
    *,
    stage_actual: Path | None = None,
    stage_ledger: Path | None = None,
    prod_ledger: Path | None = None,
) -> Path:
    payload = {
        "schema_version": "dpone.schema_migration_environments.v1",
        "chain": ["dev", "stage", "prod"],
        "policy": {
            "require_same_pack_id": True,
            "require_previous_certification": True,
            "require_impact_gate": True,
            "prod_requires_approval": True,
        },
        "environments": {
            "dev": {"ledger": str(tmp_path / "dev-ledger.json"), "actual": str(tmp_path / "actual-dev.json")},
            "stage": {
                "ledger": str(stage_ledger or tmp_path / "stage-ledger.json"),
                "actual": str(stage_actual or tmp_path / "actual-stage.json"),
                "target_connection": {"path": str(tmp_path / "stage-target.json")},
            },
            "prod": {
                "ledger": str(prod_ledger or tmp_path / "prod-ledger.json"),
                "actual": str(tmp_path / "actual-prod.json"),
                "target_connection": {"path": str(tmp_path / "prod-target.json")},
            },
        },
    }
    path = tmp_path / "environments.yaml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path
