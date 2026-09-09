from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from dpone.readiness.migration_control import (
    ArtifactMigrationLedgerStore,
    MigrationLedgerRecord,
    MigrationPack,
    MigrationTarget,
)
from dpone.readiness.schema_migration_promotion import (
    MigrationEnvironmentContract,
    MigrationEnvironmentVerifier,
    MigrationPromotionGate,
    MigrationPromotionPlanner,
)


def test_environment_contract_normalizes_chain_and_rejects_inline_secret(tmp_path: Path) -> None:
    contract_path = _write_environment_contract(
        tmp_path,
        target_connection={"password": "plain-text-secret"},
    )

    with pytest.raises(ValueError, match="inline secrets"):
        MigrationEnvironmentContract.from_file(contract_path)


def test_verify_env_reports_applied_pack_with_matching_actual_fingerprint(tmp_path: Path) -> None:
    pack = _pack()
    actual_path = _write_json(tmp_path / "actual-stage.json", pack.actual or {})
    ledger_path = tmp_path / "stage-ledger.json"
    ArtifactMigrationLedgerStore(ledger_path).append(
        MigrationLedgerRecord(
            pack_id=pack.pack_id,
            status="applied",
            target=pack.target,
            desired_fingerprint=pack.desired_fingerprint,
            actual_fingerprint=pack.actual_fingerprint,
        )
    )
    contract = MigrationEnvironmentContract.from_file(
        _write_environment_contract(tmp_path, stage_actual=actual_path, stage_ledger=ledger_path)
    )

    report = MigrationEnvironmentVerifier().verify(pack=pack, contract=contract, environment="stage")

    assert report["schema_version"] == "dpone.schema_migration_environment_report.v1"
    assert report["status"] == "ok"
    assert report["environment"] == "stage"
    assert report["pack_id"] == pack.pack_id
    assert report["actual_fingerprint"] == pack.actual_fingerprint
    assert report["ledger"]["matched_records"] == 1
    assert report["blockers"] == []


def test_certify_blocks_when_pack_was_not_applied_in_environment(tmp_path: Path) -> None:
    pack = _pack()
    actual_path = _write_json(tmp_path / "actual-stage.json", pack.actual or {})
    contract = MigrationEnvironmentContract.from_file(_write_environment_contract(tmp_path, stage_actual=actual_path))

    cert = MigrationEnvironmentVerifier().certify(pack=pack, contract=contract, environment="stage")

    assert cert["schema_version"] == "dpone.schema_migration_environment_certification.v1"
    assert cert["status"] == "blocked"
    assert "migration_environment.pack_not_applied" in cert["blockers"]


def test_certify_builds_immutable_receipt_for_applied_environment(tmp_path: Path) -> None:
    pack = _pack()
    actual_path = _write_json(tmp_path / "actual-stage.json", pack.actual or {})
    ledger_path = tmp_path / "stage-ledger.json"
    ArtifactMigrationLedgerStore(ledger_path).append(
        MigrationLedgerRecord(pack_id=pack.pack_id, status="applied", target=pack.target)
    )
    contract = MigrationEnvironmentContract.from_file(
        _write_environment_contract(tmp_path, stage_actual=actual_path, stage_ledger=ledger_path)
    )

    cert = MigrationEnvironmentVerifier().certify(pack=pack, contract=contract, environment="stage")

    assert cert["status"] == "certified"
    assert cert["certification_id"].startswith("sha256:")
    assert cert["pack_id"] == pack.pack_id
    assert cert["environment"] == "stage"
    assert cert["actual_fingerprint"] == pack.actual_fingerprint


def test_promotion_requires_valid_source_certificate_and_prod_approval(tmp_path: Path) -> None:
    pack = _pack()
    actual_path = _write_json(tmp_path / "actual-stage.json", pack.actual or {})
    ledger_path = tmp_path / "stage-ledger.json"
    ArtifactMigrationLedgerStore(ledger_path).append(
        MigrationLedgerRecord(pack_id=pack.pack_id, status="applied", target=pack.target)
    )
    contract = MigrationEnvironmentContract.from_file(
        _write_environment_contract(tmp_path, stage_actual=actual_path, stage_ledger=ledger_path)
    )
    cert = MigrationEnvironmentVerifier().certify(pack=pack, contract=contract, environment="stage")

    blocked = MigrationPromotionPlanner().promote(
        pack=pack,
        contract=contract,
        from_environment="stage",
        to_environment="prod",
        certificate=cert,
        approval=None,
    )

    assert blocked["status"] == "blocked"
    assert "migration_promotion.approval_required" in blocked["blockers"]

    allowed = MigrationPromotionPlanner().promote(
        pack=pack,
        contract=contract,
        from_environment="stage",
        to_environment="prod",
        certificate=cert,
        approval=_approval(pack_id=pack.pack_id, approved_risks=["prod_promotion"]),
    )

    assert allowed["schema_version"] == "dpone.schema_migration_promotion.v1"
    assert allowed["status"] == "promoted"
    assert allowed["promotion_id"].startswith("sha256:")
    assert allowed["from_environment"] == "stage"
    assert allowed["to_environment"] == "prod"


def test_promotion_gate_blocks_apply_without_matching_receipt(tmp_path: Path) -> None:
    pack = _pack()
    contract = MigrationEnvironmentContract.from_file(_write_environment_contract(tmp_path))

    blockers = MigrationPromotionGate().evaluate(
        pack=pack,
        contract=contract,
        environment="prod",
        promotion=None,
    )

    assert blockers == ("migration_promotion.promotion_required",)

    blockers = MigrationPromotionGate().evaluate(
        pack=pack,
        contract=contract,
        environment="prod",
        promotion={
            "schema_version": "dpone.schema_migration_promotion.v1",
            "status": "promoted",
            "pack_id": "sha256:different",
            "to_environment": "prod",
        },
    )

    assert blockers == ("migration_promotion.pack_id_mismatch",)


def _pack() -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders", "order_by": ["id"]},
        actual={"sink_type": "clickhouse", "table": "analytics.orders", "order_by": ["id"]},
        ddl=("ALTER TABLE analytics.orders MODIFY SETTING index_granularity = 8192",),
    )


def _approval(*, pack_id: str, approved_risks: list[str]) -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_migration_promotion_approval.v1",
        "pack_id": pack_id,
        "from_environment": "stage",
        "to_environment": "prod",
        "approved_by": "data-platform-owner",
        "approved_at": "2026-06-21T12:00:00Z",
        "expires_at": "2026-06-28T12:00:00Z",
        "approved_risks": approved_risks,
    }


def _write_environment_contract(
    tmp_path: Path,
    *,
    stage_actual: Path | None = None,
    stage_ledger: Path | None = None,
    target_connection: dict[str, object] | None = None,
) -> Path:
    target_ref = target_connection or {"path": ".dpone/targets/stage-clickhouse.json"}
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
            "dev": {"ledger": "dev-ledger.json", "actual": "actual-dev.json"},
            "stage": {
                "ledger": str(stage_ledger or tmp_path / "stage-ledger.json"),
                "actual": str(stage_actual or tmp_path / "actual-stage.json"),
                "target_connection": target_ref,
            },
            "prod": {"ledger": str(tmp_path / "prod-ledger.json"), "actual": str(tmp_path / "actual-prod.json")},
        },
    }
    path = tmp_path / "environments.yaml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path
