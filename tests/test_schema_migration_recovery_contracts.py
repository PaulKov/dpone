from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_migration_backup import MigrationBackupPlanner
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
)
from dpone.readiness.schema_migration_evidence_registry import MigrationEvidenceRecorder
from dpone.readiness.schema_migration_recovery import (
    RecoveryChainVerifier,
    RecoveryPointRecorder,
    RecoveryPointSelector,
    RecoveryRestoreCertifier,
    RecoveryRestorePlanner,
    RecoveryRestoreRunner,
    RecoveryRetentionPlanner,
)
from dpone.readiness.schema_migration_recovery_sqlite import SqliteRecoveryCatalogStore
from dpone.readiness.schema_migration_recovery_store import LocalJsonRecoveryCatalogStore
from dpone.readiness.schema_migration_remediation import MigrationRemediationPlanner
from dpone.runtime.sinks.clickhouse_backup import ClickHouseBackupDialect
from dpone.runtime.sinks.clickhouse_recovery import ClickHouseRecoveryDialect


def test_certified_backup_records_usable_full_restore_point(tmp_path: Path) -> None:
    pack = _pack()
    store = LocalJsonRecoveryCatalogStore(tmp_path / "recovery.json")

    point = RecoveryPointRecorder().record(
        backup_certificate=_backup_certificate(pack),
        environment="prod",
        mode="gate",
    )
    store.append(point)
    latest = RecoveryPointSelector(store).latest(
        target="clickhouse.analytics.orders",
        environment="prod",
        profile="prod_strict",
    )

    assert point["schema_version"] == "dpone.schema_migration_recovery_point.v1"
    assert point["status"] == "usable"
    assert point["kind"] == "full"
    assert point["chain_depth"] == 0
    assert point["restore_rehearsal"]["status"] == "passed"
    assert latest["restore_point_id"] == point["restore_point_id"]
    assert latest["chain_id"].startswith("sha256:")


def test_latest_selector_ignores_expired_restore_points(tmp_path: Path) -> None:
    pack = _pack()
    store = LocalJsonRecoveryCatalogStore(tmp_path / "recovery.json")
    recorder = RecoveryPointRecorder()
    usable = recorder.record(
        backup_certificate=_backup_certificate(pack, destination="Disk('backups', 'usable.zip')"),
        environment="prod",
    )
    expired = recorder.record(
        backup_certificate=_backup_certificate(
            pack,
            destination="Disk('backups', 'expired.zip')",
            valid_until="2000-01-01T00:00:00Z",
        ),
        environment="prod",
    )
    store.append(usable)
    store.append(expired)

    latest = RecoveryPointSelector(store).latest(target="clickhouse.analytics.orders", environment="prod")
    chain = RecoveryChainVerifier(store).verify(restore_point_id=expired["restore_point_id"])

    assert expired["status"] == "expired"
    assert latest["restore_point_id"] == usable["restore_point_id"]
    assert "schema_migration_recovery.restore_point_expired" in chain["blockers"]


def test_blocked_backup_certificate_blocks_in_gate_and_warns_in_observe() -> None:
    pack = _pack()
    blocked = {**_backup_certificate(pack), "status": "blocked", "blockers": ["backup failed"]}

    gate = RecoveryPointRecorder().record(backup_certificate=blocked, environment="prod", mode="gate")
    observe = RecoveryPointRecorder().record(backup_certificate=blocked, environment="prod", mode="observe")

    assert gate["status"] == "blocked"
    assert "schema_migration_recovery.backup_certificate_not_certified" in gate["blockers"]
    assert observe["status"] == "warning"
    assert "schema_migration_recovery.backup_certificate_not_certified" in observe["warnings"]


def test_recovery_store_idempotency_conflicts_and_sqlite_equivalence(tmp_path: Path) -> None:
    pack = _pack()
    point = RecoveryPointRecorder().record(backup_certificate=_backup_certificate(pack), environment="prod")
    json_store = LocalJsonRecoveryCatalogStore(tmp_path / "recovery.json")
    sqlite_store = SqliteRecoveryCatalogStore(tmp_path / "recovery.sqlite3")

    json_store.append(point)
    json_store.append(point)
    sqlite_store.append(point)

    conflict = {**point, "pack_id": "sha256:" + "9" * 64}
    try:
        json_store.append(conflict)
    except ValueError as exc:
        assert "recovery_catalog.destination_conflict" in str(exc)
    else:  # pragma: no cover - assertion guard
        raise AssertionError("destination conflict was accepted")

    assert len(json_store.query(target="clickhouse.analytics.orders", environment="prod")) == 1
    assert sqlite_store.query(target="clickhouse.analytics.orders", environment="prod") == (point,)


def test_chain_verifier_handles_full_incremental_missing_and_expired_base(tmp_path: Path) -> None:
    pack = _pack()
    store = LocalJsonRecoveryCatalogStore(tmp_path / "recovery.json")
    recorder = RecoveryPointRecorder()
    full = recorder.record(backup_certificate=_backup_certificate(pack, destination="Disk('backups', 'full.zip')"))
    incremental = recorder.record(
        backup_certificate=_backup_certificate(
            pack,
            destination="Disk('backups', 'inc.zip')",
            backup_kind="incremental",
            base_restore_point_id=full["restore_point_id"],
        ),
        base_restore_point=full,
    )
    store.append(full)
    store.append(incremental)

    verified = RecoveryChainVerifier(store).verify(
        restore_point_id=incremental["restore_point_id"],
        require_restore_rehearsal=True,
    )
    missing = RecoveryChainVerifier(LocalJsonRecoveryCatalogStore(tmp_path / "missing.json")).verify(
        restore_point_id=incremental["restore_point_id"],
        seed_points=(incremental,),
        require_restore_rehearsal=True,
    )
    expired_store = LocalJsonRecoveryCatalogStore(tmp_path / "expired.json")
    expired_store.append({**full, "status": "expired", "valid_until": "2026-01-01T00:00:00Z"})
    expired_store.append(incremental)
    expired = RecoveryChainVerifier(expired_store).verify(
        restore_point_id=incremental["restore_point_id"],
        require_restore_rehearsal=True,
    )

    assert verified["schema_version"] == "dpone.schema_migration_recovery_chain_verification.v1"
    assert verified["status"] == "verified"
    assert [item["restore_point_id"] for item in verified["chain"]] == [
        full["restore_point_id"],
        incremental["restore_point_id"],
    ]
    assert missing["status"] == "blocked"
    assert "schema_migration_recovery.base_restore_point_missing" in missing["blockers"]
    assert expired["status"] == "blocked"
    assert "schema_migration_recovery.restore_point_not_usable" in expired["blockers"]


def test_restore_plan_blocks_prod_and_renders_clickhouse_restore(tmp_path: Path) -> None:
    pack = _pack()
    point = RecoveryPointRecorder().record(backup_certificate=_backup_certificate(pack), environment="prod")
    store = LocalJsonRecoveryCatalogStore(tmp_path / "recovery.json")
    store.append(point)
    chain = RecoveryChainVerifier(store).verify(restore_point_id=point["restore_point_id"])

    prod = RecoveryRestorePlanner(dialect=ClickHouseRecoveryDialect()).plan(
        restore_point=point,
        chain_verification=chain,
        target_connection={"type": "clickhouse", "environment": "prod"},
        environment="prod",
    )
    stage = RecoveryRestorePlanner(dialect=ClickHouseRecoveryDialect()).plan(
        restore_point=point,
        chain_verification=chain,
        target_connection={"type": "clickhouse", "environment": "stage"},
        environment="stage",
    )
    run = RecoveryRestoreRunner().run(plan=stage, executor=_OkExecutor(), execute=True)
    certificate = RecoveryRestoreCertifier().certify(restore_run=run, profile="prod_strict")

    assert prod["status"] == "blocked"
    assert "schema_migration_recovery.restore_prod_blocked" in prod["blockers"]
    assert stage["status"] == "planned"
    assert (
        "RESTORE TABLE `analytics`.`orders` AS `analytics`.`__dpone_recovery_orders_" in stage["operations"][0]["sql"]
    )
    assert run["status"] == "restored"
    assert certificate["status"] == "certified"


def test_retention_bundle_and_remediation_integration(tmp_path: Path) -> None:
    pack = _pack()
    store = LocalJsonRecoveryCatalogStore(tmp_path / "recovery.json")
    recorder = RecoveryPointRecorder()
    full = recorder.record(backup_certificate=_backup_certificate(pack, destination="Disk('backups', 'full.zip')"))
    incremental = recorder.record(
        backup_certificate=_backup_certificate(
            pack,
            destination="Disk('backups', 'inc.zip')",
            backup_kind="incremental",
            base_restore_point_id=full["restore_point_id"],
        ),
        base_restore_point=full,
    )
    store.append(full)
    store.append(incremental)
    chain = RecoveryChainVerifier(store).verify(restore_point_id=incremental["restore_point_id"])
    artifacts = (
        _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
        _artifact("recovery_point", "point.json", incremental, required=False),
        _artifact("recovery_chain_verification", "chain.json", chain, required=False),
    )

    retention = RecoveryRetentionPlanner(store).plan(target="clickhouse.analytics.orders", environment="prod")
    bundle = MigrationBundleBuilder().build(artifacts=artifacts, attest=True)
    gate = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=MigrationBundlePolicyOptions.resolve(
            profile="prod_strict",
            policy_payload={
                "required_artifacts": [
                    "migration_pack",
                    "recovery_point",
                    "recovery_chain_verification",
                ],
                "fail_on_warnings": False,
            },
        ),
        artifact_payloads={artifact.kind: artifact.payload for artifact in artifacts},
    )
    record = MigrationEvidenceRecorder().build_record(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        gate=None,
        trust_verification=None,
        diff=None,
        environment="prod",
        stage="recovery_chain_verified",
        recovery_point=incremental,
        recovery_chain_verification=chain,
        actor="ci",
    )
    remediation = MigrationRemediationPlanner().plan(
        pack=pack.to_dict(command="plan"),
        watch_certificate=_watch_certificate(pack),
        ledger=_ledger(pack, phase="contract"),
        manifest=_remediation_manifest(),
        target_connection={"type": "clickhouse", "environment": "prod"},
        environment="prod",
        recovery_point=incremental,
    )

    assert retention["status"] == "warning"
    assert any(item["action"] == "protect_base_with_dependents" for item in retention["actions"])
    assert bundle["summary"]["recovery_point_id"] == incremental["restore_point_id"]
    assert bundle["summary"]["recovery_chain_verification_id"] == chain["chain_verification_id"]
    assert gate["status"] == "allowed"
    assert record["stage"] == "recovery_chain_verified"
    assert "recovery_point" in {item["kind"] for item in record["artifact_refs"]}
    assert "recovery_chain_verification" in {item["kind"] for item in record["artifact_refs"]}
    assert remediation["capability"] == "restore_to_point"
    assert remediation["recovery_point_id"] == incremental["restore_point_id"]
    assert "RESTORE TABLE" in remediation["operations"][0]["sql"]


def test_incremental_backup_plan_uses_selected_recovery_base() -> None:
    pack = _pack()
    base = RecoveryPointRecorder().record(
        backup_certificate=_backup_certificate(pack, destination="Disk('backups', 'base.zip')"),
        environment="prod",
    )

    plan = MigrationBackupPlanner(dialect=ClickHouseBackupDialect()).plan(
        pack=pack.to_dict(command="plan"),
        manifest=_incremental_manifest(),
        target_connection={"type": "clickhouse", "environment": "prod"},
        environment="prod",
        base_restore_point=base,
    )

    assert plan["backup_kind"] == "incremental"
    assert plan["base_restore_point_id"] == base["restore_point_id"]
    assert plan["chain_id"] == base["chain_id"]
    assert plan["chain_depth"] == 1
    assert "SETTINGS base_backup = Disk('backups', 'base.zip')" in plan["operations"][0]["sql"]


def test_recovery_public_json_schemas_validate_example_artifacts(tmp_path: Path) -> None:
    pack = _pack()
    store = LocalJsonRecoveryCatalogStore(tmp_path / "recovery.json")
    point = RecoveryPointRecorder().record(backup_certificate=_backup_certificate(pack), environment="prod")
    store.append(point)
    chain = RecoveryChainVerifier(store).verify(restore_point_id=point["restore_point_id"])
    restore_plan = RecoveryRestorePlanner(dialect=ClickHouseRecoveryDialect()).plan(
        restore_point=point,
        chain_verification=chain,
        target_connection={"type": "clickhouse", "environment": "stage"},
        environment="stage",
    )
    restore_run = RecoveryRestoreRunner().run(plan=restore_plan, executor=_OkExecutor(), execute=True)
    restore_certificate = RecoveryRestoreCertifier().certify(restore_run=restore_run, profile="prod_strict")
    retention = RecoveryRetentionPlanner(store).plan(target="clickhouse.analytics.orders", environment="prod")

    _validate_schema("recovery-point.schema.json", point)
    _validate_schema("recovery-chain-verification.schema.json", chain)
    _validate_schema("recovery-restore-plan.schema.json", restore_plan)
    _validate_schema("recovery-restore-run.schema.json", restore_run)
    _validate_schema("recovery-restore-certificate.schema.json", restore_certificate)
    _validate_schema("recovery-retention-plan.schema.json", retention)


class _OkExecutor:
    def execute(self, operation: dict[str, object]) -> dict[str, object]:
        return {"status": "executed", "sql": operation.get("sql")}


def _pack() -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders", "engine": "MergeTree", "order_by": ["id"]},
        actual={"sink_type": "clickhouse", "table": "analytics.orders", "engine": "MergeTree", "order_by": []},
        changes=({"change_type": "order_by", "path": "order_by"},),
        strategy="shadow",
        phases=({"name": "contract", "operations": [{"name": "contract", "sql": "DROP TABLE old"}]},),
        rollback={"supported": True, "supported_until_phase": "contract", "ddl": []},
    )


def _backup_certificate(
    pack: MigrationPack,
    *,
    destination: str = "Disk('backups', 'dpone/orders.zip')",
    backup_kind: str = "full",
    base_restore_point_id: str | None = None,
    valid_until: str = "2027-07-22T12:00:00Z",
) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": "dpone.schema_migration_backup_certificate.v1",
        "certificate_id": "sha256:" + stable_suffix(destination),
        "pack_id": pack.pack_id,
        "environment": "prod",
        "target": pack.target.to_dict(),
        "status": "certified",
        "profile": "prod_strict",
        "backup_run_id": "sha256:" + "b" * 64,
        "restore_run_id": "sha256:" + "c" * 64,
        "backup_destination": destination,
        "backup_kind": backup_kind,
        "created_at": "2026-06-22T12:00:00Z",
        "valid_until": valid_until,
        "retention": {"min_days": 7, "delete_after": "30d"},
        "restore_rehearsal": {"status": "passed", "restore_run_id": "sha256:" + "c" * 64},
        "rpo_seconds": 0,
        "checks": [],
        "blockers": [],
        "warnings": [],
        "metrics": {},
    }
    if base_restore_point_id:
        payload["base_restore_point_id"] = base_restore_point_id
    return payload


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


def _ledger(pack: MigrationPack, *, phase: str) -> dict[str, object]:
    record: dict[str, object] = {
        "schema_version": "dpone.schema_migration_ledger_record.v1",
        "pack_id": pack.pack_id,
        "status": "phase_applied",
        "phase": phase,
        "target": pack.target.to_dict(),
        "desired_fingerprint": pack.desired_fingerprint,
        "actual_fingerprint": pack.actual_fingerprint,
        "environment": "prod",
        "blockers": [],
        "warnings": [],
    }
    return {"schema_version": "dpone.schema_migration_ledger.v1", "records": [record]}


def _remediation_manifest() -> dict[str, object]:
    return {
        "sink": {
            "options": {
                "physical_design": {
                    "migration": {
                        "remediation": {
                            "enabled": True,
                            "mode": "gate",
                            "profile": "prod_strict",
                            "rollback": {"strategy": "controlled", "allow_after_contract": False},
                        }
                    }
                }
            }
        }
    }


def _incremental_manifest() -> dict[str, object]:
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
                            "clickhouse": {
                                "destination": "Disk('backups', 'inc.zip')",
                                "incremental": {"enabled": True},
                            },
                        }
                    }
                }
            }
        }
    }


def _artifact(kind: str, path: str, payload: dict[str, object], *, required: bool) -> MigrationEvidenceArtifact:
    content = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return MigrationEvidenceArtifact.from_bytes(kind=kind, path=path, content=content, required=required)


def stable_suffix(value: str) -> str:
    return (value.encode("utf-8").hex()[:64]).ljust(64, "0")


def _validate_schema(schema_name: str, payload: dict[str, object]) -> None:
    schema = json.loads(Path("docs/schemas/schema-migration", schema_name).read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate(payload)
