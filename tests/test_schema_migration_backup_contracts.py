from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_migration_backup import (
    MigrationBackupCertifier,
    MigrationBackupPlanner,
    MigrationBackupRunner,
    MigrationRestorePlanner,
    MigrationRestoreRunner,
)
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
)
from dpone.readiness.schema_migration_evidence_registry import (
    MigrationEvidenceAuditReporter,
    MigrationEvidenceRecorder,
)
from dpone.readiness.schema_migration_remediation import MigrationRemediationPlanner
from dpone.runtime.sinks.clickhouse_backup import ClickHouseBackupDialect


def test_backup_plan_renders_clickhouse_native_backup_for_shadow_contract() -> None:
    pack = _shadow_pack()

    plan = MigrationBackupPlanner(dialect=ClickHouseBackupDialect()).plan(
        pack=pack.to_dict(command="plan"),
        manifest=_manifest(),
        target_connection={"type": "clickhouse", "environment": "prod"},
        environment="prod",
    )

    assert plan["schema_version"] == "dpone.schema_migration_backup_plan.v1"
    assert plan["status"] == "planned"
    assert plan["required"] is True
    assert plan["requirement_reasons"] == ["shadow_contract", "physical_layout_change"]
    assert plan["operations"][0]["sql"].startswith("BACKUP TABLE `analytics`.`orders` TO Disk('backups',")
    assert plan["backup_destination"].endswith("/analytics.orders.zip')")
    assert plan["preconditions"]["approval_required"] is True
    assert plan["backup_plan_id"].startswith("sha256:")


def test_backup_plan_disabled_noops_and_unsupported_target_blocks() -> None:
    disabled_pack = _shadow_pack()
    unsupported_pack = MigrationPack.build(
        target=MigrationTarget(sink_type="postgres", table="public.orders"),
        desired={"sink_type": "postgres", "table": "public.orders"},
        actual={"sink_type": "postgres", "table": "public.orders"},
        changes=({"change_type": "drop_column", "path": "columns.legacy"},),
        strategy="block",
    )

    disabled = MigrationBackupPlanner(dialect=ClickHouseBackupDialect()).plan(
        pack=disabled_pack.to_dict(command="plan"),
        manifest={},
        target_connection={"type": "clickhouse", "environment": "prod"},
        environment="prod",
    )
    blocked = MigrationBackupPlanner(dialect=ClickHouseBackupDialect()).plan(
        pack=unsupported_pack.to_dict(command="plan"),
        manifest=_manifest(),
        target_connection={"type": "postgres", "environment": "prod"},
        environment="prod",
    )

    assert disabled["status"] == "disabled"
    assert "schema_migration_backup.disabled" in disabled["warnings"]
    assert blocked["status"] == "blocked"
    assert "schema_migration_backup.unsupported_target:postgres" in blocked["blockers"]


def test_backup_runner_requires_execute_approval_and_records_evidence() -> None:
    pack = _shadow_pack()
    plan = _backup_plan(pack)

    dry_run = MigrationBackupRunner().run(plan=plan, executor=_OkExecutor(), execute=False, approval=None)
    missing_approval = MigrationBackupRunner().run(plan=plan, executor=_OkExecutor(), execute=True, approval=None)
    created = MigrationBackupRunner().run(
        plan=plan,
        executor=_OkExecutor(),
        execute=True,
        approval=_approval(pack),
    )

    assert dry_run["status"] == "dry_run"
    assert "schema_migration_backup.not_executed" in dry_run["warnings"]
    assert missing_approval["status"] == "blocked"
    assert "schema_migration_backup.approval_required" in missing_approval["blockers"]
    assert created["status"] == "backed_up"
    assert created["operations"][0]["status"] == "executed"
    assert created["remediation"]["capability"] == "restore_from_backup"
    assert created["backup_run_id"].startswith("sha256:")


def test_restore_planner_blocks_prod_and_restores_to_sandbox() -> None:
    pack = _shadow_pack()
    backup_run = MigrationBackupRunner().run(
        plan=_backup_plan(pack),
        executor=_OkExecutor(),
        execute=True,
        approval=_approval(pack),
    )

    prod = MigrationRestorePlanner(dialect=ClickHouseBackupDialect()).plan(
        backup_run=backup_run,
        target_connection={"type": "clickhouse", "environment": "prod"},
        environment="prod",
    )
    stage = MigrationRestorePlanner(dialect=ClickHouseBackupDialect()).plan(
        backup_run=backup_run,
        target_connection={"type": "clickhouse", "environment": "stage"},
        environment="stage",
    )
    restored = MigrationRestoreRunner().run(plan=stage, executor=_OkExecutor(), execute=True)

    assert prod["status"] == "blocked"
    assert "schema_migration_backup.restore_prod_blocked" in prod["blockers"]
    assert stage["status"] == "planned"
    assert "RESTORE TABLE `analytics`.`orders` AS `analytics`.`__dpone_restore_orders_" in stage["operations"][0]["sql"]
    assert restored["status"] == "restored"
    assert restored["restore_run_id"].startswith("sha256:")


def test_backup_certifier_bundle_registry_and_remediation_integration() -> None:
    pack = _shadow_pack()
    backup_run = MigrationBackupRunner().run(
        plan=_backup_plan(pack),
        executor=_OkExecutor(),
        execute=True,
        approval=_approval(pack),
    )
    restore_plan = MigrationRestorePlanner(dialect=ClickHouseBackupDialect()).plan(
        backup_run=backup_run,
        target_connection={"type": "clickhouse", "environment": "stage"},
        environment="stage",
    )
    restore_run = MigrationRestoreRunner().run(plan=restore_plan, executor=_OkExecutor(), execute=True)
    certificate = MigrationBackupCertifier().certify(
        backup_run=backup_run,
        restore_run=restore_run,
        profile="prod_strict",
    )
    artifacts = (
        _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
        _artifact("backup_certificate", "backup.json", certificate, required=False),
    )

    bundle = MigrationBundleBuilder().build(artifacts=artifacts, attest=True)
    gate = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=MigrationBundlePolicyOptions.resolve(
            profile="prod_strict",
            policy_payload={"required_artifacts": ["migration_pack", "backup_certificate"]},
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
        stage="backup_certified",
        backup_certificate=certificate,
        actor="ci",
    )
    remediation_plan = MigrationRemediationPlanner().plan(
        pack=pack.to_dict(command="plan"),
        watch_certificate=_watch_certificate(pack),
        ledger=_ledger(pack, status="phase_applied", phase="contract"),
        manifest=_remediation_manifest(),
        target_connection={"type": "clickhouse", "environment": "prod"},
        environment="prod",
        backup_certificate=certificate,
    )
    report = MigrationEvidenceAuditReporter().build(
        records=(record,),
        target="clickhouse.analytics.orders",
        date_from="2026-06-22",
        date_to="2026-06-22",
    )

    assert certificate["schema_version"] == "dpone.schema_migration_backup_certificate.v1"
    assert certificate["status"] == "certified"
    assert bundle["summary"]["backup_certificate_id"] == certificate["certificate_id"]
    assert gate["status"] == "allowed"
    assert record["stage"] == "backup_certified"
    assert "backup_certificate" in {item["kind"] for item in record["artifact_refs"]}
    assert remediation_plan["status"] == "planned"
    assert remediation_plan["capability"] == "restore_from_backup"
    assert "RESTORE TABLE" in remediation_plan["operations"][0]["sql"]
    assert "missing_backup_created" in report["warnings"]
    assert "missing_restore_rehearsed" in report["warnings"]


def test_clickhouse_backup_dialect_quotes_identifiers_and_rejects_unsafe_destination() -> None:
    dialect = ClickHouseBackupDialect()

    backup_sql = dialect.render_backup_table(
        table="analytics.orders",
        destination="Disk('backups', 'orders.zip')",
        settings={"compression_method": "lzma", "compression_level": 3},
    )
    restore_sql = dialect.render_restore_table(
        source_table="analytics.orders",
        restored_table="analytics.orders_restore",
        destination="Disk('backups', 'orders.zip')",
        settings={"allow_non_empty_tables": 0},
    )

    assert backup_sql == (
        "BACKUP TABLE `analytics`.`orders` TO Disk('backups', 'orders.zip') "
        "SETTINGS compression_level = 3, compression_method = 'lzma'"
    )
    assert restore_sql == (
        "RESTORE TABLE `analytics`.`orders` AS `analytics`.`orders_restore` "
        "FROM Disk('backups', 'orders.zip') SETTINGS allow_non_empty_tables = 0"
    )
    try:
        dialect.render_backup_table(table="analytics.orders", destination="DROP TABLE x", settings={})
    except ValueError as exc:
        assert "unsafe ClickHouse backup destination" in str(exc)
    else:  # pragma: no cover - assertion guard
        raise AssertionError("unsafe destination was accepted")


def test_backup_public_json_schemas_validate_example_artifacts() -> None:
    pack = _shadow_pack()
    backup_run = MigrationBackupRunner().run(
        plan=_backup_plan(pack),
        executor=_OkExecutor(),
        execute=True,
        approval=_approval(pack),
    )
    restore_plan = MigrationRestorePlanner(dialect=ClickHouseBackupDialect()).plan(
        backup_run=backup_run,
        target_connection={"type": "clickhouse", "environment": "stage"},
        environment="stage",
    )
    restore_run = MigrationRestoreRunner().run(plan=restore_plan, executor=_OkExecutor(), execute=True)
    certificate = MigrationBackupCertifier().certify(
        backup_run=backup_run,
        restore_run=restore_run,
        profile="prod_strict",
    )

    _validate_schema("backup-plan.schema.json", _backup_plan(pack))
    _validate_schema("backup-run.schema.json", backup_run)
    _validate_schema("restore-plan.schema.json", restore_plan)
    _validate_schema("restore-run.schema.json", restore_run)
    _validate_schema("backup-certificate.schema.json", certificate)


class _OkExecutor:
    def execute(self, operation: dict[str, object]) -> dict[str, object]:
        return {"status": "executed", "sql": operation.get("sql")}


def _shadow_pack() -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders", "engine": "MergeTree", "order_by": ["id"]},
        actual={"sink_type": "clickhouse", "table": "analytics.orders", "engine": "MergeTree", "order_by": []},
        changes=({"change_type": "order_by", "path": "order_by"},),
        strategy="shadow",
        phases=(
            {"name": "create_shadow", "operations": [{"name": "create_shadow", "sql": "CREATE TABLE shadow"}]},
            {"name": "cutover", "operations": [{"name": "cutover", "sql": "EXCHANGE TABLES a AND b"}]},
            {"name": "contract", "operations": [{"name": "contract", "sql": "DROP TABLE old"}]},
        ),
        rollback={
            "supported": True,
            "supported_until_phase": "contract",
            "ddl": ["EXCHANGE TABLES `analytics`.`orders` AND `analytics`.`__dpone_shadow_orders_pack`"],
        },
    )


def _backup_plan(pack: MigrationPack) -> dict[str, object]:
    return MigrationBackupPlanner(dialect=ClickHouseBackupDialect()).plan(
        pack=pack.to_dict(command="plan"),
        manifest=_manifest(),
        target_connection={"type": "clickhouse", "environment": "prod"},
        environment="prod",
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
                            "require_for": [
                                "data_destructive",
                                "shadow_contract",
                                "physical_layout_change",
                                "direct_rename",
                            ],
                            "retention": {"min_days": 7, "delete_after": "30d"},
                            "restore_rehearsal": {
                                "enabled": True,
                                "environment": "stage",
                                "require_clean_target": True,
                            },
                            "clickhouse": {
                                "destination": "Disk('backups', 'dpone/{pack_id}/{table}.zip')",
                                "incremental": False,
                            },
                        }
                    }
                }
            }
        }
    }


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


def _ledger(pack: MigrationPack, *, status: str, phase: str | None = None) -> dict[str, object]:
    record: dict[str, object] = {
        "schema_version": "dpone.schema_migration_ledger_record.v1",
        "pack_id": pack.pack_id,
        "status": status,
        "target": pack.target.to_dict(),
        "desired_fingerprint": pack.desired_fingerprint,
        "actual_fingerprint": pack.actual_fingerprint,
        "environment": "prod",
        "blockers": [],
        "warnings": [],
    }
    if phase:
        record["phase"] = phase
    return {"schema_version": "dpone.schema_migration_ledger.v1", "records": [record]}


def _approval(pack: MigrationPack) -> dict[str, object]:
    return {"pack_id": pack.pack_id, "approved_by": "owner", "approved_risks": ["backup"]}


def _artifact(kind: str, path: str, payload: dict[str, object], *, required: bool) -> MigrationEvidenceArtifact:
    content = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return MigrationEvidenceArtifact.from_bytes(kind=kind, path=path, content=content, required=required)


def _validate_schema(schema_name: str, payload: dict[str, object]) -> None:
    schema = json.loads(Path("docs/schemas/schema-migration", schema_name).read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate(payload)
