from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
)
from dpone.readiness.schema_migration_evidence_registry import (
    MigrationEvidenceAuditReporter,
    MigrationEvidenceRecorder,
)
from dpone.readiness.schema_migration_remediation import (
    MigrationRemediationCertifier,
    MigrationRemediationPlanner,
    MigrationRemediationRunner,
)


def test_remediation_plan_builds_online_safe_rollback_from_watch_certificate() -> None:
    pack = _setting_pack()

    plan = MigrationRemediationPlanner().plan(
        pack=pack.to_dict(command="plan"),
        watch_certificate=_watch_certificate(pack, decision="rollback_required", status="blocked"),
        ledger=_ledger(pack, status="applied"),
        manifest=_manifest(),
        target_connection={"type": "clickhouse", "environment": "prod"},
        environment="prod",
    )

    assert plan["schema_version"] == "dpone.schema_migration_remediation_plan.v1"
    assert plan["status"] == "planned"
    assert plan["capability"] == "online_safe"
    assert plan["operations"] == [
        {
            "name": "rollback_1",
            "operation_type": "sql",
            "sql": "ALTER TABLE `analytics`.`orders` RESET SETTING index_granularity",
        }
    ]
    assert plan["preconditions"]["approval_required"] is True
    assert plan["remediation_plan_id"].startswith("sha256:")


def test_remediation_plan_blocks_mismatch_unjustified_watch_and_after_contract() -> None:
    pack = _shadow_pack()

    plan = MigrationRemediationPlanner().plan(
        pack=pack.to_dict(command="plan"),
        watch_certificate={**_watch_certificate(pack, decision="continue", status="stable"), "pack_id": "sha256:bad"},
        ledger=_ledger(pack, status="phase_applied", phase="contract"),
        manifest=_manifest(),
        target_connection={"type": "clickhouse", "environment": "prod"},
        environment="prod",
    )

    assert plan["status"] == "blocked"
    assert "schema_migration_remediation.watch_pack_id_mismatch" in plan["blockers"]
    assert "schema_migration_remediation.watch_does_not_require_remediation" in plan["blockers"]
    assert "schema_migration_remediation.blocked_after_contract" in plan["blockers"]


def test_remediation_plan_classifies_shadow_exchange_before_contract() -> None:
    pack = _shadow_pack()

    plan = MigrationRemediationPlanner().plan(
        pack=pack.to_dict(command="plan"),
        watch_certificate=_watch_certificate(pack, decision="rollback_required", status="blocked"),
        ledger=_ledger(pack, status="phase_applied", phase="cutover"),
        manifest=_manifest(),
        target_connection={"type": "clickhouse", "environment": "prod"},
        environment="prod",
    )

    assert plan["status"] == "planned"
    assert plan["capability"] == "shadow_exchange"
    assert (
        plan["operations"][0]["sql"]
        == "EXCHANGE TABLES `analytics`.`orders` AND `analytics`.`__dpone_shadow_orders_pack`"
    )


def test_remediation_runner_requires_execute_approval_and_records_operation_evidence() -> None:
    pack = _setting_pack()
    plan = MigrationRemediationPlanner().plan(
        pack=pack.to_dict(command="plan"),
        watch_certificate=_watch_certificate(pack, decision="rollback_required", status="blocked"),
        ledger=_ledger(pack, status="applied"),
        manifest=_manifest(),
        target_connection={"type": "clickhouse", "environment": "prod"},
        environment="prod",
    )

    dry_run = MigrationRemediationRunner().run(plan=plan, executor=_OkExecutor(), execute=False, approval=None)
    missing_approval = MigrationRemediationRunner().run(plan=plan, executor=_OkExecutor(), execute=True, approval=None)
    applied = MigrationRemediationRunner().run(
        plan=plan, executor=_OkExecutor(), execute=True, approval=_approval(pack)
    )

    assert dry_run["status"] == "dry_run"
    assert "schema_migration_remediation.not_executed" in dry_run["warnings"]
    assert missing_approval["status"] == "blocked"
    assert "schema_migration_remediation.approval_required" in missing_approval["blockers"]
    assert applied["status"] == "remediated"
    assert applied["ledger_status"] == "rolled_back"
    assert applied["operations"][0]["status"] == "executed"
    assert applied["remediation_run_id"].startswith("sha256:")


def test_remediation_certifier_and_evidence_integrations() -> None:
    pack = _setting_pack()
    plan = MigrationRemediationPlanner().plan(
        pack=pack.to_dict(command="plan"),
        watch_certificate=_watch_certificate(pack, decision="rollback_required", status="blocked"),
        ledger=_ledger(pack, status="applied"),
        manifest=_manifest(),
        target_connection={"type": "clickhouse", "environment": "prod"},
        environment="prod",
    )
    run = MigrationRemediationRunner().run(plan=plan, executor=_OkExecutor(), execute=True, approval=_approval(pack))
    certificate = MigrationRemediationCertifier().certify(run=run, profile="prod_strict")
    artifacts = (
        _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
        _artifact("remediation_certificate", "remediation.json", certificate, required=False),
    )

    bundle = MigrationBundleBuilder().build(artifacts=artifacts, attest=True)
    gate = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=MigrationBundlePolicyOptions.resolve(
            profile="prod_strict",
            policy_payload={"required_artifacts": ["migration_pack", "remediation_certificate"]},
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
        stage="remediated",
        remediation_certificate=certificate,
        actor="ci",
    )
    report = MigrationEvidenceAuditReporter().build(
        records=(record,),
        target="clickhouse.analytics.orders",
        date_from="2026-06-22",
        date_to="2026-06-22",
    )

    assert certificate["schema_version"] == "dpone.schema_migration_remediation_certificate.v1"
    assert certificate["status"] == "certified"
    assert bundle["summary"]["remediation_certificate_id"] == certificate["certificate_id"]
    assert gate["status"] == "allowed"
    assert record["stage"] == "remediated"
    assert "remediation_certificate" in {item["kind"] for item in record["artifact_refs"]}
    assert "missing_rollback_certified" in report["warnings"]


def test_remediation_public_json_schemas_validate_example_artifacts() -> None:
    pack = _setting_pack()
    plan = MigrationRemediationPlanner().plan(
        pack=pack.to_dict(command="plan"),
        watch_certificate=_watch_certificate(pack, decision="rollback_required", status="blocked"),
        ledger=_ledger(pack, status="applied"),
        manifest=_manifest(),
        target_connection={"type": "clickhouse", "environment": "prod"},
        environment="prod",
    )
    run = MigrationRemediationRunner().run(plan=plan, executor=_OkExecutor(), execute=True, approval=_approval(pack))
    certificate = MigrationRemediationCertifier().certify(run=run, profile="prod_strict")

    _validate_schema("remediation-plan.schema.json", plan)
    _validate_schema("remediation-run.schema.json", run)
    _validate_schema("remediation-certificate.schema.json", certificate)


class _OkExecutor:
    def execute(self, operation: dict[str, object]) -> dict[str, object]:
        return {"status": "executed", "sql": operation.get("sql")}


def _setting_pack() -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={
            "sink_type": "clickhouse",
            "table": "analytics.orders",
            "engine": "MergeTree",
            "order_by": ["id"],
            "table_settings": {"index_granularity": 8192},
        },
        actual={
            "sink_type": "clickhouse",
            "table": "analytics.orders",
            "engine": "MergeTree",
            "order_by": ["id"],
            "table_settings": {},
        },
        changes=({"change_type": "table_setting", "path": "table_settings.index_granularity"},),
        ddl=("ALTER TABLE `analytics`.`orders` MODIFY SETTING index_granularity = 8192",),
        strategy="online_safe",
        rollback={"supported": True, "ddl": ["ALTER TABLE `analytics`.`orders` RESET SETTING index_granularity"]},
    )


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


def _manifest() -> dict[str, object]:
    return {
        "sink": {
            "options": {
                "physical_design": {
                    "migration": {
                        "remediation": {
                            "enabled": True,
                            "mode": "gate",
                            "profile": "prod_strict",
                            "execution": {
                                "require_approval": True,
                                "require_watch_certificate": True,
                                "require_target_fingerprint": True,
                                "require_lock": True,
                            },
                            "rollback": {
                                "strategy": "controlled",
                                "allow_after_contract": False,
                                "retain_backup_required": True,
                            },
                            "certify": {
                                "physical_design": True,
                                "canary_queries": True,
                                "row_count": True,
                                "rollback_window": True,
                            },
                        }
                    }
                }
            }
        }
    }


def _watch_certificate(pack: MigrationPack, *, decision: str, status: str) -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_migration_watch_certificate.v1",
        "certificate_id": "sha256:" + "9" * 64,
        "pack_id": pack.pack_id,
        "post_apply_certificate_id": "sha256:" + "8" * 64,
        "environment": "prod",
        "target": pack.target.to_dict(),
        "status": status,
        "profile": "prod_strict",
        "samples": {"planned": 2, "executed": 2, "passed": 0, "failed": 1},
        "remediation": {"decision": decision, "commands": []},
        "checks": [],
        "blockers": ["schema_migration_watch.physical_drift"] if decision != "continue" else [],
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
    return {
        "pack_id": pack.pack_id,
        "approved_by": "data-platform-owner",
        "approved_risks": ["controlled_rollback"],
    }


def _artifact(kind: str, path: str, payload: dict[str, object], *, required: bool) -> MigrationEvidenceArtifact:
    content = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return MigrationEvidenceArtifact.from_bytes(kind=kind, path=path, content=content, required=required)


def _validate_schema(schema_name: str, payload: dict[str, object]) -> None:
    schema = json.loads(Path("docs/schemas/schema-migration", schema_name).read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate(payload)
