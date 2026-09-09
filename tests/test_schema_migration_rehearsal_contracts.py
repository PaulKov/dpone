from __future__ import annotations

import json

from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
)
from dpone.readiness.schema_migration_rehearsal import (
    MigrationRehearsalCertifier,
    MigrationRehearsalPlanner,
    MigrationRehearsalRunner,
)
from dpone.services.schema_migration_execution import MigrationOperationExecutor


def test_rehearsal_plan_binds_pack_bundle_and_builds_flat_ddl_operations() -> None:
    pack = _pack()
    bundle = _bundle(pack)

    plan = MigrationRehearsalPlanner().plan(
        pack=pack.to_dict(command="plan"),
        bundle=bundle,
        environment="stage",
        target_connection={"type": "clickhouse", "environment": "stage", "database": "analytics"},
    )

    assert plan["schema_version"] == "dpone.schema_migration_rehearsal_plan.v1"
    assert plan["status"] == "planned"
    assert plan["pack_id"] == pack.pack_id
    assert plan["bundle_id"] == bundle["bundle_id"]
    assert plan["target"] == {"sink_type": "clickhouse", "table": "analytics.orders"}
    assert [operation["name"] for operation in plan["operations"]] == ["ddl_1"]
    assert plan["checks"][0]["name"] == "bundle_binding"
    assert plan["rehearsal_plan_id"].startswith("sha256:")


def test_rehearsal_plan_blocks_mismatched_bundle_pack_blockers_and_prod_target() -> None:
    pack = _pack(blockers=("physical_design.shadow_required:order_by",))
    bundle = {**_bundle(_pack()), "pack_id": "sha256:" + "0" * 64}

    plan = MigrationRehearsalPlanner().plan(
        pack=pack.to_dict(command="plan"),
        bundle=bundle,
        environment="prod",
        target_connection={"type": "clickhouse", "environment": "prod"},
    )

    assert plan["status"] == "blocked"
    assert "schema_migration_rehearsal.bundle_pack_id_mismatch" in plan["blockers"]
    assert "schema_migration_rehearsal.pack_blocked" in plan["blockers"]
    assert "schema_migration_rehearsal.prod_environment_blocked" in plan["blockers"]


def test_rehearsal_runner_executes_operations_and_certifier_blocks_validation_mismatch() -> None:
    pack = _pack(phases=(_phase_with_mismatch(),))
    plan = MigrationRehearsalPlanner().plan(
        pack=pack.to_dict(command="plan"),
        bundle=None,
        environment="stage",
        target_connection={"type": "clickhouse", "environment": "stage"},
    )

    run = MigrationRehearsalRunner().run(plan=plan, executor=_RowsExecutor(), execute=True)
    certificate = MigrationRehearsalCertifier().certify(run=run, profile="stage")

    assert run["schema_version"] == "dpone.schema_migration_rehearsal_run.v1"
    assert run["status"] == "blocked"
    assert "migration.validation_mismatch:source_count:target_count" in run["blockers"]
    assert certificate["schema_version"] == "dpone.schema_migration_rehearsal_certificate.v1"
    assert certificate["status"] == "blocked"
    assert "schema_migration_rehearsal.run_blocked" in certificate["blockers"]


def test_rehearsal_plan_defers_contract_phase_when_rollback_supported_until_contract() -> None:
    pack = _pack(phases=_shadow_phases(), rollback_supported_until_contract=True)

    plan = MigrationRehearsalPlanner().plan(
        pack=pack.to_dict(command="plan"),
        bundle=None,
        environment="stage",
        target_connection={"type": "clickhouse", "environment": "stage"},
    )

    assert plan["deferred_phases"] == ["contract"]
    assert [operation["phase"] for operation in plan["operations"]] == ["create_shadow", "cutover"]
    assert "drop_retained_old_table" not in {operation["name"] for operation in plan["operations"]}
    assert "schema_migration_rehearsal.contract_deferred_for_rollback_check" in plan["warnings"]
    assert plan["rollback_operations"] == [
        {
            "name": "rollback_1",
            "operation_type": "rollback",
            "sql": "EXCHANGE TABLES analytics.orders AND analytics.__dpone_shadow_orders",
        }
    ]


def test_stage_certificate_blocks_dry_run_rehearsal() -> None:
    pack = _pack()
    plan = MigrationRehearsalPlanner().plan(
        pack=pack.to_dict(command="plan"),
        bundle=None,
        environment="stage",
        target_connection={"type": "clickhouse", "environment": "stage"},
    )

    run = MigrationRehearsalRunner().run(plan=plan, executor=None, execute=False)
    certificate = MigrationRehearsalCertifier().certify(run=run, profile="stage")
    advisory = MigrationRehearsalCertifier().certify(run=run, profile="advisory")

    assert certificate["status"] == "blocked"
    assert "schema_migration_rehearsal.execution_required" in certificate["blockers"]
    assert advisory["status"] == "warning"
    assert "schema_migration_rehearsal.dry_run_certificate" in advisory["warnings"]


def test_rehearsal_certified_certificate_can_be_bundled_and_required_by_policy() -> None:
    pack = _pack()
    plan = MigrationRehearsalPlanner().plan(
        pack=pack.to_dict(command="plan"),
        bundle=None,
        environment="stage",
        target_connection={"type": "clickhouse", "environment": "stage"},
    )
    run = MigrationRehearsalRunner().run(plan=plan, executor=_OkExecutor(), execute=True)
    certificate = MigrationRehearsalCertifier().certify(run=run, profile="prod_strict")

    artifacts = (
        _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
        _artifact("rehearsal_certificate", "rehearsal.json", certificate, required=False),
    )
    bundle = MigrationBundleBuilder().build(artifacts=artifacts, attest=True)
    decision = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=MigrationBundlePolicyOptions.resolve(
            profile="prod_strict",
            policy_payload={"required_artifacts": ["migration_pack", "rehearsal_certificate"]},
        ),
        artifact_payloads={artifact.kind: artifact.payload for artifact in artifacts},
    )

    assert certificate["status"] == "certified"
    assert bundle["status"] == "ready"
    assert bundle["summary"]["rehearsal_certificate_id"] == certificate["certificate_id"]
    assert decision["status"] == "allowed"


class _OkExecutor(MigrationOperationExecutor):
    def execute(self, operation: dict[str, object]) -> dict[str, object]:
        return {"status": "executed", "result_rows": []}


class _RowsExecutor(MigrationOperationExecutor):
    def execute(self, operation: dict[str, object]) -> dict[str, object]:
        if operation.get("name") == "source_count":
            return {"status": "executed", "result_rows": [{"count": 2}]}
        if operation.get("name") == "target_count":
            return {"status": "executed", "result_rows": [{"count": 1}]}
        return {"status": "executed", "result_rows": []}


def _pack(
    *,
    blockers: tuple[str, ...] = (),
    phases: tuple[dict[str, object], ...] = (),
    rollback_supported_until_contract: bool = False,
) -> MigrationPack:
    rollback = {
        "supported": bool(phases),
        "ddl": ["ALTER TABLE analytics.orders MODIFY SETTING index_granularity = 8192"],
    }
    if rollback_supported_until_contract:
        rollback = {
            "supported": True,
            "supported_until_phase": "contract",
            "ddl": ["EXCHANGE TABLES analytics.orders AND analytics.__dpone_shadow_orders"],
        }
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders", "order_by": ["id"]},
        actual={"sink_type": "clickhouse", "table": "analytics.orders", "order_by": ["id"]},
        changes=({"change_type": "table_setting", "path": "table_settings.index_granularity"},),
        blockers=blockers,
        ddl=("ALTER TABLE analytics.orders MODIFY SETTING index_granularity = 8192",),
        strategy="online_safe",
        phases=phases,
        rollback=rollback,
    )


def _phase_with_mismatch() -> dict[str, object]:
    return {
        "name": "validate",
        "operations": [{"name": "noop", "operation_type": "sql", "sql": "SELECT 1"}],
        "validations": [
            {"name": "source_count", "operation_type": "validation", "sql": "SELECT 2 AS count"},
            {"name": "target_count", "operation_type": "validation", "sql": "SELECT 1 AS count"},
        ],
    }


def _shadow_phases() -> tuple[dict[str, object], ...]:
    return (
        {
            "name": "create_shadow",
            "operations": [
                {
                    "name": "create_shadow_table",
                    "operation_type": "sql",
                    "sql": "CREATE TABLE analytics.__dpone_shadow_orders (id Int64) ENGINE = MergeTree ORDER BY id",
                }
            ],
        },
        {
            "name": "cutover",
            "operations": [
                {
                    "name": "exchange_tables",
                    "operation_type": "sql",
                    "sql": "EXCHANGE TABLES analytics.orders AND analytics.__dpone_shadow_orders",
                }
            ],
        },
        {
            "name": "contract",
            "operations": [
                {
                    "name": "drop_retained_old_table",
                    "operation_type": "sql",
                    "sql": "DROP TABLE analytics.__dpone_shadow_orders",
                }
            ],
        },
    )


def _bundle(pack: MigrationPack) -> dict[str, object]:
    return MigrationBundleBuilder().build(
        artifacts=(_artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),),
        attest=True,
    )


def _artifact(kind: str, path: str, payload: dict[str, object], *, required: bool) -> MigrationEvidenceArtifact:
    content = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return MigrationEvidenceArtifact.from_bytes(kind=kind, path=path, content=content, required=required)
