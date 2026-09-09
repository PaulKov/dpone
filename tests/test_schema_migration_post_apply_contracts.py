from __future__ import annotations

import json

from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.physical_state import PhysicalTableState
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
)
from dpone.readiness.schema_migration_evidence_registry import (
    MigrationEvidenceAuditReporter,
    MigrationEvidenceRecorder,
)
from dpone.readiness.schema_migration_post_apply import (
    PostApplyCertifier,
    PostApplyVerificationPlanner,
    PostApplyVerificationRunner,
)
from dpone.runtime.sinks.clickhouse_post_apply import ClickHousePostApplyVerifier


def test_post_apply_plan_binds_pack_bundle_ledger_and_canaries() -> None:
    pack = _pack()
    bundle = _bundle(pack)
    ledger = _ledger(pack, status="applied", environment="prod")
    manifest = _manifest()

    plan = PostApplyVerificationPlanner().plan(
        pack=pack.to_dict(command="plan"),
        bundle=bundle,
        ledger=ledger,
        manifest=manifest,
        target_connection={"type": "clickhouse", "environment": "prod"},
        environment="prod",
    )

    assert plan["schema_version"] == "dpone.schema_migration_post_apply_plan.v1"
    assert plan["status"] == "planned"
    assert plan["pack_id"] == pack.pack_id
    assert plan["bundle_id"] == bundle["bundle_id"]
    assert plan["target"] == {"sink_type": "clickhouse", "table": "analytics.orders"}
    assert plan["environment"] == "prod"
    assert [canary["id"] for canary in plan["canaries"]] == ["orders_count_positive"]
    assert plan["checks"]["ledger_state"] is True
    assert plan["post_apply_plan_id"].startswith("sha256:")


def test_post_apply_plan_blocks_mismatched_bundle_ledger_and_unsafe_canary() -> None:
    pack = _pack()
    bundle = {**_bundle(pack), "pack_id": "sha256:" + "0" * 64}
    ledger = _ledger(pack, status="baseline", environment="stage")
    manifest = _manifest(query="DROP TABLE analytics.orders")

    plan = PostApplyVerificationPlanner().plan(
        pack=pack.to_dict(command="plan"),
        bundle=bundle,
        ledger=ledger,
        manifest=manifest,
        target_connection={"type": "clickhouse", "environment": "prod"},
        environment="prod",
    )

    assert plan["status"] == "blocked"
    assert "schema_migration_post_apply.bundle_pack_id_mismatch" in plan["blockers"]
    assert "schema_migration_post_apply.ledger_applied_record_missing" in plan["blockers"]
    assert "schema_migration_post_apply.ledger_environment_mismatch" in plan["blockers"]
    assert "schema_migration_post_apply.unsafe_canary_sql:orders_count_positive" in plan["blockers"]


def test_post_apply_runner_blocks_physical_drift_and_failed_critical_canary() -> None:
    pack = _pack()
    plan = PostApplyVerificationPlanner().plan(
        pack=pack.to_dict(command="plan"),
        bundle=None,
        ledger=_ledger(pack, status="applied", environment="prod"),
        manifest=_manifest(),
        target_connection={"type": "clickhouse", "environment": "prod"},
        environment="prod",
    )

    run = PostApplyVerificationRunner().run(
        plan=plan,
        inspector=_Inspector(order_by=()),
        canary_executor=_CanaryExecutor(rows=[{"ok": 0}]),
        execute=True,
    )
    certificate = PostApplyCertifier().certify(run=run, profile="prod_strict")

    assert run["schema_version"] == "dpone.schema_migration_post_apply_run.v1"
    assert run["status"] == "blocked"
    assert "schema_migration_post_apply.physical_drift" in run["blockers"]
    assert "schema_migration_post_apply.canary_failed:orders_count_positive" in run["blockers"]
    assert certificate["schema_version"] == "dpone.schema_migration_post_apply_certificate.v1"
    assert certificate["status"] == "blocked"
    assert "schema_migration_post_apply.run_blocked" in certificate["blockers"]


def test_post_apply_runner_and_certifier_verify_matching_target() -> None:
    pack = _pack(rollback={"supported": True, "supported_until_phase": "contract", "ddl": ["EXCHANGE TABLES a AND b"]})
    plan = PostApplyVerificationPlanner().plan(
        pack=pack.to_dict(command="plan"),
        bundle=None,
        ledger=_ledger(pack, status="applied", environment="prod", phase="cutover"),
        manifest=_manifest(),
        target_connection={"type": "clickhouse", "environment": "prod"},
        environment="prod",
    )

    run = PostApplyVerificationRunner().run(
        plan=plan,
        inspector=_Inspector(order_by=("id",)),
        canary_executor=_CanaryExecutor(rows=[{"ok": 1}]),
        execute=True,
    )
    certificate = PostApplyCertifier().certify(run=run, profile="prod_strict")

    assert run["status"] == "passed"
    assert run["rollback_window"]["status"] == "open"
    assert certificate["status"] == "verified"
    assert certificate["certificate_id"].startswith("sha256:")


def test_clickhouse_post_apply_canary_adapter_returns_mapping_rows() -> None:
    connector = _ClickHouseCanaryConnector()

    rows = ClickHousePostApplyVerifier(connector).execute({"query": "SELECT 1 AS ok"})

    assert connector.as_dict is True
    assert rows == [{"ok": 1}]


def test_post_apply_certificate_can_be_bundled_required_and_recorded() -> None:
    pack = _pack()
    certificate = _certificate(pack, status="verified")
    artifacts = (
        _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
        _artifact("post_apply_certificate", "post-apply.json", certificate, required=False),
    )

    bundle = MigrationBundleBuilder().build(artifacts=artifacts, attest=True)
    decision = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=MigrationBundlePolicyOptions.resolve(
            profile="prod_strict",
            policy_payload={"required_artifacts": ["migration_pack", "post_apply_certificate"]},
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
        stage="verified",
        actor="ci",
    )
    report = MigrationEvidenceAuditReporter().build(
        records=(record,),
        target="clickhouse.analytics.orders",
        date_from="2026-06-22",
        date_to="2026-06-22",
    )

    assert bundle["summary"]["post_apply_certificate_id"] == certificate["certificate_id"]
    assert decision["status"] == "allowed"
    assert record["stage"] == "verified"
    assert "post_apply_certificate" in {item["kind"] for item in record["artifact_refs"]}
    assert "verified" in report["markdown"]


class _Inspector:
    def __init__(self, *, order_by: tuple[str, ...]) -> None:
        self._order_by = order_by

    def inspect(self, plan: dict[str, object]) -> PhysicalTableState:
        target = plan["target"]
        assert isinstance(target, dict)
        return PhysicalTableState(
            sink_type="clickhouse",
            table="analytics.orders",
            columns={},
            engine="MergeTree",
            order_by=self._order_by,
        )


class _CanaryExecutor:
    def __init__(self, *, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    def execute(self, canary: dict[str, object]) -> list[dict[str, object]]:
        assert canary["id"] == "orders_count_positive"
        return self._rows


class _ClickHouseCanaryConnector:
    def __init__(self) -> None:
        self.as_dict = False

    def get_records(self, query: str, as_dict: bool = False) -> list[dict[str, int]]:
        assert query == "SELECT 1 AS ok"
        self.as_dict = as_dict
        return [{"ok": 1}]


def _pack(*, rollback: dict[str, object] | None = None) -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={
            "sink_type": "clickhouse",
            "table": "analytics.orders",
            "engine": "MergeTree",
            "order_by": ["id"],
        },
        actual={
            "sink_type": "clickhouse",
            "table": "analytics.orders",
            "engine": "MergeTree",
            "order_by": ["id"],
        },
        changes=({"change_type": "table_setting", "path": "table_settings.index_granularity"},),
        ddl=("ALTER TABLE analytics.orders MODIFY SETTING index_granularity = 8192",),
        strategy="online_safe",
        rollback=rollback,
    )


def _manifest(*, query: str = "SELECT 1 AS ok") -> dict[str, object]:
    return {
        "sink": {
            "options": {
                "physical_design": {
                    "migration": {
                        "post_apply": {
                            "enabled": True,
                            "mode": "gate",
                            "profile": "prod_strict",
                            "verification": {
                                "ledger_state": True,
                                "physical_design": True,
                                "canary_queries": True,
                                "rollback_window": True,
                            },
                            "canaries": [
                                {
                                    "id": "orders_count_positive",
                                    "type": "sql",
                                    "owner": "data-platform",
                                    "severity": "critical",
                                    "query": query,
                                    "expect": {"column": "ok", "equals": 1},
                                }
                            ],
                        }
                    }
                }
            }
        }
    }


def _ledger(pack: MigrationPack, *, status: str, environment: str, phase: str | None = None) -> dict[str, object]:
    record: dict[str, object] = {
        "schema_version": "dpone.schema_migration_ledger_record.v1",
        "pack_id": pack.pack_id,
        "status": status,
        "target": pack.target.to_dict(),
        "desired_fingerprint": pack.desired_fingerprint,
        "actual_fingerprint": pack.actual_fingerprint,
        "environment": environment,
        "blockers": [],
        "warnings": [],
    }
    if phase:
        record["phase"] = phase
    return {"schema_version": "dpone.schema_migration_ledger.v1", "records": [record]}


def _bundle(pack: MigrationPack) -> dict[str, object]:
    return MigrationBundleBuilder().build(
        artifacts=(_artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),),
        attest=True,
    )


def _certificate(pack: MigrationPack, *, status: str) -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_migration_post_apply_certificate.v1",
        "certificate_id": "sha256:" + "6" * 64,
        "pack_id": pack.pack_id,
        "bundle_id": None,
        "environment": "prod",
        "target": pack.target.to_dict(),
        "status": status,
        "profile": "prod_strict",
        "checks": [],
        "blockers": [],
        "warnings": [],
        "metrics": {"duration_ms": 10, "checks_executed": 4, "canaries_executed": 1},
    }


def _artifact(kind: str, path: str, payload: dict[str, object], *, required: bool) -> MigrationEvidenceArtifact:
    content = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return MigrationEvidenceArtifact.from_bytes(kind=kind, path=path, content=content, required=required)
