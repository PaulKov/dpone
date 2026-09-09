from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

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
from dpone.readiness.schema_migration_watch import (
    MigrationWatchCertifier,
    MigrationWatchPlanner,
    MigrationWatchRemediationAdvisor,
    MigrationWatchRunner,
)
from dpone.runtime.sinks.clickhouse_watch import ClickHouseWatchProbe


def test_watch_plan_binds_pack_post_apply_certificate_window_and_canaries() -> None:
    pack = _pack()

    plan = MigrationWatchPlanner().plan(
        pack=pack.to_dict(command="plan"),
        post_apply_certificate=_post_apply_certificate(pack),
        manifest=_manifest(),
        target_connection={"type": "clickhouse", "environment": "prod"},
        environment="prod",
    )

    assert plan["schema_version"] == "dpone.schema_migration_watch_plan.v1"
    assert plan["status"] == "planned"
    assert plan["pack_id"] == pack.pack_id
    assert plan["post_apply_certificate_id"] == "sha256:" + "6" * 64
    assert plan["samples"]["planned"] == 2
    assert plan["window"] == {"duration_seconds": 0, "interval_seconds": 0, "min_successful_samples": 2}
    assert plan["checks"]["post_apply_recheck"] is True
    assert plan["checks"]["query_health"] is True
    assert [canary["id"] for canary in plan["canaries"]] == ["orders_count_positive"]
    assert plan["watch_plan_id"].startswith("sha256:")


def test_watch_plan_blocks_mismatched_or_unverified_post_apply_certificate_and_unsafe_canary() -> None:
    pack = _pack()
    certificate = {
        **_post_apply_certificate(pack, status="blocked"),
        "pack_id": "sha256:" + "0" * 64,
    }

    plan = MigrationWatchPlanner().plan(
        pack=pack.to_dict(command="plan"),
        post_apply_certificate=certificate,
        manifest=_manifest(query="DROP TABLE analytics.orders"),
        target_connection={"type": "clickhouse", "environment": "prod"},
        environment="prod",
    )

    assert plan["status"] == "blocked"
    assert "schema_migration_watch.post_apply_pack_id_mismatch" in plan["blockers"]
    assert "schema_migration_watch.post_apply_not_verified" in plan["blockers"]
    assert "schema_migration_watch.unsafe_canary_sql:orders_count_positive" in plan["blockers"]


def test_watch_runner_aggregates_samples_and_certifies_stable_release() -> None:
    pack = _pack(rollback={"supported": True, "supported_until_phase": "contract", "ddl": ["EXCHANGE TABLES a AND b"]})
    plan = MigrationWatchPlanner().plan(
        pack=pack.to_dict(command="plan"),
        post_apply_certificate=_post_apply_certificate(pack),
        manifest=_manifest(),
        target_connection={"type": "clickhouse", "environment": "prod"},
        environment="prod",
    )

    run = MigrationWatchRunner().run(plan=plan, probe=_Probe(order_by=("id",), canary_ok=True), execute=True)
    certificate = MigrationWatchCertifier().certify(run=run, profile="prod_strict")

    assert run["schema_version"] == "dpone.schema_migration_watch_run.v1"
    assert run["status"] == "passed"
    assert run["samples"]["executed"] == 2
    assert run["samples"]["passed"] == 2
    assert run["remediation"]["decision"] == "continue"
    assert certificate["schema_version"] == "dpone.schema_migration_watch_certificate.v1"
    assert certificate["status"] == "stable"
    assert certificate["certificate_id"].startswith("sha256:")


def test_watch_runner_failing_critical_canary_recommends_rollback_required() -> None:
    pack = _pack()
    plan = MigrationWatchPlanner().plan(
        pack=pack.to_dict(command="plan"),
        post_apply_certificate=_post_apply_certificate(pack),
        manifest=_manifest(),
        target_connection={"type": "clickhouse", "environment": "prod"},
        environment="prod",
    )

    run = MigrationWatchRunner().run(plan=plan, probe=_Probe(order_by=("id",), canary_ok=False), execute=True)
    certificate = MigrationWatchCertifier().certify(run=run, profile="prod_strict")

    assert run["status"] == "blocked"
    assert "schema_migration_watch.canary_failed:orders_count_positive" in run["blockers"]
    assert run["remediation"]["decision"] == "rollback_required"
    assert "dpone schema migration rollback --pack-id" in run["remediation"]["commands"][0]
    assert certificate["status"] == "blocked"


def test_remediation_advisor_maps_warning_and_query_health_failures() -> None:
    advisor = MigrationWatchRemediationAdvisor()

    assert advisor.decide(blockers=[], warnings=[], rollback_on=[]).decision == "continue"
    assert advisor.decide(
        blockers=[], warnings=["schema_migration_watch.query_log_unavailable"], rollback_on=[]
    ).decision == ("extend_watch")
    assert (
        advisor.decide(
            blockers=["schema_migration_watch.query_health_blocked"],
            warnings=[],
            rollback_on=["query_health_blocked"],
        ).decision
        == "rollback_required"
    )


def test_clickhouse_watch_probe_query_health_budget_and_unavailable_query_log() -> None:
    pack = _pack()
    plan = MigrationWatchPlanner().plan(
        pack=pack.to_dict(command="plan"),
        post_apply_certificate=_post_apply_certificate(pack),
        manifest=_manifest(),
        target_connection={"type": "clickhouse", "environment": "prod"},
        environment="prod",
    )

    breached = ClickHouseWatchProbe(_PostApplyForHealth(_HealthConnector({"p95_ms": 6000}))).query_health(plan)
    unavailable = ClickHouseWatchProbe(_PostApplyForHealth(_UnavailableHealthConnector())).query_health(plan)

    assert breached["checks"][0]["status"] == "failed"
    assert "schema_migration_watch.query_health_blocked:p95_ms" in breached["blockers"]
    assert unavailable["checks"][0]["status"] == "failed"
    assert "schema_migration_watch.query_log_unavailable" in unavailable["blockers"]


def test_watch_certificate_can_be_bundled_required_and_recorded() -> None:
    pack = _pack()
    certificate = _watch_certificate(pack, status="stable")
    artifacts = (
        _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
        _artifact("watch_certificate", "watch.json", certificate, required=False),
    )

    bundle = MigrationBundleBuilder().build(artifacts=artifacts, attest=True)
    decision = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=MigrationBundlePolicyOptions.resolve(
            profile="prod_strict",
            policy_payload={"required_artifacts": ["migration_pack", "watch_certificate"]},
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
        stage="watched",
        watch_certificate=certificate,
        actor="ci",
    )
    report = MigrationEvidenceAuditReporter().build(
        records=(record,),
        target="clickhouse.analytics.orders",
        date_from="2026-06-22",
        date_to="2026-06-22",
    )

    assert bundle["summary"]["watch_certificate_id"] == certificate["certificate_id"]
    assert decision["status"] == "allowed"
    assert record["stage"] == "watched"
    assert "watch_certificate" in {item["kind"] for item in record["artifact_refs"]}
    assert "missing_closed" in report["warnings"]
    assert "watched" in report["markdown"]


def test_watch_public_json_schemas_validate_example_artifacts() -> None:
    pack = _pack()
    plan = MigrationWatchPlanner().plan(
        pack=pack.to_dict(command="plan"),
        post_apply_certificate=_post_apply_certificate(pack),
        manifest=_manifest(),
        target_connection={"type": "clickhouse", "environment": "prod"},
        environment="prod",
    )
    run = MigrationWatchRunner().run(plan=plan, probe=_Probe(order_by=("id",), canary_ok=True), execute=True)
    certificate = MigrationWatchCertifier().certify(run=run, profile="prod_strict")

    _validate_schema("watch-plan.schema.json", plan)
    _validate_schema("watch-run.schema.json", run)
    _validate_schema("watch-certificate.schema.json", certificate)


class _Probe:
    def __init__(self, *, order_by: tuple[str, ...], canary_ok: bool) -> None:
        self._order_by = order_by
        self._canary_ok = canary_ok

    def inspect(self, plan: dict[str, object]) -> PhysicalTableState:
        return PhysicalTableState(
            sink_type="clickhouse",
            table="analytics.orders",
            columns={},
            engine="MergeTree",
            order_by=self._order_by,
        )

    def profile(self, plan: dict[str, object]) -> dict[str, object]:
        return {"checks": [{"name": "row_count", "status": "passed", "value": 10}], "blockers": [], "warnings": []}

    def execute(self, canary: dict[str, object]) -> list[dict[str, int]]:
        assert canary["id"] == "orders_count_positive"
        return [{"ok": 1 if self._canary_ok else 0}]

    def query_health(self, plan: dict[str, object]) -> dict[str, object]:
        return {"checks": [{"name": "query_health", "status": "passed"}], "blockers": [], "warnings": []}


class _PostApplyForHealth:
    def __init__(self, connector: object) -> None:
        self.connector = connector


class _HealthConnector:
    def __init__(self, metrics: dict[str, int]) -> None:
        self.metrics = metrics

    def get_records(self, query: str, *, as_dict: bool = False) -> list[dict[str, int]]:
        assert "system.query_log" in query
        assert as_dict is True
        return [
            {
                "query_count": self.metrics.get("query_count", 1),
                "error_count": self.metrics.get("error_count", 0),
                "p95_ms": self.metrics.get("p95_ms", 0),
                "max_read_rows": self.metrics.get("max_read_rows", 0),
                "max_memory_usage": self.metrics.get("max_memory_usage", 0),
            }
        ]


class _UnavailableHealthConnector:
    def get_records(self, query: str, *, as_dict: bool = False) -> list[dict[str, int]]:
        raise RuntimeError("query_log disabled")


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
                        "watch": {
                            "enabled": True,
                            "mode": "gate",
                            "profile": "prod_strict",
                            "window": {
                                "duration": "0s",
                                "interval": "0s",
                                "min_successful_samples": 2,
                                "max_failed_samples": 0,
                            },
                            "verification": {
                                "post_apply_recheck": True,
                                "physical_design": True,
                                "row_count": True,
                                "canary_queries": True,
                                "query_health": True,
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
                            "query_health": {
                                "lookback": "15m",
                                "max_error_count": 0,
                                "max_p95_ms": 5000,
                                "max_read_rows": 100000000,
                            },
                            "remediation": {
                                "mode": "recommend",
                                "rollback_on": ["critical_canary_failure", "physical_drift", "query_health_blocked"],
                            },
                        }
                    }
                }
            }
        }
    }


def _post_apply_certificate(pack: MigrationPack, *, status: str = "verified") -> dict[str, object]:
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
        "rollback_window": {"status": "open", "supported_until_phase": "contract"},
        "blockers": [],
        "warnings": [],
        "metrics": {"row_count": 10},
    }


def _watch_certificate(pack: MigrationPack, *, status: str) -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_migration_watch_certificate.v1",
        "certificate_id": "sha256:" + "7" * 64,
        "pack_id": pack.pack_id,
        "post_apply_certificate_id": "sha256:" + "6" * 64,
        "environment": "prod",
        "target": pack.target.to_dict(),
        "status": status,
        "profile": "prod_strict",
        "samples": {"planned": 2, "executed": 2, "passed": 2, "failed": 0},
        "remediation": {"decision": "continue", "commands": []},
        "checks": [],
        "blockers": [],
        "warnings": [],
        "metrics": {},
    }


def _artifact(kind: str, path: str, payload: dict[str, object], *, required: bool) -> MigrationEvidenceArtifact:
    content = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return MigrationEvidenceArtifact.from_bytes(kind=kind, path=path, content=content, required=required)


def _validate_schema(schema_name: str, payload: dict[str, object]) -> None:
    schema = json.loads(Path("docs/schemas/schema-migration", schema_name).read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate(payload)
