from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

from dpone.readiness.data_product_access_enforcement import (
    AccessDriftInspector,
    AccessEnforcementCertifier,
    AccessEnforcementPlanner,
    AccessEnforcementRunner,
)
from dpone.readiness.data_product_access_enforcement_clickhouse import ClickHouseAccessDialect


def test_disabled_access_enforcement_emits_noop_plan() -> None:
    plan = AccessEnforcementPlanner().plan(
        manifest=_manifest(enabled=False),
        classification={},
        entitlement_plan={},
        privacy_impact={},
        access_gate=None,
        authority_gate=None,
        target_connection={},
        dialect=ClickHouseAccessDialect(),
        environment="prod",
    )

    assert plan["schema_version"] == "dpone.data_product_access_enforcement_plan.v1"
    assert plan["status"] == "disabled"
    assert plan["operations"] == []
    assert plan["blockers"] == []


def test_clickhouse_plan_renders_grants_row_policy_and_masked_view() -> None:
    plan = AccessEnforcementPlanner().plan(
        manifest=_manifest(),
        classification=_classification(),
        entitlement_plan=_entitlement_plan(),
        privacy_impact=_privacy_impact(),
        access_gate=_access_gate(),
        authority_gate=_authority_gate(),
        target_connection=_target_connection(),
        dialect=ClickHouseAccessDialect(),
        environment="prod",
    )

    sql = "\n".join(operation["sql"] for operation in plan["operations"])

    assert plan["status"] == "ready"
    assert plan["access_enforcement_plan_id"].startswith("sha256:")
    assert "GRANT SELECT(`amount`, `customer_id`) ON `analytics`.`orders` TO `dpone_finance_daily_margin`" in sql
    assert "CREATE ROW POLICY IF NOT EXISTS" in sql
    assert "CREATE OR REPLACE VIEW `analytics`.`orders__masked__" in sql
    assert plan["desired_state"]["grants"]
    assert plan["desired_state"]["masks"]
    assert plan["desired_state"]["row_filters"]


def test_plan_blocks_missing_authority_and_unsupported_target() -> None:
    missing_authority = AccessEnforcementPlanner().plan(
        manifest=_manifest(),
        classification=_classification(),
        entitlement_plan=_entitlement_plan(),
        privacy_impact=_privacy_impact(),
        access_gate=_access_gate(),
        authority_gate=None,
        target_connection=_target_connection(),
        dialect=ClickHouseAccessDialect(),
        environment="prod",
    )
    unsupported = AccessEnforcementPlanner().plan(
        manifest=_manifest(),
        classification=_classification(),
        entitlement_plan=_entitlement_plan(),
        privacy_impact=_privacy_impact(),
        access_gate=_access_gate(),
        authority_gate=_authority_gate(),
        target_connection={**_target_connection(), "type": "postgres"},
        dialect=ClickHouseAccessDialect(),
        environment="prod",
    )

    assert missing_authority["status"] == "blocked"
    assert "data_product_access_enforcement.authority_gate_required" in missing_authority["blockers"]
    assert unsupported["status"] == "blocked"
    assert "data_product_access_enforcement.unsupported_target:postgres" in unsupported["blockers"]


def test_apply_is_dry_run_without_execute_and_executes_with_executor() -> None:
    plan = AccessEnforcementPlanner().plan(
        manifest=_manifest(),
        classification=_classification(),
        entitlement_plan=_entitlement_plan(),
        privacy_impact=_privacy_impact(),
        access_gate=_access_gate(),
        authority_gate=_authority_gate(),
        target_connection=_target_connection(),
        dialect=ClickHouseAccessDialect(),
        environment="prod",
    )
    executor = _FakeExecutor()

    dry_run = AccessEnforcementRunner().apply(plan=plan, approval=None, execute=False, executor=executor)
    applied = AccessEnforcementRunner().apply(
        plan=plan,
        approval={"status": "approved", "approved_by": "data-governance"},
        execute=True,
        executor=executor,
    )

    assert dry_run["schema_version"] == "dpone.data_product_access_enforcement_run.v1"
    assert dry_run["status"] == "dry_run"
    assert executor.executed == len(plan["operations"])
    assert applied["status"] == "applied"


def test_drift_blocks_extra_sensitive_grant_and_certifier_blocks_drift() -> None:
    plan = AccessEnforcementPlanner().plan(
        manifest=_manifest(),
        classification=_classification(),
        entitlement_plan=_entitlement_plan(),
        privacy_impact=_privacy_impact(),
        access_gate=_access_gate(),
        authority_gate=_authority_gate(),
        target_connection=_target_connection(),
        dialect=ClickHouseAccessDialect(),
        environment="prod",
    )
    drift = AccessDriftInspector().inspect(
        plan=plan,
        actual_state={
            "grants": [
                {"subject": "finance.daily_margin", "columns": ["amount", "customer_id"]},
                {"subject": "unknown.reader", "columns": ["customer_email"]},
            ],
            "masks": [],
            "row_filters": [],
        },
    )
    certificate = AccessEnforcementCertifier().certify(
        run={"status": "applied", "access_enforcement_run_id": "sha256:" + "1" * 64},
        drift_report=drift,
        profile="regulated",
    )

    assert drift["schema_version"] == "dpone.data_product_access_drift_report.v1"
    assert drift["status"] == "blocked"
    assert "data_product_access_drift.extra_sensitive_grant:unknown.reader:customer_email" in drift["blockers"]
    assert "data_product_access_drift.mask_missing:support.ops_debug:customer_email" in drift["blockers"]
    assert certificate["schema_version"] == "dpone.data_product_access_enforcement_certificate.v1"
    assert certificate["status"] == "blocked"


def test_clean_drift_certifies_enforcement() -> None:
    plan = AccessEnforcementPlanner().plan(
        manifest=_manifest(),
        classification=_classification(),
        entitlement_plan=_entitlement_plan(),
        privacy_impact=_privacy_impact(),
        access_gate=_access_gate(),
        authority_gate=_authority_gate(),
        target_connection=_target_connection(),
        dialect=ClickHouseAccessDialect(),
        environment="prod",
    )
    drift = AccessDriftInspector().inspect(plan=plan, actual_state=plan["desired_state"])
    certificate = AccessEnforcementCertifier().certify(
        run={"status": "applied", "access_enforcement_run_id": "sha256:" + "1" * 64},
        drift_report=drift,
        profile="regulated",
    )

    assert drift["status"] == "clean"
    assert certificate["status"] == "certified"
    assert certificate["access_enforcement_certificate_id"].startswith("sha256:")


def test_access_enforcement_public_json_schemas_validate_artifacts() -> None:
    plan = AccessEnforcementPlanner().plan(
        manifest=_manifest(),
        classification=_classification(),
        entitlement_plan=_entitlement_plan(),
        privacy_impact=_privacy_impact(),
        access_gate=_access_gate(),
        authority_gate=_authority_gate(),
        target_connection=_target_connection(),
        dialect=ClickHouseAccessDialect(),
        environment="prod",
    )
    run = AccessEnforcementRunner().apply(
        plan=plan,
        approval={"status": "approved", "approved_by": "data-governance"},
        execute=True,
        executor=_FakeExecutor(),
    )
    drift = AccessDriftInspector().inspect(plan=plan, actual_state=plan["desired_state"])
    certificate = AccessEnforcementCertifier().certify(run=run, drift_report=drift, profile="regulated")
    from dpone.readiness.data_product_access_enforcement_rendering import AccessEnforcementRenderer

    report = AccessEnforcementRenderer().report(certificate=certificate)

    for name, payload in (
        ("data-product-access-enforcement-plan.schema.json", plan),
        ("data-product-access-enforcement-run.schema.json", run),
        ("data-product-access-drift-report.schema.json", drift),
        ("data-product-access-enforcement-certificate.schema.json", certificate),
        ("data-product-access-enforcement-report.schema.json", report),
    ):
        schema = json.loads((Path("docs/schemas/data-product") / name).read_text(encoding="utf-8"))
        Draft202012Validator(schema).validate(payload)


class _FakeExecutor:
    def __init__(self) -> None:
        self.executed = 0

    def execute(self, operation: dict) -> dict:
        self.executed += 1
        assert operation["operation_type"] == "sql"
        return {"status": "executed"}


def _manifest(*, enabled: bool = True) -> dict:
    return {
        "sink": {
            "options": {
                "data_product": {
                    "id": "analytics.orders",
                    "owner": "data-platform",
                    "tier": "gold",
                    "criticality": "high",
                    "access_enforcement": {
                        "enabled": enabled,
                        "mode": "gate",
                        "profile": "regulated",
                        "require_access_gate": True,
                        "require_authority_gate": True,
                        "require_target_fingerprint": True,
                        "require_lock": True,
                        "drift_policy": "block",
                        "clickhouse": {
                            "masked_view_naming": "{database}.{table}__masked__{subject_hash}",
                            "role_prefix": "dpone_",
                        },
                    },
                }
            }
        }
    }


def _classification() -> dict:
    return {
        "schema_version": "dpone.data_product_access_classification.v1",
        "status": "classified",
        "access_classification_id": "sha256:" + "a" * 64,
        "product_id": "analytics.orders",
        "columns": [
            {"name": "amount", "class": "financial", "sensitive": True, "masking": "none"},
            {"name": "customer_id", "class": "internal", "sensitive": False, "masking": "none"},
            {"name": "customer_email", "class": "pii", "sensitive": True, "masking": "hash"},
        ],
    }


def _entitlement_plan() -> dict:
    return {
        "schema_version": "dpone.data_product_entitlement_plan.v1",
        "status": "ready",
        "entitlement_plan_id": "sha256:" + "b" * 64,
        "product_id": "analytics.orders",
        "decisions": [
            {
                "subject": "finance.daily_margin",
                "owner": "finance-analytics",
                "column": "amount",
                "class": "financial",
                "sensitive": True,
                "actions": ["read"],
                "blockers": [],
            },
            {
                "subject": "finance.daily_margin",
                "owner": "finance-analytics",
                "column": "customer_id",
                "class": "internal",
                "sensitive": False,
                "actions": ["read"],
                "blockers": [],
            },
            {
                "subject": "support.ops_debug",
                "owner": "support",
                "column": "customer_email",
                "class": "pii",
                "sensitive": True,
                "actions": ["read"],
                "masking_required": True,
                "masking": "hash",
                "row_filter": "tenant_id = 'support'",
                "blockers": [],
            },
        ],
        "blockers": [],
        "warnings": [],
    }


def _privacy_impact() -> dict:
    return {
        "schema_version": "dpone.data_product_privacy_impact_assessment.v1",
        "status": "assessed",
        "privacy_impact_id": "sha256:" + "c" * 64,
        "entitlement_plan_id": "sha256:" + "b" * 64,
        "blockers": [],
        "warnings": [],
    }


def _access_gate() -> dict:
    return {
        "schema_version": "dpone.data_product_access_gate.v1",
        "status": "allowed",
        "access_gate_id": "sha256:" + "d" * 64,
        "entitlement_plan_id": "sha256:" + "b" * 64,
        "privacy_impact_id": "sha256:" + "c" * 64,
        "blockers": [],
        "warnings": [],
    }


def _authority_gate() -> dict:
    return {
        "schema_version": "dpone.data_product_authority_gate.v1",
        "status": "allowed",
        "authority_gate_id": "sha256:" + "e" * 64,
        "blockers": [],
        "warnings": [],
    }


def _target_connection() -> dict:
    return {
        "type": "clickhouse",
        "database": "analytics",
        "table": "orders",
        "lock_id": "lock:orders-access",
        "capabilities": {"masking_policy": False},
    }
