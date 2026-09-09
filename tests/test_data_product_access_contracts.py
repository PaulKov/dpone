from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

from dpone.readiness.data_product_access import (
    AccessClassificationBuilder,
    AccessGateEvaluator,
    AccessGovernanceRenderer,
    EntitlementPlanBuilder,
    PrivacyImpactAssessor,
)


def test_disabled_access_governance_emits_noop_artifacts() -> None:
    classification = AccessClassificationBuilder().build(manifest=_manifest(enabled=False), schema_contract={})
    plan = EntitlementPlanBuilder().build(manifest=_manifest(enabled=False), classification=classification)
    privacy = PrivacyImpactAssessor().assess(manifest=_manifest(enabled=False), entitlement_plan=plan)
    gate = AccessGateEvaluator().evaluate(entitlement_plan=plan, privacy_impact=privacy, profile="regulated")

    assert classification["schema_version"] == "dpone.data_product_access_classification.v1"
    assert classification["status"] == "disabled"
    assert classification["columns"] == []
    assert plan["status"] == "disabled"
    assert privacy["status"] == "disabled"
    assert gate["status"] == "allowed"
    assert gate["blockers"] == []


def test_classification_defaults_explicit_sensitive_columns_and_stable_ids() -> None:
    classification = AccessClassificationBuilder().build(
        manifest=_manifest(),
        schema_contract=_schema_contract(),
    )
    repeated = AccessClassificationBuilder().build(manifest=_manifest(), schema_contract=_schema_contract())

    by_name = {item["name"]: item for item in classification["columns"]}

    assert classification["status"] == "classified"
    assert classification["access_classification_id"] == repeated["access_classification_id"]
    assert by_name["customer_email"]["class"] == "pii"
    assert by_name["customer_email"]["masking"] == "hash"
    assert by_name["customer_email"]["sensitive"] is True
    assert by_name["status"]["class"] == "internal"
    assert by_name["status"]["sensitive"] is False


def test_entitlement_plan_blocks_uncovered_sensitive_consumer_and_masking_gap() -> None:
    classification = AccessClassificationBuilder().build(manifest=_manifest(), schema_contract=_schema_contract())

    plan = EntitlementPlanBuilder().build(
        manifest=_manifest(entitlements=_finance_entitlements()),
        classification=classification,
        consumer_matrix=_consumer_matrix(
            [
                _consumer("finance.daily_margin", owner="finance-analytics", columns=["amount", "customer_id"]),
                _consumer("support.ops_debug", owner="support", columns=["customer_email"]),
            ]
        ),
    )

    assert plan["schema_version"] == "dpone.data_product_entitlement_plan.v1"
    assert plan["status"] == "blocked"
    assert "data_product_access.sensitive_access_uncovered:support.ops_debug:customer_email" in plan["blockers"]
    assert "data_product_access.masking_required:support.ops_debug:customer_email" in plan["blockers"]
    assert plan["summary"]["blocked_decisions"] == 1


def test_unknown_consumer_policy_allow_warn_block() -> None:
    classification = AccessClassificationBuilder().build(manifest=_manifest(), schema_contract=_schema_contract())
    matrix = _consumer_matrix([_consumer("unknown.reader", owner="unknown", columns=["customer_email"])])

    allowed = EntitlementPlanBuilder().build(
        manifest=_manifest(unknown_consumer="allow", entitlements=[]),
        classification=classification,
        consumer_matrix=matrix,
    )
    warned = EntitlementPlanBuilder().build(
        manifest=_manifest(unknown_consumer="warn", entitlements=[]),
        classification=classification,
        consumer_matrix=matrix,
    )
    blocked = EntitlementPlanBuilder().build(
        manifest=_manifest(unknown_consumer="block", entitlements=[]),
        classification=classification,
        consumer_matrix=matrix,
    )

    assert allowed["blockers"] == []
    assert "data_product_access.unknown_sensitive_consumer:unknown.reader:customer_email" in warned["warnings"]
    assert "data_product_access.unknown_sensitive_consumer:unknown.reader:customer_email" in blocked["blockers"]


def test_privacy_impact_requires_lawful_basis_and_authority_for_sensitive_access() -> None:
    classification = AccessClassificationBuilder().build(manifest=_manifest(), schema_contract=_schema_contract())
    plan = EntitlementPlanBuilder().build(
        manifest=_manifest(entitlements=_support_entitlements(lawful_basis=None)),
        classification=classification,
        consumer_matrix=_consumer_matrix([_consumer("support.ops_debug", owner="support", columns=["customer_email"])]),
    )

    missing_authority = PrivacyImpactAssessor().assess(manifest=_manifest(), entitlement_plan=plan)
    allowed = PrivacyImpactAssessor().assess(
        manifest=_manifest(entitlements=_support_entitlements(lawful_basis="support_contract")),
        entitlement_plan=EntitlementPlanBuilder().build(
            manifest=_manifest(entitlements=_support_entitlements(lawful_basis="support_contract")),
            classification=classification,
            consumer_matrix=_consumer_matrix(
                [_consumer("support.ops_debug", owner="support", columns=["customer_email"])]
            ),
        ),
        authority_gate=_authority_gate("allowed"),
    )

    assert missing_authority["status"] == "blocked"
    assert "data_product_privacy.lawful_basis_missing:support.ops_debug:customer_email" in missing_authority["blockers"]
    assert "data_product_privacy.authority_gate_required:customer_email" in missing_authority["blockers"]
    assert allowed["status"] == "assessed"
    assert allowed["blockers"] == []


def test_privacy_impact_blocks_sensitive_export_without_authority() -> None:
    classification = AccessClassificationBuilder().build(manifest=_manifest(), schema_contract=_schema_contract())
    export_entitlement = [
        {
            "subject": "finance.daily_margin",
            "type": "consumer",
            "owner": "finance-analytics",
            "purpose": "finance_close",
            "actions": ["export"],
            "columns": ["amount"],
        }
    ]
    plan = EntitlementPlanBuilder().build(
        manifest=_manifest(entitlements=export_entitlement),
        classification=classification,
        consumer_matrix=_consumer_matrix(
            [_consumer("finance.daily_margin", owner="finance-analytics", columns=["amount"])]
        ),
    )

    privacy = PrivacyImpactAssessor().assess(manifest=_manifest(entitlements=export_entitlement), entitlement_plan=plan)

    assert privacy["status"] == "blocked"
    assert "data_product_privacy.sensitive_export_approval_required:amount" in privacy["blockers"]


def test_access_gate_profiles_and_report_rendering() -> None:
    entitlement_plan = {
        "schema_version": "dpone.data_product_entitlement_plan.v1",
        "status": "blocked",
        "product": {"id": "analytics.orders", "owner": "data-platform"},
        "entitlement_plan_id": "sha256:" + "1" * 64,
        "decisions": [
            {
                "subject": "support.ops_debug",
                "column": "customer_email",
                "class": "pii",
                "severity": "critical",
                "sensitive": True,
                "blockers": ["data_product_access.sensitive_access_uncovered:support.ops_debug:customer_email"],
                "warnings": [],
            }
        ],
        "blockers": ["data_product_access.sensitive_access_uncovered:support.ops_debug:customer_email"],
        "warnings": [],
    }
    privacy = {
        "schema_version": "dpone.data_product_privacy_impact_assessment.v1",
        "status": "blocked",
        "privacy_impact_id": "sha256:" + "2" * 64,
        "blockers": ["data_product_privacy.authority_gate_required:customer_email"],
        "warnings": [],
    }

    advisory = AccessGateEvaluator().evaluate(
        entitlement_plan=entitlement_plan, privacy_impact=privacy, profile="advisory"
    )
    regulated = AccessGateEvaluator().evaluate(
        entitlement_plan=entitlement_plan, privacy_impact=privacy, profile="regulated"
    )
    report = AccessGovernanceRenderer().report(gate=regulated)

    assert advisory["status"] == "warning"
    assert advisory["blockers"] == []
    assert regulated["status"] == "blocked"
    assert "data_product_access.product_owner_missing" not in regulated["blockers"]
    assert report["schema_version"] == "dpone.data_product_access_report.v1"
    assert "# Data Product Access Governance Report" in report["markdown"]


def test_access_public_json_schemas_validate_artifacts() -> None:
    classification = AccessClassificationBuilder().build(manifest=_manifest(), schema_contract=_schema_contract())
    plan = EntitlementPlanBuilder().build(
        manifest=_manifest(entitlements=[*_finance_entitlements(), *_support_entitlements("support_contract")]),
        classification=classification,
        consumer_matrix=_consumer_matrix(
            [
                _consumer("finance.daily_margin", owner="finance-analytics", columns=["amount", "customer_id"]),
                _consumer("support.ops_debug", owner="support", columns=["customer_email"]),
            ]
        ),
    )
    privacy = PrivacyImpactAssessor().assess(
        manifest=_manifest(entitlements=[*_finance_entitlements(), *_support_entitlements("support_contract")]),
        entitlement_plan=plan,
        authority_gate=_authority_gate("allowed"),
    )
    gate = AccessGateEvaluator().evaluate(entitlement_plan=plan, privacy_impact=privacy, profile="regulated")
    report = AccessGovernanceRenderer().report(gate=gate)

    for name, payload in (
        ("data-product-access-classification.schema.json", classification),
        ("data-product-entitlement-plan.schema.json", plan),
        ("data-product-privacy-impact-assessment.schema.json", privacy),
        ("data-product-access-gate.schema.json", gate),
        ("data-product-access-report.schema.json", report),
    ):
        schema = json.loads((Path("docs/schemas/data-product") / name).read_text(encoding="utf-8"))
        Draft202012Validator(schema).validate(payload)


def _manifest(
    *,
    enabled: bool = True,
    unknown_consumer: str = "block",
    entitlements: list[dict] | None = None,
) -> dict:
    return {
        "sink": {
            "options": {
                "data_product": {
                    "id": "analytics.orders",
                    "owner": "data-platform",
                    "tier": "gold",
                    "criticality": "high",
                    "access_governance": {
                        "enabled": enabled,
                        "mode": "gate",
                        "profile": "regulated",
                        "unknown_consumer": unknown_consumer,
                        "classification": {
                            "default": "internal",
                            "columns": {
                                "customer_email": {
                                    "class": "pii",
                                    "glossary_terms": ["EMAIL_PLAINTEXT"],
                                    "masking": "hash",
                                    "lawful_basis_required": True,
                                },
                                "amount": {"class": "financial", "masking": "none"},
                            },
                        },
                        "entitlements": entitlements if entitlements is not None else _all_entitlements(),
                        "privacy": {
                            "require_lawful_basis_for": ["pii", "regulated"],
                            "block_sensitive_export_without_approval": True,
                            "require_authority_gate_for": ["pii", "regulated"],
                        },
                    },
                }
            }
        }
    }


def _schema_contract() -> dict:
    return {
        "schema_version": "dpone.schema_contract_version.v1",
        "contract_id": "analytics.orders",
        "version": "2.0.0",
        "columns": [
            {"name": "order_id", "type": "integer"},
            {"name": "customer_id", "type": "integer"},
            {"name": "customer_email", "type": "string"},
            {"name": "amount", "type": "decimal"},
            {"name": "status", "type": "string"},
        ],
    }


def _all_entitlements() -> list[dict]:
    return [*_finance_entitlements(), *_support_entitlements(lawful_basis="support_contract")]


def _finance_entitlements() -> list[dict]:
    return [
        {
            "subject": "finance.daily_margin",
            "type": "consumer",
            "owner": "finance-analytics",
            "purpose": "finance_close",
            "actions": ["read"],
            "columns": ["amount", "customer_id"],
        }
    ]


def _support_entitlements(lawful_basis: str | None) -> list[dict]:
    payload = {
        "subject": "support.ops_debug",
        "type": "group",
        "owner": "support",
        "purpose": "support_debug",
        "actions": ["read"],
        "columns": ["customer_email"],
        "masking_required": True,
        "masking": "hash",
    }
    if lawful_basis:
        payload["lawful_basis"] = lawful_basis
    return [payload]


def _consumer(consumer_id: str, *, owner: str, columns: list[str]) -> dict:
    return {"id": consumer_id, "owner": owner, "reads": {"columns": columns}, "status": "compatible"}


def _consumer_matrix(consumers: list[dict]) -> dict:
    return {
        "schema_version": "dpone.schema_contract_consumer_matrix.v1",
        "status": "compatible",
        "consumer_matrix_id": "sha256:" + "c" * 64,
        "consumers": consumers,
        "summary": {"consumers_count": len(consumers), "blocked_consumers": 0},
    }


def _authority_gate(status: str) -> dict:
    return {
        "schema_version": "dpone.data_product_authority_gate.v1",
        "status": status,
        "authority_gate_id": "sha256:" + "a" * 64,
        "blockers": [] if status == "allowed" else ["authority.blocked"],
        "warnings": [],
    }
