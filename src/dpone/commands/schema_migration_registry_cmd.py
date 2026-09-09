from __future__ import annotations

import argparse
import logging
from importlib import import_module
from typing import Any

from dpone.commands.base import Command
from dpone.commands.func_command import CommandGroup, FuncCommand
from dpone.commands.schema_migration_registry_output import emit_registry_payload
from dpone.readiness.schema_migration_evidence_registry_constants import STAGES


def registry_group() -> Command:
    subcommands = [
        FuncCommand("record", register_record_parser, cmd_registry_record),
        FuncCommand("history", register_history_parser, cmd_registry_history),
        FuncCommand("latest", register_latest_parser, cmd_registry_latest),
        FuncCommand("audit-report", register_audit_report_parser, cmd_registry_audit_report),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("registry", help="Record and query schema migration evidence registry")

    return CommandGroup(
        name="registry",
        help="Record and query schema migration evidence registry",
        build_parser=build,
        subcommands=subcommands,
        subdest="schema_migration_registry_cmd",
    )


def cmd_registry_record(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().record(
        bundle_path=args.bundle,
        gate_path=args.gate,
        trust_path=args.trust,
        diff_path=args.diff,
        contract_gate_path=args.contract_gate,
        consumer_gate_path=args.consumer_gate,
        consumer_certification_path=args.consumer_certification,
        compatibility_view_gate_path=args.compatibility_view_gate,
        contract_retirement_gate_path=args.contract_retirement_gate,
        data_product_slo_gate_path=args.data_product_slo_gate,
        data_product_incident_report_path=args.data_product_incident_report,
        data_product_error_budget_gate_path=args.data_product_error_budget_gate,
        data_product_incident_lifecycle_path=args.data_product_incident_lifecycle,
        data_product_release_closeout_gate_path=args.data_product_release_closeout_gate,
        data_product_fleet_gate_path=args.data_product_fleet_gate,
        data_product_fleet_report_path=args.data_product_fleet_report,
        data_product_reliability_export_path=args.data_product_reliability_export,
        data_product_route_delivery_receipt_path=args.data_product_route_delivery_receipt,
        data_product_assertion_gate_path=args.data_product_assertion_gate,
        data_product_assertion_report_path=args.data_product_assertion_report,
        data_product_policy_gate_path=args.data_product_policy_gate,
        data_product_policy_report_path=args.data_product_policy_report,
        data_product_waiver_path=args.data_product_waiver,
        data_product_authority_gate_path=args.data_product_authority_gate,
        data_product_approval_quorum_path=args.data_product_approval_quorum,
        data_product_evidence_signature_path=args.data_product_evidence_signature,
        data_product_governance_export_payload_path=args.data_product_governance_export_payload,
        data_product_governance_publish_receipt_path=args.data_product_governance_publish_receipt,
        data_product_governance_publish_verification_path=args.data_product_governance_publish_verification,
        data_product_compliance_gate_path=args.data_product_compliance_gate,
        data_product_audit_package_path=args.data_product_audit_package,
        data_product_audit_archive_verification_path=args.data_product_audit_archive_verification,
        data_product_audit_retention_plan_path=args.data_product_audit_retention_plan,
        data_product_legal_hold_path=args.data_product_legal_hold,
        data_product_access_classification_path=args.data_product_access_classification,
        data_product_entitlement_plan_path=args.data_product_entitlement_plan,
        data_product_privacy_impact_assessment_path=args.data_product_privacy_impact_assessment,
        data_product_access_gate_path=args.data_product_access_gate,
        data_product_access_report_path=args.data_product_access_report,
        data_product_access_enforcement_certificate_path=args.data_product_access_enforcement_certificate,
        data_product_access_drift_report_path=args.data_product_access_drift_report,
        data_product_cost_gate_path=args.data_product_cost_gate,
        data_product_cost_forecast_path=args.data_product_cost_forecast,
        data_product_cost_report_path=args.data_product_cost_report,
        data_product_ring_gate_path=args.data_product_ring_gate,
        data_product_shadow_validation_path=args.data_product_shadow_validation,
        data_product_rollout_promotion_path=args.data_product_rollout_promotion,
        data_product_rollout_report_path=args.data_product_rollout_report,
        data_product_trust_gate_path=args.data_product_trust_gate,
        data_product_trust_report_path=args.data_product_trust_report,
        data_product_trust_export_path=args.data_product_trust_export,
        data_product_remediation_gate_path=args.data_product_remediation_gate,
        data_product_remediation_runbook_path=args.data_product_remediation_runbook,
        data_product_remediation_closeout_path=args.data_product_remediation_closeout,
        data_product_remediation_report_path=args.data_product_remediation_report,
        data_product_remediation_execution_plan_path=args.data_product_remediation_execution_plan,
        data_product_remediation_execution_run_path=args.data_product_remediation_execution_run,
        data_product_remediation_execution_certificate_path=args.data_product_remediation_execution_certificate,
        data_product_remediation_execution_report_path=args.data_product_remediation_execution_report,
        post_apply_certificate_path=args.post_apply_certificate,
        watch_certificate_path=args.watch_certificate,
        remediation_certificate_path=args.remediation_certificate,
        backup_certificate_path=args.backup_certificate,
        recovery_point_path=args.recovery_point,
        recovery_chain_verification_path=args.recovery_chain_verification,
        environment=args.environment,
        stage=args.stage,
        actor=args.actor,
        store_backend=args.store_backend,
        store_uri=args.store_uri,
    )
    emit_registry_payload(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_registry_history(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().history(
        target=args.target,
        environment=args.environment,
        stage=args.stage,
        status=args.status,
        date_from=args.date_from,
        date_to=args.date_to,
        store_backend=args.store_backend,
        store_uri=args.store_uri,
    )
    emit_registry_payload(payload, args.format, args.output)
    return 0


def cmd_registry_latest(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().latest(
        target=args.target,
        environment=args.environment,
        stage=args.stage,
        status=args.status,
        include_blocked=args.include_blocked,
        store_backend=args.store_backend,
        store_uri=args.store_uri,
    )
    emit_registry_payload(payload, args.format, args.output)
    return 0


def cmd_registry_audit_report(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().audit_report(
        target=args.target,
        environment=args.environment,
        date_from=args.date_from,
        date_to=args.date_to,
        store_backend=args.store_backend,
        store_uri=args.store_uri,
    )
    emit_registry_payload(payload, args.format, args.output)
    return 0


def register_record_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("record", help="Record one schema migration evidence bundle in the registry")
    parser.add_argument("--bundle", required=True, help="bundle.json path")
    parser.add_argument("--gate", help="Optional bundle gate receipt JSON/YAML")
    parser.add_argument("--trust", help="Optional trust verification receipt JSON/YAML")
    parser.add_argument("--diff", help="Optional bundle diff receipt JSON/YAML")
    parser.add_argument("--contract-gate", help="Optional schema contract gate receipt JSON/YAML")
    parser.add_argument("--consumer-gate", help="Optional schema contract consumer gate receipt JSON/YAML")
    parser.add_argument("--consumer-certification", help="Optional consumer contract certification receipt JSON/YAML")
    parser.add_argument("--compatibility-view-gate", help="Optional compatibility view gate receipt JSON/YAML")
    parser.add_argument("--contract-retirement-gate", help="Optional contract retirement gate receipt JSON/YAML")
    parser.add_argument("--data-product-slo-gate", help="Optional data product SLO gate receipt JSON/YAML")
    parser.add_argument("--data-product-incident-report", help="Optional data product incident report JSON/YAML")
    parser.add_argument("--data-product-error-budget-gate", help="Optional data product error-budget gate JSON/YAML")
    parser.add_argument("--data-product-incident-lifecycle", help="Optional data product incident lifecycle JSON/YAML")
    parser.add_argument(
        "--data-product-release-closeout-gate", help="Optional data product release closeout gate JSON/YAML"
    )
    parser.add_argument("--data-product-fleet-gate", help="Optional data product fleet gate JSON/YAML")
    parser.add_argument("--data-product-fleet-report", help="Optional data product fleet report JSON/YAML")
    parser.add_argument("--data-product-reliability-export", help="Optional data product reliability export JSON/YAML")
    parser.add_argument(
        "--data-product-route-delivery-receipt", help="Optional data product route delivery receipt JSON/YAML"
    )
    parser.add_argument("--data-product-assertion-gate", help="Optional data product assertion gate JSON/YAML")
    parser.add_argument("--data-product-assertion-report", help="Optional data product assertion report JSON/YAML")
    parser.add_argument("--data-product-policy-gate", help="Optional data product policy gate JSON/YAML")
    parser.add_argument("--data-product-policy-report", help="Optional data product policy report JSON/YAML")
    parser.add_argument("--data-product-waiver", help="Optional data product policy waiver JSON/YAML")
    parser.add_argument("--data-product-authority-gate", help="Optional data product authority gate JSON/YAML")
    parser.add_argument("--data-product-approval-quorum", help="Optional data product approval quorum JSON/YAML")
    parser.add_argument("--data-product-evidence-signature", help="Optional data product evidence signature JSON/YAML")
    parser.add_argument("--data-product-governance-export-payload", help="Optional governance payload JSON/YAML")
    parser.add_argument("--data-product-governance-publish-receipt", help="Optional governance publish receipt")
    parser.add_argument(
        "--data-product-governance-publish-verification",
        help="Optional governance publish verification JSON/YAML",
    )
    parser.add_argument("--data-product-compliance-gate", help="Optional data product compliance gate JSON/YAML")
    parser.add_argument("--data-product-audit-package", help="Optional data product audit package JSON/YAML")
    parser.add_argument(
        "--data-product-audit-archive-verification",
        help="Optional data product audit archive verification JSON/YAML",
    )
    parser.add_argument(
        "--data-product-audit-retention-plan", help="Optional data product audit retention plan JSON/YAML"
    )
    parser.add_argument("--data-product-legal-hold", help="Optional data product legal hold JSON/YAML")
    parser.add_argument("--data-product-access-classification", help="Optional data product access classification")
    parser.add_argument("--data-product-entitlement-plan", help="Optional data product entitlement plan")
    parser.add_argument(
        "--data-product-privacy-impact-assessment",
        help="Optional data product privacy impact assessment JSON/YAML",
    )
    parser.add_argument("--data-product-access-gate", help="Optional data product access gate JSON/YAML")
    parser.add_argument("--data-product-access-report", help="Optional data product access report JSON/YAML")
    parser.add_argument(
        "--data-product-access-enforcement-certificate",
        help="Optional data product access enforcement certificate JSON/YAML",
    )
    parser.add_argument(
        "--data-product-access-drift-report", help="Optional data product access drift report JSON/YAML"
    )
    parser.add_argument("--data-product-cost-gate", help="Optional data product cost gate JSON/YAML")
    parser.add_argument("--data-product-cost-forecast", help="Optional data product cost forecast JSON/YAML")
    parser.add_argument("--data-product-cost-report", help="Optional data product cost report JSON/YAML")
    parser.add_argument("--data-product-ring-gate", help="Optional data product rollout ring gate JSON/YAML")
    parser.add_argument("--data-product-shadow-validation", help="Optional data product shadow validation JSON/YAML")
    parser.add_argument("--data-product-rollout-promotion", help="Optional data product rollout promotion JSON/YAML")
    parser.add_argument("--data-product-rollout-report", help="Optional data product rollout report JSON/YAML")
    parser.add_argument("--data-product-trust-gate", help="Optional data product Trust Center gate JSON/YAML")
    parser.add_argument("--data-product-trust-report", help="Optional data product Trust Center report JSON/YAML")
    parser.add_argument("--data-product-trust-export", help="Optional data product Trust Center export JSON/YAML")
    parser.add_argument("--data-product-remediation-gate", help="Optional data product remediation gate JSON/YAML")
    parser.add_argument(
        "--data-product-remediation-runbook", help="Optional data product remediation runbook JSON/YAML"
    )
    parser.add_argument(
        "--data-product-remediation-closeout", help="Optional data product remediation closeout JSON/YAML"
    )
    parser.add_argument("--data-product-remediation-report", help="Optional data product remediation report JSON/YAML")
    parser.add_argument(
        "--data-product-remediation-execution-plan",
        help="Optional data product remediation execution plan JSON/YAML",
    )
    parser.add_argument(
        "--data-product-remediation-execution-run",
        help="Optional data product remediation execution run JSON/YAML",
    )
    parser.add_argument(
        "--data-product-remediation-execution-certificate",
        help="Optional data product remediation execution certificate JSON/YAML",
    )
    parser.add_argument(
        "--data-product-remediation-execution-report",
        help="Optional data product remediation execution report JSON/YAML",
    )
    parser.add_argument("--post-apply-certificate", help="Optional post-apply certificate JSON/YAML")
    parser.add_argument("--watch-certificate", help="Optional release watch certificate JSON/YAML")
    parser.add_argument("--remediation-certificate", help="Optional remediation certificate JSON/YAML")
    parser.add_argument("--backup-certificate", help="Optional backup certificate JSON/YAML")
    parser.add_argument("--recovery-point", help="Optional recovery point JSON/YAML")
    parser.add_argument("--recovery-chain-verification", help="Optional recovery chain verification JSON/YAML")
    parser.add_argument("--environment", required=True, help="Environment name")
    parser.add_argument(
        "--stage",
        required=True,
        choices=sorted(STAGES),
        help="Evidence lifecycle stage",
    )
    parser.add_argument("--actor", default="ci", help="Actor recorded in the evidence record")
    _add_store_args(parser)
    _add_output_args(parser)
    return parser


def register_history_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("history", help="Query schema migration evidence registry history")
    _add_query_args(parser)
    _add_store_args(parser)
    _add_output_args(parser, default="table", formats=("text", "json", "md", "table"))
    return parser


def register_latest_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("latest", help="Read latest schema migration evidence registry record")
    _add_query_args(parser)
    parser.add_argument("--include-blocked", action="store_true", help="Allow latest blocked record in result")
    _add_store_args(parser)
    _add_output_args(parser, formats=("text", "json", "md", "table"))
    return parser


def register_audit_report_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("audit-report", help="Render schema migration evidence audit report")
    parser.add_argument("--target", help="Target key, e.g. clickhouse.analytics.orders")
    parser.add_argument("--environment", help="Environment filter")
    parser.add_argument("--from", dest="date_from", help="Start date YYYY-MM-DD")
    parser.add_argument("--to", dest="date_to", help="End date YYYY-MM-DD")
    _add_store_args(parser)
    _add_output_args(parser, formats=("text", "json", "md", "table"))
    return parser


def _add_query_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--target", help="Target key, e.g. clickhouse.analytics.orders")
    parser.add_argument("--environment", help="Environment filter")
    parser.add_argument("--stage", help="Lifecycle stage filter")
    parser.add_argument("--status", help="Record status filter")
    parser.add_argument("--from", dest="date_from", help="Start date YYYY-MM-DD")
    parser.add_argument("--to", dest="date_to", help="End date YYYY-MM-DD")
    parser.add_argument("--verify-artifacts", action="store_true", help="Reserved for slower future artifact recheck")


def _add_store_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--store-backend", choices=["local_json", "sqlite"], default="local_json")
    parser.add_argument("--store-uri", help="Registry path or SQLite database path")


def _add_output_args(
    parser: argparse.ArgumentParser,
    *,
    formats: tuple[str, ...] = ("text", "json", "md"),
    default: str = "text",
) -> None:
    parser.add_argument("--format", choices=list(formats), default=default)
    parser.add_argument("--output", help="Optional output artifact path")


def _facade() -> Any:
    module = import_module("dpone.services.schema_migration_evidence_registry")
    return module.MigrationEvidenceRegistryFacade()


__all__ = ["registry_group"]
