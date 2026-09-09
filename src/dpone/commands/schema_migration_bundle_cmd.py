from __future__ import annotations

import argparse
import logging
from importlib import import_module
from typing import Any

from dpone.commands.base import Command
from dpone.commands.func_command import CommandGroup, FuncCommand
from dpone.commands.schema_migration_bundle_output import emit_bundle_payload


def bundle_group() -> Command:
    subcommands = [
        FuncCommand("build", register_bundle_build_parser, cmd_schema_migration_bundle_build),
        FuncCommand("verify", register_bundle_verify_parser, cmd_schema_migration_bundle_verify),
        FuncCommand("gate", register_bundle_gate_parser, cmd_schema_migration_bundle_gate),
        FuncCommand("diff", register_bundle_diff_parser, cmd_schema_migration_bundle_diff),
        FuncCommand("attest", register_bundle_attest_parser, cmd_schema_migration_bundle_attest),
        trust_group(),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("bundle", help="Build and verify schema migration evidence bundles")

    return CommandGroup(
        name="bundle",
        help="Build and verify schema migration evidence bundles",
        build_parser=build,
        subcommands=subcommands,
        subdest="schema_migration_bundle_cmd",
    )


def review_group() -> Command:
    subcommands = [FuncCommand("render", register_review_render_parser, cmd_schema_migration_review_render)]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("review", help="Render PR/MR schema migration review artifacts")

    return CommandGroup(
        name="review",
        help="Render PR/MR schema migration review artifacts",
        build_parser=build,
        subcommands=subcommands,
        subdest="schema_migration_review_cmd",
    )


def cmd_schema_migration_bundle_build(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().build(
        pack_path=args.pack,
        impact_path=args.impact,
        environment_contract_path=args.environment_contract,
        certificate_path=args.certificate,
        rehearsal_certificate_path=args.rehearsal_certificate,
        post_apply_certificate_path=args.post_apply_certificate,
        watch_certificate_path=args.watch_certificate,
        remediation_certificate_path=args.remediation_certificate,
        backup_certificate_path=args.backup_certificate,
        contract_gate_path=args.contract_gate,
        consumer_gate_path=args.consumer_gate,
        consumer_test_kit_path=args.consumer_test_kit,
        consumer_certification_path=args.consumer_certification,
        compatibility_view_gate_path=args.compatibility_view_gate,
        contract_adoption_status_path=args.contract_adoption_status,
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
        data_product_access_gate_path=args.data_product_access_gate,
        data_product_access_report_path=args.data_product_access_report,
        data_product_privacy_impact_assessment_path=args.data_product_privacy_impact_assessment,
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
        recovery_point_path=args.recovery_point,
        recovery_chain_verification_path=args.recovery_chain_verification,
        fixture_build_path=args.fixture_build,
        quality_profile_path=args.quality_profile,
        promotion_path=args.promotion,
        approval_path=args.approval,
        attest=args.attest,
        output_dir=args.output_dir,
    )
    emit_bundle_payload(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_schema_migration_bundle_verify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().verify(bundle_path=args.bundle, require_attestation=args.require_attestation)
    emit_bundle_payload(payload, args.format, args.output)
    return 2 if payload.get("blockers") else 0


def cmd_schema_migration_bundle_gate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _gate_facade().gate(
        bundle_path=args.bundle,
        profile=args.profile,
        policy_path=args.policy,
        target_environment=args.target_environment,
        trust_verification_path=args.trust_verification,
    )
    emit_bundle_payload(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_schema_migration_bundle_diff(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _diff_facade().diff(
        base_path=args.base,
        head_path=args.head,
        base_gate_path=args.base_gate,
        head_gate_path=args.head_gate,
        require_attestation=args.require_attestation,
    )
    emit_bundle_payload(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_schema_migration_bundle_attest(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _trust_facade().attest(
        bundle_path=args.bundle,
        output_path=args.output,
        provenance_source=args.provenance_source,
        repository=args.repository,
        commit_sha=args.commit_sha,
        ref=args.ref,
        signing_key_env=args.signing_key_env,
        signing_key_id=args.signing_key_id,
        run_id=args.run_id,
        workflow=args.workflow,
        actor=args.actor,
        run_url=args.run_url,
        builder_id=args.builder_id,
        protected_ref=args.protected_ref,
    )
    emit_bundle_payload(payload, args.format, None)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_schema_migration_bundle_trust_verify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _trust_facade().verify(
        bundle_path=args.bundle,
        provenance_path=args.provenance,
        policy_path=args.policy,
        signing_key_env=args.signing_key_env,
        external_attestation_paths=tuple(args.external_attestation or ()),
    )
    emit_bundle_payload(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_schema_migration_review_render(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().render_review(bundle_path=args.bundle)
    emit_bundle_payload(payload, args.format, args.output)
    return 0


def register_bundle_build_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("build", help="Build one schema migration evidence bundle")
    parser.add_argument("--pack", required=True, help="Migration pack JSON path")
    parser.add_argument("--impact", help="Schema impact plan JSON path")
    parser.add_argument("--environment-contract", help="Environment contract YAML/JSON path")
    parser.add_argument("--certificate", help="Environment certification receipt JSON/YAML")
    parser.add_argument("--rehearsal-certificate", help="Migration rehearsal certificate JSON/YAML")
    parser.add_argument("--post-apply-certificate", help="Migration post-apply certificate JSON/YAML")
    parser.add_argument("--watch-certificate", help="Migration release watch certificate JSON/YAML")
    parser.add_argument("--remediation-certificate", help="Migration remediation certificate JSON/YAML")
    parser.add_argument("--backup-certificate", help="Migration backup certificate JSON/YAML")
    parser.add_argument("--contract-gate", help="Schema contract gate receipt JSON/YAML")
    parser.add_argument("--consumer-gate", help="Schema contract consumer gate receipt JSON/YAML")
    parser.add_argument("--consumer-test-kit", help="Schema contract consumer test kit JSON/YAML")
    parser.add_argument("--consumer-certification", help="Schema contract consumer certification JSON/YAML")
    parser.add_argument("--compatibility-view-gate", help="Schema contract compatibility view gate receipt JSON/YAML")
    parser.add_argument("--contract-adoption-status", help="Schema contract adoption status JSON/YAML")
    parser.add_argument("--contract-retirement-gate", help="Schema contract retirement gate receipt JSON/YAML")
    parser.add_argument("--data-product-slo-gate", help="Data product SLO gate receipt JSON/YAML")
    parser.add_argument("--data-product-incident-report", help="Data product incident report JSON/YAML")
    parser.add_argument("--data-product-error-budget-gate", help="Data product error-budget gate JSON/YAML")
    parser.add_argument("--data-product-incident-lifecycle", help="Data product incident lifecycle JSON/YAML")
    parser.add_argument("--data-product-release-closeout-gate", help="Data product release closeout gate JSON/YAML")
    parser.add_argument("--data-product-fleet-gate", help="Data product fleet gate JSON/YAML")
    parser.add_argument("--data-product-fleet-report", help="Data product fleet report JSON/YAML")
    parser.add_argument("--data-product-reliability-export", help="Data product reliability export JSON/YAML")
    parser.add_argument("--data-product-route-delivery-receipt", help="Data product route delivery receipt JSON/YAML")
    parser.add_argument("--data-product-assertion-gate", help="Data product assertion gate JSON/YAML")
    parser.add_argument("--data-product-assertion-report", help="Data product assertion report JSON/YAML")
    parser.add_argument("--data-product-policy-gate", help="Data product policy gate JSON/YAML")
    parser.add_argument("--data-product-policy-report", help="Data product policy report JSON/YAML")
    parser.add_argument("--data-product-waiver", help="Data product policy waiver JSON/YAML")
    parser.add_argument("--data-product-authority-gate", help="Data product authority gate JSON/YAML")
    parser.add_argument("--data-product-approval-quorum", help="Data product approval quorum JSON/YAML")
    parser.add_argument("--data-product-evidence-signature", help="Data product detached evidence signature JSON/YAML")
    parser.add_argument("--data-product-governance-export-payload", help="Data product governance payload JSON/YAML")
    parser.add_argument("--data-product-governance-publish-receipt", help="Data product governance publish receipt")
    parser.add_argument(
        "--data-product-governance-publish-verification",
        help="Data product governance publish verification JSON/YAML",
    )
    parser.add_argument("--data-product-compliance-gate", help="Data product compliance gate JSON/YAML")
    parser.add_argument("--data-product-audit-package", help="Data product audit package JSON/YAML")
    parser.add_argument(
        "--data-product-audit-archive-verification",
        help="Data product audit archive verification JSON/YAML",
    )
    parser.add_argument("--data-product-audit-retention-plan", help="Data product audit retention plan JSON/YAML")
    parser.add_argument("--data-product-legal-hold", help="Data product legal hold JSON/YAML")
    parser.add_argument("--data-product-access-gate", help="Data product access gate JSON/YAML")
    parser.add_argument("--data-product-access-report", help="Data product access report JSON/YAML")
    parser.add_argument(
        "--data-product-privacy-impact-assessment",
        help="Data product privacy impact assessment JSON/YAML",
    )
    parser.add_argument(
        "--data-product-access-enforcement-certificate",
        help="Data product access enforcement certificate JSON/YAML",
    )
    parser.add_argument("--data-product-access-drift-report", help="Data product access drift report JSON/YAML")
    parser.add_argument("--data-product-cost-gate", help="Data product cost gate JSON/YAML")
    parser.add_argument("--data-product-cost-forecast", help="Data product cost forecast JSON/YAML")
    parser.add_argument("--data-product-cost-report", help="Data product cost report JSON/YAML")
    parser.add_argument("--data-product-ring-gate", help="Data product rollout ring gate JSON/YAML")
    parser.add_argument("--data-product-shadow-validation", help="Data product shadow validation JSON/YAML")
    parser.add_argument("--data-product-rollout-promotion", help="Data product rollout promotion JSON/YAML")
    parser.add_argument("--data-product-rollout-report", help="Data product rollout report JSON/YAML")
    parser.add_argument("--data-product-trust-gate", help="Data product Trust Center gate JSON/YAML")
    parser.add_argument("--data-product-trust-report", help="Data product Trust Center report JSON/YAML")
    parser.add_argument("--data-product-trust-export", help="Data product Trust Center export JSON/YAML")
    parser.add_argument("--recovery-point", help="Migration recovery point JSON/YAML")
    parser.add_argument("--recovery-chain-verification", help="Migration recovery chain verification JSON/YAML")
    parser.add_argument("--fixture-build", help="Migration rehearsal fixture-build JSON/YAML")
    parser.add_argument("--quality-profile", help="Migration rehearsal data-profile JSON/YAML")
    parser.add_argument("--promotion", help="Promotion receipt JSON/YAML")
    parser.add_argument("--approval", help="Approval artifact JSON/YAML")
    parser.add_argument("--attest", action="store_true", help="Embed artifact digests and bundle attestation")
    parser.add_argument("--output-dir", required=True, help="Directory where bundle.json is written")
    _add_output_args(parser)
    return parser


def register_bundle_verify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("verify", help="Verify schema migration evidence bundle integrity offline")
    parser.add_argument("--bundle", required=True, help="bundle.json path")
    parser.add_argument("--require-attestation", action="store_true", help="Require attestation in bundle.json")
    _add_output_args(parser)
    return parser


def register_bundle_gate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("gate", help="Evaluate release policy for one schema migration bundle")
    parser.add_argument("--bundle", required=True, help="bundle.json path")
    parser.add_argument(
        "--profile",
        choices=["advisory", "pr_review", "stage_certified", "prod_strict", "regulated"],
        help="Built-in bundle gate profile",
    )
    parser.add_argument("--policy", help="Optional custom bundle policy YAML/JSON")
    parser.add_argument("--target-environment", help="Expected promotion target environment")
    parser.add_argument("--trust-verification", help="Optional trusted provenance verification receipt JSON")
    _add_output_args(parser, formats=("text", "json", "md", "table"))
    return parser


def register_bundle_diff_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("diff", help="Compare two schema migration evidence bundles")
    parser.add_argument("--base", required=True, help="Base bundle.json path")
    parser.add_argument("--head", required=True, help="Head bundle.json path")
    parser.add_argument("--base-gate", help="Optional base bundle gate receipt JSON")
    parser.add_argument("--head-gate", help="Optional head bundle gate receipt JSON")
    parser.add_argument("--require-attestation", action="store_true", help="Require attestation in both bundles")
    _add_output_args(parser, formats=("text", "json", "md", "table"))
    return parser


def register_bundle_attest_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("attest", help="Build signed provenance for one schema migration bundle")
    parser.add_argument("--bundle", required=True, help="bundle.json path")
    parser.add_argument("--output", required=True, help="provenance.json output path")
    parser.add_argument("--signing-key-env", help="Environment variable containing local HMAC signing key")
    parser.add_argument("--signing-key-id", default="local-hmac", help="Signature key id written to provenance")
    parser.add_argument("--provenance-source", required=True, help="CI/SCM source, e.g. github_actions")
    parser.add_argument("--repository", required=True, help="Repository URL bound to provenance")
    parser.add_argument("--commit-sha", required=True, help="Commit SHA bound to provenance")
    parser.add_argument("--ref", required=True, help="Git ref bound to provenance")
    parser.add_argument("--run-id", help="CI run id")
    parser.add_argument("--workflow", help="CI workflow path or name")
    parser.add_argument("--actor", help="CI actor")
    parser.add_argument("--run-url", help="CI run URL")
    parser.add_argument("--builder-id", default="dpone.local", help="Builder identity")
    parser.add_argument("--protected-ref", action="store_true", help="Mark the source ref as protected")
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    return parser


def trust_group() -> Command:
    subcommands = [FuncCommand("verify", register_bundle_trust_verify_parser, cmd_schema_migration_bundle_trust_verify)]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("trust", help="Verify schema migration bundle provenance trust")

    return CommandGroup(
        name="trust",
        help="Verify schema migration bundle provenance trust",
        build_parser=build,
        subcommands=subcommands,
        subdest="schema_migration_bundle_trust_cmd",
    )


def register_bundle_trust_verify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("verify", help="Verify signed provenance and external attestation receipts")
    parser.add_argument("--bundle", required=True, help="bundle.json path")
    parser.add_argument("--provenance", required=True, help="provenance.json path")
    parser.add_argument("--policy", help="Optional trust policy YAML/JSON")
    parser.add_argument("--signing-key-env", help="Environment variable containing local HMAC verification key")
    parser.add_argument(
        "--external-attestation",
        action="append",
        help="External GitHub/Sigstore/Cosign verification receipt JSON/YAML; can be repeated",
    )
    _add_output_args(parser, formats=("text", "json", "md", "table"))
    return parser


def register_review_render_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("render", help="Render deterministic PR/MR review Markdown from bundle.json")
    parser.add_argument("--bundle", required=True, help="bundle.json path")
    _add_output_args(parser)
    return parser


def _add_output_args(parser: argparse.ArgumentParser, *, formats: tuple[str, ...] = ("text", "json", "md")) -> None:
    parser.add_argument("--format", choices=list(formats), default="text")
    parser.add_argument("--output", help="Optional output artifact path")


def _facade() -> Any:
    module = import_module("dpone.services.schema_migration_bundle")
    return module.MigrationBundleFacade()


def _gate_facade() -> Any:
    module = import_module("dpone.services.schema_migration_bundle_gate")
    return module.MigrationBundleGateFacade()


def _diff_facade() -> Any:
    module = import_module("dpone.services.schema_migration_bundle_diff")
    return module.MigrationBundleDiffFacade()


def _trust_facade() -> Any:
    module = import_module("dpone.services.schema_migration_trust")
    return module.MigrationTrustFacade()


__all__ = ["bundle_group", "review_group"]
