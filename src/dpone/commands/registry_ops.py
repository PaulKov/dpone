from __future__ import annotations

import argparse

from . import (
    certification_automation_plan_cmd,
    certification_suite_cmd,
    evidence_chain_verify_cmd,
    integration_matrix_report_cmd,
    ops_cmd,
    ops_object_storage_cmd,
    release_summary_cmd,
)
from .base import Command
from .func_command import CommandGroup, FuncCommand


def operations_group() -> Command:
    sub = [
        FuncCommand("artifact-index", ops_cmd.register_artifact_index_parser, ops_cmd.cmd_artifact_index),
        FuncCommand("certification-run", ops_cmd.register_certification_run_parser, ops_cmd.cmd_certification_run),
        FuncCommand(
            "certification-history", ops_cmd.register_certification_history_parser, ops_cmd.cmd_certification_history
        ),
        FuncCommand("connector-badges", ops_cmd.register_connector_badges_parser, ops_cmd.cmd_connector_badges),
        FuncCommand("contract-check", ops_cmd.register_contract_check_parser, ops_cmd.cmd_contract_check),
        FuncCommand("quarantine-export", ops_cmd.register_quarantine_export_parser, ops_cmd.cmd_quarantine_export),
        FuncCommand("quarantine-replay", ops_cmd.register_quarantine_replay_parser, ops_cmd.cmd_quarantine_replay),
        FuncCommand("package-start", ops_cmd.register_package_start_parser, ops_cmd.cmd_package_start),
        FuncCommand("package-commit", ops_cmd.register_package_commit_parser, ops_cmd.cmd_package_commit),
        FuncCommand("rollback-plan", ops_cmd.register_rollback_plan_parser, ops_cmd.cmd_rollback_plan),
        FuncCommand("rollback-apply", ops_cmd.register_rollback_apply_parser, ops_cmd.cmd_rollback_apply),
        FuncCommand("rollback-execute", ops_cmd.register_rollback_execute_parser, ops_cmd.cmd_rollback_execute),
        FuncCommand("marketplace", ops_cmd.register_marketplace_parser, ops_cmd.cmd_marketplace),
        FuncCommand("evidence-bundle", ops_cmd.register_evidence_bundle_parser, ops_cmd.cmd_evidence_bundle),
        FuncCommand("evidence-chain", ops_cmd.register_evidence_chain_parser, ops_cmd.cmd_evidence_chain),
        FuncCommand(
            "evidence-chain-verify",
            evidence_chain_verify_cmd.register_parser,
            evidence_chain_verify_cmd.cmd_evidence_chain_verify,
        ),
        FuncCommand("go-live-gate", ops_cmd.register_go_live_gate_parser, ops_cmd.cmd_go_live_gate),
        FuncCommand("policy-evaluate", ops_cmd.register_policy_evaluate_parser, ops_cmd.cmd_policy_evaluate),
        FuncCommand("diff", ops_cmd.register_diff_parser, ops_cmd.cmd_diff),
        FuncCommand("security-audit", ops_cmd.register_security_audit_parser, ops_cmd.cmd_security_audit),
        FuncCommand("slo-evaluate", ops_cmd.register_slo_evaluate_parser, ops_cmd.cmd_slo_evaluate),
        FuncCommand("incident-pack", ops_cmd.register_incident_pack_parser, ops_cmd.cmd_incident_pack),
        FuncCommand("release-gate", ops_cmd.register_release_gate_parser, ops_cmd.cmd_release_gate),
        FuncCommand(
            "release-rc-collect",
            ops_cmd.register_release_rc_collect_parser,
            ops_cmd.cmd_release_rc_collect,
        ),
        FuncCommand(
            "release-rc-finalize",
            ops_cmd.register_release_rc_finalize_parser,
            ops_cmd.cmd_release_rc_finalize,
        ),
        FuncCommand(
            "production-maturity",
            ops_cmd.register_production_maturity_parser,
            ops_cmd.cmd_production_maturity,
        ),
        FuncCommand(
            "industrial-readiness",
            ops_cmd.register_industrial_readiness_parser,
            ops_cmd.cmd_industrial_readiness,
        ),
        FuncCommand("connection-doctor", ops_cmd.register_connection_doctor_parser, ops_cmd.cmd_connection_doctor),
        FuncCommand("source-discover", ops_cmd.register_source_discover_parser, ops_cmd.cmd_source_discover),
        FuncCommand("route-bootstrap", ops_cmd.register_route_bootstrap_parser, ops_cmd.cmd_route_bootstrap),
        FuncCommand("route-doctor", ops_cmd.register_route_doctor_parser, ops_cmd.cmd_route_doctor),
        FuncCommand("route-conformance", ops_cmd.register_route_conformance_parser, ops_cmd.cmd_route_conformance),
        FuncCommand("route-readiness", ops_cmd.register_route_readiness_parser, ops_cmd.cmd_route_readiness),
        FuncCommand(
            "route-schema-evolution",
            ops_cmd.register_route_schema_evolution_parser,
            ops_cmd.cmd_route_schema_evolution,
        ),
        FuncCommand(
            "route-reconciliation-repair",
            ops_cmd.register_route_reconciliation_repair_parser,
            ops_cmd.cmd_route_reconciliation_repair,
        ),
        FuncCommand("route-data-quality", ops_cmd.register_route_data_quality_parser, ops_cmd.cmd_route_data_quality),
        FuncCommand(
            "route-refresh-plan",
            ops_cmd.register_route_refresh_plan_parser,
            ops_cmd.cmd_route_refresh_plan,
        ),
        FuncCommand(
            "route-refresh-execute",
            ops_cmd.register_route_refresh_execute_parser,
            ops_cmd.cmd_route_refresh_execute,
        ),
        FuncCommand(
            "route-refresh-capture-snapshots",
            ops_cmd.register_route_refresh_capture_snapshots_parser,
            ops_cmd.cmd_route_refresh_capture_snapshots,
        ),
        FuncCommand(
            "route-refresh-verify",
            ops_cmd.register_route_refresh_verify_parser,
            ops_cmd.cmd_route_refresh_verify,
        ),
        FuncCommand(
            "route-run-supervisor",
            ops_cmd.register_route_run_supervisor_parser,
            ops_cmd.cmd_route_run_supervisor,
        ),
        FuncCommand(
            "route-live-certification",
            ops_cmd.register_route_live_certification_parser,
            ops_cmd.cmd_route_live_certification,
        ),
        FuncCommand("route-certify", ops_cmd.register_route_certify_parser, ops_cmd.cmd_route_certify),
        FuncCommand(
            "route-attestation-build",
            ops_cmd.register_route_attestation_build_parser,
            ops_cmd.cmd_route_attestation_build,
        ),
        FuncCommand(
            "route-attestation-verify",
            ops_cmd.register_route_attestation_verify_parser,
            ops_cmd.cmd_route_attestation_verify,
        ),
        FuncCommand(
            "route-certify-release",
            ops_cmd.register_route_certify_release_parser,
            ops_cmd.cmd_route_certify_release,
        ),
        FuncCommand(
            "route-release-finalize",
            ops_cmd.register_route_release_finalize_parser,
            ops_cmd.cmd_route_release_finalize,
        ),
        FuncCommand(
            "route-rc-orchestrator",
            ops_cmd.register_route_rc_orchestrator_parser,
            ops_cmd.cmd_route_rc_orchestrator,
        ),
        FuncCommand("route-rc-execute", ops_cmd.register_route_rc_execute_parser, ops_cmd.cmd_route_rc_execute),
        FuncCommand("route-release-gate", ops_cmd.register_route_release_gate_parser, ops_cmd.cmd_route_release_gate),
        FuncCommand(
            "route-execution-ledger",
            ops_cmd.register_route_execution_ledger_parser,
            ops_cmd.cmd_route_execution_ledger,
        ),
        FuncCommand(
            "route-state-promote",
            ops_cmd.register_route_state_promote_parser,
            ops_cmd.cmd_route_state_promote,
        ),
        FuncCommand(
            "route-transport-certification",
            ops_cmd.register_route_transport_certification_parser,
            ops_cmd.cmd_route_transport_certification,
        ),
        FuncCommand("release-summary", release_summary_cmd.register_parser, release_summary_cmd.cmd_release_summary),
        FuncCommand(
            "release-verify",
            release_summary_cmd.register_release_verify_parser,
            release_summary_cmd.cmd_release_verify,
        ),
        FuncCommand(
            "release-orchestrator", ops_cmd.register_release_orchestrator_parser, ops_cmd.cmd_release_orchestrator
        ),
        FuncCommand("release-promote", ops_cmd.register_release_promote_parser, ops_cmd.cmd_release_promote),
        FuncCommand("env-drift", ops_cmd.register_env_drift_parser, ops_cmd.cmd_env_drift),
        FuncCommand("change-request", ops_cmd.register_change_request_parser, ops_cmd.cmd_change_request),
        FuncCommand("approval-record", ops_cmd.register_approval_record_parser, ops_cmd.cmd_approval_record),
        FuncCommand("deployment-record", ops_cmd.register_deployment_record_parser, ops_cmd.cmd_deployment_record),
        FuncCommand("post-deploy-verify", ops_cmd.register_post_deploy_verify_parser, ops_cmd.cmd_post_deploy_verify),
        FuncCommand("release-close", ops_cmd.register_release_close_parser, ops_cmd.cmd_release_close),
        FuncCommand("docs-publish-pack", ops_cmd.register_docs_publish_pack_parser, ops_cmd.cmd_docs_publish_pack),
        FuncCommand("manifest-bundle", ops_cmd.register_manifest_bundle_parser, ops_cmd.cmd_manifest_bundle),
        FuncCommand("runbook-pack", ops_cmd.register_runbook_pack_parser, ops_cmd.cmd_runbook_pack),
        FuncCommand("run-registry", ops_cmd.register_run_registry_parser, ops_cmd.cmd_run_registry),
        FuncCommand("lineage-export", ops_cmd.register_lineage_export_parser, ops_cmd.cmd_lineage_export),
        FuncCommand("benchmark-baseline", ops_cmd.register_benchmark_baseline_parser, ops_cmd.cmd_benchmark_baseline),
        FuncCommand(
            "certification-automation-plan",
            certification_automation_plan_cmd.register_parser,
            certification_automation_plan_cmd.cmd_certification_automation_plan,
        ),
        FuncCommand("dbt-lineage", ops_cmd.register_dbt_lineage_parser, ops_cmd.cmd_dbt_lineage),
        FuncCommand(
            "integration-matrix-report",
            integration_matrix_report_cmd.register_parser,
            integration_matrix_report_cmd.cmd_integration_matrix_report,
        ),
        FuncCommand(
            "certification-suite",
            certification_suite_cmd.register_parser,
            certification_suite_cmd.cmd_certification_suite,
        ),
        FuncCommand("certification-pack", ops_cmd.register_certification_pack_parser, ops_cmd.cmd_certification_pack),
        FuncCommand(
            "route-certification-pack",
            ops_cmd.register_route_certification_pack_parser,
            ops_cmd.cmd_route_certification_pack,
        ),
        FuncCommand("cdc-handoff", ops_cmd.register_cdc_handoff_parser, ops_cmd.cmd_cdc_handoff),
        FuncCommand(
            "cdc-apply-certification",
            ops_cmd.register_cdc_apply_certification_parser,
            ops_cmd.cmd_cdc_apply_certification,
        ),
        FuncCommand(
            "cdc-observability-evidence",
            ops_cmd.register_cdc_observability_evidence_parser,
            ops_cmd.cmd_cdc_observability_evidence,
        ),
        FuncCommand(
            "cdc-recovery-evidence",
            ops_cmd.register_cdc_recovery_evidence_parser,
            ops_cmd.cmd_cdc_recovery_evidence,
        ),
        FuncCommand(
            "cdc-quarantine-inspect",
            ops_cmd.register_cdc_quarantine_inspect_parser,
            ops_cmd.cmd_cdc_quarantine_inspect,
        ),
        FuncCommand(
            "cdc-replay-execute",
            ops_cmd.register_cdc_replay_execute_parser,
            ops_cmd.cmd_cdc_replay_execute,
        ),
        FuncCommand(
            "cdc-compare-repair",
            ops_cmd.register_cdc_compare_repair_parser,
            ops_cmd.cmd_cdc_compare_repair,
        ),
        FuncCommand(
            "cdc-repair-execute",
            ops_cmd.register_cdc_repair_execute_parser,
            ops_cmd.cmd_cdc_repair_execute,
        ),
        FuncCommand(
            "cdc-retention-check",
            ops_cmd.register_cdc_retention_check_parser,
            ops_cmd.cmd_cdc_retention_check,
        ),
        FuncCommand(
            "cdc-resync-plan",
            ops_cmd.register_cdc_resync_plan_parser,
            ops_cmd.cmd_cdc_resync_plan,
        ),
        FuncCommand(
            "cdc-resync-execute",
            ops_cmd.register_cdc_resync_execute_parser,
            ops_cmd.cmd_cdc_resync_execute,
        ),
        FuncCommand(
            "cdc-schema-evolution-evidence",
            ops_cmd.register_cdc_schema_evolution_evidence_parser,
            ops_cmd.cmd_cdc_schema_evolution_evidence,
        ),
        FuncCommand(
            "cdc-schema-apply",
            ops_cmd.register_cdc_schema_apply_parser,
            ops_cmd.cmd_cdc_schema_apply,
        ),
        FuncCommand(
            "cdc-promotion-gate",
            ops_cmd.register_cdc_promotion_gate_parser,
            ops_cmd.cmd_cdc_promotion_gate,
        ),
        FuncCommand(
            "cdc-runtime-run",
            ops_cmd.register_cdc_runtime_run_parser,
            ops_cmd.cmd_cdc_runtime_run,
        ),
        FuncCommand(
            "safe-sample-runtime-run",
            ops_cmd.register_safe_sample_runtime_run_parser,
            ops_cmd.cmd_safe_sample_runtime_run,
        ),
        FuncCommand(
            "cdc-materialize-clickhouse",
            ops_cmd.register_cdc_materialize_clickhouse_parser,
            ops_cmd.cmd_cdc_materialize_clickhouse,
        ),
        FuncCommand(
            "cdc-materialize-clickhouse-typed",
            ops_cmd.register_cdc_materialize_clickhouse_typed_parser,
            ops_cmd.cmd_cdc_materialize_clickhouse_typed,
        ),
        FuncCommand("recovery-plan", ops_cmd.register_recovery_plan_parser, ops_cmd.cmd_recovery_plan),
        FuncCommand("reconcile", ops_cmd.register_reconcile_parser, ops_cmd.cmd_reconcile),
        FuncCommand("observability-pack", ops_cmd.register_observability_pack_parser, ops_cmd.cmd_observability_pack),
        FuncCommand("deploy-render", ops_cmd.register_deploy_render_parser, ops_cmd.cmd_deploy_render),
        FuncCommand("staging-evidence", ops_cmd.register_staging_evidence_parser, ops_cmd.cmd_staging_evidence),
        FuncCommand("catalog-publish", ops_cmd.register_catalog_publish_parser, ops_cmd.cmd_catalog_publish),
        FuncCommand(
            "live-certification-plan",
            ops_cmd.register_live_certification_plan_parser,
            ops_cmd.cmd_live_certification_plan,
        ),
        FuncCommand(
            "managed-credentials-readiness",
            ops_cmd.register_managed_credentials_readiness_parser,
            ops_cmd.cmd_managed_credentials_readiness,
        ),
        FuncCommand("benchmark-slo-gate", ops_cmd.register_benchmark_slo_gate_parser, ops_cmd.cmd_benchmark_slo_gate),
        FuncCommand(
            "performance-certification",
            ops_cmd.register_performance_certification_parser,
            ops_cmd.cmd_performance_certification,
        ),
        FuncCommand(
            "live-state-reconciliation",
            ops_cmd.register_live_state_reconciliation_parser,
            ops_cmd.cmd_live_state_reconciliation,
        ),
        FuncCommand(
            "release-evidence-pack", ops_cmd.register_release_evidence_pack_parser, ops_cmd.cmd_release_evidence_pack
        ),
        FuncCommand(
            "pre-release-checklist", ops_cmd.register_pre_release_checklist_parser, ops_cmd.cmd_pre_release_checklist
        ),
        ops_object_storage_cmd.object_storage_group(),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("ops", help="Operational maturity utilities")

    return CommandGroup(
        name="ops",
        help="Operational maturity utilities",
        build_parser=build,
        subcommands=sub,
        subdest="ops_cmd",
    )
