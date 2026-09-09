from __future__ import annotations

import argparse


def register_certification_run_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("certification-run", help="Run credential-free certification matrix artifacts")
    parser.add_argument("--artifact-dir", default="test_artifacts/certification/latest")
    parser.add_argument("--source")
    parser.add_argument("--sink")
    parser.add_argument("--strategy")
    parser.add_argument("--row-count", type=int)
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_artifact_index_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("artifact-index", help="Index local ops artifacts for docs and release review")
    parser.add_argument("--output-dir", default=".dpone/artifact-index")
    parser.add_argument("--root", action="append")
    parser.add_argument("--release")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_certification_history_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("certification-history", help="Record certification history and regression deltas")
    parser.add_argument("--history-dir", default="test_artifacts/certification/history")
    parser.add_argument("--release", required=True)
    parser.add_argument("--current-report", required=True)
    parser.add_argument("--previous-report")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_connector_badges_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("connector-badges", help="Generate connector badge matrix artifacts")
    parser.add_argument("--output-dir", default="test_artifacts/certification/badges")
    parser.add_argument("--history-index")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_contract_check_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("contract-check", help="Evaluate a data contract against JSON rows")
    parser.add_argument("--rows-json", required=True)
    parser.add_argument("--contract-json", required=True)
    parser.add_argument("--mode", choices=["fail", "warn"], default="fail")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_quarantine_export_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("quarantine-export", help="Export safe DLQ metadata for a run")
    parser.add_argument("--dir", default=".dpone/dlq")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_quarantine_replay_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("quarantine-replay", help="Build a safe DLQ replay preview")
    parser.add_argument("--dir", default=".dpone/dlq")
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Deprecated: generic replay requires a route-specific resolver and sink executor",
    )
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_package_start_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("package-start", help="Create a load package")
    parser.add_argument("--dir", default=".dpone/load-packages")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--chunk-id")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_package_commit_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("package-commit", help="Mark a load package committed")
    parser.add_argument("--dir", default=".dpone/load-packages")
    parser.add_argument("--load-id", required=True)
    parser.add_argument("--rows-loaded", type=int, required=True)
    parser.add_argument("--state-after-json", default="{}")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_rollback_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("rollback-plan", help="Build a target-native rollback plan")
    parser.add_argument("--sink", required=True, choices=["mssql", "postgres", "clickhouse", "bigquery"])
    parser.add_argument("--target", required=True)
    parser.add_argument("--load-id", required=True)
    parser.add_argument("--strategy", default="shadow_swap")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_rollback_apply_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("rollback-apply", help="Apply an externally reviewed rollback plan")
    parser.add_argument("--plan-json", required=True)
    parser.add_argument("--yes", action="store_true")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_rollback_execute_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("rollback-execute", help="Safely preview or apply a rollback plan")
    parser.add_argument("--sink", required=True, choices=["mssql", "postgres", "clickhouse", "bigquery"])
    parser.add_argument("--target", required=True)
    parser.add_argument("--load-id", required=True)
    parser.add_argument("--strategy", default="shadow_swap")
    parser.add_argument("--require-backup", action="store_true")
    parser.add_argument("--yes", action="store_true")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_marketplace_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("marketplace", help="Render connector capability marketplace")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_evidence_bundle_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("evidence-bundle", help="Build an auditable ops evidence bundle")
    parser.add_argument("--artifact-dir", default="test_artifacts/ops/evidence/latest")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--sink", required=True)
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--rows-json", required=True)
    parser.add_argument("--contract-json", required=True)
    parser.add_argument("--row-count", type=int)
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_evidence_chain_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("evidence-chain", help="Append a tamper-evident evidence chain entry")
    parser.add_argument("--chain-dir", default=".dpone/evidence-chain")
    parser.add_argument("--release", required=True)
    parser.add_argument("--artifact-index", required=True)
    parser.add_argument("--previous-entry")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_go_live_gate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("go-live-gate", help="Evaluate an ops evidence bundle")
    parser.add_argument("--bundle-json", required=True)
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_policy_evaluate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("policy-evaluate", help="Evaluate policy-as-code against an evidence bundle")
    parser.add_argument("--bundle-json", required=True)
    parser.add_argument("--policy-json", required=True)
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_diff_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("diff", help="Compare bounded source and target row sets")
    parser.add_argument("--source-rows-json", required=True)
    parser.add_argument("--target-rows-json", required=True)
    parser.add_argument("--key", required=True, help="Comma-separated business key columns")
    parser.add_argument("--compare-columns", help="Comma-separated columns to compare; defaults to all non-key columns")
    parser.add_argument("--sample-limit", type=int, default=20)
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_security_audit_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("security-audit", help="Audit manifests and logs for credential safety")
    parser.add_argument("--manifest-json")
    parser.add_argument("--log-text")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_slo_evaluate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("slo-evaluate", help="Evaluate runtime metrics against SLO objectives")
    parser.add_argument("--metrics-json", required=True)
    parser.add_argument("--objectives-json", required=True)
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_incident_pack_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("incident-pack", help="Build one incident/release review artifact bundle")
    parser.add_argument("--artifact-dir", default=".dpone/incidents/latest")
    parser.add_argument("--incident-id", required=True)
    parser.add_argument("--title", required=True)
    parser.add_argument("--severity", default="review")
    parser.add_argument("--artifact", action="append", default=[], help="Artifact reference as name=/path/to/file")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_release_gate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("release-gate", help="Aggregate ops artifacts into one release readiness gate")
    parser.add_argument("--artifact-dir", default=".dpone/releases/latest")
    parser.add_argument("--release", required=True)
    parser.add_argument("--artifact", action="append", default=[], help="Artifact reference as name=/path/to/file")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_release_rc_finalize_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "release-rc-finalize",
        help="Finalize a release-candidate merge train, version context, and evidence pack before tagging",
    )
    parser.add_argument("--output-dir", default=".dpone/release-rc-finalize/latest")
    parser.add_argument("--release", required=True)
    parser.add_argument("--previous-release", required=True)
    parser.add_argument("--package-version", required=True)
    parser.add_argument("--merge-train-json", required=True, help="Path to merge_train.json in base-to-head order")
    parser.add_argument(
        "--mode",
        choices=["pre_merge", "post_merge"],
        default="pre_merge",
        help="pre_merge validates clean open PRs; post_merge requires every PR to be merged",
    )
    parser.add_argument("--artifact", action="append", default=[], help="Evidence reference as name=/path/to/file")
    parser.add_argument(
        "--require-artifact",
        action="append",
        default=None,
        help="Required release RC evidence domain; can be repeated",
    )
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_release_rc_collect_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "release-rc-collect",
        help="Release RC collector: collect GitHub CLI PR exports and evidence refs for finalization",
    )
    parser.add_argument(
        "--output-dir",
        default=".dpone/release-rc-collect/latest",
        help="Release RC collector output directory",
    )
    parser.add_argument("--release", required=True)
    parser.add_argument("--previous-release", required=True)
    parser.add_argument("--package-version", required=True)
    parser.add_argument("--base-branch", required=True)
    parser.add_argument("--head-branch", required=True)
    parser.add_argument(
        "--pull-request-json",
        action="append",
        default=[],
        help="Path to `gh pr view --json ...` output; repeat in base-to-head order",
    )
    parser.add_argument("--pr-json", action="append", default=[], help="Short alias for --pull-request-json")
    parser.add_argument("--artifact", action="append", default=[], help="Evidence reference as name=/path/to/file")
    parser.add_argument(
        "--require-artifact",
        action="append",
        default=None,
        help="Required release RC evidence domain; can be repeated",
    )
    parser.add_argument("--finalizer-output-dir", default=".dpone/release-rc-finalize/latest")
    parser.add_argument(
        "--mode",
        choices=["pre_merge", "post_merge"],
        default="pre_merge",
        help="Mode to embed in the generated release-rc-finalize command",
    )
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_production_maturity_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "production-maturity",
        help="Aggregate certification, CDC, performance, security and governance evidence into one GA gate",
    )
    parser.add_argument("--output-dir", default=".dpone/production-maturity/latest")
    parser.add_argument("--release", required=True)
    parser.add_argument("--artifact", action="append", default=[], help="Artifact reference as name=/path/to/file")
    parser.add_argument(
        "--require",
        action="append",
        default=None,
        help="Required evidence domain; can be repeated",
    )
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_industrial_readiness_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "industrial-readiness",
        help="Aggregate local matrix, correctness, reliability, performance, UX, and governance evidence",
    )
    parser.add_argument("--output-dir", default=".dpone/industrial-readiness/latest")
    parser.add_argument("--release", required=True)
    parser.add_argument("--artifact", action="append", default=[], help="Artifact reference as name=/path/to/file")
    parser.add_argument("--require", action="append", default=None, help="Required evidence domain; can be repeated")
    parser.add_argument(
        "--case",
        action="append",
        default=[],
        help="Required matrix case as source:sink:strategy; can be repeated",
    )
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_release_orchestrator_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("release-orchestrator", help="Run the standard ops release evidence pipeline")
    parser.add_argument("--output-dir", default=".dpone/release")
    parser.add_argument("--release", required=True)
    parser.add_argument("--root", action="append")
    parser.add_argument("--artifact", action="append", default=[], help="Artifact reference as name=/path/to/file")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_release_promote_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("release-promote", help="Build an environment promotion evidence manifest")
    parser.add_argument("--output-dir", default=".dpone/promotions/latest")
    parser.add_argument("--release", required=True)
    parser.add_argument("--from-env", required=True)
    parser.add_argument("--to-env", required=True)
    parser.add_argument("--artifact", action="append", default=[], help="Artifact reference as name=/path/to/file")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_env_drift_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("env-drift", help="Compare environment manifests and block unapproved drift")
    parser.add_argument("--output-dir", default=".dpone/env-drift/latest")
    parser.add_argument("--source-env", required=True)
    parser.add_argument("--target-env", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--allowlist-path", action="append", default=[])
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_change_request_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("change-request", help="Build a machine-readable release approval artifact")
    parser.add_argument("--output-dir", default=".dpone/change-requests/latest")
    parser.add_argument("--change-id", required=True)
    parser.add_argument("--release", required=True)
    parser.add_argument("--target-env", required=True)
    parser.add_argument("--risk-level", choices=["low", "medium", "high", "critical"], required=True)
    parser.add_argument("--requested-by", required=True)
    parser.add_argument("--approver", action="append", default=[])
    parser.add_argument("--expires-at")
    parser.add_argument("--artifact", action="append", default=[], help="Artifact reference as name=/path/to/file")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_approval_record_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("approval-record", help="Record approval or rejection for a change request")
    parser.add_argument("--output-dir", default=".dpone/approvals/latest")
    parser.add_argument("--change-request", required=True)
    parser.add_argument("--actor", required=True)
    parser.add_argument("--decision", choices=["approved", "rejected"], required=True)
    parser.add_argument("--comment", default="")
    parser.add_argument("--quorum-required", type=int, default=1)
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_deployment_record_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("deployment-record", help="Record the factual deployment outcome")
    parser.add_argument("--output-dir", default=".dpone/deployments/latest")
    parser.add_argument("--deployment-id", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--actor", required=True)
    parser.add_argument("--approval-record", required=True)
    parser.add_argument("--status", choices=["succeeded", "completed", "failed", "partial"], required=True)
    parser.add_argument("--post-check", action="append", default=[], help="Post-deploy check as name=true|false")
    parser.add_argument("--rollback-artifact")
    parser.add_argument("--started-at")
    parser.add_argument("--finished-at")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_post_deploy_verify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("post-deploy-verify", help="Aggregate final post-deploy checks")
    parser.add_argument("--output-dir", default=".dpone/post-deploy/latest")
    parser.add_argument("--deployment-record", required=True)
    parser.add_argument("--artifact", action="append", default=[], help="Artifact reference as name=/path/to/file")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_release_close_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("release-close", help="Write final release close or rollback-required evidence")
    parser.add_argument("--output-dir", default=".dpone/release-close/latest")
    parser.add_argument("--release", required=True)
    parser.add_argument("--closed-by", required=True)
    parser.add_argument("--post-deploy-verify", required=True)
    parser.add_argument("--notes", default="")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser
