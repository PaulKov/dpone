from __future__ import annotations

import argparse

from dpone.commands.ops_parsers_cdc_runtime import (
    CREDENTIALS_SOURCE_CHOICES as CREDENTIALS_SOURCE_CHOICES,
)
from dpone.commands.ops_parsers_cdc_runtime import (
    register_cdc_materialize_clickhouse_parser as register_cdc_materialize_clickhouse_parser,
)
from dpone.commands.ops_parsers_cdc_runtime import (
    register_cdc_materialize_clickhouse_typed_parser as register_cdc_materialize_clickhouse_typed_parser,
)
from dpone.commands.ops_parsers_cdc_runtime import (
    register_cdc_runtime_run_parser as register_cdc_runtime_run_parser,
)
from dpone.contracts import LIVE_CERTIFICATION_PROFILE_CHOICES

_CREDENTIALS_SOURCE_CHOICES = CREDENTIALS_SOURCE_CHOICES


def register_docs_publish_pack_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "docs-publish-pack", help="Build README and GitHub Pages snippets from ops artifacts"
    )
    parser.add_argument("--output-dir", default=".dpone/docs-publish")
    parser.add_argument("--release", required=True)
    parser.add_argument("--artifact", action="append", default=[], help="Artifact reference as name=/path/to/file")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_manifest_bundle_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("manifest-bundle", help="Create a redacted portable support/release bundle")
    parser.add_argument("--output-dir", default=".dpone/manifest-bundles/latest")
    parser.add_argument("--bundle-id", required=True)
    parser.add_argument("--file", action="append", default=[], help="File reference as name=/path/to/file")
    parser.add_argument("--redact", action="store_true")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_runbook_pack_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("runbook-pack", help="Generate an operator runbook from ops artifacts")
    parser.add_argument("--output-dir", default=".dpone/runbooks/latest")
    parser.add_argument("--runbook-id", required=True)
    parser.add_argument("--title", required=True)
    parser.add_argument("--artifact", action="append", default=[], help="Artifact reference as name=/path/to/file")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_run_registry_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("run-registry", help="Record a dpone run result in the auditable run registry")
    parser.add_argument("--output-dir", default=".dpone/run-registry")
    parser.add_argument("--run-result", required=True)
    parser.add_argument("--artifact", action="append", default=[], help="Artifact reference as name=/path/to/file")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_lineage_export_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("lineage-export", help="Export a run registry entry as an OpenLineage event")
    parser.add_argument("--output-dir", default=".dpone/lineage/latest")
    parser.add_argument("--run-registry-entry", required=True)
    parser.add_argument("--namespace", default="dpone.local")
    parser.add_argument("--input", action="append", default=[], help="Input dataset as namespace=name")
    parser.add_argument("--output", action="append", default=[], help="Output dataset as namespace=name")
    parser.add_argument("--event-type", choices=["auto", "START", "COMPLETE", "FAIL"], default="auto")
    parser.add_argument(
        "--airflow-evidence-bundle",
        help="Optional final Airflow evidence bundle used for run correlation",
    )
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_benchmark_baseline_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("benchmark-baseline", help="Compare benchmark metrics against a baseline profile")
    parser.add_argument("--output-dir", default=".dpone/benchmarks/latest")
    parser.add_argument("--metrics-json", required=True)
    parser.add_argument("--baseline-json", required=True)
    parser.add_argument("--allowed-regression-ratio", type=float, default=0.10)
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_dbt_lineage_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("dbt-lineage", help="Export dbt artifacts as dpone lineage evidence")
    parser.add_argument("--output-dir", default=".dpone/dbt-lineage/latest")
    parser.add_argument("--manifest", required=True, help="Path to dbt target/manifest.json")
    parser.add_argument("--run-results", help="Path to dbt target/run_results.json")
    parser.add_argument("--run-registry-entry", help="Optional dpone run registry entry JSON")
    parser.add_argument("--namespace", default="dbt.local")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_certification_pack_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("certification-pack", help="Build connector certification 2.0 evidence pack")
    parser.add_argument("--output-dir", default=".dpone/certification-pack/latest")
    parser.add_argument("--pack-id", required=True)
    parser.add_argument("--artifact", action="append", default=[], help="Artifact reference as name=/path/to/file")
    parser.add_argument("--require", action="append", default=None, help="Required evidence artifact name")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_route_certification_pack_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "route-certification-pack",
        help="Generate readiness-compatible evidence pack for one source -> sink -> strategy route",
    )
    parser.add_argument("--output-dir", default=".dpone/route-certification-pack/latest")
    parser.add_argument("--source", required=True)
    parser.add_argument("--sink", required=True)
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--artifact", action="append", default=[], help="Evidence reference as name=/path/to/file")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_cdc_handoff_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "cdc-handoff",
        help="Evaluate CDC snapshot handoff and apply evidence for one source stream",
    )
    parser.add_argument("--output-dir", default=".dpone/cdc-handoff/latest")
    parser.add_argument("--source", required=True)
    parser.add_argument("--sink", required=True)
    parser.add_argument("--strategy", default="cdc")
    parser.add_argument("--source-dataset", required=True)
    parser.add_argument("--target-dataset", required=True)
    parser.add_argument("--artifact", action="append", default=[], help="CDC evidence reference as name=/path/to/file")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_cdc_apply_certification_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "cdc-apply-certification",
        help="Generate CDC apply correctness evidence and embedded handoff report",
    )
    parser.add_argument("--output-dir", default=".dpone/cdc-apply-certification/latest")
    parser.add_argument("--source", required=True)
    parser.add_argument("--sink", required=True)
    parser.add_argument("--strategy", default="cdc")
    parser.add_argument("--source-dataset", required=True)
    parser.add_argument("--target-dataset", required=True)
    parser.add_argument("--fixture-json", required=True, help="Credential-free CDC apply fixture JSON")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_cdc_observability_evidence_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "cdc-observability-evidence",
        help="Generate CDC lag, freshness, retention, offset, replay, and throughput evidence",
    )
    parser.add_argument("--output-dir", default=".dpone/cdc-observability/latest")
    parser.add_argument("--handoff-json", required=True, help="Path to cdc_handoff.json")
    parser.add_argument("--apply-certification-json", required=True, help="Path to cdc_apply_certification.json")
    parser.add_argument("--metrics-json", required=True, help="Path to normalized CDC telemetry JSON")
    parser.add_argument("--slo-json", help="Optional CDC SLO profile JSON")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_cdc_recovery_evidence_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "cdc-recovery-evidence",
        help="Generate CDC fault-injection recovery evidence from upstream CDC artifacts",
    )
    parser.add_argument("--output-dir", default=".dpone/cdc-recovery/latest")
    parser.add_argument("--handoff-json", required=True, help="Path to cdc_handoff.json")
    parser.add_argument("--apply-certification-json", required=True, help="Path to cdc_apply_certification.json")
    parser.add_argument("--observability-json", required=True, help="Path to cdc_observability.json")
    parser.add_argument("--scenario-json", required=True, help="Path to CDC fault-injection scenario JSON")
    parser.add_argument("--policy-json", help="Optional CDC recovery policy JSON")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_cdc_schema_evolution_evidence_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "cdc-schema-evolution-evidence",
        help="Generate CDC schema evolution and DDL governance evidence",
    )
    parser.add_argument("--output-dir", default=".dpone/cdc-schema-evolution/latest")
    parser.add_argument("--handoff-json", required=True, help="Path to cdc_handoff.json")
    parser.add_argument("--apply-certification-json", required=True, help="Path to cdc_apply_certification.json")
    parser.add_argument("--observability-json", required=True, help="Path to cdc_observability.json")
    parser.add_argument("--recovery-json", required=True, help="Path to cdc_recovery_evidence.json")
    parser.add_argument("--schema-change-json", required=True, help="Path to CDC schema change and plan JSON")
    parser.add_argument("--policy-json", help="Optional CDC schema evolution policy JSON")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_cdc_promotion_gate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "cdc-promotion-gate",
        help="Evaluate CDC replication readiness and offset promotion evidence",
    )
    parser.add_argument("--output-dir", default=".dpone/cdc-promotion/latest")
    parser.add_argument("--apply-certification-json", required=True, help="Path to cdc_apply_certification.json")
    parser.add_argument("--handoff-json", required=True, help="Path to cdc_handoff.json")
    parser.add_argument("--observability-json", required=True, help="Path to cdc_observability.json")
    parser.add_argument("--recovery-json", required=True, help="Path to cdc_recovery_evidence.json")
    parser.add_argument("--schema-evolution-json", required=True, help="Path to cdc_schema_evolution.json")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_recovery_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("recovery-plan", help="Plan recovery for failed runs, locks, and load packages")
    parser.add_argument("--state-dir", default=".dpone/orchestration-state")
    parser.add_argument("--lock-dir", default=".dpone/locks")
    parser.add_argument("--load-package-dir", default=".dpone/load-packages")
    parser.add_argument("--output-dir", default=".dpone/recovery/latest")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_reconcile_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("reconcile", help="Run bounded source-target reconciliation with repair actions")
    parser.add_argument("--output-dir", default=".dpone/reconciliation/latest")
    parser.add_argument("--source-rows-json", required=True)
    parser.add_argument("--target-rows-json", required=True)
    parser.add_argument("--key", required=True, help="Comma-separated key columns")
    parser.add_argument("--compare-columns", help="Comma-separated compare columns")
    parser.add_argument("--delete-column", help="Source delete marker/timestamp column")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_observability_pack_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("observability-pack", help="Generate Grafana dashboard and Prometheus alerts")
    parser.add_argument("--output-dir", default=".dpone/observability-pack/latest")
    parser.add_argument("--service-name", default="dpone")
    parser.add_argument("--dashboard-title", default="dpone runtime")
    parser.add_argument(
        "--alert", action="append", default=[], help="Alert threshold as metric.max=value or metric.min=value"
    )
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_deploy_render_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("deploy-render", help="Render Docker/Kubernetes/Airflow/Dagster deployment profile")
    parser.add_argument("--output-dir", default=".dpone/deploy/latest")
    parser.add_argument("--target", choices=["docker-compose", "k8s-cronjob", "airflow", "dagster"], required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--selector")
    parser.add_argument("--image", default="ghcr.io/paulkov/dpone:latest")
    parser.add_argument("--schedule", default="0 2 * * *")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_staging_evidence_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("staging-evidence", help="Validate object-storage staging manifest")
    parser.add_argument("--output-dir", default=".dpone/staging-evidence/latest")
    parser.add_argument("--manifest", required=True, help="Path to object storage staging manifest JSON")
    parser.add_argument("--sink", required=True, choices=["mssql", "postgres", "clickhouse", "bigquery"])
    parser.add_argument("--target-table", required=True)
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_catalog_publish_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("catalog-publish", help="Build OpenLineage, dbt, and DataHub catalog artifacts")
    parser.add_argument("--output-dir", default=".dpone/catalog/latest")
    parser.add_argument("--run-registry-entry", required=True)
    parser.add_argument("--namespace", default="dpone.local")
    parser.add_argument("--input", action="append", default=[], help="Input dataset as namespace=name")
    parser.add_argument("--output", action="append", default=[], help="Output dataset as namespace=name")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_live_certification_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("live-certification-plan", help="Plan local-live or vendor-live certification")
    parser.add_argument("--output-dir", default=".dpone/live-certification/latest")
    parser.add_argument("--profile", choices=LIVE_CERTIFICATION_PROFILE_CHOICES, default="local_live")
    parser.add_argument("--row-count", type=int, default=10000)
    parser.add_argument("--include-vendor-live", action="store_true")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_managed_credentials_readiness_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "managed-credentials-readiness",
        help="Verify required managed/vendor credential environment without exposing values",
    )
    parser.add_argument("--output-dir", default=".dpone/managed-credentials/latest")
    parser.add_argument("--profile", default="vendor_live")
    parser.add_argument(
        "--required-env",
        action="append",
        default=[],
        help="Required environment variable name; can be repeated",
    )
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_benchmark_slo_gate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("benchmark-slo-gate", help="Evaluate benchmark baseline and SLO objectives together")
    parser.add_argument("--output-dir", default=".dpone/benchmark-slo/latest")
    parser.add_argument("--metrics-json", required=True)
    parser.add_argument("--baseline-json", required=True)
    parser.add_argument("--objectives-json", required=True)
    parser.add_argument("--allowed-regression-ratio", type=float, default=0.10)
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_performance_certification_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "performance-certification", help="Build release-grade performance certification evidence"
    )
    parser.add_argument("--output-dir", default=".dpone/performance-certification/latest")
    parser.add_argument("--profile", default="real_local")
    parser.add_argument("--row-count", type=int, default=10000)
    parser.add_argument("--metrics-json", required=True)
    parser.add_argument("--minimum-json", default="{}")
    parser.add_argument("--maximum-json", default="{}")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_live_state_reconciliation_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "live-state-reconciliation", help="Build live state and physical-delete reconciliation evidence"
    )
    parser.add_argument("--output-dir", default=".dpone/live-state-reconciliation/latest")
    parser.add_argument("--profile", default="real_local")
    parser.add_argument("--artifact", action="append", default=[], help="Artifact reference as name=/path/to/file")
    parser.add_argument("--require", action="append", default=None, help="Required evidence artifact name")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_release_evidence_pack_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("release-evidence-pack", help="Build final release evidence go/no-go pack")
    parser.add_argument("--output-dir", default=".dpone/release-evidence/latest")
    parser.add_argument("--release", required=True)
    parser.add_argument("--profile", default="real_local")
    parser.add_argument("--artifact", action="append", default=[], help="Artifact reference as name=/path/to/file")
    parser.add_argument("--require", action="append", default=None, help="Required evidence artifact name")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_pre_release_checklist_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("pre-release-checklist", help="Build minor/major pre-release checklist evidence")
    parser.add_argument("--output-dir", default=".dpone/pre-release/latest")
    parser.add_argument("--release", required=True)
    parser.add_argument("--release-type", choices=["patch", "minor", "major"], default="minor")
    parser.add_argument("--check", action="append", default=[], help="Check result as name=true|false")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser
