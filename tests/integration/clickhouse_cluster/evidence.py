"""Order-independent receipt producer for the cluster Docker profile."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from tests.integration.clickhouse_cluster.evidence_identity import FIXTURE_FILES
from tests.integration.clickhouse_cluster.evidence_identity import fixture_digest as _fixture_digest
from tests.integration.clickhouse_cluster.evidence_identity import source_binding as _source_binding

_FIXTURE_FILES = FIXTURE_FILES

RECEIPT = Path("test_artifacts/clickhouse-cluster-publication/docker-receipt.json")
EXTERNAL_RECEIPT = Path("test_artifacts/clickhouse-external-publication/docker-receipt.json")

SCENARIOS = (
    "keeper_cas_and_log_comment",
    "normal_existing_and_absent_target",
    "lost_publication_response",
    "partial_in_progress_then_converged",
    "terminal_partial",
    "worker_race",
    "lost_cas_response",
    "duplicate_correlation_token",
    "lost_cleanup_response",
    "identity_and_membership_drift",
    "partial_authority_bootstrap",
    "queue_status_and_host_matrix",
)
EXTERNAL_SCENARIOS = (
    "external_replication_fresh_cleanup",
    "external_replication_lost_load_response",
    "external_replication_lost_publication_response",
    "external_replication_lost_cleanup_response",
    "external_replication_staging_fresh_service_recovery",
    "external_replication_empty_generation",
    "external_replication_terminal_partial_no_redispatch",
)


def reset_receipt() -> None:
    """Discard only stale internal-publication evidence."""

    if RECEIPT.exists():
        RECEIPT.unlink()


def reset_external_receipt() -> None:
    """Discard only stale external-publication evidence."""

    if EXTERNAL_RECEIPT.exists():
        EXTERNAL_RECEIPT.unlink()


def record_scenario(
    name: str,
    result: str,
    *,
    server_version: str,
    details: dict[str, Any] | None = None,
) -> None:
    """Merge one observed scenario and recompute the complete-matrix status."""

    if name not in SCENARIOS:
        raise ValueError(f"unknown cluster publication scenario: {name}")
    evidence = json.loads(RECEIPT.read_text()) if RECEIPT.exists() else {}
    results = {item: "unverified" for item in SCENARIOS}
    results.update(evidence.get("scenario_results", {}))
    results[name] = result
    source_commit, source_tree = _source_binding()
    evidence.update(
        {
            "schema_version": "dpone.clickhouse.cluster-publication-docker.v1",
            "source_commit": source_commit,
            "source_tree": source_tree,
            "tracked_tree_status": "clean",
            "fixture_digest": _fixture_digest(source_commit),
            "server_version": server_version,
            "replicas": 2,
            "scenario_results": results,
            "status": ("passed_live" if all(value == "passed_live" for value in results.values()) else "unverified"),
        }
    )
    if details:
        evidence.update(details)
    RECEIPT.parent.mkdir(parents=True, exist_ok=True)
    RECEIPT.write_text(json.dumps(evidence, sort_keys=True) + "\n")


def record_external_scenario(
    name: str,
    result: str,
    *,
    server_version: str,
    details: dict[str, Any] | None = None,
) -> None:
    """Record only external-replication Docker evidence with per-case scope."""

    if name not in EXTERNAL_SCENARIOS:
        raise ValueError(f"unknown external publication scenario: {name}")
    evidence = json.loads(EXTERNAL_RECEIPT.read_text()) if EXTERNAL_RECEIPT.exists() else {}
    scenarios: dict[str, dict[str, Any]] = {
        item: {"status": "UNVERIFIED", "evidence_scope": "local_synthetic"} for item in EXTERNAL_SCENARIOS
    }
    scenarios.update(evidence.get("scenarios", {}))
    scenarios[name] = {
        "status": result,
        "evidence_scope": "local_synthetic",
        "details": details or {},
    }
    source_commit, source_tree = _source_binding()
    evidence.update(
        {
            "schema_version": "dpone.clickhouse.external-publication-docker.v1",
            "source_commit": source_commit,
            "source_tree": source_tree,
            "tracked_tree_status": "clean",
            "fixture_digest": _fixture_digest(source_commit),
            "server_version": server_version,
            "replicas": 2,
            "evidence_scope": "local_synthetic",
            "scenarios": scenarios,
            "status": "PASS" if all(value["status"] == "PASS" for value in scenarios.values()) else "UNVERIFIED",
            "production_certification": "UNVERIFIED",
        }
    )
    EXTERNAL_RECEIPT.parent.mkdir(parents=True, exist_ok=True)
    EXTERNAL_RECEIPT.write_text(json.dumps(evidence, sort_keys=True) + "\n")
