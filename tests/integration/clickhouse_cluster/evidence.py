"""Order-independent receipt producer for the cluster Docker profile."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

RECEIPT = Path("test_artifacts/clickhouse-cluster-publication/docker-receipt.json")

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


def reset_receipt() -> None:
    """Start one pytest session without reusing stale generated evidence."""

    if RECEIPT.exists():
        RECEIPT.unlink()


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
    evidence.update(
        {
            "schema_version": "dpone.clickhouse.cluster-publication-docker.v1",
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
