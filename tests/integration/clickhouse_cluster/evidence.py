"""Order-independent receipt producer for the cluster Docker profile."""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

RECEIPT = Path("test_artifacts/clickhouse-cluster-publication/docker-receipt.json")
EXTERNAL_RECEIPT = Path("test_artifacts/clickhouse-external-publication/docker-receipt.json")
_FIXTURE_FILES = (
    Path("tests/integration/clickhouse_cluster/docker-compose.yml"),
    Path("tests/integration/clickhouse_cluster/conftest.py"),
    Path("tests/integration/clickhouse_cluster/evidence.py"),
    Path("tests/integration/clickhouse_cluster/test_clickhouse_external_replication_live.py"),
    Path("tests/integration/clickhouse_cluster/config/node1/cluster.xml"),
    Path("tests/integration/clickhouse_cluster/config/node2/cluster.xml"),
)

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
EXTERNAL_SCENARIOS = ("external_replication_fresh_cleanup",)


def reset_receipt() -> None:
    """Start one pytest session without reusing stale generated evidence."""

    if RECEIPT.exists():
        RECEIPT.unlink()
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
    evidence.update(
        {
            "schema_version": "dpone.clickhouse.cluster-publication-docker.v1",
            "source_commit": _source_commit(),
            "fixture_digest": _fixture_digest(),
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
    scenarios = {item: {"status": "unverified", "evidence_scope": "local_synthetic"} for item in EXTERNAL_SCENARIOS}
    scenarios.update(evidence.get("scenarios", {}))
    scenarios[name] = {
        "status": result,
        "evidence_scope": "local_synthetic",
        "details": details or {},
    }
    evidence.update(
        {
            "schema_version": "dpone.clickhouse.external-publication-docker.v1",
            "source_commit": _source_commit(),
            "fixture_digest": _fixture_digest(),
            "server_version": server_version,
            "replicas": 2,
            "evidence_scope": "local_synthetic",
            "scenarios": scenarios,
            "status": (
                "passed_live" if all(value["status"] == "passed_live" for value in scenarios.values()) else "unverified"
            ),
            "production_certification": "UNVERIFIED",
        }
    )
    EXTERNAL_RECEIPT.parent.mkdir(parents=True, exist_ok=True)
    EXTERNAL_RECEIPT.write_text(json.dumps(evidence, sort_keys=True) + "\n")


def _source_commit() -> str:
    return subprocess.run(
        ("git", "rev-parse", "HEAD"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _fixture_digest() -> str:
    digest = hashlib.sha256()
    for path in _FIXTURE_FILES:
        digest.update(path.as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()
