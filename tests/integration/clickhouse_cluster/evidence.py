"""Order-independent receipt producer for the cluster Docker profile."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
from pathlib import Path
from typing import Any

RECEIPT = Path("test_artifacts/clickhouse-cluster-publication/docker-receipt.json")
EXTERNAL_RECEIPT = Path("test_artifacts/clickhouse-external-publication/docker-receipt.json")
EXTERNAL_PERFORMANCE_RECEIPT = Path("test_artifacts/clickhouse-external-publication/benchmark-receipt.json")
EXTERNAL_PERFORMANCE_BUDGET = Path("tests/integration/clickhouse_cluster/external_replication_performance_budget.json")
_FIXTURE_FILES = (
    Path("tests/integration/clickhouse_cluster/docker-compose.yml"),
    Path("tests/integration/clickhouse_cluster/conftest.py"),
    Path("tests/integration/clickhouse_cluster/evidence.py"),
    Path("tests/integration/clickhouse_cluster/external_replication_live_support.py"),
    EXTERNAL_PERFORMANCE_BUDGET,
    Path("tests/integration/clickhouse_cluster/test_clickhouse_external_replication_live.py"),
    Path("tests/integration/clickhouse_cluster/test_clickhouse_external_replication_performance_live.py"),
    Path("tests/integration/clickhouse_cluster/config/keeper/keeper.xml"),
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


def reset_external_performance_receipt() -> None:
    """Discard only stale external-publication benchmark evidence."""

    if EXTERNAL_PERFORMANCE_RECEIPT.exists():
        EXTERNAL_PERFORMANCE_RECEIPT.unlink()


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


def write_external_performance_receipt(
    *,
    canonical_digest: dict[str, Any],
    member_fanout: dict[str, Any],
    server_version: str,
) -> None:
    """Atomically write exact-commit local performance evidence without secrets."""

    source_commit, source_tree = _source_binding()
    benchmark_config = EXTERNAL_PERFORMANCE_BUDGET.read_bytes()
    passed = canonical_digest.get("status") == "PASS" and member_fanout.get("status") == "PASS"
    evidence = {
        "schema_version": "dpone.clickhouse.external-publication-benchmark.v1",
        "status": "PASS" if passed else "FAIL",
        "evidence_scope": "local_synthetic",
        "production_certification": "UNVERIFIED",
        "source_commit": source_commit,
        "source_tree": source_tree,
        "tracked_tree_status": "clean",
        "fixture_digest": _fixture_digest(source_commit),
        "benchmark_config_sha256": hashlib.sha256(benchmark_config).hexdigest(),
        "environment": {
            "clickhouse_version": server_version,
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "cpu_count": os.cpu_count() or 0,
        },
        "canonical_digest": canonical_digest,
        "member_fanout": member_fanout,
    }
    EXTERNAL_PERFORMANCE_RECEIPT.parent.mkdir(parents=True, exist_ok=True)
    temporary = EXTERNAL_PERFORMANCE_RECEIPT.with_suffix(".tmp")
    temporary.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(EXTERNAL_PERFORMANCE_RECEIPT)


def _git(*args: str, text: bool = True) -> str | bytes:
    return subprocess.run(
        ("git", *args),
        check=True,
        capture_output=True,
        text=text,
    ).stdout


def _source_binding() -> tuple[str, str]:
    if str(_git("status", "--porcelain=v1", "--untracked-files=no")).strip():
        raise RuntimeError("tracked worktree must be clean before writing Docker evidence")
    return (
        str(_git("rev-parse", "HEAD")).strip(),
        str(_git("rev-parse", "HEAD^{tree}")).strip(),
    )


def _fixture_digest(source_commit: str) -> str:
    digest = hashlib.sha256()
    for path in _FIXTURE_FILES:
        digest.update(path.as_posix().encode("utf-8"))
        digest.update(b"\0")
        content = _git("show", f"{source_commit}:{path.as_posix()}", text=False)
        if not isinstance(content, bytes):
            raise TypeError("git blob output must be bytes")
        digest.update(content)
        digest.update(b"\0")
    return digest.hexdigest()
