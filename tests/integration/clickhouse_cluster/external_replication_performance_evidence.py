"""Immutable evidence producer for external-publication performance observations."""

from __future__ import annotations

import hashlib
import json
import os
import platform
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from uuid import uuid4

from tests.integration.clickhouse_cluster.evidence_identity import (
    PERFORMANCE_V1_SOURCE_COMMIT,
    fixture_digest,
    historical_performance_binding,
    performance_fixture_digest,
    require_exact_source_commit,
    source_binding,
    tracked_file_bytes,
    tracked_source_tree,
)
from tests.integration.clickhouse_cluster.external_replication_performance_validation import (
    EXTERNAL_PERFORMANCE_COMMAND,
    EXTERNAL_PERFORMANCE_TEST_NODEID,
    SCHEMA_V2,
    validate_external_performance_receipt,
)

EXTERNAL_PERFORMANCE_RECEIPT = Path("test_artifacts/clickhouse-external-publication/benchmark-receipt.json")
EXTERNAL_PERFORMANCE_BUDGET = Path("tests/integration/clickhouse_cluster/external_replication_performance_budget.json")


def write_external_performance_receipt(
    *,
    canonical_digest: dict[str, Any],
    member_fanout: dict[str, Any],
    observation: dict[str, str],
    server_version: str,
) -> dict[str, Any]:
    """Write one immutable exact-commit observation; never replace prior evidence."""

    source_commit, source_tree = source_binding()
    benchmark_config = EXTERNAL_PERFORMANCE_BUDGET.read_bytes()
    budget = json.loads(benchmark_config)
    bound_fixture_digest = fixture_digest(source_commit)
    benchmark_config_sha256 = hashlib.sha256(benchmark_config).hexdigest()
    passed = canonical_digest.get("status") == "PASS" and member_fanout.get("status") == "PASS"
    evidence = {
        "schema_version": SCHEMA_V2,
        "status": "PASS" if passed else "FAIL",
        "evidence_scope": "local_synthetic",
        "production_certification": "UNVERIFIED",
        "source_commit": source_commit,
        "source_tree": source_tree,
        "tracked_tree_status": "clean",
        "fixture_digest": bound_fixture_digest,
        "benchmark_config_sha256": benchmark_config_sha256,
        "environment": {
            "clickhouse_version": server_version,
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "cpu_count": os.cpu_count() or 0,
        },
        "observation": observation,
        "canonical_digest": canonical_digest,
        "member_fanout": member_fanout,
    }
    validate_external_performance_receipt(
        evidence,
        source_commit=source_commit,
        source_tree=source_tree,
        fixture_digest=bound_fixture_digest,
        benchmark_config_sha256=benchmark_config_sha256,
        budget=budget,
    )
    _write_json_exclusive(EXTERNAL_PERFORMANCE_RECEIPT, evidence)
    return evidence


def verify_external_performance_receipt(
    *,
    expected_source_commit: str | None = None,
    receipt_path: Path | None = None,
) -> dict[str, Any]:
    """Validate a receipt against a caller-trusted commit, defaulting to clean HEAD."""

    resolved_source_tree: str | None = None
    if expected_source_commit is not None:
        require_exact_source_commit(expected_source_commit)
        if expected_source_commit != PERFORMANCE_V1_SOURCE_COMMIT:
            resolved_source_tree = tracked_source_tree(expected_source_commit)
    evidence = json.loads((receipt_path or EXTERNAL_PERFORMANCE_RECEIPT).read_text(encoding="utf-8"))
    schema_version = str(evidence.get("schema_version") or "")
    if expected_source_commit is None:
        source_commit, source_tree = source_binding()
        benchmark_config = EXTERNAL_PERFORMANCE_BUDGET.read_bytes()
        bound_fixture_digest = performance_fixture_digest(source_commit, schema_version)
    else:
        source_commit = expected_source_commit
        historical = historical_performance_binding(source_commit, schema_version)
        if historical is None:
            source_tree = resolved_source_tree or tracked_source_tree(source_commit)
            benchmark_config = tracked_file_bytes(source_commit, EXTERNAL_PERFORMANCE_BUDGET)
            bound_fixture_digest = performance_fixture_digest(source_commit, schema_version)
        else:
            source_tree = historical.source_tree
            benchmark_config = historical.benchmark_config
            bound_fixture_digest = historical.fixture_digest
    validate_external_performance_receipt(
        evidence,
        source_commit=source_commit,
        source_tree=source_tree,
        fixture_digest=bound_fixture_digest,
        benchmark_config_sha256=hashlib.sha256(benchmark_config).hexdigest(),
        budget=json.loads(benchmark_config),
    )
    return evidence


def _write_json_exclusive(target: Path, evidence: Mapping[str, Any]) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.parent / f".{target.name}.{uuid4().hex}.tmp"
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(evidence, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, target)
        directory = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


__all__ = [
    "EXTERNAL_PERFORMANCE_BUDGET",
    "EXTERNAL_PERFORMANCE_COMMAND",
    "EXTERNAL_PERFORMANCE_RECEIPT",
    "EXTERNAL_PERFORMANCE_TEST_NODEID",
    "validate_external_performance_receipt",
    "verify_external_performance_receipt",
    "write_external_performance_receipt",
]
