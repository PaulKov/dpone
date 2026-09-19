"""Exact-commit performance evidence for bounded external publication."""

from __future__ import annotations

import json
import os
import statistics
import time
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from dpone.runtime.sinks.clickhouse_external_replication_canonical import canonical_rows_digest
from tests.integration.clickhouse_cluster.external_replication_live_support import (
    CLUSTER,
    execute,
    publication_case,
)
from tests.integration.clickhouse_cluster.external_replication_performance_evidence import (
    EXTERNAL_PERFORMANCE_BUDGET,
    EXTERNAL_PERFORMANCE_COMMAND,
    EXTERNAL_PERFORMANCE_TEST_NODEID,
    verify_external_performance_receipt,
    write_external_performance_receipt,
)

pytestmark = pytest.mark.integration_live


@pytest.mark.skipif(
    os.getenv("DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION") != "1",
    reason="set DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION=1 for the opt-in Docker fixture",
)
def test_external_replication_performance_budget() -> None:
    """Measure the real digest and two-member sink path against declared budgets."""

    started_at = datetime.now(UTC)
    observation_id = uuid4().hex
    budget = json.loads(EXTERNAL_PERFORMANCE_BUDGET.read_text(encoding="utf-8"))
    server_version = execute(18123, "SELECT version()")[0][0]
    canonical_result = _empty_canonical_result(budget["canonical_digest"])
    fanout_result = _empty_fanout_result(budget["member_fanout"])
    try:
        canonical_result = _benchmark_canonical_digest(budget["canonical_digest"])
        fanout_result = _benchmark_member_fanout(budget["member_fanout"])
    except Exception as exc:
        target = canonical_result if canonical_result.get("status") != "PASS" else fanout_result
        target["status"] = "FAIL"
        target["error_type"] = type(exc).__name__
        write_external_performance_receipt(
            canonical_digest=canonical_result,
            member_fanout=fanout_result,
            observation=_observation(observation_id, started_at),
            server_version=server_version,
        )
        raise

    write_external_performance_receipt(
        canonical_digest=canonical_result,
        member_fanout=fanout_result,
        observation=_observation(observation_id, started_at),
        server_version=server_version,
    )
    persisted = verify_external_performance_receipt()
    assert persisted["status"] == "PASS"


def _benchmark_canonical_digest(budget: dict[str, Any]) -> dict[str, Any]:
    _require_positive_budget(budget, "rows", "trials")
    rows = [
        (index, f"value-{index % 1000:04d}", None if index % 7 == 0 else index % 97)
        for index in range(int(budget["rows"]))
    ]
    warmups = int(budget["warmups"])
    trials = int(budget["trials"])
    digests: list[str] = []
    trial_seconds: list[float] = []
    for index in range(warmups + trials):
        started_ns = time.monotonic_ns()
        digest = canonical_rows_digest(rows)
        elapsed = (time.monotonic_ns() - started_ns) / 1_000_000_000
        if index >= warmups:
            digests.append(digest)
            trial_seconds.append(elapsed)
    maximum = max(trial_seconds)
    stable = len(set(digests)) == 1
    passed = stable and maximum <= float(budget["max_trial_seconds"])
    return {
        "rows": len(rows),
        "columns": int(budget["columns"]),
        "warmups": warmups,
        "trials": trials,
        "trial_seconds": _rounded(trial_seconds),
        "median_seconds": round(statistics.median(trial_seconds), 6),
        "max_seconds": round(maximum, 6),
        "budget_max_seconds": float(budget["max_trial_seconds"]),
        "digest": digests[0],
        "stable_across_trials": stable,
        "status": "PASS" if passed else "FAIL",
    }


def _benchmark_member_fanout(budget: dict[str, Any]) -> dict[str, Any]:
    _require_positive_budget(budget, "members", "logical_rows", "physical_rows", "trials", "max_source_bytes")
    logical_rows = int(budget["logical_rows"])
    members = int(budget["members"])
    warmups = int(budget["warmups"])
    trials = int(budget["trials"])
    rows = [{"id": index} for index in range(logical_rows)]
    expected_digest = canonical_rows_digest((index,) for index in range(logical_rows))
    trial_seconds: list[float] = []
    last_counts: list[int] = []
    digests_equal = False
    cleanup_proven = False
    publication_phase = "UNKNOWN"

    for index in range(warmups + trials):
        sink, config, payload, database = publication_case(
            f"benchmark_{index}",
            rows=rows,
            source_byte_budget=int(budget["max_source_bytes"]),
            content_row_budget=logical_rows,
        )
        try:
            started_ns = time.monotonic_ns()
            admitted = sink._full_refresh_publication.prepare_admission(config)
            sink.preflight_before_extract(load_config=admitted)
            result = sink.load(admitted, payload)
            elapsed = (time.monotonic_ns() - started_ns) / 1_000_000_000
            receipt = (result.reconciliation_metrics or {})["clickhouse_cluster_external_full_refresh"]
            publication_phase = str(receipt["phase"])
            last_counts, member_digests, cleanup_proven = _observe_members(database)
            digests_equal = len(set(member_digests)) == 1 and member_digests[0] == expected_digest
            _require_correct_fanout(
                counts=last_counts,
                member_digests_equal=digests_equal,
                cleanup_proven=cleanup_proven,
                publication_phase=publication_phase,
                members=members,
                logical_rows=logical_rows,
            )
            if index >= warmups:
                trial_seconds.append(elapsed)
        finally:
            execute(18123, f"DROP DATABASE IF EXISTS {database} ON CLUSTER `{CLUSTER}` SYNC")

    maximum = max(trial_seconds)
    physical_rows = sum(last_counts)
    passed = maximum <= float(budget["max_trial_seconds"]) and physical_rows == int(budget["physical_rows"])
    return {
        "members": members,
        "logical_rows": logical_rows,
        "physical_rows": physical_rows,
        "columns": int(budget["columns"]),
        "warmups": warmups,
        "trials": trials,
        "trial_seconds": _rounded(trial_seconds),
        "median_seconds": round(statistics.median(trial_seconds), 6),
        "max_seconds": round(maximum, 6),
        "budget_max_seconds": float(budget["max_trial_seconds"]),
        "max_source_bytes": int(budget["max_source_bytes"]),
        "per_member_row_counts": last_counts,
        "content_digests_equal": digests_equal,
        "publication_phase": publication_phase,
        "cleanup_proven": cleanup_proven,
        "status": "PASS" if passed else "FAIL",
    }


def _observe_members(database: str) -> tuple[list[int], list[str], bool]:
    counts: list[int] = []
    digests: list[str] = []
    cleanup = True
    for port in (18123, 28123):
        counts.append(int(execute(port, f"SELECT count() FROM {database}.target")[0][0]))
        observed_rows = execute(port, f"SELECT id FROM {database}.target ORDER BY id")
        digests.append(canonical_rows_digest((int(row[0]),) for row in observed_rows))
        candidate_count = int(
            execute(
                port,
                f"SELECT count() FROM system.tables WHERE database='{database}' AND name LIKE 'target__dpone_ext_%'",
            )[0][0]
        )
        cleanup = cleanup and candidate_count == 0
    return counts, digests, cleanup


def _require_correct_fanout(
    *,
    counts: list[int],
    member_digests_equal: bool,
    cleanup_proven: bool,
    publication_phase: str,
    members: int,
    logical_rows: int,
) -> None:
    if counts != [logical_rows] * members:
        raise AssertionError("member row counts differ from the declared workload")
    if not member_digests_equal:
        raise AssertionError("member content digests diverged")
    if publication_phase != "COMMITTED":
        raise AssertionError("publication did not reach COMMITTED")
    if not cleanup_proven:
        raise AssertionError("owned predecessor cleanup was not proven")


def _rounded(values: list[float]) -> list[float]:
    return [round(value, 6) for value in values]


def _observation(observation_id: str, started_at: datetime) -> dict[str, str]:
    return {
        "observation_id": observation_id,
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "command": EXTERNAL_PERFORMANCE_COMMAND,
        "test_nodeid": EXTERNAL_PERFORMANCE_TEST_NODEID,
    }


def _empty_canonical_result(budget: dict[str, Any]) -> dict[str, Any]:
    return {
        "rows": int(budget["rows"]),
        "columns": int(budget["columns"]),
        "warmups": int(budget["warmups"]),
        "trials": int(budget["trials"]),
        "trial_seconds": [],
        "max_seconds": 0.0,
        "budget_max_seconds": float(budget["max_trial_seconds"]),
        "status": "FAIL",
        "error_type": "not_run",
    }


def _empty_fanout_result(budget: dict[str, Any]) -> dict[str, Any]:
    return {
        "members": int(budget["members"]),
        "logical_rows": int(budget["logical_rows"]),
        "physical_rows": int(budget["physical_rows"]),
        "columns": int(budget["columns"]),
        "warmups": int(budget["warmups"]),
        "trials": int(budget["trials"]),
        "trial_seconds": [],
        "max_seconds": 0.0,
        "budget_max_seconds": float(budget["max_trial_seconds"]),
        "max_source_bytes": int(budget["max_source_bytes"]),
        "per_member_row_counts": [],
        "status": "FAIL",
        "error_type": "not_run",
    }


def _require_positive_budget(budget: dict[str, Any], *fields: str) -> None:
    invalid = [field for field in fields if int(budget[field]) <= 0]
    if int(budget["warmups"]) < 0 or float(budget["max_trial_seconds"]) <= 0:
        invalid.append("timing")
    if invalid:
        raise ValueError("benchmark budget must be positive: " + ",".join(invalid))
