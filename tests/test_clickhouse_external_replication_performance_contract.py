"""Offline contracts for the external-publication performance producer."""

from __future__ import annotations

import json

from tests.integration.clickhouse_cluster.evidence import EXTERNAL_PERFORMANCE_BUDGET
from tests.integration.clickhouse_cluster.test_clickhouse_external_replication_performance_live import (
    _benchmark_canonical_digest,
)


def test_external_performance_budget_is_bounded_and_internally_consistent() -> None:
    budget = json.loads(EXTERNAL_PERFORMANCE_BUDGET.read_text(encoding="utf-8"))

    assert budget["schema_version"] == "dpone.clickhouse.external-publication-benchmark-config.v1"
    assert budget["canonical_digest"] == {
        "rows": 100_000,
        "columns": 3,
        "warmups": 1,
        "trials": 5,
        "max_trial_seconds": 2.0,
    }
    fanout = budget["member_fanout"]
    assert fanout["members"] == 2
    assert fanout["physical_rows"] == fanout["logical_rows"] * fanout["members"]
    assert fanout["warmups"] == 1
    assert fanout["trials"] == 3
    assert fanout["max_trial_seconds"] == 30.0


def test_canonical_benchmark_producer_reports_stable_digest_and_timings() -> None:
    result = _benchmark_canonical_digest(
        {
            "rows": 100,
            "columns": 3,
            "warmups": 1,
            "trials": 2,
            "max_trial_seconds": 2.0,
        }
    )

    assert result["status"] == "PASS"
    assert result["stable_across_trials"] is True
    assert len(result["digest"]) == 64
    assert len(result["trial_seconds"]) == 2
