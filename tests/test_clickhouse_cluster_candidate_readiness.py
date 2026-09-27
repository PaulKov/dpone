"""Exact replica-row readiness remains bounded and fail-closed."""

from __future__ import annotations

import pytest

from dpone.contracts.clickhouse_cluster_publication import ClusterPublicationError
from dpone.runtime.sinks.clickhouse_cluster_candidate_readiness import require_candidate_rows


class Catalog:
    def __init__(self, observations):
        self.observations = iter(observations)
        self.calls = 0

    def candidate_counts(self, cluster, database, candidate):
        assert (cluster, database, candidate) == ("cluster", "database", "candidate")
        self.calls += 1
        return next(self.observations)


def test_readiness_repolls_missing_and_mismatched_hosts_until_exact():
    catalog = Catalog(({"a": 3}, {"a": 4, "b": 3}, {"a": 4, "b": 4}))
    clock = iter((0.0, 1.0, 2.0))
    sleeps = []

    require_candidate_rows(
        ClusterPublicationError,
        catalog.candidate_counts,
        "cluster",
        "database",
        "candidate",
        ("a", "b"),
        4,
        wait_seconds=5,
        poll_seconds=0.25,
        monotonic=lambda: next(clock),
        sleep=sleeps.append,
    )

    assert catalog.calls == 3
    assert sleeps == [0.25, 0.25]


def test_readiness_timeout_reports_each_expected_host_and_fails_closed():
    catalog = Catalog(({"a": 3}, {"a": 3}))
    clock = iter((0.0, 5.0))

    with pytest.raises(ClusterPublicationError) as raised:
        require_candidate_rows(
            ClusterPublicationError,
            catalog.candidate_counts,
            "cluster",
            "database",
            "candidate",
            ("a", "b"),
            4,
            wait_seconds=5,
            poll_seconds=0.25,
            monotonic=lambda: next(clock),
            sleep=lambda _: pytest.fail("deadline must stop before sleeping"),
        )

    assert raised.value.code == "DPONE_CLICKHOUSE_CLUSTER_CANDIDATE_NOT_READY"
    assert raised.value.detail == "row count differs by replica observed=a=3,b=missing expected=4"
