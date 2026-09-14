"""Unavailable metadata must not authorize an absent-target creation path."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.runtime.sinks.clickhouse_cluster_topology import ClickHouseClusterTopologyProbe


class _TopologyClient:
    """Provide deterministic metadata or errors for a synthetic two-host cluster."""

    def __init__(self, scenario: dict[str, Any]) -> None:
        self.scenario = scenario

    def get_records(self, query: str) -> list[tuple[str, ...]]:
        if "system.clusters" in query:
            return [("node_a",), ("node_b",)]
        if "clusterAllReplicas" in query:
            if error := self.scenario.get("aggregate_error"):
                raise RuntimeError(error)
            if self.scenario.get("observed_target"):
                return [("node_a", "synthetic-uuid", "MergeTree")]
            return []
        for host, error in self.scenario.get("host_errors", {}).items():
            if f"remote('{host}'" in query:
                raise RuntimeError(error)
        return []


def _inspect_scenario(scenario: dict[str, Any]) -> dict[str, Any]:
    """Call the public probe; serialize either evidence or its propagated error."""
    config = SimpleNamespace(
        target_schema="synthetic",
        target_table="items",
        options={"physical_design": {"storage": {"clickhouse": {"cluster": "synthetic_cluster"}}}},
    )
    try:
        result = ClickHouseClusterTopologyProbe(_TopologyClient(scenario)).inspect(config)
    except RuntimeError as error:
        return {"error": str(error)}
    return {"passed": result.passed, **result.to_dict()}


def _observe(scenario: dict[str, Any], *, skip_unavailable: str = "1") -> dict[str, Any]:
    """Isolate the existing process-level option without monkeypatching services."""
    script = (
        "import json, sys; "
        "from tests.test_clickhouse_topology_absence_safety import _inspect_scenario; "
        "print(json.dumps(_inspect_scenario(json.loads(sys.argv[1]))))"
    )
    result = subprocess.run(
        [sys.executable, "-c", script, json.dumps(scenario)],
        env={**os.environ, "DPONE_CLICKHOUSE_SKIP_UNAVAILABLE_SHARDS": skip_unavailable},
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


@pytest.mark.parametrize("aggregate_error", [None, "Code: 279. All connection tries failed"])
@pytest.mark.parametrize("unknown_hosts", [("node_b",), ("node_a", "node_b")])
def test_unobserved_hosts_do_not_authorize_target_absence(
    aggregate_error: str | None, unknown_hosts: tuple[str, ...]
) -> None:
    result = _observe(
        {
            "aggregate_error": aggregate_error,
            "host_errors": {host: "Code: 279. All connection tries failed" for host in unknown_hosts},
        }
    )

    assert result["passed"] is False
    assert result["status"] == "blocked"
    assert result["blockers"] == ["clickhouse_cluster_target_observation_unavailable"]
    assert result["expected_hosts"] == ["node_a", "node_b"]
    assert result["missing_hosts"] == ["node_a", "node_b"]
    assert result["unreachable_hosts"] == list(unknown_hosts)
    assert result["actual_hosts"] == []
    assert result["schema_version"] == "dpone.clickhouse.cluster_topology.v1"


@pytest.mark.parametrize("code", [192, 193, 194, 195, 291, 497, 516])
@pytest.mark.parametrize("error_location", ["aggregate", "host"])
def test_access_failure_takes_precedence_over_remote_transport_context(code: int, error_location: str) -> None:
    message = f"Code: {code}. Rejected. While executing Remote (HedgedConnections). NetException"
    scenario = {"aggregate_error": message} if error_location == "aggregate" else {"host_errors": {"node_b": message}}

    assert _observe(scenario) == {"error": message}


@pytest.mark.parametrize("context", ["While executing Remote", "HedgedConnections"])
def test_remote_execution_context_alone_does_not_prove_unavailability(context: str) -> None:
    message = f"Unclassified metadata error. {context}"

    assert _observe({"host_errors": {"node_b": message}}) == {"error": message}


@pytest.mark.parametrize("denial", ["ACCESS_DENIED", "Authentication failed", "Not enough privileges"])
def test_access_failure_without_numeric_code_is_propagated(denial: str) -> None:
    message = f"{denial}. While executing Remote. NetException"

    assert _observe({"host_errors": {"node_b": message}}) == {"error": message}


@pytest.mark.parametrize("skip_unavailable", ["0", "1"])
def test_successful_absence_observations_remain_supported(skip_unavailable: str) -> None:
    result = _observe({}, skip_unavailable=skip_unavailable)

    assert result["passed"] is True
    assert result["status"] == "target_absent"
    assert result["unreachable_hosts"] == []


def test_strict_observation_error_is_propagated() -> None:
    message = "Code: 279. All connection tries failed"

    assert _observe({"aggregate_error": message}, skip_unavailable="0") == {"error": message}


def test_observed_target_retains_explicit_partial_topology_policy() -> None:
    result = _observe({"observed_target": True, "host_errors": {"node_b": "Code: 279. All connection tries failed"}})

    assert result["passed"] is True
    assert result["status"] == "passed_partial"
    assert result["actual_hosts"] == ["node_a"]
    assert result["unreachable_hosts"] == ["node_b"]
