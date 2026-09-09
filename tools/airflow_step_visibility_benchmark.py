"""Produce deterministic build-plane evidence for Airflow step visibility v1."""

from __future__ import annotations

import argparse
import json
import platform
import socket
import statistics
import time
from pathlib import Path
from typing import Any

from dpone.gitops.airflow_dag_spec import (
    DagSpecDeclaration,
    DagSpecEdge,
    DagSpecNode,
    DagSpecWiring,
    GitOpsAirflowDagSpec,
)

DAG_COUNT = 100
NODES_PER_DAG = 5
REPETITIONS = 30
P95_BUDGET_MS = 250.0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("test_artifacts/airflow-step-visibility-v1/benchmark.json"),
    )
    args = parser.parse_args()
    report = run_benchmark()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(args.output.as_posix())
    return 0 if report["passed"] else 1


def run_benchmark() -> dict[str, Any]:
    specs = _specs()
    durations: list[float] = []
    fingerprint_sets: set[tuple[str, ...]] = set()
    network_calls = [0]
    original_create_connection = socket.create_connection
    original_connect = socket.socket.connect

    def blocked_network(*args: object, **kwargs: object) -> object:
        network_calls[0] += 1
        raise AssertionError("network access during step-visibility benchmark")

    setattr(socket, "create_connection", blocked_network)
    setattr(socket.socket, "connect", blocked_network)
    try:
        for _ in range(REPETITIONS):
            started = time.perf_counter()
            payloads = tuple(spec.to_jsonable() for spec in specs)
            durations.append((time.perf_counter() - started) * 1000)
            fingerprint_sets.add(tuple(str(payload["spec_fingerprint"]) for payload in payloads))
    finally:
        setattr(socket, "create_connection", original_create_connection)
        setattr(socket.socket, "connect", original_connect)

    p95_ms = _percentile(durations, 0.95)
    deterministic = len(fingerprint_sets) == 1
    passed = deterministic and p95_ms <= P95_BUDGET_MS and network_calls[0] == 0
    return {
        "schema": "dpone.airflow-step-visibility-benchmark.v1",
        "passed": passed,
        "fixture": {
            "dags": DAG_COUNT,
            "nodes": DAG_COUNT * NODES_PER_DAG,
            "repetitions": REPETITIONS,
            "visibility_modes": ["inline", "task", "group"],
        },
        "performance": {
            "budget_p95_ms": P95_BUDGET_MS,
            "p50_ms": round(statistics.median(durations), 3),
            "p95_ms": round(p95_ms, 3),
            "max_ms": round(max(durations), 3),
            "passed": p95_ms <= P95_BUDGET_MS,
        },
        "determinism": {
            "fingerprint_sets": len(fingerprint_sets),
            "passed": deterministic,
        },
        "side_effects": {
            "network_calls": network_calls[0],
            "provider_parse_slo_test": "tests/test_airflow_provider_parse_slo.py",
            "passed": network_calls[0] == 0,
        },
        "runner": {
            "platform": platform.platform(),
            "processor": platform.machine(),
            "python": platform.python_version(),
        },
        "live_certification": "N/A: scheduler-static build-plane capability",
    }


def _specs() -> tuple[GitOpsAirflowDagSpec, ...]:
    return tuple(_spec(index) for index in range(DAG_COUNT))


def _spec(dag_index: int) -> GitOpsAirflowDagSpec:
    dag_id = f"visibility_benchmark_{dag_index:03d}"
    nodes = tuple(_node(dag_index, node_index) for node_index in range(NODES_PER_DAG))
    edges = tuple(
        DagSpecEdge(
            upstream=nodes[index].node_id,
            downstream=nodes[index + 1].node_id,
            reason="declared",
            origin="benchmark",
        )
        for index in range(NODES_PER_DAG - 1)
    )
    return GitOpsAirflowDagSpec(
        declaration=DagSpecDeclaration(
            dag_id=dag_id,
            description=None,
            schedule=None,
            start_date="2026-01-01",
            timezone="UTC",
            catchup=False,
            max_active_runs=1,
            tags=("benchmark",),
            default_args={},
            operator_overrides={},
            workloads=tuple(node.workload_id for node in nodes),
            group=None,
            wiring=DagSpecWiring(),
        ),
        domain="benchmark",
        source_path="benchmark/domain.yaml",
        nodes=nodes,
        edges=edges,
        topological_order=tuple(node.node_id for node in nodes),
    )


def _node(dag_index: int, node_index: int) -> DagSpecNode:
    visibility = ("inline", "task", "group")[node_index % 3]
    workload_id = f"workload_{dag_index:03d}_{node_index:02d}"
    return DagSpecNode(
        node_id=workload_id,
        workload_id=workload_id,
        selector=f"process_{node_index:02d}",
        task_group=f"group_{node_index:02d}" if visibility == "group" else None,
        visibility=visibility,
        estimated_visible_tasks=1 if visibility == "inline" else 2,
    )


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(len(ordered) * percentile) - 1))
    return ordered[index]


if __name__ == "__main__":
    raise SystemExit(main())
