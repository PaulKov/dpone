"""Produce deterministic performance evidence for workload selectors v1."""

from __future__ import annotations

import argparse
import json
import platform
import socket
import statistics
import time
from pathlib import Path
from typing import Any

from dpone.manifest.selection import SelectionEngine, SelectionGraph, SelectionNode, SelectionRequest

NODE_COUNT = 500
EDGE_COUNT = 1000
REPETITIONS = 100
P95_BUDGET_MS = 150.0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("test_artifacts/airflow-selectors-v1/benchmark.json"),
    )
    args = parser.parse_args()
    report = run_benchmark()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(args.output.as_posix())
    return 0 if report["passed"] else 1


def run_benchmark() -> dict[str, Any]:
    graph = _graph()
    request = SelectionRequest(select=("tag:finance+",), exclude=("tag:deprecated",), max_selected=NODE_COUNT)
    engine = SelectionEngine()
    network_calls = [0]
    original_create_connection = socket.create_connection
    original_connect = socket.socket.connect

    def blocked_network(*args: object, **kwargs: object) -> object:
        network_calls[0] += 1
        raise AssertionError("network access during selector benchmark")

    durations: list[float] = []
    fingerprints: set[str] = set()
    selected_counts: set[int] = set()
    explained_counts: set[int] = set()
    setattr(socket, "create_connection", blocked_network)
    setattr(socket.socket, "connect", blocked_network)
    try:
        for _ in range(REPETITIONS):
            started = time.perf_counter()
            report = engine.select(graph, request)
            durations.append((time.perf_counter() - started) * 1000)
            fingerprints.add(report.selection_fingerprint)
            selected_counts.add(len(report.selected))
            explained_counts.add(sum(bool(item.reasons) for item in report.selected))
    finally:
        setattr(socket, "create_connection", original_create_connection)
        setattr(socket.socket, "connect", original_connect)

    p95_ms = _percentile(durations, 0.95)
    selected_count = next(iter(selected_counts)) if len(selected_counts) == 1 else -1
    explained_count = next(iter(explained_counts)) if len(explained_counts) == 1 else -1
    deterministic = len(fingerprints) == len(selected_counts) == len(explained_counts) == 1
    passed = (
        deterministic
        and p95_ms <= P95_BUDGET_MS
        and selected_count > 0
        and explained_count == selected_count
        and network_calls[0] == 0
    )
    return {
        "schema": "dpone.airflow-selectors-benchmark.v1",
        "passed": passed,
        "fixture": {"nodes": NODE_COUNT, "edges": len(graph.edges), "repetitions": REPETITIONS},
        "selection": {
            "expression": "tag:finance+",
            "exclude": "tag:deprecated",
            "selected": selected_count,
            "explained": explained_count,
            "fingerprint_count": len(fingerprints),
            "deterministic": deterministic,
        },
        "performance": {
            "budget_p95_ms": P95_BUDGET_MS,
            "p50_ms": round(statistics.median(durations), 3),
            "p95_ms": round(p95_ms, 3),
            "max_ms": round(max(durations), 3),
            "passed": p95_ms <= P95_BUDGET_MS,
        },
        "side_effects": {
            "network_calls": network_calls[0],
            "provider_selector_parsing": 0,
            "provider_contract_test": "tests/test_airflow_selectors_v1.py",
            "passed": network_calls[0] == 0,
        },
        "runner": {
            "platform": platform.platform(),
            "processor": platform.machine(),
            "python": platform.python_version(),
        },
        "live_certification": "N/A: build-plane graph selection capability",
    }


def _graph() -> SelectionGraph:
    nodes = tuple(
        SelectionNode(
            node_id=f"workload_{index:04d}",
            source_path=f"pipelines/workload_{index:04d}/pipeline.yaml",
            semantic_fingerprint=f"sha256:{index:064x}",
            domain=f"domain_{index % 10}",
            owner=f"team_{index % 20}",
            tags=tuple(
                tag
                for tag, enabled in (
                    ("finance", index % 10 == 0),
                    ("deprecated", index % 37 == 0),
                )
                if enabled
            ),
            sources=("mssql",),
            sinks=("clickhouse",),
            groups=(f"group_{index % 5}",),
        )
        for index in range(NODE_COUNT)
    )
    edges: list[tuple[str, str]] = []
    for distance in (1, 2):
        edges.extend(
            (f"workload_{index:04d}", f"workload_{index + distance:04d}") for index in range(NODE_COUNT - distance)
        )
    edges.extend(("workload_0000", f"workload_{index:04d}") for index in (3, 4, 5))
    assert len(edges) == EDGE_COUNT
    return SelectionGraph.build(nodes=nodes, edges=tuple(edges))


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(len(ordered) * percentile) - 1))
    return ordered[index]


if __name__ == "__main__":
    raise SystemExit(main())
