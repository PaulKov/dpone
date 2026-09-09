"""DAG membership index for GitOps Airflow workload nodes."""

from __future__ import annotations

from collections import defaultdict

from dpone.gitops.airflow_dag_spec import GitOpsAirflowDagSpec


def node_dag_index(specs: tuple[GitOpsAirflowDagSpec, ...]) -> dict[str, tuple[str, ...]]:
    mapping: dict[str, set[str]] = defaultdict(set)
    for spec in specs:
        for node in spec.nodes:
            mapping[node.node_id].add(spec.dag_id)
    return {node_id: tuple(sorted(dags)) for node_id, dags in mapping.items()}


__all__ = ["node_dag_index"]
