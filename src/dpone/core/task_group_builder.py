"""Build Airflow TaskGroups/ETL tasks from ProcessNode with unified dependency semantics.

For Variant C (batch manifests):
- several tasks may come from the same YAML file
- ``depends_on`` without selector means "wait for all tasks from that file"
- ``file.yaml#selector`` means dependency on a specific process
- ``#selector`` means local selector inside the current YAML file

Important: the actual dependency semantics are centralized in
:mod:`dpone.dag.edge_resolver` so that Airflow DAG building, graph building and
all explain/report tools stay in sync.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from airflow import DAG
from airflow.utils.task_group import TaskGroup

from dpone.dag.edge_resolver import (
    build_adjacency,
    build_dependency_indexes,
    resolve_dependency_path,
    split_dependency_ref,
)

if TYPE_CHECKING:
    from dpone.dag.yaml_types import ProcessNode

from operators.etl_operator import ETLOperator

logger = logging.getLogger(__name__)


def _split_dep_ref(raw: str, *, current_node: ProcessNode) -> tuple[str, str | None]:
    """Backward-compatible wrapper over :func:`dpone.dag.edge_resolver.split_dependency_ref`."""
    return split_dependency_ref(raw, current_node=current_node)


class TaskGroupBuilder:
    """Build Airflow TaskGroup objects and dependencies from ProcessNode."""

    def __init__(self, dag: DAG):
        self.dag = dag
        self.task_groups: dict[str, TaskGroup] = {}
        self.tasks: dict[str, ETLOperator] = {}
        self.group_membership: dict[str, str] = {}

    def collect_task_groups(self, nodes: list[ProcessNode]) -> None:
        for node in nodes:
            if node.task_group:
                self.group_membership[node.name] = node.task_group

                if node.task_group not in self.task_groups:
                    self.task_groups[node.task_group] = TaskGroup(
                        group_id=node.task_group,
                        dag=self.dag,
                        tooltip=f"Parallel execution group: {node.task_group}",
                    )

        logger.info("Collected %s task groups: %s", len(self.task_groups), list(self.task_groups.keys()))

    def create_task(self, node: ProcessNode) -> ETLOperator:
        from operators.etl_operator import ETLOperator

        task_group = None
        if node.task_group and node.task_group in self.task_groups:
            task_group = self.task_groups[node.task_group]

        task = ETLOperator(
            task_id=node.name,
            config_path=node.ref,
            dag=self.dag,
            task_group=task_group,
        )

        self.tasks[node.name] = task
        return task

    def build_dependencies(self, nodes: list[ProcessNode]) -> None:
        """Build task-to-task dependencies using unified edge semantics."""
        adjacency = build_adjacency(nodes)

        for upstream_name, downstream_names in adjacency.items():
            upstream_task = self.tasks.get(upstream_name)
            if not upstream_task:
                logger.warning("Task not found for upstream node: %s", upstream_name)
                continue

            for downstream_name in sorted(downstream_names):
                downstream_task = self.tasks.get(downstream_name)
                if not downstream_task:
                    logger.warning("Task not found for downstream node: %s", downstream_name)
                    continue
                upstream_task >> downstream_task

    def _resolve_dependency_task_names(
        self,
        dep_path: str,
        current_node: ProcessNode,
        nodes: list[ProcessNode],
    ) -> list[str]:
        """Backward-compatible resolver used by validation/debug helpers."""
        indexes = build_dependency_indexes(nodes)
        resolution = resolve_dependency_path(dep_path, current_node=current_node, indexes=indexes)
        return list(resolution.upstream_names)

    def validate_task_groups(self, nodes: list[ProcessNode]) -> list[str]:
        """Best-effort validation of dependency targets before Airflow operators are created."""
        errors: list[str] = []
        all_names = {node.name for node in nodes}
        indexes = build_dependency_indexes(nodes)

        for node in nodes:
            for dep in node.dependencies:
                group = getattr(dep, "group", None)
                if group:
                    if group not in indexes.all_groups:
                        errors.append(
                            f"Unknown group '{group}' in task '{node.name}'. "
                            f"Available groups: {sorted(indexes.all_groups)}"
                        )
                    continue

                dep_path = getattr(dep, "path", "") or ""
                if not dep_path:
                    continue

                resolution = resolve_dependency_path(dep_path, current_node=node, indexes=indexes)
                dep_names = list(resolution.upstream_names)
                if not dep_names:
                    errors.append(f"Unknown dependency '{dep_path}' in task '{node.name}'")
                    continue

                for dep_name in dep_names:
                    if dep_name not in all_names:
                        errors.append(f"Dependency '{dep_name}' not found for task '{node.name}'")
                        continue

                    if node.task_group:
                        dep_node = next((candidate for candidate in nodes if candidate.name == dep_name), None)
                        if dep_node and dep_node.task_group and dep_node.task_group != node.task_group:
                            logger.info(
                                "Cross-group dependency: task '%s' (group: %s) depends on '%s' (group: %s)",
                                node.name,
                                node.task_group,
                                dep_name,
                                dep_node.task_group,
                            )

        return errors


__all__ = ["TaskGroupBuilder"]
