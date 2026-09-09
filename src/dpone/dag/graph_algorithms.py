"""Graph algorithms for dependency planning."""

from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Iterable, Mapping

from dpone.dag.yaml_types import ProcessNode


class GraphAlgorithms:
    """Pure graph algorithms over ProcessNode collections."""

    @staticmethod
    def topological_sort(nodes: Mapping[str, ProcessNode] | Iterable[ProcessNode]) -> list[ProcessNode]:
        nodes_by_name = GraphAlgorithms._coerce_nodes(nodes)
        in_degree = defaultdict(int)

        for node in nodes_by_name.values():
            in_degree[node.name] = 0

        for node in nodes_by_name.values():
            for dependent in node.dependents:
                in_degree[dependent.name] += 1

        queue = deque([n for n in nodes_by_name.values() if in_degree[n.name] == 0])

        result: list[ProcessNode] = []
        while queue:
            current = queue.popleft()
            result.append(current)

            for dependent in current.dependents:
                in_degree[dependent.name] -= 1
                if in_degree[dependent.name] == 0:
                    queue.append(dependent)

        if len(result) != len(nodes_by_name):
            unprocessed = [name for name in nodes_by_name if name not in {n.name for n in result}]
            cycles = GraphAlgorithms.detect_cycles(nodes_by_name)
            cycle_info = f" Циклы: {cycles}" if cycles else ""
            raise RuntimeError(
                f"Обнаружена циклическая зависимость в графе процессов. Необработанные узлы: {unprocessed}.{cycle_info}"
            )

        return result

    @staticmethod
    def detect_cycles(nodes: Mapping[str, ProcessNode] | Iterable[ProcessNode]) -> list[list[str]]:
        nodes_by_name = GraphAlgorithms._coerce_nodes(nodes)
        visited: set[str] = set()
        rec_stack: set[str] = set()
        cycles: list[list[str]] = []

        def dfs(node_name: str, path: list[str]) -> None:
            if node_name in rec_stack:
                cycle_start = path.index(node_name)
                cycles.append(path[cycle_start:] + [node_name])
                return

            if node_name in visited:
                return

            visited.add(node_name)
            rec_stack.add(node_name)

            node = nodes_by_name.get(node_name)
            if node:
                for dependent in node.dependents:
                    dfs(dependent.name, path + [node_name])

            rec_stack.remove(node_name)

        for node_name in list(nodes_by_name):
            if node_name not in visited:
                dfs(node_name, [])

        return cycles

    @staticmethod
    def _coerce_nodes(nodes: Mapping[str, ProcessNode] | Iterable[ProcessNode]) -> dict[str, ProcessNode]:
        if isinstance(nodes, Mapping):
            return dict(nodes.items())
        return {node.name: node for node in nodes}


__all__ = ["GraphAlgorithms"]
