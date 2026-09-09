"""Node registry for DAG dependency graphs.

Keeps indexing concerns isolated from graph algorithms and loading logic.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path

from dpone.dag.yaml_types import ProcessNode


class NodeRegistry:
    """Indexes process nodes by name, file path, selector ref and task group."""

    def __init__(self) -> None:
        self._nodes: dict[str, ProcessNode] = {}
        self._nodes_by_path: defaultdict[Path, list[ProcessNode]] = defaultdict(list)
        self._nodes_by_ref: dict[str, ProcessNode] = {}
        self._nodes_by_group: defaultdict[str, list[ProcessNode]] = defaultdict(list)

    @property
    def nodes(self) -> dict[str, ProcessNode]:
        return self._nodes

    @property
    def nodes_by_path(self) -> defaultdict[Path, list[ProcessNode]]:
        return self._nodes_by_path

    @property
    def nodes_by_ref(self) -> dict[str, ProcessNode]:
        return self._nodes_by_ref

    @property
    def nodes_by_group(self) -> defaultdict[str, list[ProcessNode]]:
        return self._nodes_by_group

    def add(self, node: ProcessNode) -> None:
        """Add node with uniqueness checks."""
        if node.name in self._nodes:
            raise ValueError(f"Duplicate process name detected: '{node.name}'")

        ref = node.ref
        if ref in self._nodes_by_ref:
            raise ValueError(f"Duplicate process ref detected: '{ref}'")

        self._nodes[node.name] = node
        self._nodes_by_path[node.config_path].append(node)
        self._nodes_by_ref[ref] = node

        if node.task_group:
            self._nodes_by_group[node.task_group].append(node)

    def get(self, name: str) -> ProcessNode | None:
        return self._nodes.get(name)

    def get_by_path(self, path: Path) -> list[ProcessNode]:
        return list(self._nodes_by_path.get(path, []))

    def get_by_ref(self, path: Path, selector: str) -> ProcessNode | None:
        return self._nodes_by_ref.get(f"{path}#{selector}")

    def find_by_selector(self, path: Path, selector: str) -> ProcessNode | None:
        node = self.get_by_ref(path, selector)
        if node:
            return node

        for candidate in self.get_by_path(path):
            if (candidate.selector and candidate.selector == selector) or candidate.name == selector:
                return candidate
        return None

    def iter_nodes(self) -> Iterable[ProcessNode]:
        return self._nodes.values()


__all__ = ["NodeRegistry"]
