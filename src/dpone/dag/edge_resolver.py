"""Unified dependency/edge semantics for DAG building and debugging.

This module is the single source of truth for how ``depends_on`` declarations
produce concrete task-to-task edges.

Why it exists:
- Airflow DAG building must follow exactly the same rules as debug/explain tools.
- Graph loading (execution order / cycle detection) must use the same semantics.
- We want one small, pure module with no Airflow/runtime imports.

Covered dependency modes:
- ``{group: ...}``
  - task outside task_group  -> group_to_task
  - task inside task_group   -> group_to_group expansion for the whole downstream group
- ``path`` string:
  - direct task_id
  - group name string (legacy convenience)
  - ``file.yaml#selector``
  - ``#selector``
  - file-level dependency (wait-for-all tasks from file)
  - stem fallback (legacy)
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dpone.dag.yaml_types import DependencyConfig, ProcessNode


@dataclass(frozen=True, slots=True)
class GroupTrigger:
    """A task that introduced a group dependency for its task_group."""

    task_name: str
    selector: str | None
    dep_index: int
    alias: str | None = None

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "task_name": self.task_name,
            "selector": self.selector,
            "dep_index": self.dep_index,
            "alias": self.alias,
        }


@dataclass(slots=True)
class DependencyIndexes:
    """Indexes derived from a list of ProcessNode."""

    nodes: Sequence[ProcessNode]
    nodes_by_name: dict[str, ProcessNode] = field(default_factory=dict)
    nodes_by_file: defaultdict[str, list[ProcessNode]] = field(default_factory=lambda: defaultdict(list))
    nodes_by_group: defaultdict[str, list[ProcessNode]] = field(default_factory=lambda: defaultdict(list))
    group_triggers: defaultdict[tuple[str, str], list[GroupTrigger]] = field(default_factory=lambda: defaultdict(list))
    all_groups: set[str] = field(default_factory=set)


@dataclass(frozen=True, slots=True)
class PathDependencyResolution:
    """Resolved targets for a ``depends_on.path`` declaration."""

    upstream_names: tuple[str, ...]
    mode: str
    detail: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DependencyResolution:
    """Resolved edge contribution produced by one declared dependency."""

    trigger_task_name: str
    trigger_selector: str | None
    dependency_index: int
    kind: str
    upstream_names: tuple[str, ...]
    downstream_names: tuple[str, ...]
    raw_path: str | None = None
    group: str | None = None
    alias: str | None = None
    detail: Mapping[str, Any] = field(default_factory=dict)

    def matches_edge(self, *, upstream_name: str, downstream_name: str) -> bool:
        return upstream_name in self.upstream_names and downstream_name in self.downstream_names


def build_dependency_indexes(nodes: Sequence[ProcessNode]) -> DependencyIndexes:
    """Build reusable indexes for dependency resolution."""

    indexes = DependencyIndexes(nodes=nodes)
    for node in nodes:
        indexes.nodes_by_name[node.name] = node
        indexes.nodes_by_file[str(node.config_path)].append(node)
        if node.task_group:
            indexes.nodes_by_group[node.task_group].append(node)
            indexes.all_groups.add(node.task_group)

    for node in nodes:
        if not node.task_group:
            continue
        for dep_index, dep in enumerate(node.dependencies or []):
            group = getattr(dep, "group", None)
            if not group:
                continue
            indexes.group_triggers[(str(group), str(node.task_group))].append(
                GroupTrigger(
                    task_name=node.name,
                    selector=node.selector,
                    dep_index=dep_index,
                    alias=getattr(dep, "alias", None),
                )
            )

    return indexes


def split_dependency_ref(dep_path: str, *, current_node: ProcessNode) -> tuple[str, str | None]:
    """Split ``depends_on.path`` into ``(file_part, selector)``.

    Mirrors the historic semantics from ``TaskGroupBuilder`` / explain helpers:
    - ``#selector`` points to the current manifest file
    - ``file.yaml#selector`` points to a process inside another file
    - plain ``file.yaml`` has no selector
    """

    raw = (dep_path or "").strip()
    if not raw:
        return "", None

    if raw.startswith("#"):
        return str(current_node.config_path), raw[1:].strip() or None

    if "#" in raw:
        file_part, selector = raw.split("#", 1)
        return file_part.strip(), selector.strip() or None

    return raw, None


def resolve_dependency_path(
    dep_path: str,
    *,
    current_node: ProcessNode,
    indexes: DependencyIndexes,
) -> PathDependencyResolution:
    """Resolve ``depends_on.path`` into concrete upstream task names.

    Returned ``mode`` values are intentionally stable because they are surfaced
    in explain/report UX.
    """

    raw = (dep_path or "").strip()
    if not raw:
        return PathDependencyResolution((), "unresolved", {"raw": dep_path})

    # 0) group name as string (legacy convenience)
    if raw in indexes.all_groups:
        matches = tuple(node.name for node in indexes.nodes_by_group.get(raw, []))
        return PathDependencyResolution(
            matches,
            "group_string",
            {"group": raw, "match_count": len(matches), "matched_by": "exact"},
        )

    # 1) direct task_id reference
    direct = indexes.nodes_by_name.get(raw)
    if direct is not None:
        return PathDependencyResolution((direct.name,), "task_id", {"task_id": raw})

    file_part, selector = split_dependency_ref(raw, current_node=current_node)

    # 2) selector-based reference
    if selector:
        matches = tuple(
            node.name
            for node in indexes.nodes_by_file.get(str(file_part), [])
            if ((node.selector and node.selector == selector) or node.name == selector)
        )
        return PathDependencyResolution(
            matches,
            "selector",
            {
                "file": file_part,
                "selector": selector,
                "match_count": len(matches),
            },
        )

    # 3) file-level reference (wait for all tasks from that file)
    matches = tuple(node.name for node in indexes.nodes_by_file.get(str(file_part), []))
    if matches:
        return PathDependencyResolution(matches, "file", {"file": file_part, "match_count": len(matches)})

    # 3b) group name recovered from a resolved path/basename (legacy convenience)
    for group_candidate, matched_by in (
        (Path(raw).name, "basename"),
        (Path(raw).stem, "stem"),
    ):
        if group_candidate and group_candidate in indexes.all_groups:
            group_matches = tuple(node.name for node in indexes.nodes_by_group.get(group_candidate, []))
            return PathDependencyResolution(
                group_matches,
                "group_string",
                {
                    "group": group_candidate,
                    "match_count": len(group_matches),
                    "matched_by": matched_by,
                    "raw": raw,
                },
            )

    # 4) stem fallback (legacy behaviour)
    stem = Path(raw).stem
    matches = tuple(node.name for node in indexes.nodes if Path(node.config_path).stem == stem)
    if matches:
        return PathDependencyResolution(matches, "stem", {"stem": stem, "match_count": len(matches)})

    return PathDependencyResolution((), "unresolved", {"raw": raw, "file_part": file_part, "selector": selector})


def resolve_declared_dependency(
    current_node: ProcessNode,
    dep: DependencyConfig,
    *,
    dep_index: int,
    indexes: DependencyIndexes,
) -> DependencyResolution:
    """Resolve one declared dependency into concrete upstream/downstream tasks."""

    alias = getattr(dep, "alias", None)
    group = getattr(dep, "group", None)
    if group:
        upstream_names = tuple(node.name for node in indexes.nodes_by_group.get(str(group), []))
        if current_node.task_group:
            downstream_names = tuple(node.name for node in indexes.nodes_by_group.get(str(current_node.task_group), []))
            return DependencyResolution(
                trigger_task_name=current_node.name,
                trigger_selector=current_node.selector,
                dependency_index=dep_index,
                kind="group_to_group",
                upstream_names=upstream_names,
                downstream_names=downstream_names,
                group=str(group),
                alias=alias,
                detail={
                    "upstream_group": str(group),
                    "downstream_group": str(current_node.task_group),
                    "trigger_task": current_node.name,
                    "trigger_selector": current_node.selector,
                },
            )

        return DependencyResolution(
            trigger_task_name=current_node.name,
            trigger_selector=current_node.selector,
            dependency_index=dep_index,
            kind="group_to_task",
            upstream_names=upstream_names,
            downstream_names=(current_node.name,),
            group=str(group),
            alias=alias,
            detail={
                "group": str(group),
                "trigger_task": current_node.name,
                "trigger_selector": current_node.selector,
            },
        )

    dep_path = getattr(dep, "path", None) or ""
    path_resolution = resolve_dependency_path(dep_path, current_node=current_node, indexes=indexes)
    kind_map = {
        "task_id": "depends_on_task_id",
        "group_string": "depends_on_group_string",
        "selector": "depends_on_selector",
        "file": "depends_on_file_all",
        "stem": "depends_on_stem_fallback",
        "unresolved": "unresolved",
    }
    return DependencyResolution(
        trigger_task_name=current_node.name,
        trigger_selector=current_node.selector,
        dependency_index=dep_index,
        kind=kind_map.get(path_resolution.mode, "depends_on_other"),
        upstream_names=tuple(path_resolution.upstream_names),
        downstream_names=(current_node.name,),
        raw_path=dep_path,
        alias=alias,
        detail=dict(path_resolution.detail),
    )


def resolve_node_dependencies(
    current_node: ProcessNode,
    *,
    indexes: DependencyIndexes,
) -> list[DependencyResolution]:
    """Resolve every dependency declared on ``current_node``."""

    out: list[DependencyResolution] = []
    for dep_index, dep in enumerate(current_node.dependencies or []):
        out.append(resolve_declared_dependency(current_node, dep, dep_index=dep_index, indexes=indexes))
    return out


def collect_dependency_resolutions(
    nodes: Sequence[ProcessNode],
    *,
    indexes: DependencyIndexes | None = None,
) -> list[DependencyResolution]:
    """Resolve every declared dependency in the DAG."""

    indexes = indexes or build_dependency_indexes(nodes)
    out: list[DependencyResolution] = []
    for node in nodes:
        out.extend(resolve_node_dependencies(node, indexes=indexes))
    return out


def build_adjacency_from_resolutions(
    resolutions: Iterable[DependencyResolution],
) -> defaultdict[str, set[str]]:
    """Build adjacency ``upstream -> downstream`` from resolved dependency plans."""

    adjacency: defaultdict[str, set[str]] = defaultdict(set)
    for resolution in resolutions:
        for upstream_name in resolution.upstream_names:
            for downstream_name in resolution.downstream_names:
                if upstream_name == downstream_name:
                    continue
                adjacency[upstream_name].add(downstream_name)
    return adjacency


def build_adjacency(
    nodes: Sequence[ProcessNode],
    *,
    indexes: DependencyIndexes | None = None,
    resolutions: Sequence[DependencyResolution] | None = None,
) -> defaultdict[str, set[str]]:
    """Build DAG adjacency with the same semantics everywhere in dpone."""

    indexes = indexes or build_dependency_indexes(nodes)
    resolved = list(resolutions) if resolutions is not None else collect_dependency_resolutions(nodes, indexes=indexes)
    return build_adjacency_from_resolutions(resolved)


__all__ = [
    "DependencyIndexes",
    "DependencyResolution",
    "GroupTrigger",
    "PathDependencyResolution",
    "build_adjacency",
    "build_adjacency_from_resolutions",
    "build_dependency_indexes",
    "collect_dependency_resolutions",
    "resolve_declared_dependency",
    "resolve_dependency_path",
    "resolve_node_dependencies",
    "split_dependency_ref",
]
