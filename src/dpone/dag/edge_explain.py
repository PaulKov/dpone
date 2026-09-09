"""Explain why DAG edges exist.

This module provides a pure, debug-friendly view over DAG edge semantics.

The actual dependency semantics live in :mod:`dpone.dag.edge_resolver` and are
shared with:
- Airflow TaskGroup building
- dependency graph building / cycle detection
- report / subgraph / node explain tools

Public API in this module is intentionally kept backward-compatible because it
is already used by many explain/report helpers.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.dag.yaml_types import ProcessNode


from collections import defaultdict, deque
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dpone.dag.edge_reason_collector import collect_edge_reasons
from dpone.dag.edge_resolver import (
    DependencyIndexes,
    DependencyResolution,
    GroupTrigger,
    build_adjacency,
    build_dependency_indexes,
    collect_dependency_resolutions,
    resolve_dependency_path,
)


@dataclass(frozen=True)
class EdgeReason:
    """A single reason (evidence) for an edge."""

    kind: str
    description: str
    evidence: dict[str, Any] = field(default_factory=dict)

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "description": self.description,
            "evidence": self.evidence,
        }


@dataclass
class DagEdgeContext:
    """Indexes derived from a list of ProcessNode for edge explanation."""

    nodes: Sequence[ProcessNode]
    nodes_by_name: dict[str, ProcessNode] = field(default_factory=dict)
    nodes_by_file: defaultdict[str, list[ProcessNode]] = field(default_factory=lambda: defaultdict(list))
    nodes_by_group: defaultdict[str, list[ProcessNode]] = field(default_factory=lambda: defaultdict(list))
    group_triggers: defaultdict[tuple[str, str], list[GroupTrigger]] = field(default_factory=lambda: defaultdict(list))
    all_groups: set[str] = field(default_factory=set)

    # adjacency: upstream task_name -> set(downstream task_name)
    adjacency: defaultdict[str, set[str]] = field(default_factory=lambda: defaultdict(set))

    # internal reusable resolution state
    indexes: DependencyIndexes | None = None
    dependency_resolutions: tuple[DependencyResolution, ...] = ()

    def build(self) -> DagEdgeContext:
        self.indexes = build_dependency_indexes(self.nodes)
        self.nodes_by_name = dict(self.indexes.nodes_by_name)
        self.nodes_by_file = self.indexes.nodes_by_file
        self.nodes_by_group = self.indexes.nodes_by_group
        self.group_triggers = self.indexes.group_triggers
        self.all_groups = set(self.indexes.all_groups)
        self.dependency_resolutions = tuple(collect_dependency_resolutions(self.nodes, indexes=self.indexes))
        self.adjacency = build_adjacency(
            self.nodes,
            indexes=self.indexes,
            resolutions=self.dependency_resolutions,
        )
        return self


@dataclass
class DagEdgeExplanation:
    """Explanation for a direct edge in the DAG."""

    upstream: dict[str, Any]
    downstream: dict[str, Any]
    direct_edge: bool
    reasons: list[EdgeReason] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    # optional transitive path
    path: list[str] | None = None

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "upstream": self.upstream,
            "downstream": self.downstream,
            "direct_edge": self.direct_edge,
            "reasons": [r.to_jsonable() for r in self.reasons],
            "warnings": list(self.warnings),
            "path": list(self.path) if self.path else None,
        }


# ---------------- compatibility wrappers ----------------


def resolve_dependency_path_to_task_names(
    dep_path: str,
    *,
    current_node: ProcessNode,
    nodes: Sequence[ProcessNode],
    all_groups: set[str],
) -> tuple[list[str], str, dict[str, Any]]:
    """Backward-compatible wrapper over :func:`edge_resolver.resolve_dependency_path`."""

    indexes = build_dependency_indexes(nodes)
    # Preserve caller-provided group set if it differs for any reason.
    if all_groups:
        indexes.all_groups = set(all_groups)

    resolution = resolve_dependency_path(dep_path, current_node=current_node, indexes=indexes)
    return list(resolution.upstream_names), resolution.mode, dict(resolution.detail)


def build_edge_context(nodes: Sequence[ProcessNode]) -> DagEdgeContext:
    """Build indexes required for edge explanation."""
    return DagEdgeContext(nodes=nodes).build()


def _resolve_token_file_path(token_path: str, *, root_file: Path, base_path: Path) -> Path:
    token_path = str(token_path or "").strip()
    candidate = Path(token_path)
    if candidate.is_absolute():
        return candidate
    # Prefer base_path because CLI tokens are documented relative to manifest dir.
    return (base_path / candidate) if token_path else root_file


def locate_node(
    nodes: Sequence[ProcessNode],
    *,
    token: str,
    root_file: Path,
    base_path: Path,
) -> ProcessNode:
    """Locate a ProcessNode by task_id / selector / ref-like token.

    Supported forms:
    - task_id
    - selector (unique across loaded nodes)
    - ``#selector`` relative to ``root_file``
    - ``file.yaml#selector``
    - absolute ``<file>#<selector>`` ref
    - plain file path when it contains exactly one process
    """

    raw = str(token or "").strip()
    if not raw:
        raise ValueError("Empty node token")

    # 1) direct task_id
    direct = [node for node in nodes if node.name == raw]
    if len(direct) == 1:
        return direct[0]
    if len(direct) > 1:
        raise ValueError(f"Ambiguous task_id token '{raw}'")

    # 2) local selector or explicit file#selector
    if raw.startswith("#") or "#" in raw:
        if raw.startswith("#"):
            file_path = Path(root_file)
            selector = raw[1:].strip()
        else:
            file_part, selector = raw.split("#", 1)
            file_path = _resolve_token_file_path(
                file_part.strip(), root_file=Path(root_file), base_path=Path(base_path)
            )
            selector = selector.strip()

        if selector:
            matches = [
                node
                for node in nodes
                if Path(node.config_path) == Path(file_path)
                and ((node.selector and node.selector == selector) or node.name == selector)
            ]
            if len(matches) == 1:
                return matches[0]
            if len(matches) > 1:
                raise ValueError(f"Ambiguous selector token '{raw}' matched {len(matches)} nodes")

    # 3) unique selector across loaded nodes
    selector_matches = [node for node in nodes if node.selector == raw]
    if len(selector_matches) == 1:
        return selector_matches[0]
    if len(selector_matches) > 1:
        raise ValueError(f"Ambiguous selector token '{raw}' matched {len(selector_matches)} nodes")

    # 4) exact process ref
    ref_matches = [node for node in nodes if node.ref == raw]
    if len(ref_matches) == 1:
        return ref_matches[0]
    if len(ref_matches) > 1:
        raise ValueError(f"Ambiguous ref token '{raw}'")

    # 5) single-process file reference
    file_path = _resolve_token_file_path(raw, root_file=Path(root_file), base_path=Path(base_path))
    file_matches = [node for node in nodes if Path(node.config_path) == Path(file_path)]
    if len(file_matches) == 1:
        return file_matches[0]
    if len(file_matches) > 1:
        raise ValueError(
            f"Token '{raw}' resolves to file '{file_path}' with {len(file_matches)} processes; add '#selector' or use task_id"
        )

    # 6) legacy stem fallback (unique)
    stem = Path(raw).stem
    stem_matches = [node for node in nodes if Path(node.config_path).stem == stem]
    if len(stem_matches) == 1:
        return stem_matches[0]
    if len(stem_matches) > 1:
        raise ValueError(f"Ambiguous stem token '{raw}' matched {len(stem_matches)} nodes")

    raise ValueError(f"Node not found for token '{raw}'")


# ---------------- edge explanation ----------------


def explain_direct_edge(
    ctx: DagEdgeContext,
    *,
    upstream_name: str,
    downstream_name: str,
    max_triggers: int = 10,
    include_transitive_path: bool = False,
) -> DagEdgeExplanation:
    """Explain why there is a direct edge upstream -> downstream.

    If include_transitive_path=True and direct edge is absent, a shortest
    upstream ~> downstream path will be searched and returned.
    """
    up = ctx.nodes_by_name.get(upstream_name)
    dn = ctx.nodes_by_name.get(downstream_name)

    if not up or not dn:
        missing = []
        if not up:
            missing.append(f"upstream '{upstream_name}'")
        if not dn:
            missing.append(f"downstream '{downstream_name}'")
        raise ValueError("Node not found: " + ", ".join(missing))

    direct = downstream_name in ctx.adjacency.get(upstream_name, set())

    exp = DagEdgeExplanation(
        upstream=_node_brief(up),
        downstream=_node_brief(dn),
        direct_edge=direct,
    )

    if direct:
        exp.reasons.extend(
            collect_edge_reasons(ctx, upstream=up, downstream=dn, max_triggers=max_triggers, reason_cls=EdgeReason)
        )
        if not exp.reasons:
            exp.warnings.append("Edge exists in computed DAG, but no specific reason was attributed (unexpected)")
    elif include_transitive_path:
        path = find_shortest_path(ctx, upstream_name, downstream_name)
        if path:
            exp.path = path
    return exp


def _node_brief(node: ProcessNode) -> dict[str, Any]:
    lc = node.config.load_config
    source = None
    if lc.source_schema and lc.source_table:
        source = f"{lc.source_schema}.{lc.source_table}"
    sink = None
    if lc.target_schema and lc.target_table:
        sink = f"{lc.target_schema}.{lc.target_table}"
    return {
        "name": node.name,
        "ref": node.ref,
        "selector": node.selector,
        "task_group": node.task_group,
        "config_path": str(node.config_path),
        "source": source,
        "sink": sink,
    }


def find_shortest_path(ctx: DagEdgeContext, upstream: str, downstream: str) -> list[str] | None:
    """Find a shortest path upstream ~> downstream in the computed DAG (BFS)."""
    if upstream == downstream:
        return [upstream]

    queue = deque([upstream])
    prev: dict[str, str | None] = {upstream: None}

    while queue:
        current = queue.popleft()
        for nxt in ctx.adjacency.get(current, set()):
            if nxt in prev:
                continue
            prev[nxt] = current
            if nxt == downstream:
                queue.clear()
                break
            queue.append(nxt)

    if downstream not in prev:
        return None

    path: list[str] = []
    cur: str | None = downstream
    while cur is not None:
        path.append(cur)
        cur = prev[cur]
    path.reverse()
    return path


__all__ = [
    "DagEdgeContext",
    "DagEdgeExplanation",
    "EdgeReason",
    "GroupTrigger",
    "build_edge_context",
    "explain_direct_edge",
    "find_shortest_path",
    "resolve_dependency_path_to_task_names",
    "locate_node",
]
