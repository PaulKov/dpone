"""Subgraph extraction utilities for dpone DAGs.

This module provides minimal subgraph extraction between two nodes, built on
`DagEdgeContext` (TaskGroupBuilder semantics).

It is designed for debugging / UX. No Airflow imports.
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any

from dpone.dag.edge_explain import DagEdgeContext, EdgeReason, explain_direct_edge, find_shortest_path


@dataclass(frozen=True)
class SubgraphEdge:
    upstream: str
    downstream: str
    reasons: tuple[EdgeReason, ...] = ()

    def reason_kinds(self) -> list[str]:
        return sorted({r.kind for r in self.reasons})

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "upstream": self.upstream,
            "downstream": self.downstream,
            "reason_kinds": self.reason_kinds(),
            "reasons": [r.to_jsonable() for r in self.reasons],
        }


@dataclass
class Subgraph:
    nodes: list[str]
    edges: list[SubgraphEdge]
    shortest_distance: int | None = None
    warnings: list[str] = field(default_factory=list)

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "nodes": list(self.nodes),
            "edges": [e.to_jsonable() for e in self.edges],
            "shortest_distance": self.shortest_distance,
            "warnings": list(self.warnings),
        }


def _bfs_distances(adjacency: dict[str, set[str]], start: str) -> dict[str, int]:
    dist: dict[str, int] = {start: 0}
    q = deque([start])
    while q:
        cur = q.popleft()
        for nxt in adjacency.get(cur, set()):
            if nxt in dist:
                continue
            dist[nxt] = dist[cur] + 1
            q.append(nxt)
    return dist


def _reverse_adjacency(adjacency: dict[str, set[str]]) -> dict[str, set[str]]:
    rev: defaultdict[str, set[str]] = defaultdict(set)
    for u, ds in adjacency.items():
        for d in ds:
            rev[d].add(u)
    return dict(rev)


def extract_shortest_path_subgraph(
    ctx: DagEdgeContext,
    *,
    source: str,
    target: str,
    all_shortest: bool = False,
    with_reasons: bool = False,
    with_evidence: bool = False,
    max_triggers: int = 10,
    max_edges: int = 500,
) -> Subgraph:
    """Extract a minimal subgraph between source and target.

    If all_shortest=False, subgraph is one shortest path (nodes + path edges).
    If all_shortest=True, subgraph is the union of all shortest paths.

    Args:
        ctx: DAG edge context.
        source/target: task names.
        all_shortest: union of all shortest paths.
        with_reasons: attach EdgeReason kinds.
        with_evidence: include full evidence in EdgeReason (still stored inside EdgeReason).
        max_triggers: evidence truncation.
        max_edges: safety limit.
    """
    if source not in ctx.nodes_by_name:
        raise ValueError(f"Node '{source}' not found")
    if target not in ctx.nodes_by_name:
        raise ValueError(f"Node '{target}' not found")

    adj = {k: set(v) for k, v in ctx.adjacency.items()}

    if not all_shortest:
        path = find_shortest_path(ctx, source, target)
        if not path:
            return Subgraph(nodes=[], edges=[], shortest_distance=None, warnings=["No path found"])
        path_edges: list[tuple[str, str]] = [(path[i], path[i + 1]) for i in range(len(path) - 1)]
        sg_nodes = list(path)
        path_subgraph_edges: list[SubgraphEdge] = []
        for u, d in path_edges:
            path_edge_reasons: tuple[EdgeReason, ...] = ()
            if with_reasons or with_evidence:
                de = explain_direct_edge(ctx, upstream_name=u, downstream_name=d, max_triggers=max_triggers)
                path_edge_reasons = tuple(de.reasons)
            path_subgraph_edges.append(SubgraphEdge(upstream=u, downstream=d, reasons=path_edge_reasons))
        return Subgraph(nodes=sg_nodes, edges=path_subgraph_edges, shortest_distance=len(path_edges))

    # union of all shortest paths
    dist_s = _bfs_distances(adj, source)
    if target not in dist_s:
        return Subgraph(nodes=[], edges=[], shortest_distance=None, warnings=["No path found"])

    rev_adj = _reverse_adjacency(adj)
    dist_t = _bfs_distances(rev_adj, target)

    shortest = dist_s[target]

    nodes_on: set[str] = set()
    edges_on: list[tuple[str, str]] = []

    for u, ds in adj.items():
        if u not in dist_s:
            continue
        for d in ds:
            if d not in dist_s or d not in dist_t:
                continue
            if dist_s[u] + 1 + dist_t[d] == shortest:
                nodes_on.add(u)
                nodes_on.add(d)
                edges_on.append((u, d))

    # keep nodes ordered by dist_s then name for stable UX
    sg_nodes = sorted(nodes_on, key=lambda n: (dist_s.get(n, 10**9), n))

    if len(edges_on) > max_edges:
        edges_on = sorted(edges_on)[:max_edges]
        warnings = [f"Edges truncated: exceeded max_edges={max_edges}"]
    else:
        warnings = []

    sg_edges: list[SubgraphEdge] = []
    for u, d in sorted(edges_on):
        edge_reasons: tuple[EdgeReason, ...] = ()
        if with_reasons or with_evidence:
            de = explain_direct_edge(ctx, upstream_name=u, downstream_name=d, max_triggers=max_triggers)
            edge_reasons = tuple(de.reasons)
        sg_edges.append(SubgraphEdge(upstream=u, downstream=d, reasons=edge_reasons))

    return Subgraph(nodes=sg_nodes, edges=sg_edges, shortest_distance=shortest, warnings=warnings)
