"""Node-centric DAG explanation utilities.

These helpers build on :mod:`dpone.dag.edge_explain`.

Goal: provide a convenient way to inspect *why* a given task has its
upstream/downstream edges in the computed DAG, mirroring TaskGroupBuilder
semantics.

This module is pure (no Airflow imports) and intended for UX/debugging.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from dpone.dag.edge_explain import DagEdgeContext, EdgeReason, explain_direct_edge


def build_reverse_adjacency(ctx: DagEdgeContext) -> dict[str, set[str]]:
    """Build reverse adjacency: downstream task_name -> set(upstream task_name)."""
    rev: defaultdict[str, set[str]] = defaultdict(set)
    for up, downs in ctx.adjacency.items():
        for dn in downs:
            rev[dn].add(up)
    return dict(rev)


@dataclass(frozen=True)
class EdgeSummary:
    """A direct DAG edge summary with optional explanation."""

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
class NodeExplanation:
    """Explanation for a node neighborhood."""

    node: dict[str, Any]
    incoming: list[EdgeSummary] = field(default_factory=list)
    outgoing: list[EdgeSummary] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "node": self.node,
            "incoming": [e.to_jsonable() for e in self.incoming],
            "outgoing": [e.to_jsonable() for e in self.outgoing],
            "warnings": list(self.warnings),
        }


def _node_brief_from_ctx(ctx: DagEdgeContext, name: str) -> dict[str, Any]:
    n = ctx.nodes_by_name.get(name)
    if not n:
        return {"name": name}
    lc = n.config.load_config
    src = f"{lc.source_schema}.{lc.source_table}" if lc.source_schema and lc.source_table else None
    snk = f"{lc.target_schema}.{lc.target_table}" if lc.target_schema and lc.target_table else None
    return {
        "name": n.name,
        "ref": n.ref,
        "selector": n.selector,
        "task_group": n.task_group,
        "config_path": str(n.config_path),
        "source": src,
        "sink": snk,
    }


def explain_node_neighborhood(
    ctx: DagEdgeContext,
    *,
    node_name: str,
    max_in: int = 50,
    max_out: int = 50,
    max_triggers: int = 10,
    include_reasons: bool = True,
) -> NodeExplanation:
    """Explain incoming/outgoing edges for a node.

    Args:
        ctx: built DAG edge context.
        node_name: ProcessNode.name.
        max_in/max_out: safety limits.
        max_triggers: evidence truncation for group-to-group explanations.
        include_reasons: if False, only edges are listed; reasons are empty.

    Returns:
        NodeExplanation with EdgeSummary items.
    """
    if node_name not in ctx.nodes_by_name:
        raise ValueError(f"Node '{node_name}' not found")

    rev = build_reverse_adjacency(ctx)
    incoming_names = sorted(rev.get(node_name, set()))
    outgoing_names = sorted(ctx.adjacency.get(node_name, set()))

    exp = NodeExplanation(node=_node_brief_from_ctx(ctx, node_name))

    if len(incoming_names) > max_in:
        exp.warnings.append(f"Incoming edges truncated: {len(incoming_names)} > max_in={max_in}")
        incoming_names = incoming_names[:max_in]

    if len(outgoing_names) > max_out:
        exp.warnings.append(f"Outgoing edges truncated: {len(outgoing_names)} > max_out={max_out}")
        outgoing_names = outgoing_names[:max_out]

    # incoming
    for up in incoming_names:
        reasons: tuple[EdgeReason, ...] = ()
        if include_reasons:
            de = explain_direct_edge(ctx, upstream_name=up, downstream_name=node_name, max_triggers=max_triggers)
            reasons = tuple(de.reasons)
        exp.incoming.append(EdgeSummary(upstream=up, downstream=node_name, reasons=reasons))

    # outgoing
    for dn in outgoing_names:
        reasons = ()
        if include_reasons:
            de = explain_direct_edge(ctx, upstream_name=node_name, downstream_name=dn, max_triggers=max_triggers)
            reasons = tuple(de.reasons)
        exp.outgoing.append(EdgeSummary(upstream=node_name, downstream=dn, reasons=reasons))

    return exp
