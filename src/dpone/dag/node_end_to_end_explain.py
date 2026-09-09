"""Step 25: Node-centric end-to-end explanation.

This module provides a higher-level UX helper that answers:

  - "Why does this task have these upstream edges?"
  - "Why does this task have these downstream edges?"

...but in an *end-to-end* way, combining:
  1) DAG edge reasons (TaskGroupBuilder parity)
  2) Manifest compilation provenance (Variant C overrides/vars/naming)
  3) Post-parse normalization provenance (ETLProcessConfig.from_dict)

It is intentionally *read-only* and does not alter runtime semantics.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.dag.deps_end_to_end_explain import ProcessExplainCache
from dpone.dag.edge_end_to_end_explain import EdgeEndToEndResult, explain_edge_end_to_end
from dpone.dag.edge_explain import DagEdgeContext
from dpone.dag.node_explain import build_reverse_adjacency
from dpone.dag.yaml_types import ProcessNode


@dataclass(frozen=True, slots=True)
class NodeEndToEndResult:
    node: dict[str, Any]
    incoming: tuple[EdgeEndToEndResult, ...] = ()
    outgoing: tuple[EdgeEndToEndResult, ...] = ()
    warnings: tuple[str, ...] = ()

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "node": dict(self.node),
            "incoming": [e.to_json_dict() for e in self.incoming],
            "outgoing": [e.to_json_dict() for e in self.outgoing],
            "warnings": list(self.warnings),
        }


def _node_brief(n: ProcessNode) -> dict[str, Any]:
    lc = n.config.load_config
    src = None
    if lc.source_schema and lc.source_table:
        src = f"{lc.source_schema}.{lc.source_table}"
    snk = None
    if lc.target_schema and lc.target_table:
        snk = f"{lc.target_schema}.{lc.target_table}"
    return {
        "name": n.name,
        "ref": n.ref,
        "selector": n.selector,
        "task_group": n.task_group,
        "config_path": str(n.config_path),
        "source": src,
        "sink": snk,
    }


def explain_node_end_to_end(
    ctx: DagEdgeContext,
    *,
    node_name: str,
    focus_peer_name: str | None = None,
    direction: str = "both",
    max_in: int = 50,
    max_out: int = 50,
    max_triggers: int = 10,
    registry_paths: Sequence[Path] = (),
    cache: ProcessExplainCache | None = None,
) -> NodeEndToEndResult:
    """Explain a node neighborhood end-to-end."""

    if node_name not in ctx.nodes_by_name:
        raise ValueError(f"Node '{node_name}' not found")

    direction = (direction or "both").strip().lower()
    if direction not in ("in", "out", "both"):
        raise ValueError("direction must be one of: in, out, both")

    node = ctx.nodes_by_name[node_name]
    warnings: list[str] = []

    if cache is None:
        cache = ProcessExplainCache(registry_paths=registry_paths)

    rev = build_reverse_adjacency(ctx)
    incoming_names = sorted(rev.get(node_name, set()))
    outgoing_names = sorted(ctx.adjacency.get(node_name, set()))

    # Optional: focus on a single peer node to reduce output/noise.
    if focus_peer_name:
        incoming_names = [n for n in incoming_names if n == focus_peer_name]
        outgoing_names = [n for n in outgoing_names if n == focus_peer_name]

    if len(incoming_names) > max_in:
        warnings.append(f"Incoming edges truncated: {len(incoming_names)} > max_in={max_in}")
        incoming_names = incoming_names[: max(0, int(max_in))]

    if len(outgoing_names) > max_out:
        warnings.append(f"Outgoing edges truncated: {len(outgoing_names)} > max_out={max_out}")
        outgoing_names = outgoing_names[: max(0, int(max_out))]

    incoming: list[EdgeEndToEndResult] = []
    outgoing: list[EdgeEndToEndResult] = []

    if direction in ("in", "both"):
        for up_name in incoming_names:
            up = ctx.nodes_by_name.get(up_name)
            if not up:
                continue
            incoming.append(
                explain_edge_end_to_end(
                    upstream_node=up,
                    downstream_node=node,
                    ctx=ctx,
                    registry_paths=registry_paths,
                    cache=cache,
                    max_triggers=max_triggers,
                    include_transitive_path=False,
                )
            )

    if direction in ("out", "both"):
        for dn_name in outgoing_names:
            dn = ctx.nodes_by_name.get(dn_name)
            if not dn:
                continue
            outgoing.append(
                explain_edge_end_to_end(
                    upstream_node=node,
                    downstream_node=dn,
                    ctx=ctx,
                    registry_paths=registry_paths,
                    cache=cache,
                    max_triggers=max_triggers,
                    include_transitive_path=False,
                )
            )

    return NodeEndToEndResult(
        node=_node_brief(node),
        incoming=tuple(incoming),
        outgoing=tuple(outgoing),
        warnings=tuple(warnings),
    )
