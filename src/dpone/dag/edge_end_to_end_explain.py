"""Step 21: end-to-end explain for a *specific* DAG edge A -> B.

This is a focused UX tool that answers:

  "Why does edge A -> B exist?" and "Which exact depends_on item (and which file)
   produced it?"

It combines:
  - DAG edge reasons (TaskGroupBuilder parity) from `edge_explain`
  - Manifest compilation provenance (Variant C overrides/vars/naming)
  - Post-parse normalization provenance (ETLProcessConfig.from_dict)

CLI (Step 21):

  dpone dag explain-edge-e2e <root.yaml> --from <A> --to <B>
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.dag.deps_end_to_end_explain import (
    DirectDependencyExplanation,
    OriginInfo,
    ProcessExplainCache,
    explain_dependency_item_for_node,
)
from dpone.dag.edge_explain import DagEdgeContext, DagEdgeExplanation, explain_direct_edge
from dpone.dag.yaml_types import ProcessNode


@dataclass(frozen=True, slots=True)
class TriggerDependencyAttribution:
    """A dependency declared in some *trigger* task that caused group-to-group edges."""

    trigger_task: dict[str, Any]
    depends_on_index: int
    compiled_item: Any
    compiled_origins: tuple[OriginInfo, ...] = ()
    parsed_dependency: dict[str, Any] | None = None
    parse_records: tuple[dict[str, Any], ...] = ()
    warnings: tuple[str, ...] = ()

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "trigger_task": dict(self.trigger_task),
            "depends_on_index": self.depends_on_index,
            "compiled_item": self.compiled_item,
            "compiled_origins": [o.to_json_dict() for o in self.compiled_origins],
            "parsed_dependency": self.parsed_dependency,
            "parse_records": list(self.parse_records),
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True, slots=True)
class GroupToGroupAttribution:
    upstream_group: str
    dependent_group: str
    trigger_count: int
    triggers: tuple[TriggerDependencyAttribution, ...] = ()
    warnings: tuple[str, ...] = ()

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "upstream_group": self.upstream_group,
            "dependent_group": self.dependent_group,
            "trigger_count": self.trigger_count,
            "triggers": [t.to_json_dict() for t in self.triggers],
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True, slots=True)
class EdgeEndToEndResult:
    upstream: dict[str, Any]
    downstream: dict[str, Any]
    direct_edge: bool
    reasons: tuple[dict[str, Any], ...] = ()
    direct_dep_attributions: tuple[DirectDependencyExplanation, ...] = ()
    group_to_group_attributions: tuple[GroupToGroupAttribution, ...] = ()
    warnings: tuple[str, ...] = ()
    path: tuple[str, ...] | None = None

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "upstream": dict(self.upstream),
            "downstream": dict(self.downstream),
            "direct_edge": self.direct_edge,
            "reasons": list(self.reasons),
            "direct_dep_attributions": [d.to_json_dict() for d in self.direct_dep_attributions],
            "group_to_group_attributions": [g.to_json_dict() for g in self.group_to_group_attributions],
            "warnings": list(self.warnings),
            "path": list(self.path) if self.path else None,
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


def explain_edge_end_to_end(
    *,
    upstream_node: ProcessNode,
    downstream_node: ProcessNode,
    ctx: DagEdgeContext,
    registry_paths: Sequence[Path] = (),
    cache: ProcessExplainCache | None = None,
    max_triggers: int = 10,
    include_transitive_path: bool = False,
) -> EdgeEndToEndResult:
    """Explain a *specific* edge A -> B end-to-end."""

    edge_exp: DagEdgeExplanation = explain_direct_edge(
        ctx,
        upstream_name=upstream_node.name,
        downstream_name=downstream_node.name,
        max_triggers=int(max_triggers),
        include_transitive_path=bool(include_transitive_path),
    )

    if cache is None:
        cache = ProcessExplainCache(registry_paths=registry_paths)
    warnings: list[str] = []

    # Convert reasons to JSONable dicts early (CLI prints them as-is).
    reasons_json = tuple(r.to_jsonable() for r in edge_exp.reasons)

    if not edge_exp.direct_edge:
        return EdgeEndToEndResult(
            upstream=_node_brief(upstream_node),
            downstream=_node_brief(downstream_node),
            direct_edge=False,
            reasons=reasons_json,
            warnings=tuple(edge_exp.warnings) or tuple(warnings),
            path=tuple(edge_exp.path) if edge_exp.path else None,
        )

    # 1) direct depends_on indices in the *downstream* task
    dep_indices: list[int] = []
    for r in edge_exp.reasons:
        idx = r.evidence.get("depends_on_index")
        if idx is None:
            continue
        try:
            dep_indices.append(int(idx))
        except Exception:
            continue
    dep_indices = sorted(set(dep_indices))

    direct_dep_attribs: list[DirectDependencyExplanation] = []
    for idx in dep_indices:
        dd = explain_dependency_item_for_node(
            node=downstream_node,
            dep_index=idx,
            ctx=ctx,
            cache=cache,
            max_upstreams=1_000_000,  # we will filter to the requested upstream below
            max_triggers=max_triggers,
        )

        # Filter edges to keep only the requested upstream (focus mode)
        filtered_edges = tuple(e for e in dd.edges if e.upstream == upstream_node.name)
        filtered_upstreams = tuple([upstream_node.name]) if filtered_edges else ()
        direct_dep_attribs.append(
            DirectDependencyExplanation(
                index=dd.index,
                compiled_item=dd.compiled_item,
                compiled_origins=dd.compiled_origins,
                parsed_dependency=dd.parsed_dependency,
                parse_records=dd.parse_records,
                upstream_tasks=filtered_upstreams,
                edges=filtered_edges,
                warnings=dd.warnings,
            )
        )

    # 2) group-to-group reasons: attribute to trigger tasks inside the dependent group
    group_attribs: list[GroupToGroupAttribution] = []
    for r in edge_exp.reasons:
        if r.kind != "group_to_group":
            continue
        up_g = str(r.evidence.get("upstream_group") or "")
        dep_g = str(r.evidence.get("downstream_group") or "")
        if not up_g or not dep_g:
            continue

        # find triggers from context (authoritative)
        triggers = ctx.group_triggers.get((up_g, dep_g)) or []
        trig_attribs: list[TriggerDependencyAttribution] = []

        for tr in list(triggers)[: max(0, int(max_triggers))]:
            tn = ctx.nodes_by_name.get(tr.task_name)
            tw: list[str] = []
            if not tn:
                trig_attribs.append(
                    TriggerDependencyAttribution(
                        trigger_task={"name": tr.task_name, "ref": None, "selector": tr.selector, "task_group": dep_g},
                        depends_on_index=int(tr.dep_index),
                        compiled_item=None,
                        compiled_origins=(),
                        parsed_dependency=None,
                        parse_records=(),
                        warnings=("Trigger task not found in DAG nodes",),
                    )
                )
                continue

            pctx = cache.get(tn)
            # compiled item + origins
            compiled_item = None
            if 0 <= tr.dep_index < len(pctx.compiled_depends_on):
                compiled_item = pctx.compiled_depends_on[tr.dep_index]
            else:
                tw.append(
                    f"depends_on[{tr.dep_index}] not found in compiled config (len={len(pctx.compiled_depends_on)})."
                )

            # Reuse the dependency-explain helper for parsing provenance, but keep it lightweight.
            dd = explain_dependency_item_for_node(
                node=tn,
                dep_index=int(tr.dep_index),
                ctx=ctx,
                cache=cache,
                # edges are not relevant here (group expansion affects *all* tasks in dependent group)
                max_upstreams=0,
                max_triggers=max_triggers,
            )

            trig_attribs.append(
                TriggerDependencyAttribution(
                    trigger_task=_node_brief(tn),
                    depends_on_index=int(tr.dep_index),
                    compiled_item=compiled_item,
                    compiled_origins=dd.compiled_origins,
                    parsed_dependency=dd.parsed_dependency,
                    parse_records=dd.parse_records,
                    warnings=tuple(tw) + tuple(dd.warnings),
                )
            )

        group_attribs.append(
            GroupToGroupAttribution(
                upstream_group=up_g,
                dependent_group=dep_g,
                trigger_count=len(triggers),
                triggers=tuple(trig_attribs),
            )
        )

    return EdgeEndToEndResult(
        upstream=_node_brief(upstream_node),
        downstream=_node_brief(downstream_node),
        direct_edge=True,
        reasons=reasons_json,
        direct_dep_attributions=tuple(direct_dep_attribs),
        group_to_group_attributions=tuple(group_attribs),
        warnings=tuple(edge_exp.warnings) or tuple(warnings),
        path=tuple(edge_exp.path) if edge_exp.path else None,
    )
