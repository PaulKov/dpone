"""Step 20: end-to-end dependency explanation.

This module powers the CLI command:

  dpone dag explain-deps <root.yaml> --for <task>

It bridges three layers:
  1) Variant C compilation provenance (overrides/vars/naming)
  2) Post-parse normalization (ETLProcessConfig.from_dict -> DependencyConfig)
  3) DAG semantics (TaskGroupBuilder parity)

The implementation is UX-oriented: it does not change runtime semantics.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.manifest.explain import ExplainResult


from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.dag.deps_end_to_end_models import (
    DirectDependencyExplanation,
    EndToEndDependenciesResult,
    InheritedGroupDependencyExplanation,
    OriginInfo,
    ProducedEdge,
)
from dpone.dag.edge_explain import (
    DagEdgeContext,
    explain_direct_edge,
    resolve_dependency_path_to_task_names,
)
from dpone.dag.post_parse_explain import PostParseResult, explain_post_parse
from dpone.dag.yaml_types import ProcessNode
from dpone.manifest.explain import explain_manifest

# ------------------------- internal helpers -------------------------


@dataclass(frozen=True, slots=True)
class ProcessExplainContext:
    """Cached per-process context for end-to-end explanations."""

    explain: ExplainResult
    compiled_config: dict[str, Any]
    compiled_depends_on: list[Any]
    layer_titles: Mapping[str, str]
    post_parse: PostParseResult


class ProcessExplainCache:
    """Cache explain_manifest/post-parse per process to keep CLI snappy."""

    def __init__(self, *, registry_paths: Sequence[Path]) -> None:
        self._registry_paths = tuple(Path(p) for p in registry_paths if str(p))
        self._cache: dict[str, ProcessExplainContext] = {}

    def get(self, node: ProcessNode) -> ProcessExplainContext:
        key = f"{node.config_path.resolve(strict=False)}#{node.selector or node.name}"
        if key in self._cache:
            return self._cache[key]

        exp = explain_manifest(
            Path(node.config_path),
            selector=(node.selector or node.name),
            registry_paths=self._registry_paths,
            metadata_only=True,
            include_snapshots=False,
        )
        compiled_config = dict(exp.final_config)
        compiled_depends = _normalize_depends_on(compiled_config.get("depends_on"))
        layer_titles = {layer.id: layer.title for layer in exp.config_layers}
        post_parse = explain_post_parse(
            dict(compiled_config),
            base_path=Path(node.config_path).parent,
            origin_map=exp.config_origin,
            metadata_only=True,
        )

        ctx = ProcessExplainContext(
            explain=exp,
            compiled_config=compiled_config,
            compiled_depends_on=compiled_depends,
            layer_titles=layer_titles,
            post_parse=post_parse,
        )
        self._cache[key] = ctx
        return ctx


def _normalize_depends_on(v: Any) -> list[Any]:
    """Normalize compiled depends_on to a list to align indices with ETL parsing."""
    if v is None:
        return []
    if isinstance(v, list):
        return list(v)
    if isinstance(v, dict | str):
        return [v]
    return []


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


def _collect_origins(exp: ExplainResult, layer_titles: Mapping[str, str], prefix: str) -> tuple[OriginInfo, ...]:
    pfx = str(prefix)
    out: list[OriginInfo] = []
    for path, origin in (exp.config_origin or {}).items():
        if path == pfx or path.startswith(pfx + ".") or path.startswith(pfx + "["):
            out.append(OriginInfo(path=path, origin=origin, origin_title=layer_titles.get(origin)))

    out.sort(key=lambda x: x.path)
    if out:
        return tuple(out)

    # fallback: show something meaningful
    origin = (exp.config_origin or {}).get(pfx) or "unknown"
    return (OriginInfo(path=pfx, origin=origin, origin_title=layer_titles.get(origin)),)


def _collect_parse_records(post_parse: PostParseResult, dep_index: int) -> tuple[dict[str, Any], ...]:
    idx = int(dep_index)
    pfx = f"dependencies[{idx}]"

    out: list[dict[str, Any]] = []
    for r in post_parse.records:
        if r.target == "etl.depends_on" or r.target.startswith(pfx):
            out.append(
                {
                    "kind": r.kind,
                    "target": r.target,
                    "operation": r.operation,
                    "sources": [{"path": s.path, "origin": s.origin} for s in r.sources],
                    "details": dict(r.details or {}),
                    "value": r.value,
                }
            )
    return tuple(out)


def _get_parsed_dependency(post_parse: PostParseResult, dep_index: int) -> dict[str, Any] | None:
    try:
        deps = post_parse.normalized.get("dependencies") or []
        if isinstance(deps, list) and 0 <= dep_index < len(deps):
            v = deps[dep_index]
            if isinstance(v, Mapping):
                return dict(v)
    except Exception:
        return None
    return None


def _produced_edges_for_dependency(
    *,
    ctx: DagEdgeContext,
    downstream_node: ProcessNode,
    dep_index: int,
    max_upstreams: int,
    max_triggers: int,
) -> tuple[tuple[str, ...], tuple[ProducedEdge, ...]]:
    dep_cfg = None
    if 0 <= dep_index < len(downstream_node.dependencies or []):
        dep_cfg = downstream_node.dependencies[dep_index]

    if dep_cfg is None:
        return (), ()

    upstream_names: list[str] = []
    if getattr(dep_cfg, "group", None):
        g = str(dep_cfg.group)
        upstream_names = [n.name for n in ctx.nodes_by_group.get(g, [])]
    else:
        dep_path = str(getattr(dep_cfg, "path", "") or "")
        names, _mode, _detail = resolve_dependency_path_to_task_names(
            dep_path,
            current_node=downstream_node,
            nodes=ctx.nodes,
            all_groups=ctx.all_groups,
        )
        upstream_names = list(names)

    upstream_names = [n for n in upstream_names if n and n != downstream_node.name]
    if not upstream_names:
        return (), ()

    edges: list[ProducedEdge] = []
    for up in upstream_names[: max(0, int(max_upstreams))]:
        up_node = ctx.nodes_by_name.get(up)
        up_ref = up_node.ref if up_node else None
        exp = explain_direct_edge(
            ctx,
            upstream_name=up,
            downstream_name=downstream_node.name,
            max_triggers=int(max_triggers),
            include_transitive_path=False,
        )
        kinds = tuple(sorted({r.kind for r in exp.reasons}))
        reasons = tuple(r.to_jsonable() for r in exp.reasons)
        edges.append(ProducedEdge(upstream=up, upstream_ref=up_ref, reason_kinds=kinds, reasons=reasons))

    return tuple(upstream_names), tuple(edges)


def explain_dependency_item_for_node(
    *,
    node: ProcessNode,
    dep_index: int,
    ctx: DagEdgeContext,
    cache: ProcessExplainCache,
    max_upstreams: int = 30,
    max_triggers: int = 10,
) -> DirectDependencyExplanation:
    """Explain one depends_on item of a node (compiled -> parsed -> edges)."""

    idx = int(dep_index)
    warnings: list[str] = []

    pctx = cache.get(node)
    compiled_item: Any = None
    if 0 <= idx < len(pctx.compiled_depends_on):
        compiled_item = pctx.compiled_depends_on[idx]
    else:
        warnings.append(f"depends_on[{idx}] not found in compiled config (len={len(pctx.compiled_depends_on)}).")

    compiled_origins = _collect_origins(pctx.explain, pctx.layer_titles, f"depends_on[{idx}]")
    parsed_dependency = _get_parsed_dependency(pctx.post_parse, idx)
    parse_records = _collect_parse_records(pctx.post_parse, idx)

    upstream_tasks, edges = _produced_edges_for_dependency(
        ctx=ctx,
        downstream_node=node,
        dep_index=idx,
        max_upstreams=max_upstreams,
        max_triggers=max_triggers,
    )
    if not upstream_tasks:
        warnings.append("Dependency produced no upstream task matches (resolver could not match it)")

    return DirectDependencyExplanation(
        index=idx,
        compiled_item=compiled_item,
        compiled_origins=compiled_origins,
        parsed_dependency=parsed_dependency,
        parse_records=parse_records,
        upstream_tasks=upstream_tasks,
        edges=edges,
        warnings=tuple(warnings),
    )


# ------------------------- public API -------------------------


def explain_end_to_end_dependencies(
    *,
    downstream_node: ProcessNode,
    ctx: DagEdgeContext,
    registry_paths: Sequence[Path] = (),
    include_inherited_group_deps: bool = True,
    dep_indexes: Sequence[int] | None = None,
    max_upstreams: int = 30,
    max_triggers: int = 10,
) -> EndToEndDependenciesResult:
    """Explain dependencies for one downstream task (Step 20 UX)."""

    cache = ProcessExplainCache(registry_paths=registry_paths)
    warnings: list[str] = []

    direct: list[DirectDependencyExplanation] = []
    if dep_indexes is None:
        indexes = list(range(len(downstream_node.dependencies or [])))
    else:
        indexes = [int(i) for i in dep_indexes]

    for i in indexes:
        if i < 0:
            continue
        direct.append(
            explain_dependency_item_for_node(
                node=downstream_node,
                dep_index=int(i),
                ctx=ctx,
                cache=cache,
                max_upstreams=max_upstreams,
                max_triggers=max_triggers,
            )
        )

    inherited: list[InheritedGroupDependencyExplanation] = []
    if include_inherited_group_deps and downstream_node.task_group:
        dependent_group = str(downstream_node.task_group)

        for (up_g, dep_g), triggers in sorted(ctx.group_triggers.items(), key=lambda kv: (kv[0][0], kv[0][1])):
            if str(dep_g) != dependent_group:
                continue

            up_tasks = tuple(n.name for n in ctx.nodes_by_group.get(str(up_g), []))
            trig_dicts: list[dict[str, Any]] = []

            for tr in list(triggers)[: max(0, int(max_triggers))]:
                tn = ctx.nodes_by_name.get(tr.task_name)
                if not tn:
                    trig_dicts.append(
                        {
                            "task_name": tr.task_name,
                            "selector": tr.selector,
                            "dep_index": tr.dep_index,
                            "alias": tr.alias,
                            "origin": "unknown",
                            "manifest": None,
                            "compiled_item": None,
                        }
                    )
                    continue

                pctx = cache.get(tn)
                origins = _collect_origins(pctx.explain, pctx.layer_titles, f"depends_on[{tr.dep_index}]")
                origin = origins[0].origin if origins else "unknown"
                compiled_item = None
                if 0 <= tr.dep_index < len(pctx.compiled_depends_on):
                    compiled_item = pctx.compiled_depends_on[tr.dep_index]

                trig_dicts.append(
                    {
                        "task_name": tr.task_name,
                        "selector": tr.selector,
                        "dep_index": tr.dep_index,
                        "alias": tr.alias,
                        "origin": origin,
                        "manifest": str(tn.config_path),
                        "compiled_item": compiled_item,
                    }
                )

            inherited.append(
                InheritedGroupDependencyExplanation(
                    upstream_group=str(up_g),
                    dependent_group=str(dep_g),
                    upstream_tasks=up_tasks,
                    trigger_count=len(triggers),
                    triggers=tuple(trig_dicts),
                )
            )

    return EndToEndDependenciesResult(
        downstream=_node_brief(downstream_node),
        direct=tuple(direct),
        inherited_group_deps=tuple(inherited),
        warnings=tuple(warnings),
    )
