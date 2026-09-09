from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable

from .import_graph import slice_group
from .models import LayerCouplingStats, LayerSummary

COMPAT_LAYER_PREFIXES: tuple[str, ...] = (
    "dpone.source",
    "dpone.sink",
    "dpone.etl",
    "dpone.etl_logging",
    "dpone.credentials",
    "dpone.state",
    "dpone.reconciliation",
    "dpone.sql_helpers",
    "dpone.xmin",
    "dpone.yaml_config_handler",
    "dpone.cli.legacy",
    "dpone.lib.connectors",
    "dpone.dbt_publish",
)


def layer_group(module_name: str, *, package_name: str = "dpone") -> str:
    """Map a module to a coarse architectural layer.

    Most modules map to their top-level package (e.g. ``dpone.manifest``), while
    deprecated compatibility shims are grouped under ``dpone.compat`` to keep the
    architecture view focused on the canonical structure.
    """

    for prefix in COMPAT_LAYER_PREFIXES:
        if module_name == prefix or module_name.startswith(prefix + "."):
            return f"{package_name}.compat"

    parts = module_name.split(".")
    if len(parts) <= 1:
        return f"{package_name}.root"
    return f"{package_name}.{parts[1]}"


def compute_layer_coupling_stats(
    deps_out: dict[str, set[str]],
    *,
    top_n: int,
    package_name: str,
    exclude_layers: Iterable[str] = (),
) -> LayerCouplingStats:
    """Compute architecture metrics for coarse layers and slices.

    ``exclude_layers`` removes compatibility-only or otherwise non-canonical
    layers from the reported subgraph while keeping track of how many modules and
    edges were excluded. This keeps the architectural view focused on the target
    structure without hiding that transitional layers still exist in the codebase.
    """

    excluded = set(exclude_layers)
    modules_all = sorted(deps_out)
    module_to_layer_all = {m: layer_group(m, package_name=package_name) for m in modules_all}

    modules = [m for m in modules_all if module_to_layer_all[m] not in excluded]
    module_to_layer = {m: module_to_layer_all[m] for m in modules}
    module_to_slice = {m: slice_group(m, package_name=package_name) for m in modules}

    excluded_modules = len(modules_all) - len(modules)
    excluded_edges = 0
    for src, tgts in deps_out.items():
        src_excluded = module_to_layer_all[src] in excluded
        for tgt in tgts:
            if src_excluded or module_to_layer_all.get(tgt) in excluded:
                excluded_edges += 1

    module_ca: dict[str, int] = {m: 0 for m in modules}
    for src in modules:
        for tgt in deps_out.get(src, set()):
            if tgt in module_ca:
                module_ca[tgt] = module_ca.get(tgt, 0) + 1

    layer_modules: dict[str, list[str]] = defaultdict(list)
    for mod in modules:
        layer_modules[module_to_layer[mod]].append(mod)

    outgoing_edges: Counter[str] = Counter()
    incoming_edges: Counter[str] = Counter()
    intra_edges: Counter[str] = Counter()
    cross_out_edges: Counter[str] = Counter()
    cross_in_edges: Counter[str] = Counter()
    target_layers: dict[str, set[str]] = defaultdict(set)
    source_layers: dict[str, set[str]] = defaultdict(set)
    layer_pair_counts: Counter[tuple[str, str]] = Counter()
    cross_slice_counts: Counter[tuple[str, str]] = Counter()

    total_edges = 0
    intra_total = 0

    for src in modules:
        tgts = {t for t in deps_out.get(src, set()) if t in module_to_layer}
        src_layer = module_to_layer[src]
        src_slice = module_to_slice[src]
        outgoing_edges[src_layer] += len(tgts)
        total_edges += len(tgts)
        for tgt in tgts:
            tgt_layer = module_to_layer[tgt]
            tgt_slice = module_to_slice[tgt]
            incoming_edges[tgt_layer] += 1
            layer_pair_counts[(src_layer, tgt_layer)] += 1
            if src_layer == tgt_layer:
                intra_edges[src_layer] += 1
                intra_total += 1
            else:
                cross_out_edges[src_layer] += 1
                cross_in_edges[tgt_layer] += 1
                target_layers[src_layer].add(tgt_layer)
                source_layers[tgt_layer].add(src_layer)
            if src_slice != tgt_slice:
                cross_slice_counts[(src_slice, tgt_slice)] += 1

    layer_names = sorted(layer_modules)
    summaries: list[LayerSummary] = []
    for layer in layer_names:
        mods = sorted(layer_modules[layer])
        mod_count = len(mods)
        avg_ce = (
            float(sum(len({t for t in deps_out[m] if t in module_to_layer}) for m in mods) / mod_count)
            if mod_count
            else 0.0
        )
        avg_ca = float(sum(module_ca.get(m, 0) for m in mods) / mod_count) if mod_count else 0.0
        layer_out = int(outgoing_edges[layer])
        layer_intra = int(intra_edges[layer])
        summaries.append(
            LayerSummary(
                layer=layer,
                modules=mod_count,
                outgoing_edges=layer_out,
                incoming_edges=int(incoming_edges[layer]),
                intra_edges=layer_intra,
                cross_out_edges=int(cross_out_edges[layer]),
                cross_in_edges=int(cross_in_edges[layer]),
                distinct_target_layers=len(target_layers[layer]),
                distinct_source_layers=len(source_layers[layer]),
                avg_ce=avg_ce,
                avg_ca=avg_ca,
                cohesion_ratio=(float(layer_intra / layer_out) if layer_out else 1.0),
            )
        )

    cross_total = total_edges - intra_total
    top_cross_layer = sorted(
        ((src, tgt, n) for (src, tgt), n in layer_pair_counts.items() if src != tgt),
        key=lambda x: (-x[2], x[0], x[1]),
    )[:top_n]
    top_cross_slice = sorted(
        ((src, tgt, n) for (src, tgt), n in cross_slice_counts.items()),
        key=lambda x: (-x[2], x[0], x[1]),
    )[:top_n]

    return LayerCouplingStats(
        layers=len(layer_names),
        internal_edges=total_edges,
        intra_layer_edges=intra_total,
        cross_layer_edges=cross_total,
        intra_layer_ratio=(float(intra_total / total_edges) if total_edges else 1.0),
        cross_layer_ratio=(float(cross_total / total_edges) if total_edges else 0.0),
        summaries=summaries,
        top_cross_layer=top_cross_layer,
        top_cross_slice=top_cross_slice,
        excluded_layers=sorted(excluded),
        excluded_modules=excluded_modules,
        excluded_edges=excluded_edges,
    )
