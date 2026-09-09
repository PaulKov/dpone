from __future__ import annotations

import math
import statistics
from collections.abc import Sequence
from pathlib import Path

from .models import CouplingStats
from .python_imports import build_module_files, iter_import_occurrences


def percentile(sorted_vals: Sequence[int], p: float) -> float:
    """Nearest-rank percentile for sorted integer list."""

    if not sorted_vals:
        return 0.0
    if p <= 0:
        return float(sorted_vals[0])
    if p >= 100:
        return float(sorted_vals[-1])
    k = int(math.ceil((p / 100.0) * len(sorted_vals)))
    k = max(1, min(len(sorted_vals), k))
    return float(sorted_vals[k - 1])


def collect_internal_deps(
    package_dir: Path,
    *,
    module_files: Sequence[Path],
    package_name: str,
) -> dict[str, set[str]]:
    """Import dependency graph: src_module -> {target_modules} only inside package_name.*"""

    files = build_module_files(package_dir, package_name=package_name)
    module_set = {m.module_name for m in files}
    deps_out: dict[str, set[str]] = {m: set() for m in module_set}

    for occ in iter_import_occurrences(package_dir, package_name=package_name, module_files=files):
        tgt = occ.target_module
        while tgt not in module_set and "." in tgt:
            tgt = tgt.rsplit(".", 1)[0]
        if tgt in module_set and tgt != occ.source_module:
            deps_out[occ.source_module].add(tgt)

    return deps_out


def slice_group(module_name: str, *, package_name: str) -> str:
    """Group modules into coarse-grained slices.

    Heuristics:
      - dpone.commands.<group>
      - dpone.services.<group>
      - otherwise dpone.<top>

    It is not perfect, but stable and useful for tracking cohesion trends.
    """

    parts = module_name.split(".")
    if len(parts) == 1:
        return f"{package_name}.root"
    if parts[1] in {"commands", "services"} and len(parts) >= 3:
        return f"{package_name}.{parts[1]}.{parts[2]}"
    return f"{package_name}.{parts[1]}"


def connected_components(nodes: Sequence[str], undirected_adj: dict[str, set[str]]) -> list[set[str]]:
    seen: set[str] = set()
    comps: list[set[str]] = []
    for n in nodes:
        if n in seen:
            continue
        stack = [n]
        comp: set[str] = set()
        seen.add(n)
        while stack:
            x = stack.pop()
            comp.add(x)
            for y in undirected_adj.get(x, set()):
                if y not in seen:
                    seen.add(y)
                    stack.append(y)
        comps.append(comp)
    return comps


def avg_clustering(nodes: Sequence[str], undirected_adj: dict[str, set[str]]) -> float:
    """Average clustering coefficient for undirected graph."""

    total = 0.0
    for n in nodes:
        neigh = list(undirected_adj.get(n, set()))
        k = len(neigh)
        if k < 2:
            continue
        edges = 0
        neigh_set = set(neigh)
        for u in neigh:
            edges += len(undirected_adj.get(u, set()) & neigh_set)
        edges = edges // 2
        total += (2.0 * edges) / (k * (k - 1))
    return total / max(1, len(nodes))


def compute_coupling_stats(deps_out: dict[str, set[str]], *, top_n: int, package_name: str) -> CouplingStats:
    modules = sorted(deps_out)
    n = len(modules)
    ce_list = [len(deps_out[m]) for m in modules]
    ce_sorted = sorted(ce_list)

    ca: dict[str, int] = {m: 0 for m in modules}
    edges = 0
    for src, tgts in deps_out.items():
        for tgt in tgts:
            ca[tgt] = ca.get(tgt, 0) + 1
            edges += 1

    avg_ce = float(sum(ce_list) / n) if n else 0.0
    med_ce = float(statistics.median(ce_sorted)) if ce_sorted else 0.0
    p90 = percentile(ce_sorted, 90)
    p95 = percentile(ce_sorted, 95)
    max_ce = max(ce_list) if ce_list else 0
    max_ce_module = modules[ce_list.index(max_ce)] if ce_list else ""

    max_ca = 0
    max_ca_module = ""
    for m, v in ca.items():
        if v > max_ca:
            max_ca = v
            max_ca_module = m

    undirected: dict[str, set[str]] = {m: set() for m in modules}
    for src, tgts in deps_out.items():
        for tgt in tgts:
            undirected[src].add(tgt)
            undirected[tgt].add(src)

    comps = connected_components(modules, undirected)
    lcc = max((len(c) for c in comps), default=0)
    lcc_ratio = (lcc / n) if n else 0.0
    clustering = avg_clustering(modules, undirected)

    intra = 0
    for src, tgts in deps_out.items():
        g_src = slice_group(src, package_name=package_name)
        for tgt in tgts:
            if g_src == slice_group(tgt, package_name=package_name):
                intra += 1
    cohesion_ratio = (intra / edges) if edges else 0.0

    top_out = sorted(((m, len(deps_out[m])) for m in modules), key=lambda x: (-x[1], x[0]))[:top_n]
    top_in = sorted(((m, ca.get(m, 0)) for m in modules), key=lambda x: (-x[1], x[0]))[:top_n]

    return CouplingStats(
        modules=n,
        internal_edges=edges,
        avg_ce=avg_ce,
        median_ce=med_ce,
        p90_ce=float(p90),
        p95_ce=float(p95),
        max_ce=max_ce,
        max_ce_module=max_ce_module,
        max_ca=max_ca,
        max_ca_module=max_ca_module,
        lcc_ratio=float(lcc_ratio),
        avg_clustering=float(clustering),
        cohesion_ratio=float(cohesion_ratio),
        top_out=top_out,
        top_in=top_in,
    )
