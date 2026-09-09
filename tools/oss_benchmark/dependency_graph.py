"""Dependency graph metrics for OSS benchmark project collectors."""

from __future__ import annotations

import math
import statistics
from collections import Counter, deque
from pathlib import Path

from tools.oss_benchmark.models import CouplingMetrics
from tools.oss_benchmark.source_files import parse_import_targets, read_text


def collect_internal_deps(
    source_files: tuple[Path, ...],
    module_names: dict[Path, str],
    production_modules: set[str],
) -> dict[str, set[str]]:
    """Collect internal production dependency edges from parsed imports."""

    module_set = {module for module in module_names.values() if module in production_modules}
    deps: dict[str, set[str]] = {module: set() for module in production_modules}
    for path in source_files:
        source_module = module_names[path]
        if source_module not in production_modules:
            continue
        for target in parse_import_targets(path, read_text(path)):
            resolved = resolve_internal_target(target, module_set)
            if resolved and resolved != source_module and resolved in production_modules:
                deps[source_module].add(resolved)
    return deps


def resolve_internal_target(target: str, module_set: set[str]) -> str | None:
    """Resolve an import target to a known internal module."""

    target = target.strip().removesuffix(".*")
    candidates = [target]
    current = target
    while "." in current:
        current = current.rsplit(".", 1)[0]
        candidates.append(current)
    for candidate in candidates:
        if candidate in module_set:
            return candidate
        if "." in candidate:
            suffix_matches = sorted(module for module in module_set if module.endswith("." + candidate))
            if len(suffix_matches) == 1:
                return suffix_matches[0]
    return None


def compute_coupling_metrics(
    deps_out: dict[str, set[str]],
    *,
    module_slices: dict[str, str],
    top_n: int,
) -> CouplingMetrics:
    """Compute fan-in/fan-out, cohesion and clustering metrics."""

    modules = sorted(deps_out)
    ce_values = [len(deps_out[module]) for module in modules]
    ca: Counter[str] = Counter()
    internal_edges = 0
    for targets in deps_out.values():
        for target in targets:
            ca[target] += 1
            internal_edges += 1

    undirected = {module: set() for module in modules}
    intra_slice = 0
    for source, targets in deps_out.items():
        for target in targets:
            undirected.setdefault(source, set()).add(target)
            undirected.setdefault(target, set()).add(source)
            if module_slices.get(source) == module_slices.get(target):
                intra_slice += 1

    max_ce = max(ce_values, default=0)
    lcc = max((len(component) for component in connected_components(modules, undirected)), default=0)
    cohesion = (intra_slice / internal_edges) if internal_edges else 0.0
    return CouplingMetrics(
        modules=len(modules),
        internal_edges=internal_edges,
        avg_ce=(float(sum(ce_values) / len(ce_values)) if ce_values else 0.0),
        median_ce=(float(statistics.median(ce_values)) if ce_values else 0.0),
        p90_ce=percentile(sorted(ce_values), 90),
        p95_ce=percentile(sorted(ce_values), 95),
        max_ce=max_ce,
        max_ce_module=modules[ce_values.index(max_ce)] if ce_values else "",
        max_ca=max((ca[module] for module in modules), default=0),
        max_ca_module=_max_ca_module(modules, ca),
        lcc_ratio=(float(lcc / len(modules)) if modules else 0.0),
        avg_clustering=avg_clustering(modules, undirected),
        cohesion_ratio=float(cohesion),
        cross_slice_ratio=float(1.0 - cohesion if internal_edges else 0.0),
        top_out=tuple(sorted(((m, len(deps_out[m])) for m in modules), key=lambda item: (-item[1], item[0]))[:top_n]),
        top_in=tuple(sorted(((m, ca[m]) for m in modules), key=lambda item: (-item[1], item[0]))[:top_n]),
    )


def percentile(sorted_values: list[int], p: float) -> float:
    if not sorted_values:
        return 0.0
    if p <= 0:
        return float(sorted_values[0])
    if p >= 100:
        return float(sorted_values[-1])
    rank = int(math.ceil((p / 100.0) * len(sorted_values)))
    return float(sorted_values[max(1, min(len(sorted_values), rank)) - 1])


def connected_components(nodes: list[str], undirected: dict[str, set[str]]) -> list[set[str]]:
    seen: set[str] = set()
    components: list[set[str]] = []
    for node in nodes:
        if node in seen:
            continue
        component: set[str] = set()
        queue: deque[str] = deque([node])
        seen.add(node)
        while queue:
            current = queue.popleft()
            component.add(current)
            for neighbour in undirected.get(current, set()):
                if neighbour not in seen:
                    seen.add(neighbour)
                    queue.append(neighbour)
        components.append(component)
    return components


def avg_clustering(nodes: list[str], undirected: dict[str, set[str]]) -> float:
    total = 0.0
    for node in nodes:
        neighbours = list(undirected.get(node, set()))
        degree = len(neighbours)
        if degree < 2:
            continue
        neighbour_set = set(neighbours)
        edge_count = sum(len(undirected.get(neighbour, set()) & neighbour_set) for neighbour in neighbours) // 2
        total += (2.0 * edge_count) / (degree * (degree - 1))
    return float(total / max(1, len(nodes)))


def _max_ca_module(modules: list[str], ca: Counter[str]) -> str:
    max_module = ""
    max_value = 0
    for module in modules:
        if ca[module] > max_value:
            max_value = ca[module]
            max_module = module
    return max_module
