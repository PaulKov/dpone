from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LocStats:
    files: int
    total_lines: int
    total_sloc: int
    min_lines: int
    min_path: str
    max_lines: int
    max_path: str
    avg_lines: float
    median_lines: float
    top: list[tuple[str, int]]


@dataclass(frozen=True)
class CouplingStats:
    """Summary for internal import graph inside dpone.*"""

    modules: int
    internal_edges: int
    avg_ce: float
    median_ce: float
    p90_ce: float
    p95_ce: float
    max_ce: int
    max_ce_module: str
    max_ca: int
    max_ca_module: str
    lcc_ratio: float
    avg_clustering: float
    cohesion_ratio: float
    top_out: list[tuple[str, int]]
    top_in: list[tuple[str, int]]


@dataclass(frozen=True)
class LayerSummary:
    layer: str
    modules: int
    outgoing_edges: int
    incoming_edges: int
    intra_edges: int
    cross_out_edges: int
    cross_in_edges: int
    distinct_target_layers: int
    distinct_source_layers: int
    avg_ce: float
    avg_ca: float
    cohesion_ratio: float


@dataclass(frozen=True)
class LayerCouplingStats:
    """Coarse-grained coupling/cohesion summary by architectural layer/slice."""

    layers: int
    internal_edges: int
    intra_layer_edges: int
    cross_layer_edges: int
    intra_layer_ratio: float
    cross_layer_ratio: float
    summaries: list[LayerSummary]
    top_cross_layer: list[tuple[str, str, int]]
    top_cross_slice: list[tuple[str, str, int]]
    excluded_layers: list[str]
    excluded_modules: int
    excluded_edges: int
