from __future__ import annotations

from dpone.metrics.layer_metrics import compute_layer_coupling_stats, layer_group
from dpone.metrics.models import CouplingStats, LocStats
from dpone.metrics.render import build_metrics_block_md


def test_layer_group_maps_compat_shims() -> None:
    assert layer_group("dpone.source.base", package_name="dpone") == "dpone.compat"
    assert layer_group("dpone.yaml_config_handler.edge_explain", package_name="dpone") == "dpone.compat"
    assert layer_group("dpone.runtime.sinks.base", package_name="dpone") == "dpone.runtime"
    assert layer_group("dpone.commands.registry", package_name="dpone") == "dpone.commands"


def test_compute_layer_coupling_stats_counts_cross_layer_and_slice_flows() -> None:
    deps_out = {
        "dpone.commands.registry": {
            "dpone.commands.base",
            "dpone.services.docs.update_dev_metrics_service",
        },
        "dpone.commands.base": {"dpone.services.docs.update_dev_metrics_service"},
        "dpone.services.docs.update_dev_metrics_service": {
            "dpone.metrics.render",
            "dpone.ports.filesystem",
        },
        "dpone.metrics.render": set(),
        "dpone.ports.filesystem": set(),
        "dpone.source.base": {"dpone.runtime.sources.base"},
        "dpone.runtime.sources.base": set(),
    }

    stats = compute_layer_coupling_stats(deps_out, top_n=10, package_name="dpone")
    by_layer = {row.layer: row for row in stats.summaries}

    assert stats.layers == 6
    assert stats.internal_edges == 6
    assert stats.intra_layer_edges == 1
    assert stats.cross_layer_edges == 5
    assert stats.cross_layer_ratio == 5 / 6

    commands = by_layer["dpone.commands"]
    assert commands.modules == 2
    assert commands.outgoing_edges == 3
    assert commands.intra_edges == 1
    assert commands.cross_out_edges == 2
    assert commands.cohesion_ratio == 1 / 3

    compat = by_layer["dpone.compat"]
    assert compat.modules == 1
    assert compat.outgoing_edges == 1
    assert compat.cross_out_edges == 1

    assert stats.top_cross_layer[0] == (
        "dpone.commands",
        "dpone.services",
        2,
    )
    assert (
        "dpone.commands.registry",
        "dpone.services.docs",
        1,
    ) in stats.top_cross_slice
    assert (
        "dpone.commands.base",
        "dpone.services.docs",
        1,
    ) in stats.top_cross_slice


def test_build_metrics_block_md_contains_layer_section() -> None:
    coupling = CouplingStats(
        modules=2,
        internal_edges=1,
        avg_ce=0.5,
        median_ce=0.5,
        p90_ce=1.0,
        p95_ce=1.0,
        max_ce=1,
        max_ce_module="dpone.commands.registry",
        max_ca=1,
        max_ca_module="dpone.services.docs",
        lcc_ratio=1.0,
        avg_clustering=0.0,
        cohesion_ratio=0.5,
        top_out=[("dpone.commands.registry", 1)],
        top_in=[("dpone.services.docs", 1)],
    )
    layer_stats = compute_layer_coupling_stats(
        {
            "dpone.commands.registry": {"dpone.services.docs.update_dev_metrics_service"},
            "dpone.services.docs.update_dev_metrics_service": set(),
        },
        top_n=5,
        package_name="dpone",
    )
    block = build_metrics_block_md(
        start_marker="<!-- START -->",
        end_marker="<!-- END -->",
        repo_loc=LocStats(1, 10, 6, 10, "a.py", 10, "a.py", 10.0, 10.0, [("a.py", 10)]),
        dpone_loc=LocStats(1, 10, 6, 10, "src/dpone/a.py", 10, "src/dpone/a.py", 10.0, 10.0, [("src/dpone/a.py", 10)]),
        coupling=coupling,
        layer_stats=layer_stats,
    )
    assert "### Layer / Slice architecture metrics" in block
    assert "Top 1 cross-layer dependency flows" in block
    assert "`dpone.commands`" in block
    assert "`dpone.services`" in block
