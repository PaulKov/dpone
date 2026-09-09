from __future__ import annotations

import subprocess
from pathlib import Path

from dpone.metrics.import_graph import (
    collect_internal_deps,
    compute_coupling_stats,
    connected_components,
    percentile,
    slice_group,
)
from dpone.metrics.loc import calc_loc_stats, count_lines, count_sloc, iter_py_files
from dpone.metrics.models import CouplingStats, LayerCouplingStats, LayerSummary, LocStats
from dpone.metrics.python_imports import build_module_files, iter_import_occurrences, module_name_from_path
from dpone.metrics.render import (
    build_metrics_block_md,
    render_cross_flow_table_md,
    render_layer_summary_table_md,
    render_loc_table_md,
    render_top_list_md,
)
from dpone.metrics.stable_contracts import stable_contract_for


def write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def test_loc_metrics_ignore_cache_and_build_dirs(tmp_path: Path) -> None:
    keep = write(tmp_path / "src" / "package" / "module.py", "a = 1\n\n# comment\nb = 2\n")
    hidden = write(tmp_path / ".venv" / "lib.py", "ignored = True\n")
    built = write(tmp_path / "dist" / "artifact.py", "ignored = True\n")

    files = list(iter_py_files(tmp_path))
    stats = calc_loc_stats(files, root=tmp_path, top_n=5)

    assert files == [keep]
    assert hidden not in files
    assert built not in files
    assert count_lines("a\nb\n") == 2
    assert count_sloc("a = 1\n\n# comment\nb = 2\n") == 2
    assert stats == LocStats(
        files=1,
        total_lines=4,
        total_sloc=2,
        min_lines=4,
        min_path="src/package/module.py",
        max_lines=4,
        max_path="src/package/module.py",
        avg_lines=4.0,
        median_lines=4.0,
        top=[("src/package/module.py", 4)],
    )


def test_loc_metrics_tracked_only_uses_git_index(tmp_path: Path) -> None:
    tracked = write(tmp_path / "src" / "package" / "tracked.py", "a = 1\n")
    untracked = write(tmp_path / "src" / "package" / "scratch.py", "b = 2\n")

    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True, text=True)
    subprocess.run(["git", "add", "src/package/tracked.py"], cwd=tmp_path, check=True, capture_output=True, text=True)

    filesystem_files = list(iter_py_files(tmp_path))
    tracked_files = list(iter_py_files(tmp_path, tracked_only=True, repo_root=tmp_path))

    assert tracked in filesystem_files
    assert untracked in filesystem_files
    assert tracked_files == [tracked.resolve()]


def test_loc_metrics_tie_breaks_paths_independently_from_input_order(tmp_path: Path) -> None:
    later = write(tmp_path / "z_later.py", "")
    earlier = write(tmp_path / "a_earlier.py", "")
    largest_later = write(tmp_path / "z_largest.py", "x = 1\n")
    largest_earlier = write(tmp_path / "a_largest.py", "y = 1\n")

    stats = calc_loc_stats([later, largest_later, earlier, largest_earlier], root=tmp_path, top_n=5)

    assert stats.min_lines == 0
    assert stats.min_path == "a_earlier.py"
    assert stats.max_lines == 1
    assert stats.max_path == "a_largest.py"


def test_loc_metrics_return_empty_stats_for_no_readable_files(tmp_path: Path) -> None:
    stats = calc_loc_stats([], root=tmp_path, top_n=10)

    assert stats.files == 0
    assert stats.total_lines == 0
    assert stats.total_sloc == 0
    assert stats.min_lines == 0
    assert stats.max_lines == 0
    assert stats.top == []


def test_python_import_parser_resolves_absolute_and_relative_internal_imports(tmp_path: Path) -> None:
    package_dir = tmp_path / "demo"
    init = write(package_dir / "__init__.py", "from __future__ import annotations\n")
    app = write(
        package_dir / "app.py",
        "\n".join(
            [
                "import os",
                "import demo.core",
                "from .services import runner",
                "from . import config",
                "from __future__ import annotations",
            ]
        ),
    )
    write(package_dir / "services" / "__init__.py", "")
    write(package_dir / "services" / "runner.py", "from ..core import Engine\n")
    write(package_dir / "core.py", "class Engine: pass\n")
    write(package_dir / "broken.py", "def nope(:\n")

    module_files = build_module_files(package_dir, package_name="demo")
    occurrences = list(iter_import_occurrences(package_dir, package_name="demo", module_files=module_files))

    assert module_name_from_path(init, package_dir=package_dir, package_name="demo") == "demo"
    assert module_name_from_path(app, package_dir=package_dir, package_name="demo") == "demo.app"
    assert {(occ.source_module, occ.target_module) for occ in occurrences} == {
        ("demo.app", "demo.core"),
        ("demo.app", "demo.services.runner"),
        ("demo.app", "demo.config"),
        ("demo.services.runner", "demo.core.Engine"),
    }


def test_python_import_parser_ignores_type_checking_only_imports(tmp_path: Path) -> None:
    package_dir = tmp_path / "demo"
    write(package_dir / "__init__.py", "")
    write(
        package_dir / "app.py",
        "\n".join(
            [
                "from __future__ import annotations",
                "from typing import TYPE_CHECKING",
                "from demo.runtime import loader",
                "if TYPE_CHECKING:",
                "    from demo.config import LoadConfig",
            ]
        ),
    )
    write(package_dir / "runtime" / "__init__.py", "")
    write(package_dir / "runtime" / "loader.py", "")
    write(package_dir / "config.py", "class LoadConfig: ...\n")

    occurrences = list(
        iter_import_occurrences(
            package_dir,
            package_name="demo",
            module_files=build_module_files(package_dir, package_name="demo"),
        )
    )

    assert {(occ.source_module, occ.target_module) for occ in occurrences} == {("demo.app", "demo.runtime.loader")}


def test_import_graph_collects_internal_deps_and_computes_coupling_stats(tmp_path: Path) -> None:
    package_dir = tmp_path / "demo"
    write(package_dir / "__init__.py", "")
    write(package_dir / "app.py", "from demo.services import runner\nfrom demo.core import Engine\n")
    write(package_dir / "core.py", "class Engine: pass\n")
    write(package_dir / "services" / "__init__.py", "")
    write(package_dir / "services" / "runner.py", "from demo.core import Engine\n")

    deps = collect_internal_deps(
        package_dir,
        module_files=build_module_files(package_dir, package_name="demo"),
        package_name="demo",
    )
    stats = compute_coupling_stats(deps, top_n=3, package_name="demo")

    assert deps["demo.app"] == {"demo.core", "demo.services.runner"}
    assert deps["demo.services.runner"] == {"demo.core"}
    assert percentile([1, 2, 3, 4], 90) == 4.0
    assert slice_group("demo.commands.docs.update", package_name="demo") == "demo.commands.docs"
    assert connected_components(["a", "b", "c"], {"a": {"b"}, "b": {"a"}, "c": set()}) == [{"a", "b"}, {"c"}]
    assert stats.modules == 5
    assert stats.internal_edges == 3
    assert stats.max_ce == 2
    assert stats.max_ce_module == "demo.app"
    assert stats.max_ca == 2
    assert stats.max_ca_module == "demo.core"
    assert stats.top_out[0] == ("demo.app", 2)
    assert stats.top_in[0] == ("demo.core", 2)


def test_metrics_renderers_cover_empty_and_populated_markdown_blocks() -> None:
    assert render_loc_table_md(LocStats(0, 0, 0, 0, "", 0, "", 0.0, 0.0, [])) == "(no files)\n"
    assert render_top_list_md([]) == ""
    assert render_cross_flow_table_md([], src_label="From", tgt_label="To") == "(no cross-boundary flows)\n"

    loc = LocStats(
        files=2,
        total_lines=30,
        total_sloc=21,
        min_lines=10,
        min_path="a.py",
        max_lines=20,
        max_path="b.py",
        avg_lines=15.0,
        median_lines=15.0,
        top=[("b.py", 20), ("a.py", 10)],
    )

    assert "| `b.py` | 20 |" in render_loc_table_md(loc)
    assert "- `demo.app`: **2**\n" == render_top_list_md([("demo.app", 2)])


def test_metrics_blocks_include_architecture_interpretation_for_markdown() -> None:
    repo_loc = LocStats(1, 10, 6, 10, "a.py", 10, "a.py", 10.0, 10.0, [("a.py", 10)])
    dpone_loc = LocStats(1, 20, 11, 20, "dpone/a.py", 20, "dpone/a.py", 20.0, 20.0, [("dpone/a.py", 20)])
    coupling = CouplingStats(
        modules=2,
        internal_edges=1,
        avg_ce=0.5,
        median_ce=0.5,
        p90_ce=1.0,
        p95_ce=1.0,
        max_ce=1,
        max_ce_module="dpone.app",
        max_ca=1,
        max_ca_module="dpone.core",
        lcc_ratio=1.0,
        avg_clustering=0.0,
        cohesion_ratio=0.0,
        top_out=[("dpone.app", 1)],
        top_in=[("dpone.core", 1)],
    )
    layer_stats = LayerCouplingStats(
        layers=2,
        internal_edges=1,
        intra_layer_edges=0,
        cross_layer_edges=1,
        intra_layer_ratio=0.0,
        cross_layer_ratio=1.0,
        summaries=[
            LayerSummary(
                layer="dpone.app",
                modules=1,
                outgoing_edges=1,
                incoming_edges=0,
                intra_edges=0,
                cross_out_edges=1,
                cross_in_edges=0,
                distinct_target_layers=1,
                distinct_source_layers=0,
                avg_ce=1.0,
                avg_ca=0.0,
                cohesion_ratio=0.0,
            )
        ],
        top_cross_layer=[("dpone.app", "dpone.core", 1)],
        top_cross_slice=[("dpone.app", "dpone.core", 1)],
        excluded_layers=["dpone.runtime"],
        excluded_modules=3,
        excluded_edges=4,
    )

    md = build_metrics_block_md(
        start_marker="<!-- start -->",
        end_marker="<!-- end -->",
        repo_loc=repo_loc,
        dpone_loc=dpone_loc,
        coupling=coupling,
        layer_stats=layer_stats,
    )
    assert "AVG Ce = **0.50**" in md
    assert "Quality summary" in md
    assert "Architecture graph/hard-gate status" in md
    assert "Cross-layer ratio = **1.000**" in md
    assert "| `dpone.app` | 1 | 1 | 0 | 0 | 1 | 0 |" in render_layer_summary_table_md(layer_stats)


def test_metrics_mark_current_architecture_fitness_threshold_as_green() -> None:
    repo_loc = LocStats(1, 10, 6, 10, "a.py", 10, "a.py", 10.0, 10.0, [("a.py", 10)])
    dpone_loc = LocStats(1, 20, 11, 20, "dpone/a.py", 20, "dpone/a.py", 20.0, 20.0, [("dpone/a.py", 20)])
    coupling = CouplingStats(
        modules=2,
        internal_edges=1,
        avg_ce=0.5,
        median_ce=0.5,
        p90_ce=1.0,
        p95_ce=1.0,
        max_ce=1,
        max_ce_module="dpone.app",
        max_ca=1,
        max_ca_module="dpone.core",
        lcc_ratio=1.0,
        avg_clustering=0.18,
        cohesion_ratio=0.0,
        top_out=[],
        top_in=[],
    )
    layer_stats = LayerCouplingStats(
        layers=1,
        internal_edges=1,
        intra_layer_edges=1,
        cross_layer_edges=0,
        intra_layer_ratio=1.0,
        cross_layer_ratio=0.0,
        summaries=[],
        top_cross_layer=[],
        top_cross_slice=[],
        excluded_layers=[],
        excluded_modules=0,
        excluded_edges=0,
    )

    md = build_metrics_block_md(
        start_marker="<!-- start -->",
        end_marker="<!-- end -->",
        repo_loc=repo_loc,
        dpone_loc=dpone_loc,
        coupling=coupling,
        layer_stats=layer_stats,
    )

    assert "Architecture graph/hard-gate status: **🟢 excellent**" in md
    assert "Avg clustering: **0.180** → **OK**" in md


def test_metrics_render_approved_stable_contracts_in_fan_in_table() -> None:
    repo_loc = LocStats(1, 10, 6, 10, "a.py", 10, "a.py", 10.0, 10.0, [("a.py", 10)])
    dpone_loc = LocStats(1, 20, 11, 20, "dpone/a.py", 20, "dpone/a.py", 20.0, 20.0, [("dpone/a.py", 20)])
    coupling = CouplingStats(
        modules=2,
        internal_edges=1,
        avg_ce=0.5,
        median_ce=0.5,
        p90_ce=1.0,
        p95_ce=1.0,
        max_ce=1,
        max_ce_module="dpone.runtime.etl.processor",
        max_ca=43,
        max_ca_module="dpone.commands.output_json",
        lcc_ratio=1.0,
        avg_clustering=0.0,
        cohesion_ratio=1.0,
        top_out=[],
        top_in=[("dpone.commands.output_json", 43), ("dpone.runtime.etl.processor", 19)],
    )
    layer_stats = LayerCouplingStats(
        layers=1,
        internal_edges=1,
        intra_layer_edges=1,
        cross_layer_edges=0,
        intra_layer_ratio=1.0,
        cross_layer_ratio=0.0,
        summaries=[],
        top_cross_layer=[],
        top_cross_slice=[],
        excluded_layers=[],
        excluded_modules=0,
        excluded_edges=0,
    )

    md = build_metrics_block_md(
        start_marker="<!-- start -->",
        end_marker="<!-- end -->",
        repo_loc=repo_loc,
        dpone_loc=dpone_loc,
        coupling=coupling,
        layer_stats=layer_stats,
    )

    contract = stable_contract_for("dpone.commands.output_json")
    assert contract is not None
    assert "- `dpone.commands.output_json`: **43** - approved stable contract: output serialization port" in md
    assert "Approved stable high fan-in contracts are expected shared DTO/port modules" in md
