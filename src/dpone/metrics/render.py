from __future__ import annotations

from .models import CouplingStats, LayerCouplingStats, LocStats
from .stable_contracts import stable_contract_for


def _grade(value: float, *, ok_max: float, warn_max: float) -> str:
    if value <= ok_max:
        return "OK"
    if value <= warn_max:
        return "WARN"
    return "ALERT"


def _grade_min(value: float, *, ok_min: float, warn_min: float) -> str:
    if value >= ok_min:
        return "OK"
    if value >= warn_min:
        return "WARN"
    return "ALERT"


def render_loc_table_md(stats: LocStats) -> str:
    if not stats.top:
        return "(no files)\n"
    lines: list[str] = []
    lines.append("| Module | Lines |\n")
    lines.append("|---|---:|\n")
    for path, n in stats.top:
        lines.append(f"| `{path}` | {n} |\n")
    return "".join(lines)


def _traffic_light_md(*grades: str) -> tuple[str, str]:
    if "ALERT" in grades:
        return "🔴", "critical"
    if "WARN" in grades:
        return "🟡", "warning"
    return "🟢", "excellent"


def render_top_list_md(items: list[tuple[str, int]], *, annotate_stable_contracts: bool = False) -> str:
    lines: list[str] = []
    for module, value in items:
        suffix = ""
        if annotate_stable_contracts:
            contract = stable_contract_for(module)
            if contract is not None:
                suffix = f" - approved stable contract: {contract.kind}"
        lines.append(f"- `{module}`: **{value}**{suffix}")
    return "\n".join(lines) + ("\n" if lines else "")


def render_layer_summary_table_md(stats: LayerCouplingStats) -> str:
    if not stats.summaries:
        return "(no layers)\n"
    lines: list[str] = []
    lines.append(
        "| Layer | Modules | Out | In | Intra | Cross-out | Cross-in | Avg Ce | Avg Ca | Cohesion | Target layers | Source layers |\n"
    )
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
    for row in stats.summaries:
        lines.append(
            f"| `{row.layer}` | {row.modules} | {row.outgoing_edges} | {row.incoming_edges} | "
            f"{row.intra_edges} | {row.cross_out_edges} | {row.cross_in_edges} | "
            f"{row.avg_ce:.2f} | {row.avg_ca:.2f} | {row.cohesion_ratio:.3f} | "
            f"{row.distinct_target_layers} | {row.distinct_source_layers} |\n"
        )
    return "".join(lines)


def render_cross_flow_table_md(items: list[tuple[str, str, int]], *, src_label: str, tgt_label: str) -> str:
    if not items:
        return "(no cross-boundary flows)\n"
    lines: list[str] = []
    lines.append(f"| {src_label} | {tgt_label} | Edges |\n")
    lines.append("|---|---|---:|\n")
    for src, tgt, count in items:
        lines.append(f"| `{src}` | `{tgt}` | {count} |\n")
    return "".join(lines)


def _render_layer_metrics_md(layer_stats: LayerCouplingStats) -> str:
    parts: list[str] = []
    parts.append("### Layer / Slice architecture metrics\n\n")
    parts.append(
        f"- Layers observed: **{layer_stats.layers}**\n"
        f"- Internal import edges: **{layer_stats.internal_edges}**\n"
        f"- Intra-layer edges: **{layer_stats.intra_layer_edges}**\n"
        f"- Cross-layer edges: **{layer_stats.cross_layer_edges}**\n"
        f"- Intra-layer ratio: **{layer_stats.intra_layer_ratio:.3f}**\n"
        f"- Cross-layer ratio: **{layer_stats.cross_layer_ratio:.3f}**\n\n"
    )
    if layer_stats.excluded_layers:
        parts.append(
            "- Excluded layers: **{layers}** ({mods} modules, {edges} edges removed from this coarse architecture view)\n\n".format(
                layers=", ".join(f"`{x}`" for x in layer_stats.excluded_layers),
                mods=layer_stats.excluded_modules,
                edges=layer_stats.excluded_edges,
            )
        )

    intra_grade = _grade_min(layer_stats.intra_layer_ratio, ok_min=0.55, warn_min=0.35)
    cross_grade = _grade(layer_stats.cross_layer_ratio, ok_max=0.45, warn_max=0.65)
    parts.append("**Heuristic interpretation**\n\n")
    parts.append(
        f"- Intra-layer ratio = **{layer_stats.intra_layer_ratio:.3f}** → **{intra_grade}** (higher means more dependencies stay within their own layer)\n"
        f"- Cross-layer ratio = **{layer_stats.cross_layer_ratio:.3f}** → **{cross_grade}** (lower means cleaner boundaries between layers)\n"
        "- Top cross-layer / cross-slice flows below help spot architecture seams that may need ports, facades or further extraction.\n\n"
    )

    parts.append("**Layer summary**\n\n")
    parts.append(render_layer_summary_table_md(layer_stats))
    parts.append("\n")
    parts.append(f"**Top {len(layer_stats.top_cross_layer)} cross-layer dependency flows**\n\n")
    parts.append(render_cross_flow_table_md(layer_stats.top_cross_layer, src_label="From layer", tgt_label="To layer"))
    parts.append("\n")
    parts.append(f"**Top {len(layer_stats.top_cross_slice)} cross-slice dependency flows**\n\n")
    parts.append(render_cross_flow_table_md(layer_stats.top_cross_slice, src_label="From slice", tgt_label="To slice"))
    parts.append("\n")
    return "".join(parts)


def build_metrics_block_md(
    *,
    start_marker: str,
    end_marker: str,
    repo_loc: LocStats,
    dpone_loc: LocStats,
    coupling: CouplingStats,
    layer_stats: LayerCouplingStats,
    repo_without_tests_loc: LocStats | None = None,
) -> str:
    parts: list[str] = []
    parts.append(f"{start_marker}\n")
    parts.append("_This section is auto-generated by `dpone docs update-dev-metrics`._\n\n")

    max_loc_grade = _grade(dpone_loc.max_lines, ok_max=600, warn_max=1000)
    ce_grade = _grade(coupling.avg_ce, ok_max=6.0, warn_max=12.0)
    p90_grade = _grade(coupling.p90_ce, ok_max=12.0, warn_max=18.0)
    cl_grade = _grade(coupling.avg_clustering, ok_max=0.18, warn_max=0.25)
    intra_grade = _grade_min(layer_stats.intra_layer_ratio, ok_min=0.55, warn_min=0.35)
    cross_grade = _grade(layer_stats.cross_layer_ratio, ok_max=0.45, warn_max=0.65)
    icon, level = _traffic_light_md(max_loc_grade, ce_grade, p90_grade, cl_grade, intra_grade, cross_grade)
    parts.append("### Quality summary\n\n")
    parts.append(
        f"- Architecture graph/hard-gate status: **{icon} {level}**\n"
        "- Repository-wide traffic light also includes exact-cap legacy debt; see the manual summary above.\n"
        f"- Biggest module: **{dpone_loc.max_lines} LOC** (`{dpone_loc.max_path}`) → **{max_loc_grade}**\n"
        f"- Avg clustering: **{coupling.avg_clustering:.3f}** → **{cl_grade}**\n"
        f"- Cross-layer ratio: **{layer_stats.cross_layer_ratio:.3f}** → **{cross_grade}**\n"
        "- First improvement target: keep broad service/command seams thin before adding new runtime features.\n\n"
    )

    parts.append("### LOC (lines of code)\n\n")
    parts.append("**Repository-wide (`**/*.py`)**\n\n")
    parts.append(
        f"- Files: **{repo_loc.files}**\n"
        f"- Total lines: **{repo_loc.total_lines}**\n"
        f"- Total SLOC: **{repo_loc.total_sloc}**\n"
        f"- Min: **{repo_loc.min_lines}** (`{repo_loc.min_path}`)\n"
        f"- Max: **{repo_loc.max_lines}** (`{repo_loc.max_path}`)\n"
        f"- Avg: **{repo_loc.avg_lines:.2f}**\n"
        f"- Median: **{repo_loc.median_lines:.0f}**\n\n"
    )
    if repo_without_tests_loc is not None:
        parts.append("**Repository-wide without tests (`**/*.py`, excluding `tests/`)**\n\n")
        parts.append(
            f"- Files: **{repo_without_tests_loc.files}**\n"
            f"- Total lines: **{repo_without_tests_loc.total_lines}**\n"
            f"- Total SLOC: **{repo_without_tests_loc.total_sloc}**\n"
            f"- Max: **{repo_without_tests_loc.max_lines}** (`{repo_without_tests_loc.max_path}`)\n"
            f"- Avg: **{repo_without_tests_loc.avg_lines:.2f}**\n"
            f"- Median: **{repo_without_tests_loc.median_lines:.0f}**\n\n"
        )

    parts.append(f"**Top {len(dpone_loc.top)} largest modules in `src/dpone/`**\n\n")
    parts.append(render_loc_table_md(dpone_loc))
    parts.append("\n")

    parts.append("### Coupling / Cohesion (internal imports inside `dpone.*`)\n\n")
    parts.append(
        f"- Modules: **{coupling.modules}**\n"
        f"- Internal import edges: **{coupling.internal_edges}**\n"
        f"- AVG Ce (fan-out): **{coupling.avg_ce:.2f}**\n"
        f"- Median Ce: **{coupling.median_ce:.0f}**\n"
        f"- P90 Ce: **{coupling.p90_ce:.0f}**\n"
        f"- P95 Ce: **{coupling.p95_ce:.0f}**\n"
        f"- MAX Ce: **{coupling.max_ce}** (`{coupling.max_ce_module}`)\n"
        f"- MAX Ca (fan-in): **{coupling.max_ca}** (`{coupling.max_ca_module}`)\n"
        f"- LCC ratio (largest connected component): **{coupling.lcc_ratio:.3f}**\n"
        f"- Avg clustering coefficient: **{coupling.avg_clustering:.3f}**\n"
        f"- Cohesion ratio (share of deps within slice): **{coupling.cohesion_ratio:.3f}**\n\n"
    )

    parts.append("**Heuristic interpretation**\n\n")
    parts.append(
        f"- AVG Ce = **{coupling.avg_ce:.2f}** → **{ce_grade}** (lower is usually better; big fan-out hints 'god modules')\n"
        f"- P90 Ce = **{coupling.p90_ce:.0f}** → **{p90_grade}** (if P90 grows, responsibilities are spreading)\n"
        f"- Avg clustering = **{coupling.avg_clustering:.3f}** → **{cl_grade}** (higher clustering often means more cycles/tight coupling)\n\n"
    )

    parts.append(f"**Top {len(coupling.top_out)} modules by fan-out (Ce)**\n\n")
    parts.append(render_top_list_md(coupling.top_out))
    parts.append("\n")
    parts.append(f"**Top {len(coupling.top_in)} modules by fan-in (Ca)**\n\n")
    parts.append(
        "Approved stable high fan-in contracts are expected shared DTO/port modules; they remain visible here but are treated differently from god-module risks.\n\n"
    )
    parts.append(render_top_list_md(coupling.top_in, annotate_stable_contracts=True))
    parts.append("\n")
    parts.append(_render_layer_metrics_md(layer_stats))

    parts.append(f"{end_marker}")
    return "".join(parts)
