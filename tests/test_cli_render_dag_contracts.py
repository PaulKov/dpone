from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

from dpone.cli_render.dag.common import compact_yaml, render_reasons_section, render_warnings
from dpone.cli_render.dag.deps import render_explain_deps_text
from dpone.cli_render.dag.e2e_attribution import (
    render_direct_dependency_explanations,
    render_group_to_group_attributions,
)
from dpone.cli_render.dag.edge import render_explain_edge_text
from dpone.cli_render.dag.edge_e2e import render_edge_e2e_text
from dpone.cli_render.dag.node import render_explain_node_text
from dpone.cli_render.dag.node_e2e import render_explain_node_e2e_text


class RenderEdge:
    def __init__(
        self,
        upstream: str,
        downstream: str,
        reasons: list[Any],
    ) -> None:
        self.upstream = upstream
        self.downstream = downstream
        self.reasons = reasons

    def reason_kinds(self) -> list[str]:
        return [str(reason.kind) for reason in self.reasons]


def meta(options: dict[str, Any] | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        root=Path("/repo"),
        base_path=Path("/repo/manifests"),
        task_count=3,
        options=options or {},
    )


def edge_context() -> SimpleNamespace:
    return SimpleNamespace(
        nodes_by_name={
            "extract": SimpleNamespace(
                name="extract",
                ref="/repo/manifests/batch.yaml#extract",
                task_group="loaders",
                config_path=Path("/repo/manifests/batch.yaml"),
            ),
            "load": SimpleNamespace(
                name="load",
                ref="/repo/manifests/batch.yaml#load",
                task_group="loaders",
                config_path=Path("/repo/manifests/batch.yaml"),
            ),
            "publish": SimpleNamespace(
                name="publish",
                ref="/repo/manifests/batch.yaml#publish",
                task_group="publishers",
                config_path=Path("/repo/manifests/batch.yaml"),
            ),
        }
    )


def reason(kind: str = "depends_on", description: str = "declared dependency") -> SimpleNamespace:
    return SimpleNamespace(kind=kind, description=description, evidence={"path": "extract"})


def reason_dict(kind: str = "depends_on", description: str = "declared dependency") -> dict[str, Any]:
    return {"kind": kind, "description": description, "evidence": {"path": "extract"}}


def origin(path: str = "source.yaml", origin_name: str = "process") -> SimpleNamespace:
    return SimpleNamespace(path=path, origin=origin_name, origin_title="Process defaults")


def direct_dep(index: int = 0) -> SimpleNamespace:
    return SimpleNamespace(
        index=index,
        compiled_item={"path": "extract.yaml#orders"},
        warnings=["ambiguous selector"],
        compiled_origins=[origin()],
        parsed_dependency={"path": "batch.yaml#extract"},
        parse_records=[
            {
                "target": "path",
                "operation": "normalize",
                "sources": [{"path": "depends_on[0]", "origin": "process"}],
            }
        ],
        edges=[
            SimpleNamespace(
                upstream="extract",
                upstream_ref="/repo/manifests/batch.yaml#extract",
                reason_kinds=("depends_on",),
                reasons=[{"kind": "depends_on", "description": "declared dependency", "evidence": {"path": "x"}}],
            )
        ],
    )


def group_attr() -> SimpleNamespace:
    trigger = SimpleNamespace(
        trigger_task={"name": "load", "config_path": "/repo/manifests/batch.yaml"},
        depends_on_index=0,
        compiled_item={"group": "extractors"},
        compiled_origins=[origin("batch.yaml", "process")],
        parsed_dependency={"group": "extractors"},
        parse_records=[],
        warnings=["group expanded"],
    )
    return SimpleNamespace(
        dependent_group="loaders",
        upstream_group="extractors",
        trigger_count=1,
        triggers=[trigger],
        warnings=["cross-group"],
        upstream_tasks=["extract"],
    )


def test_common_dag_render_helpers_cover_compact_yaml_reasons_and_warnings() -> None:
    assert compact_yaml(None) == "null"
    assert compact_yaml("hello") == '"hello"'
    assert compact_yaml({"b": 2, "a": [1, 2]}) == "a: - 1 - 2 b: 2"

    rendered = render_reasons_section([reason_dict()], include_evidence=True)
    assert "Reasons:" in rendered
    assert "declared dependency" in rendered
    assert "[depends_on] evidence:" in rendered
    assert "path: extract" in rendered
    assert render_warnings(["careful"]) == "WARN: careful"


def test_e2e_attribution_renderers_show_provenance_trace_edge_match_and_group_triggers() -> None:
    direct = render_direct_dependency_explanations([direct_dep()])
    grouped = render_group_to_group_attributions([group_attr()])

    assert "depends_on[0]: path: extract.yaml#orders" in direct
    assert "Compilation provenance:" in direct
    assert "Normalization trace:" in direct
    assert "Edge match:" in direct
    assert "[depends_on] declared dependency" in direct
    assert "Group 'loaders' depends on 'extractors'" in grouped
    assert "Triggers (first N):" in grouped
    assert "Trigger load depends_on[0]:" in grouped


def test_render_explain_deps_text_includes_direct_and_inherited_group_details() -> None:
    view = SimpleNamespace(
        meta=meta(),
        result=SimpleNamespace(
            downstream={"name": "load", "ref": "batch.yaml#load", "task_group": "loaders"},
            warnings=["missing optional metadata"],
            direct=[direct_dep()],
            inherited_group_deps=[
                SimpleNamespace(
                    dependent_group="loaders",
                    upstream_group="extractors",
                    warnings=["expanded"],
                    upstream_tasks=["extract"],
                    trigger_count=1,
                    triggers=[
                        {
                            "task_name": "load",
                            "dep_index": 0,
                            "origin": "process",
                            "manifest": "/repo/manifests/batch.yaml",
                            "compiled_item": {"group": "extractors"},
                        }
                    ],
                )
            ],
        ),
    )

    rendered = render_explain_deps_text(view)

    assert "Downstream:" in rendered
    assert "WARN: missing optional metadata" in rendered
    assert "Direct depends_on (end-to-end):" in rendered
    assert "Inherited TaskGroup dependencies" in rendered
    assert "Group 'loaders' depends on 'extractors'" in rendered
    assert "Trigger load depends_on[0]:" in rendered


def test_render_explain_node_text_supports_compact_and_full_outputs() -> None:
    result = SimpleNamespace(
        node={"name": "load", "ref": "batch.yaml#load", "task_group": "loaders", "source": "pg", "sink": "bq"},
        warnings=["node warning"],
        incoming=[RenderEdge("extract", "load", [reason()])],
        outgoing=[RenderEdge("load", "publish", [reason("task_group", "group expansion")])],
    )

    compact = render_explain_node_text(
        SimpleNamespace(meta=meta({"direction": "both", "output": "compact"}), result=result),
        edge_ctx=edge_context(),
    )
    full = render_explain_node_text(
        SimpleNamespace(meta=meta({"direction": "in", "output": "full"}), result=result),
        edge_ctx=edge_context(),
    )

    assert "Incoming edges (upstream -> this):" in compact
    assert "Outgoing edges (this -> downstream):" in compact
    assert "reason_kinds" in compact
    assert "extract -> load" in full
    assert "[depends_on] evidence:" in full
    assert "path: extract" in full


def test_render_explain_edge_text_supports_grouped_path_explanations() -> None:
    step_one = SimpleNamespace(
        upstream_name="extract",
        downstream_name="load",
        result=SimpleNamespace(reasons=[reason_dict("depends_on", "direct")]),
    )
    step_two = SimpleNamespace(
        upstream_name="load",
        downstream_name="publish",
        result=SimpleNamespace(reasons=[reason_dict("depends_on", "direct")]),
    )
    view = SimpleNamespace(
        meta=meta({"path_view": "grouped", "path_output": "full"}),
        upstream_name="extract",
        downstream_name="publish",
        result=SimpleNamespace(
            upstream={"name": "extract", "ref": "batch.yaml#extract"},
            downstream={"name": "publish", "ref": "batch.yaml#publish"},
            direct_edge=False,
            warnings=["path fallback"],
            reasons=[reason_dict("shortest_path", "no direct edge")],
            path=["extract", "load", "publish"],
        ),
        path_edges=[step_one, step_two],
    )

    rendered = render_explain_edge_text(view)

    assert "Shortest path (no direct edge):" in rendered
    assert "Path edge explanations:" in rendered
    assert "Segment 1/1 (edges=2):" in rendered
    assert "extract -> load -> publish" in rendered
    assert "extract -> load" in rendered


def test_render_node_e2e_text_shows_focus_peer_and_attribution_summary() -> None:
    edge = SimpleNamespace(
        upstream={"name": "extract"},
        downstream={"name": "load"},
        reasons=[reason_dict()],
        direct_dep_attributions=[direct_dep()],
        group_to_group_attributions=[group_attr()],
    )
    view = SimpleNamespace(
        meta=meta({"direction": "both", "output": "compact"}),
        focus_peer_raw="publish",
        focus_peer_resolved="publish",
        result=SimpleNamespace(
            node={"name": "load", "ref": "batch.yaml#load", "task_group": "loaders"},
            warnings=[],
            incoming=[edge],
            outgoing=[edge],
        ),
    )

    rendered = render_explain_node_e2e_text(view, edge_ctx=edge_context())

    assert "Focus peer: publish -> resolved: publish" in rendered
    assert "dep[0]@process" in rendered
    assert "group:loaders<-extractors(1)" in rendered
    assert "attribution" in rendered


def test_render_edge_e2e_text_handles_direct_attribution_and_non_direct_paths() -> None:
    direct_view = SimpleNamespace(
        meta=meta(),
        upstream_name="extract",
        downstream_name="load",
        result=SimpleNamespace(
            upstream={"name": "extract", "ref": "batch.yaml#extract"},
            downstream={"name": "load", "ref": "batch.yaml#load"},
            direct_edge=True,
            warnings=[],
            reasons=[reason_dict()],
            path=[],
            direct_dep_attributions=[direct_dep()],
            group_to_group_attributions=[group_attr()],
        ),
        path_edges=[],
    )
    path_edge = SimpleNamespace(
        upstream={"name": "extract"},
        downstream={"name": "load"},
        reasons=[reason_dict()],
        direct_dep_attributions=[direct_dep()],
        group_to_group_attributions=[],
    )
    path_view = SimpleNamespace(
        meta=meta({"path_view": "edges", "path_output": "compact"}),
        upstream_name="extract",
        downstream_name="publish",
        result=SimpleNamespace(
            upstream={"name": "extract", "ref": "batch.yaml#extract"},
            downstream={"name": "publish", "ref": "batch.yaml#publish"},
            direct_edge=False,
            warnings=[],
            reasons=[],
            path=["extract", "load", "publish"],
            direct_dep_attributions=[],
            group_to_group_attributions=[],
        ),
        path_edges=[path_edge],
    )

    direct_rendered = render_edge_e2e_text(direct_view)
    path_rendered = render_edge_e2e_text(path_view)

    assert "End-to-end attribution: downstream depends_on items" in direct_rendered
    assert "End-to-end attribution: group-to-group expansion triggers" in direct_rendered
    assert "Path edge explanations (end-to-end):" in path_rendered
    assert "Attribution (depends_on):" in path_rendered
    assert "depends_on[0]: path: extract.yaml#orders" in path_rendered
