from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from dpone.dag.edge_end_to_end_explain import EdgeEndToEndResult
from dpone.services.dag.path_view import reasons_signature

from .common import DagViewMeta, PathSegmentView, build_meta, build_path_segments


@dataclass(frozen=True, slots=True)
class ExplainEdgeE2EView:
    meta: DagViewMeta
    upstream_name: str
    downstream_name: str
    result: EdgeEndToEndResult
    path_edges: tuple[EdgeEndToEndResult, ...] = ()
    path_segments: tuple[PathSegmentView, ...] = ()

    @property
    def direct_edge(self) -> bool:
        return bool(self.result.direct_edge)

    def exit_code(self) -> int:
        return 0 if self.direct_edge else 1

    def to_jsonable(self) -> dict[str, Any]:
        result_json = self.result.to_json_dict()
        data = self.meta.to_jsonable()
        data.update(
            {
                "upstream_name": self.upstream_name,
                "downstream_name": self.downstream_name,
                "result": result_json,
                "path_edges": [e.to_json_dict() for e in self.path_edges] if self.path_edges else None,
                "path_segments": [s.to_jsonable() for s in self.path_segments] if self.path_segments else None,
            }
        )
        return data


def build_explain_edge_e2e_view(
    *,
    dag: Any,
    upstream_name: str,
    downstream_name: str,
    result: EdgeEndToEndResult,
    path_edges: Sequence[EdgeEndToEndResult] | None = None,
    path_view: str = "edges",
    path_output: str = "compact",
) -> ExplainEdgeE2EView:
    path_edges_tuple = tuple(path_edges or ())
    segments: tuple[PathSegmentView, ...] = ()

    if path_view == "grouped" and path_edges_tuple:
        segments = build_path_segments(
            path_edges_tuple,
            key_fn=lambda edge: reasons_signature(edge.reasons),
            signature_fn=lambda key: repr(key),
            nodes_fn=lambda seg: (
                [str(seg[0].upstream.get("name") or "-")] + [str(item.downstream.get("name") or "-") for item in seg]
            ),
            primary_reason_fn=lambda seg: dict(seg[0].reasons[0]) if seg[0].reasons else None,
        )

    return ExplainEdgeE2EView(
        meta=build_meta(
            "dag.explain_edge_e2e",
            dag,
            options={
                "path_view": path_view,
                "path_output": path_output,
            },
        ),
        upstream_name=upstream_name,
        downstream_name=downstream_name,
        result=result,
        path_edges=path_edges_tuple,
        path_segments=segments,
    )
