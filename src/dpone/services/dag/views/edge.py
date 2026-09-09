from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from dpone.dag.edge_explain import DagEdgeExplanation
from dpone.services.dag.path_view import reasons_signature

from .common import DagViewMeta, PathSegmentView, build_meta, build_path_segments


@dataclass(frozen=True, slots=True)
class EdgeStepView:
    upstream_name: str
    downstream_name: str
    result: DagEdgeExplanation

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "upstream_name": self.upstream_name,
            "downstream_name": self.downstream_name,
            "result": self.result.to_jsonable(),
        }


@dataclass(frozen=True, slots=True)
class ExplainEdgeView:
    meta: DagViewMeta
    upstream_name: str
    downstream_name: str
    result: DagEdgeExplanation
    path_edges: tuple[EdgeStepView, ...] = ()
    path_segments: tuple[PathSegmentView, ...] = ()

    @property
    def direct_edge(self) -> bool:
        return bool(self.result.direct_edge)

    def exit_code(self) -> int:
        return 0 if self.direct_edge else 1

    def to_jsonable(self) -> dict[str, Any]:
        result_json = self.result.to_jsonable()
        data = self.meta.to_jsonable()
        data.update(result_json)  # legacy-compatible top-level fields
        data.update(
            {
                "upstream_name": self.upstream_name,
                "downstream_name": self.downstream_name,
                "result": result_json,
                "path_edges": [e.to_jsonable() for e in self.path_edges] if self.path_edges else None,
                "path_segments": [s.to_jsonable() for s in self.path_segments] if self.path_segments else None,
            }
        )
        return data


def build_explain_edge_view(
    *,
    dag: Any,
    upstream_name: str,
    downstream_name: str,
    result: DagEdgeExplanation,
    path_edges: Sequence[tuple[str, str, DagEdgeExplanation]] | None = None,
    path_view: str = "edges",
    path_output: str = "compact",
) -> ExplainEdgeView:
    step_views = tuple(EdgeStepView(a, b, e2) for a, b, e2 in (path_edges or ()))
    segments: tuple[PathSegmentView, ...] = ()

    if path_view == "grouped" and step_views:
        segments = build_path_segments(
            step_views,
            key_fn=lambda step: reasons_signature(step.result.reasons),
            signature_fn=lambda key: repr(key),
            nodes_fn=lambda seg: [seg[0].upstream_name] + [item.downstream_name for item in seg],
            primary_reason_fn=lambda seg: seg[0].result.reasons[0].to_jsonable() if seg[0].result.reasons else None,
        )

    return ExplainEdgeView(
        meta=build_meta(
            "dag.explain_edge",
            dag,
            options={
                "path_view": path_view,
                "path_output": path_output,
            },
        ),
        upstream_name=upstream_name,
        downstream_name=downstream_name,
        result=result,
        path_edges=step_views,
        path_segments=segments,
    )
