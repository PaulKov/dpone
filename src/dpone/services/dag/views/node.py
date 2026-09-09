from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dpone.dag.node_explain import NodeExplanation

from .common import DagViewMeta, build_meta


@dataclass(frozen=True, slots=True)
class ExplainNodeView:
    meta: DagViewMeta
    result: NodeExplanation

    def to_jsonable(self) -> dict[str, Any]:
        result_json = self.result.to_jsonable()
        data = self.meta.to_jsonable()
        data.update(result_json)  # legacy-compatible top-level fields
        data["result"] = result_json
        return data


def build_explain_node_view(*, dag: Any, result: NodeExplanation, direction: str, output: str) -> ExplainNodeView:
    return ExplainNodeView(
        meta=build_meta(
            "dag.explain_node",
            dag,
            options={
                "direction": direction,
                "output": output,
            },
        ),
        result=result,
    )
