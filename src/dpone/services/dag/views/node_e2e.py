from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dpone.dag.node_end_to_end_explain import NodeEndToEndResult

from .common import DagViewMeta, build_meta


@dataclass(frozen=True, slots=True)
class ExplainNodeE2EView:
    meta: DagViewMeta
    result: NodeEndToEndResult
    focus_peer_raw: str | None = None
    focus_peer_resolved: str | None = None

    def to_jsonable(self) -> dict[str, Any]:
        result_json = self.result.to_json_dict()
        data = self.meta.to_jsonable()
        data.update(result_json)  # legacy-compatible top-level fields
        data.update(
            {
                "focus_peer": self.focus_peer_raw,
                "focus_peer_resolved": self.focus_peer_resolved,
                "result": result_json,
            }
        )
        return data


def build_explain_node_e2e_view(
    *,
    dag: Any,
    result: NodeEndToEndResult,
    direction: str,
    output: str,
    focus_peer_raw: str | None = None,
    focus_peer_resolved: str | None = None,
) -> ExplainNodeE2EView:
    return ExplainNodeE2EView(
        meta=build_meta(
            "dag.explain_node_e2e",
            dag,
            options={
                "direction": direction,
                "output": output,
                "focus_peer": focus_peer_raw,
            },
        ),
        result=result,
        focus_peer_raw=focus_peer_raw,
        focus_peer_resolved=focus_peer_resolved,
    )
