from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from dpone.dag.deps_end_to_end_explain import EndToEndDependenciesResult

from .common import DagViewMeta, build_meta


@dataclass(frozen=True, slots=True)
class ExplainDependenciesView:
    meta: DagViewMeta
    result: EndToEndDependenciesResult

    def to_jsonable(self) -> dict[str, Any]:
        result_json = self.result.to_json_dict()
        data = self.meta.to_jsonable()
        data["result"] = result_json
        return data


def build_explain_dependencies_view(
    *,
    dag: Any,
    result: EndToEndDependenciesResult,
    dep_indexes: Sequence[int] | None,
    include_inherited_group_deps: bool,
    max_upstreams: int,
    max_triggers: int,
) -> ExplainDependenciesView:
    return ExplainDependenciesView(
        meta=build_meta(
            "dag.explain_deps",
            dag,
            options={
                "dep_indexes": list(dep_indexes or ()),
                "include_inherited_group_deps": bool(include_inherited_group_deps),
                "max_upstreams": int(max_upstreams),
                "max_triggers": int(max_triggers),
            },
        ),
        result=result,
    )
