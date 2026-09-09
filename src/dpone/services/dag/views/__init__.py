"""Typed DAG CLI view-model facade."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "DagViewMeta",
    "PathSegmentView",
    "build_meta",
    "build_path_segments",
    "ExplainDependenciesView",
    "build_explain_dependencies_view",
    "EdgeStepView",
    "ExplainEdgeView",
    "build_explain_edge_view",
    "ExplainEdgeE2EView",
    "build_explain_edge_e2e_view",
    "ExplainNodeView",
    "build_explain_node_view",
    "ExplainNodeE2EView",
    "build_explain_node_e2e_view",
    "DagReportView",
    "build_dag_report_view",
]

_EXPORTS: dict[str, str] = {
    "DagViewMeta": "dpone.services.dag.views.common:DagViewMeta",
    "PathSegmentView": "dpone.services.dag.views.common:PathSegmentView",
    "build_meta": "dpone.services.dag.views.common:build_meta",
    "build_path_segments": "dpone.services.dag.views.common:build_path_segments",
    "ExplainDependenciesView": "dpone.services.dag.views.deps:ExplainDependenciesView",
    "build_explain_dependencies_view": "dpone.services.dag.views.deps:build_explain_dependencies_view",
    "EdgeStepView": "dpone.services.dag.views.edge:EdgeStepView",
    "ExplainEdgeView": "dpone.services.dag.views.edge:ExplainEdgeView",
    "build_explain_edge_view": "dpone.services.dag.views.edge:build_explain_edge_view",
    "ExplainEdgeE2EView": "dpone.services.dag.views.edge_e2e:ExplainEdgeE2EView",
    "build_explain_edge_e2e_view": "dpone.services.dag.views.edge_e2e:build_explain_edge_e2e_view",
    "ExplainNodeView": "dpone.services.dag.views.node:ExplainNodeView",
    "build_explain_node_view": "dpone.services.dag.views.node:build_explain_node_view",
    "ExplainNodeE2EView": "dpone.services.dag.views.node_e2e:ExplainNodeE2EView",
    "build_explain_node_e2e_view": "dpone.services.dag.views.node_e2e:build_explain_node_e2e_view",
    "DagReportView": "dpone.services.dag.views.report:DagReportView",
    "build_dag_report_view": "dpone.services.dag.views.report:build_dag_report_view",
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = target.split(":", 1)
    value = getattr(import_module(module_name), attr)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
