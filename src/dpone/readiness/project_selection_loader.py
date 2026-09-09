"""Airflow-aware composition root for the orchestrator-neutral project loader."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.manifest.project_selection_api import AuthoringCompiler


from pathlib import Path
from typing import cast

from dpone.gitops.airflow_dag_spec import parse_dag_declaration
from dpone.manifest.project_selection_api import (
    LoadedProjectSelectionGraph,
    NamedSelection,
    ProjectCheckedSource,
    ProjectDagDeclaration,
    ProjectDagParser,
    ProjectPipelinePathResolver,
    ProjectSelectionDag,
    ProjectSelectionOutcome,
    SelectionEngine,
    SelectionError,
    SelectionRequest,
    SelectionState,
    parse_selection_expression,
    parse_selection_state,
    state_from_graph,
)
from dpone.manifest.project_selection_api import (
    ProjectSelectionLoader as _ManifestProjectSelectionLoader,
)
from dpone.readiness.airflow_pipeline_source_reader import PipelineSourcePathError, resolve_pipeline_path


def _resolve_project_pipeline_path(root: str | Path, target: str | Path) -> Path:
    try:
        return resolve_pipeline_path(root, target)
    except PipelineSourcePathError as exc:
        raise SelectionError(
            "DPONE_SELECTION_CATALOG_INVALID",
            "Project selection target must stay inside the project root.",
        ) from exc


class ProjectSelectionLoader(_ManifestProjectSelectionLoader):
    """Inject the canonical Airflow DAG declaration parser at the build boundary."""

    def __init__(self, *, root: str | Path, compiler: AuthoringCompiler | None = None) -> None:
        super().__init__(
            root=Path(root),
            dag_parser=cast(ProjectDagParser, parse_dag_declaration),
            pipeline_path_resolver=cast(ProjectPipelinePathResolver, _resolve_project_pipeline_path),
            compiler=compiler,
        )


__all__ = [
    "LoadedProjectSelectionGraph",
    "NamedSelection",
    "ProjectCheckedSource",
    "ProjectDagDeclaration",
    "ProjectPipelinePathResolver",
    "ProjectSelectionDag",
    "ProjectSelectionLoader",
    "ProjectSelectionOutcome",
    "SelectionEngine",
    "SelectionError",
    "SelectionRequest",
    "SelectionState",
    "parse_selection_expression",
    "parse_selection_state",
    "state_from_graph",
]
