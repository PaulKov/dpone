"""Layout dispatcher from project authoring files to one selection graph."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from dpone.manifest.authoring import default_authoring_compiler
from dpone.manifest.pipeline_source_reference import (
    PipelineSourceReferenceError,
    resolve_pipeline_reference,
)
from dpone.manifest.project_config import (
    ProjectConfigError,
    ProjectConfigSnapshot,
    ProjectLayout,
    load_project_layout,
)
from dpone.manifest.project_discovery import ProjectDiscoveryService, ProjectDiscoverySnapshot
from dpone.manifest.project_layout_authority import authoring_layout_is_compatible
from dpone.manifest.project_selection_contracts import LoadedProjectSelectionGraph
from dpone.manifest.project_selection_domain_first import (
    is_project_target,
    load_domain_first_selection,
)
from dpone.manifest.project_selection_flat import FlatProjectSelectionLoader
from dpone.manifest.selection import SelectionError

if TYPE_CHECKING:
    from dpone.manifest.authoring import AuthoringCompiler
    from dpone.manifest.project_selection_contracts import ProjectDagParser, ProjectPipelinePathResolver

_LAYOUT_MIGRATION_ERROR = (
    "DPONE_PROJECT_LAYOUT_MIGRATION_REQUIRED",
    "Existing authoring authority does not match the configured project layout.",
)


class ProjectSelectionLoader:
    """Dispatch one authoritative project layout into a canonical graph."""

    def __init__(
        self,
        *,
        root: Path,
        dag_parser: ProjectDagParser,
        pipeline_path_resolver: ProjectPipelinePathResolver,
        compiler: AuthoringCompiler | None = None,
    ) -> None:
        self._root = root.resolve(strict=True)
        self._dag_parser = dag_parser
        self._compiler = compiler or default_authoring_compiler()
        self._flat = FlatProjectSelectionLoader(
            root=self._root,
            dag_parser=dag_parser,
            pipeline_path_resolver=pipeline_path_resolver,
            compiler=self._compiler,
        )

    def load(self, target: str | Path) -> LoadedProjectSelectionGraph:
        layout, config_snapshot = self._project_layout()
        if not authoring_layout_is_compatible(self._root, layout.mode, layout.root):
            raise SelectionError(*_LAYOUT_MIGRATION_ERROR)
        if not layout.is_domain_first:
            return self._flat.load(target, config_snapshot=config_snapshot)
        snapshot = ProjectDiscoveryService(self._root, compiler=self._compiler).discover(layout=layout)
        return self._load_domain_first(
            snapshot,
            layout=layout,
            config_snapshot=config_snapshot,
            target=target,
        )

    def _load_domain_first(
        self,
        snapshot: ProjectDiscoverySnapshot,
        *,
        layout: ProjectLayout,
        config_snapshot: ProjectConfigSnapshot | None,
        target: str | Path,
    ) -> LoadedProjectSelectionGraph:
        selected_ids: frozenset[str] | None = None
        if not is_project_target(self._root, target):
            try:
                reference = resolve_pipeline_reference(
                    self._root,
                    target,
                    layout_snapshot=layout,
                    config_snapshot=config_snapshot,
                    discovery_snapshot=snapshot,
                )
            except PipelineSourceReferenceError as exc:
                raise SelectionError(exc.code, str(exc)) from exc
            assert reference.checked_source is not None
            selected_ids = frozenset({reference.checked_source.workload_id})
        return load_domain_first_selection(
            snapshot,
            dag_parser=self._dag_parser,
            project_config_inputs=_project_config_input(config_snapshot),
            workload_ids=selected_ids,
        )

    def _project_layout(self) -> tuple[ProjectLayout, ProjectConfigSnapshot | None]:
        try:
            return load_project_layout(self._root)
        except ProjectConfigError as exc:
            raise SelectionError(
                "DPONE_PROJECT_CONFIG_INVALID",
                "Project layout configuration is invalid.",
            ) from exc


def _project_config_input(snapshot: ProjectConfigSnapshot | None) -> dict[str, str]:
    if snapshot is None:
        raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", "Project dpone.yaml was not found.")
    return {"dpone.yaml": snapshot.sha256}


__all__ = ["ProjectSelectionLoader"]
