"""Orchestrator-neutral contracts for loading and selecting project workloads."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from dpone.manifest.authoring import AuthoringCompilation
from dpone.manifest.selection import SelectionGraph, SelectionReport, SelectionState


class ProjectDagDeclaration(Protocol):
    dag_id: str
    workloads: tuple[str, ...] | None
    group: str | None
    schedule: Any
    start_date: str
    timezone: str
    catchup: bool
    max_active_runs: int
    tags: tuple[str, ...]
    default_args: dict[str, Any]
    operator_overrides: dict[str, Any]
    description: str | None


class ProjectDagIssue(Protocol):
    message: str


class ProjectDagParser(Protocol):
    def __call__(
        self,
        dag_id: str,
        raw: object,
    ) -> tuple[ProjectDagDeclaration | None, Sequence[ProjectDagIssue]]: ...


class ProjectPipelinePathResolver(Protocol):
    def __call__(self, root: str | Path, target: str | Path) -> Path: ...


@dataclass(frozen=True, slots=True)
class ProjectCheckedSource:
    workload_id: str
    source_path: Path
    source_label: str
    payload: Mapping[str, Any]
    compilation: AuthoringCompilation
    source_sha256: str


@dataclass(frozen=True, slots=True)
class ProjectSelectionDag:
    declaration: ProjectDagDeclaration
    domain: str | None
    workload_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class LoadedProjectSelectionGraph:
    graph: SelectionGraph
    checked_sources: Mapping[str, ProjectCheckedSource]
    consumed_files: Mapping[str, str]
    dags: tuple[ProjectSelectionDag, ...]


@dataclass(frozen=True, slots=True)
class ProjectSelectionOutcome:
    graph: SelectionGraph
    report: SelectionReport
    state: SelectionState
    checked_sources: Mapping[str, ProjectCheckedSource]
    consumed_files: Mapping[str, str]
    dags: tuple[ProjectSelectionDag, ...] = ()


__all__ = [
    "LoadedProjectSelectionGraph",
    "ProjectCheckedSource",
    "ProjectDagDeclaration",
    "ProjectDagParser",
    "ProjectPipelinePathResolver",
    "ProjectSelectionDag",
    "ProjectSelectionOutcome",
]
