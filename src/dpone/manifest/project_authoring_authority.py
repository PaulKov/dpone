"""Application-facing facade for project authoring authority."""

from __future__ import annotations

from dpone.manifest.domain_identity import DomainId, DomainIdError
from dpone.manifest.project_config import (
    ProjectConfigError,
    load_project_layout,
    project_airflow_decision,
)
from dpone.manifest.project_layout_authority import conflicting_authoring_layout
from dpone.manifest.project_root import (
    ProjectRootError,
    ProjectRootIdentity,
    ensure_project_root,
    inspect_project_root,
    project_root_candidate,
    verify_project_root,
)

__all__ = [
    "DomainId",
    "DomainIdError",
    "ProjectConfigError",
    "ProjectRootError",
    "ProjectRootIdentity",
    "conflicting_authoring_layout",
    "ensure_project_root",
    "inspect_project_root",
    "load_project_layout",
    "project_airflow_decision",
    "project_root_candidate",
    "verify_project_root",
]
