"""Domain-first catalog membership glue for pipeline scaffold apply."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.manifest.project_config import ProjectLayout
from dpone.manifest.project_root import ProjectRootIdentity
from dpone.readiness.airflow_pipeline_catalog_membership import (
    preflight_domain_membership,
    register_domain_membership,
)
from dpone.readiness.airflow_pipeline_scaffold_preflight import DomainFirstScaffoldGuard
from dpone.readiness.airflow_scaffold_apply import ScaffoldApplier, ScaffoldFile
from dpone.readiness.airflow_scaffold_result import scaffold_result
from dpone.readiness.airflow_self_service_models import SelfServiceResult


def apply_pipeline_scaffold_with_membership(
    *,
    root: Path,
    root_identity: ProjectRootIdentity | None,
    layout: ProjectLayout,
    domain: str,
    pipeline_id: str,
    pipeline_path: Path,
    airflow: bool,
    files: tuple[ScaffoldFile, ...],
    guard: DomainFirstScaffoldGuard | None,
    details: Mapping[str, Any],
) -> SelfServiceResult:
    """Apply one pipeline scaffold bundle and register domain catalog membership."""

    applier = ScaffoldApplier(root, root_identity=root_identity)
    if layout.is_domain_first and airflow:
        preflight_failure = preflight_domain_membership(
            root,
            layout=layout,
            domain=domain,
            pipeline_id=pipeline_id,
            pipeline_path=pipeline_path,
        )
        if preflight_failure is not None:
            return preflight_failure
    plan = applier.apply(
        files,
        precondition=(guard.before_apply if guard is not None else None),
        postcondition=(guard.after_apply if guard is not None else None),
    )
    if layout.is_domain_first and airflow:
        plan, membership_failure = register_domain_membership(
            plan,
            root=root,
            layout=layout,
            domain=domain,
            pipeline_id=pipeline_id,
            pipeline_path=pipeline_path,
            applier=applier,
        )
        if membership_failure is not None:
            return membership_failure
    return scaffold_result(plan, stage="init_pipeline", details=dict(details))


__all__ = ["apply_pipeline_scaffold_with_membership"]
