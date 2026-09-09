"""Colocated domain DAG scaffolding for domain-first self-service."""

from __future__ import annotations

import re
import shlex
from datetime import date
from pathlib import Path

from dpone.manifest.domain_identity import DomainId, DomainIdError
from dpone.manifest.pipeline_identity import PipelineId, PipelineIdError
from dpone.manifest.project_config import ProjectConfigError, load_project_layout
from dpone.manifest.project_discovery import ProjectDiscoveryService
from dpone.manifest.project_layout_authority import (
    AuthoringAuthorityGuard,
    conflicting_authoring_layout,
    detect_authoring_layout,
)
from dpone.manifest.project_root import ProjectRootIdentity
from dpone.readiness.airflow_project_scaffold import (
    project_config_failure,
    project_layout_migration_failure,
)
from dpone.readiness.airflow_scaffold_apply import ScaffoldApplier, ScaffoldFile
from dpone.readiness.airflow_scaffold_result import scaffold_result
from dpone.readiness.airflow_self_service_models import SelfServiceResult, dpone_error
from dpone.readiness.error_contract import error_docs_url, manual_fix

_DAG_ID_RE = re.compile(r"^DAG__[A-Za-z0-9_]+$")


class AirflowDagScaffoldService:
    """Create one colocated ``dpone.domain-dag.v1`` authoring file."""

    def __init__(
        self,
        root: Path,
        *,
        root_identity: ProjectRootIdentity | None = None,
    ) -> None:
        self._root = root
        self._root_identity = root_identity

    def init_dag(
        self,
        *,
        dag_id: str,
        domain: str,
        schedule: str | None = None,
        pipelines: tuple[str, ...] = (),
        description: str | None = None,
    ) -> SelfServiceResult:
        try:
            layout, config_snapshot = load_project_layout(self._root)
        except (ProjectConfigError, OSError) as exc:
            reason = exc.reason if isinstance(exc, ProjectConfigError) else "project_root_invalid"
            return project_config_failure(stage="init_dag", reason=reason)
        conflicting_layout = conflicting_authoring_layout(
            self._root,
            expected_mode=layout.mode,
            domain_first_root=layout.root,
        )
        if conflicting_layout is not None:
            return project_layout_migration_failure(
                stage="init_dag",
                detected_layout=conflicting_layout,
                requested_layout=layout.mode,
            )
        if not layout.is_domain_first:
            return _dag_failure(
                code="DPONE_DOMAIN_LAYOUT_REQUIRED",
                message="Domain DAG authoring is available only in a domain-first project.",
                fix_id="create_domain_first_project_or_plan_migration",
            )
        if not _DAG_ID_RE.fullmatch(dag_id.strip()):
            return _dag_failure(
                code="DPONE_DOMAIN_DAG_ID_INVALID",
                message="dag_id must match DAG__<name> with letters, digits, and underscores.",
                fix_id="use_canonical_dag_id",
                fix_command="dpone init dag --help",
            )
        normalized_dag_id = dag_id.strip()
        try:
            parsed_domain = DomainId.parse(domain)
        except DomainIdError as exc:
            suggested = exc.suggested_id or "<domain>"
            return _dag_failure(
                code="DPONE_DOMAIN_ID_INVALID",
                message=str(exc),
                extra={"suggested_id": exc.suggested_id},
                fix_id="use_canonical_domain_id",
                fix_command=shlex.join(("dpone", "init", "dag", normalized_dag_id, "--domain", suggested)),
            )
        pipeline_ids, pipeline_failure = _parse_pipelines(pipelines)
        if pipeline_failure is not None:
            return pipeline_failure
        discovery = ProjectDiscoveryService(self._root)
        ownership_issue = discovery.ownership_issue(str(parsed_domain), layout=layout)
        if ownership_issue is not None:
            return _dag_failure(
                code=ownership_issue.code,
                message=ownership_issue.message,
                fix_id="init_domain_ownership",
                fix_command=shlex.join(
                    (
                        "dpone",
                        "init",
                        "domain",
                        str(parsed_domain),
                        "--owner-team",
                        "<team>",
                        "--owner-contact",
                        "<contact>",
                        "--approver-team",
                        "<approvers>",
                    )
                ),
            )
        snapshot = discovery.discover(layout=layout)
        if snapshot.issues:
            first = snapshot.issues[0]
            return _dag_failure(code=first.code, message=first.message, fix_id="fix_discovery_blockers")
        known = {item.pipeline_id: item.domain for item in snapshot.workloads}
        for pipeline_id in pipeline_ids:
            owner = known.get(pipeline_id)
            if owner is None:
                return _dag_failure(
                    code="DPONE_DOMAIN_DAG_PIPELINE_MISSING",
                    message=f"Domain DAG references unknown pipeline id {pipeline_id!r}.",
                    fix_id="init_pipeline_first",
                    fix_command="dpone init pipeline --help",
                )
            if owner != str(parsed_domain):
                return _dag_failure(
                    code="DPONE_DOMAIN_DAG_CROSS_DOMAIN_PIPELINE",
                    message=f"Domain DAG cannot reference pipeline {pipeline_id!r} owned by domain {owner!r}.",
                    fix_id="use_same_domain_pipelines",
                )
        if any(item.dag_id == normalized_dag_id for item in snapshot.domain_dags):
            return _dag_failure(
                code="DPONE_DOMAIN_DAG_ID_DUPLICATE",
                message="Domain DAG id must be unique across the project.",
                fix_id="choose_unique_dag_id",
            )
        path = Path(layout.root) / str(parsed_domain) / "dags" / f"{normalized_dag_id}.yaml"
        payload = {
            "schema": "dpone.domain-dag.v1",
            "dag_id": normalized_dag_id,
            "domain": str(parsed_domain),
            "description": description,
            "start_date": date.today().isoformat(),
            "timezone": "UTC",
            "schedule": schedule,
            "catchup": False,
            "pipelines": list(pipeline_ids),
            "wiring": {"mode": "waves", "max_parallel_workloads": 2},
        }
        authority_guard = AuthoringAuthorityGuard(
            root=self._root,
            domain_first_root=layout.root,
            before_layout=detect_authoring_layout(self._root, domain_first_root=layout.root),
            before_config_sha256=config_snapshot.sha256 if config_snapshot is not None else None,
            after_config_sha256=config_snapshot.sha256 if config_snapshot is not None else None,
            allowed_after_layouts=frozenset({"domain_first"}),
        )
        plan = ScaffoldApplier(self._root, root_identity=self._root_identity).apply(
            (ScaffoldFile.yaml(path, payload),),
            precondition=authority_guard.before_apply,
            postcondition=authority_guard.after_apply,
        )
        return scaffold_result(
            plan,
            stage="init_dag",
            details={
                "dag_id": normalized_dag_id,
                "domain": str(parsed_domain),
                "dag_path": path.as_posix(),
                "pipelines": list(pipeline_ids),
            },
        )


def _parse_pipelines(values: tuple[str, ...]) -> tuple[tuple[str, ...], SelfServiceResult | None]:
    if not values:
        return (), _dag_failure(
            code="DPONE_DOMAIN_DAG_PIPELINES_INVALID",
            message="At least one --pipeline is required.",
            fix_id="provide_pipeline_ids",
            fix_command="dpone init dag --help",
        )
    parsed: list[str] = []
    for value in values:
        try:
            parsed.append(str(PipelineId.parse(value)))
        except PipelineIdError:
            return (), _dag_failure(
                code="DPONE_DOMAIN_DAG_PIPELINE_ID_INVALID",
                message=f"Invalid pipeline id in --pipeline: {value!r}",
                fix_id="use_canonical_pipeline_id",
            )
    return tuple(parsed), None


def _dag_failure(
    *,
    code: str,
    message: str,
    fix_id: str | None = None,
    fix_command: str | None = None,
    extra: dict[str, object] | None = None,
) -> SelfServiceResult:
    fixes = [manual_fix(fix_id, command=fix_command)] if fix_id is not None else None
    return SelfServiceResult(
        passed=False,
        errors=(
            dpone_error(
                code,
                message,
                stage="init_dag",
                extra=extra,
                docs_url=error_docs_url(code),
                fixes=fixes,
            ),
        ),
        exit_code=2,
    )


__all__ = ["AirflowDagScaffoldService"]
