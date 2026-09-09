"""Domain ownership scaffolding for domain-first self-service."""

from __future__ import annotations

import shlex
from pathlib import Path
from typing import Protocol

from dpone.manifest.domain_identity import DomainId, DomainIdError
from dpone.manifest.domain_ownership import normalize_domain_ownership
from dpone.manifest.project_config import ProjectConfigError, load_project_layout
from dpone.manifest.project_layout_authority import (
    AuthoringAuthorityGuard,
    conflicting_authoring_layout,
    detect_authoring_layout,
)
from dpone.manifest.project_root import ProjectRootIdentity
from dpone.readiness.airflow_scaffold_apply import ScaffoldApplier, ScaffoldFile
from dpone.readiness.airflow_scaffold_result import scaffold_result
from dpone.readiness.airflow_self_service_models import SelfServiceResult, dpone_error
from dpone.readiness.error_contract import error_docs_url, manual_fix


class ProjectConfigFailureFactory(Protocol):
    """Build the canonical invalid-project result for a self-service stage."""

    def __call__(self, *, stage: str, reason: str) -> SelfServiceResult: ...


class ProjectLayoutMigrationFailureFactory(Protocol):
    """Build the canonical layout-migration result for a self-service stage."""

    def __call__(
        self,
        *,
        stage: str,
        detected_layout: str,
        requested_layout: str,
    ) -> SelfServiceResult: ...


class AirflowDomainScaffoldService:
    """Create one authoritative domain ownership document."""

    def __init__(
        self,
        root: Path,
        *,
        root_identity: ProjectRootIdentity | None = None,
        project_config_failure: ProjectConfigFailureFactory,
        project_layout_migration_failure: ProjectLayoutMigrationFailureFactory,
    ) -> None:
        self._root = root
        self._root_identity = root_identity
        self._project_config_failure = project_config_failure
        self._project_layout_migration_failure = project_layout_migration_failure

    def init_domain(
        self,
        *,
        domain: str,
        owner_team: str,
        owner_contact: str,
        approver_team: str,
    ) -> SelfServiceResult:
        try:
            layout, config_snapshot = load_project_layout(self._root)
        except (ProjectConfigError, OSError) as exc:
            reason = exc.reason if isinstance(exc, ProjectConfigError) else "project_root_invalid"
            return self._project_config_failure(stage="init_domain", reason=reason)
        conflicting_layout = conflicting_authoring_layout(
            self._root,
            expected_mode=layout.mode,
            domain_first_root=layout.root,
        )
        if conflicting_layout is not None:
            return self._project_layout_migration_failure(
                stage="init_domain",
                detected_layout=conflicting_layout,
                requested_layout=layout.mode,
            )
        if config_snapshot is None and layout.is_domain_first:
            return self._project_layout_migration_failure(
                stage="init_domain",
                detected_layout="domain_first",
                requested_layout=layout.mode,
            )
        if not layout.is_domain_first:
            return _domain_failure(
                code="DPONE_DOMAIN_LAYOUT_REQUIRED",
                message="Domain ownership is available only in a domain-first project.",
                fix_id="create_domain_first_project_or_plan_migration",
            )
        parsed_domain = _parse_domain(
            domain,
            owner_team=owner_team,
            owner_contact=owner_contact,
            approver_team=approver_team,
        )
        if isinstance(parsed_domain, SelfServiceResult):
            return parsed_domain
        owner_values = normalize_domain_ownership(owner_team, owner_contact, approver_team)
        if owner_values is None:
            return _domain_failure(
                code="DPONE_DOMAIN_OWNERSHIP_INVALID",
                message="Owner team, contact, and approver team must be explicit non-placeholder values.",
            )
        normalized_owner_team, normalized_owner_contact, normalized_approver_team = owner_values
        path = Path(layout.root) / str(parsed_domain) / "ownership.yaml"
        payload = {
            "schema": "dpone.domain-ownership.v1",
            "domain": str(parsed_domain),
            "owner": {
                "team": normalized_owner_team,
                "contact": normalized_owner_contact,
            },
            "approvers": {
                "github_team": normalized_approver_team,
            },
        }
        authority_guard = AuthoringAuthorityGuard(
            root=self._root,
            domain_first_root=layout.root,
            before_layout=detect_authoring_layout(
                self._root,
                domain_first_root=layout.root,
            ),
            before_config_sha256=config_snapshot.sha256 if config_snapshot is not None else None,
            after_config_sha256=config_snapshot.sha256 if config_snapshot is not None else None,
            allowed_after_layouts=frozenset({"domain_first"}),
        )
        plan = ScaffoldApplier(
            self._root,
            root_identity=self._root_identity,
        ).apply(
            (ScaffoldFile.yaml(path, payload),),
            precondition=authority_guard.before_apply,
            postcondition=authority_guard.after_apply,
        )
        return scaffold_result(
            plan,
            stage="init_domain",
            details={
                "domain": str(parsed_domain),
                "ownership_path": path.as_posix(),
            },
        )


def _parse_domain(
    value: str,
    *,
    owner_team: str,
    owner_contact: str,
    approver_team: str,
) -> DomainId | SelfServiceResult:
    try:
        return DomainId.parse(value)
    except DomainIdError as exc:
        suggested = exc.suggested_id or "<domain>"
        command = shlex.join(
            (
                "dpone",
                "init",
                "domain",
                suggested,
                "--owner-team",
                owner_team,
                "--owner-contact",
                owner_contact,
                "--approver-team",
                approver_team,
            )
        )
        return _domain_failure(
            code="DPONE_DOMAIN_ID_INVALID",
            message=str(exc),
            extra={"suggested_id": exc.suggested_id},
            fix_id="use_canonical_domain_id",
            fix_command=command,
        )


def _domain_failure(
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
                stage="init_domain",
                extra=extra,
                docs_url=error_docs_url(code),
                fixes=fixes,
            ),
        ),
        exit_code=2,
    )


__all__ = ["AirflowDomainScaffoldService"]
