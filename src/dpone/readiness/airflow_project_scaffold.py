"""Project scaffolding for beginner Airflow self-service."""

from __future__ import annotations

from pathlib import Path

from dpone.manifest.project_config import (
    DEFAULT_DOMAIN_FIRST_ROOT,
    PROJECT_CONFIG_PATH,
    ProjectConfigError,
    load_project_layout,
)
from dpone.manifest.project_layout_authority import (
    AuthoringAuthorityGuard,
    detect_authoring_layout,
    normalize_project_layout_option,
)
from dpone.manifest.project_root import ProjectRootIdentity
from dpone.readiness.airflow_loader_migration import (
    AIRFLOW_DAG_FILE,
    AirflowLoaderMigrationConflict,
    AirflowLoaderMigrationReceipt,
    AirflowLoaderMigrator,
)
from dpone.readiness.airflow_project_scaffold_files import project_scaffold_files
from dpone.readiness.airflow_scaffold_apply import (
    ScaffoldApplier,
    ScaffoldApplyPlan,
    ScaffoldFile,
)
from dpone.readiness.airflow_scaffold_result import scaffold_result
from dpone.readiness.airflow_self_service_models import Change, SelfServiceResult, dpone_error
from dpone.readiness.airflow_self_service_templates import (
    airflow_loader_template,
)
from dpone.readiness.error_contract import error_docs_url, manual_fix


class AirflowProjectScaffoldService:
    """Create project policy and provider loader files."""

    def __init__(
        self,
        root: Path,
        *,
        root_identity: ProjectRootIdentity | None = None,
    ) -> None:
        self._root = root
        self._root_identity = root_identity
        self._loader_migrator = AirflowLoaderMigrator(
            root,
            root_identity=root_identity,
        )

    def init_project(self, *, airflow: bool, layout: str | None = None) -> SelfServiceResult:
        normalized_layout = normalize_project_layout_option(layout)
        if normalized_layout is None and layout is not None:
            return _project_policy_failure(
                code="DPONE_PROJECT_LAYOUT_INVALID",
                message="Project layout must be flat or domain-first.",
                stage="init_project",
                exit_code=2,
            )
        requested_layout = normalized_layout or "flat"
        try:
            configured_layout, config_snapshot = load_project_layout(self._root)
        except (ProjectConfigError, OSError) as exc:
            reason = exc.reason if isinstance(exc, ProjectConfigError) else "project_root_invalid"
            return project_config_failure(stage="init_project", reason=reason)
        existing_layout = detect_authoring_layout(self._root)
        if (
            existing_layout in {"mixed", "unsafe"}
            or (existing_layout != "empty" and existing_layout != requested_layout)
            or (config_snapshot is not None and configured_layout.mode != requested_layout)
            or (config_snapshot is None and existing_layout == "domain_first")
        ):
            return project_layout_migration_failure(
                stage="init_project",
                detected_layout=existing_layout,
                requested_layout=requested_layout,
            )
        files = project_scaffold_files(
            airflow=airflow,
            requested_layout=requested_layout,
            normalized_layout=normalized_layout,
        )
        authority_guard = AuthoringAuthorityGuard(
            root=self._root,
            domain_first_root=DEFAULT_DOMAIN_FIRST_ROOT,
            before_layout=existing_layout,
            before_config_sha256=config_snapshot.sha256 if config_snapshot is not None else None,
            after_config_sha256=files[0].sha256,
            allowed_after_layouts=frozenset({"empty", requested_layout}),
        )
        plan = self._apply_project_scaffold(
            files,
            migrate_loader=airflow,
            authority_guard=authority_guard,
        )
        return scaffold_result(
            plan,
            stage="init_project",
            details={
                "layout_mode": normalized_layout or "flat",
            },
        )

    def _apply_project_scaffold(
        self,
        files: tuple[ScaffoldFile, ...],
        *,
        migrate_loader: bool,
        authority_guard: AuthoringAuthorityGuard,
    ) -> ScaffoldApplyPlan:
        applier = ScaffoldApplier(
            self._root,
            root_identity=self._root_identity,
        )
        planned = applier.plan(files)
        loader_change = _change_for_path(planned.changes, AIRFLOW_DAG_FILE)
        can_migrate_loader = (
            migrate_loader
            and loader_change is not None
            and loader_change.action == "conflict"
            and not _has_other_conflict(planned.changes, AIRFLOW_DAG_FILE)
        )
        upgrade = self._loader_migrator.plan() if can_migrate_loader else None
        if upgrade is None:
            if planned.has_conflict:
                return planned
            return applier.apply(
                files,
                precondition=authority_guard.before_apply,
                postcondition=authority_guard.after_apply,
            )
        if not authority_guard.before_apply():
            return _authority_conflict_plan(planned)

        desired = airflow_loader_template().encode("utf-8")
        try:
            loader_receipt = self._loader_migrator.apply(upgrade, desired)
        except AirflowLoaderMigrationConflict as exc:
            if exc.committed or exc.cleanup_required or exc.recovery_artifacts:
                return _loader_recovery_plan(
                    planned,
                    AirflowLoaderMigrationReceipt(
                        committed=exc.committed,
                        cleanup_required=exc.cleanup_required,
                        recovery_artifacts=exc.recovery_artifacts,
                        issues=(str(exc),),
                    ),
                )
            return _loader_conflict_plan(planned, message=str(exc))
        if loader_receipt.cleanup_required:
            return _loader_recovery_plan(planned, loader_receipt)

        try:
            applied = applier.apply(
                files,
                precondition=authority_guard.before_apply,
                postcondition=authority_guard.after_apply,
            )
        except OSError as exc:
            rollback_receipt = self._loader_migrator.rollback(upgrade, desired)
            if rollback_receipt.restored is not True or rollback_receipt.cleanup_required:
                setattr(
                    exc,
                    "scaffold_receipt",
                    _loader_recovery_plan(planned, rollback_receipt),
                )
            raise
        if applied.has_conflict:
            rollback_receipt = self._loader_migrator.rollback(upgrade, desired)
            restored = rollback_receipt.restored is True and not rollback_receipt.cleanup_required
            message = (
                "Project files changed concurrently; the generated loader upgrade was rolled back."
                if restored
                else "Project files changed concurrently; later loader bytes were preserved."
            )
            if not restored:
                return _loader_recovery_plan(
                    applied,
                    rollback_receipt,
                    message=message,
                )
            return _loader_conflict_plan(applied, message=message)

        return ScaffoldApplyPlan(
            changes=_loader_updated_changes(planned=planned.changes, applied=applied.changes),
            rollback_journal=applied.rollback_journal,
            rollback_issues=applied.rollback_issues,
            recovery_artifacts=applied.recovery_artifacts,
            apply_failed=applied.apply_failed,
        )


def project_config_failure(*, stage: str, reason: str) -> SelfServiceResult:
    return _project_policy_failure(
        code="DPONE_PROJECT_CONFIG_INVALID",
        message="Project configuration must be a valid dpone.project.v1 object.",
        stage=stage,
        exit_code=2,
        path=PROJECT_CONFIG_PATH,
        extra={"reason": reason},
        fix_id="inspect_project_schema",
        fix_command="dpone gitops schema show dpone.project.v1",
    )


def project_layout_migration_failure(
    *,
    stage: str,
    detected_layout: str,
    requested_layout: str,
) -> SelfServiceResult:
    """Return one consistent fail-closed authority migration result."""

    return _project_policy_failure(
        code="DPONE_PROJECT_LAYOUT_MIGRATION_REQUIRED",
        message="Existing authoring layout must be migrated explicitly before it can become project authority.",
        stage=stage,
        exit_code=1,
        extra={
            "detected_layout": detected_layout,
            "requested_layout": requested_layout,
        },
        fix_id="plan_authoring_layout_migration",
        fix_command="dpone migrate authoring --plan",
    )


def _project_policy_failure(
    *,
    code: str,
    message: str,
    stage: str,
    exit_code: int,
    fix_id: str | None = None,
    fix_command: str | None = None,
    path: str | None = None,
    extra: dict[str, object] | None = None,
) -> SelfServiceResult:
    fixes = [manual_fix(fix_id, command=fix_command)] if fix_id is not None else None
    return SelfServiceResult(
        passed=False,
        errors=(
            dpone_error(
                code,
                message,
                stage=stage,
                path=path,
                extra=extra,
                docs_url=error_docs_url(code),
                fixes=fixes,
            ),
        ),
        exit_code=exit_code,
    )


def _change_for_path(changes: tuple[Change, ...], path: str) -> Change | None:
    return next((change for change in changes if change.path == path), None)


def _has_other_conflict(changes: tuple[Change, ...], loader_path: str) -> bool:
    return any(change.action == "conflict" and change.path != loader_path for change in changes)


def _loader_conflict_plan(plan: ScaffoldApplyPlan, *, message: str) -> ScaffoldApplyPlan:
    changes = tuple(
        Change("conflict", change.path, message, diff=change.diff) if change.path == AIRFLOW_DAG_FILE else change
        for change in plan.changes
    )
    return ScaffoldApplyPlan(
        changes=changes,
        rollback_journal=plan.rollback_journal,
        rollback_issues=plan.rollback_issues,
        recovery_artifacts=plan.recovery_artifacts,
        apply_failed=plan.apply_failed,
    )


def _loader_recovery_plan(
    plan: ScaffoldApplyPlan,
    receipt: AirflowLoaderMigrationReceipt,
    *,
    message: str = "Airflow loader mutation requires manual recovery.",
) -> ScaffoldApplyPlan:
    artifacts = tuple(dict.fromkeys((*plan.recovery_artifacts, *receipt.recovery_artifacts)))
    issues = tuple(dict.fromkeys((*plan.rollback_issues, *receipt.issues, message)))
    loader_change = Change(
        "recovery_required",
        AIRFLOW_DAG_FILE,
        message,
    )
    changes = tuple(loader_change if change.path == AIRFLOW_DAG_FILE else change for change in plan.changes)
    recovery_changes = tuple(
        Change(
            "recovery",
            path,
            "Airflow loader transaction artifact requires manual recovery.",
        )
        for path in artifacts
        if all(change.path != path for change in changes)
    )
    return ScaffoldApplyPlan(
        changes=(*changes, *recovery_changes),
        rollback_journal=plan.rollback_journal,
        rollback_issues=issues,
        recovery_artifacts=artifacts,
        apply_failed=True,
    )


def _authority_conflict_plan(plan: ScaffoldApplyPlan) -> ScaffoldApplyPlan:
    guarded = tuple(
        (
            Change(
                "not_applied",
                change.path,
                "Not applied because project authoring authority changed.",
                diff=change.diff,
            )
            if change.action == "create"
            else change
        )
        for change in plan.changes
    )
    return ScaffoldApplyPlan(
        changes=(
            *guarded,
            Change(
                "conflict",
                "project-authority",
                "Project authoring authority changed while the scaffold was being applied.",
            ),
        ),
        rollback_journal=plan.rollback_journal,
        rollback_issues=plan.rollback_issues,
        recovery_artifacts=plan.recovery_artifacts,
        apply_failed=True,
    )


def _loader_updated_changes(
    *,
    planned: tuple[Change, ...],
    applied: tuple[Change, ...],
) -> tuple[Change, ...]:
    loader_plan = _change_for_path(planned, AIRFLOW_DAG_FILE)
    loader_diff = loader_plan.diff if loader_plan is not None else ""
    return tuple(
        Change(
            "update",
            change.path,
            "upgraded a known dpone-generated Airflow loader fingerprint",
            diff=loader_diff,
        )
        if change.path == AIRFLOW_DAG_FILE
        else change
        for change in applied
    )


__all__ = [
    "AirflowProjectScaffoldService",
    "project_config_failure",
    "project_layout_migration_failure",
]
