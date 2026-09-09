"""Domain catalog membership registration for domain-first ``init pipeline``.

Domain-first reconcile resolves DAG ``pipelines[]`` ids against the workload
catalog (``workloads:`` blocks of ``<system_root>/config/domains/*.yaml``).
Historically ``dpone init pipeline`` wrote that catalog only for legacy flat
layouts, leaving domain-first authors with a manual registration step; a
forgotten entry made the pipeline silently invisible to reconcile/pack.

This module closes that gap: after a successful pipeline scaffold it patches
``<system_root>/config/domains/<domain>.yaml`` with one ``workloads:`` entry
through the existing compare-and-swap catalog storage. The patch is textual
and append/insert-only, so tenant comments and formatting survive. The
``dags:`` block is never touched: colocated DAG YAML files stay the only
schedule/wiring source of truth.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING

from dpone.manifest.domain_catalog_membership import (
    catalog_manifest_ref,
    catalog_membership_entry_text,
    domain_catalog_path,
    membership_catalog_snippet,
)
from dpone.readiness.airflow_pipeline_catalog_text import (
    CatalogTextError,
)
from dpone.readiness.airflow_pipeline_catalog_text import (
    catalog_payload as _catalog_payload,
)
from dpone.readiness.airflow_pipeline_catalog_text import (
    insert_workload_entry as _insert_workload_entry,
)
from dpone.readiness.airflow_pipeline_catalog_text import (
    registered_manifest as _registered_manifest,
)
from dpone.readiness.airflow_pipeline_catalog_text import (
    require_inserted_manifest as _self_check,
)
from dpone.readiness.airflow_self_service_models import Change, SelfServiceResult
from dpone.readiness.error_contract import dpone_error, error_docs_url, manual_fix
from dpone.readiness.workload_init_catalog_merge import catalog_diff
from dpone.readiness.workload_init_catalog_storage import CatalogPatchConflict, apply_catalog_content, read_catalog_text

if TYPE_CHECKING:
    from dpone.manifest.project_config import ProjectLayout
    from dpone.readiness.airflow_scaffold_apply import ScaffoldApplier, ScaffoldApplyPlan

_DEFAULT_SYSTEM_ROOT = ".dpone"
MEMBERSHIP_CONFLICT_CODE = "DPONE_DOMAIN_CATALOG_MEMBERSHIP_CONFLICT"


@dataclass(frozen=True, slots=True)
class MembershipPatch:
    """One planned compare-and-swap update of a domain catalog file."""

    catalog_path: Path
    expected: bytes | None
    desired: bytes


@dataclass(frozen=True, slots=True)
class _CrossFileRegistration:
    catalog_path: Path
    manifest: str
    matches: bool


def preflight_domain_membership(
    root: Path,
    *,
    layout: ProjectLayout,
    domain: str,
    pipeline_id: str,
    pipeline_path: Path,
) -> SelfServiceResult | None:
    """Validate catalog membership can be written before pipeline files are created."""

    try:
        plan_membership_patch(
            root,
            layout=layout,
            domain=domain,
            pipeline_id=pipeline_id,
            pipeline_path=pipeline_path,
        )
    except MembershipConflictError as exc:
        return _membership_failure(str(exc))
    return None


def register_domain_membership(
    plan: ScaffoldApplyPlan,
    *,
    root: Path,
    layout: ProjectLayout,
    domain: str,
    pipeline_id: str,
    pipeline_path: Path,
    applier: ScaffoldApplier | None = None,
) -> tuple[ScaffoldApplyPlan, SelfServiceResult | None]:
    """Patch domain catalog membership after one applied pipeline scaffold.

    Returns the (possibly augmented) plan plus an optional failure result.
    A no-op (already registered) returns the plan unchanged.
    When membership cannot be applied, any scaffold files from ``plan`` are
    compensated when ``applier`` is provided.
    """

    if plan.apply_failed or plan.has_conflict or plan.recovery_required:
        return plan, None
    try:
        patch = plan_membership_patch(
            root,
            layout=layout,
            domain=domain,
            pipeline_id=pipeline_id,
            pipeline_path=pipeline_path,
        )
    except MembershipConflictError as exc:
        return _membership_failure_plan(plan, applier, _membership_failure(str(exc)))
    if patch is None:
        return plan, None
    try:
        receipt = apply_catalog_content(
            repo_root=root,
            path=patch.catalog_path,
            expected=patch.expected,
            desired=patch.desired,
        )
    except CatalogPatchConflict as exc:
        return _membership_failure_plan(plan, applier, _membership_failure(str(exc)))
    if receipt is None:
        return _membership_failure_plan(
            plan,
            applier,
            _membership_failure(
                f"Domain catalog {patch.catalog_path.as_posix()} changed concurrently; "
                "re-run init or register the membership manually."
            ),
        )
    change = Change(
        action="create" if patch.expected is None else "update",
        path=patch.catalog_path.as_posix(),
        message=f"register workload membership {pipeline_id}",
        diff=catalog_diff(
            (patch.expected or b"").decode("utf-8", errors="replace"),
            patch.desired.decode("utf-8", errors="replace"),
            fromfile=f"a/{patch.catalog_path.as_posix()}",
            tofile=f"b/{patch.catalog_path.as_posix()}",
        ),
    )
    return replace(plan, changes=(*plan.changes, change)), None


MembershipConflictError = CatalogTextError


def plan_membership_patch(
    root: Path,
    *,
    layout: ProjectLayout,
    domain: str,
    pipeline_id: str,
    pipeline_path: Path,
) -> MembershipPatch | None:
    """Plan one membership insert; ``None`` when the entry already matches."""

    manifest_ref = catalog_manifest_ref(domain_catalog_path(layout, domain), pipeline_path)
    cross_file = _scan_cross_file_registration(
        root,
        layout=layout,
        pipeline_id=pipeline_id,
        pipeline_path=pipeline_path,
    )
    if cross_file is not None:
        if cross_file.matches:
            return None
        raise MembershipConflictError(
            f"Workload {pipeline_id!r} is already registered in "
            f"{cross_file.catalog_path.as_posix()} with manifest {cross_file.manifest!r}; "
            f"expected pipeline {pipeline_path.as_posix()!r}. Resolve the existing entry manually."
        )

    catalog_path = domain_catalog_path(layout, domain)
    current = read_catalog_text(repo_root=root, path=catalog_path)
    if current is None:
        desired = f"domain: {domain}\nworkloads:\n{catalog_membership_entry_text(pipeline_id, manifest_ref)}"
        return MembershipPatch(
            catalog_path=catalog_path,
            expected=None,
            desired=desired.encode("utf-8"),
        )
    payload = _catalog_payload(current, catalog_path)
    _validate_catalog_domain(payload, catalog_path, domain)
    registered = _registered_manifest(payload, pipeline_id)
    if registered is not None:
        if _resolved_manifest_path(root, catalog_path, registered) == pipeline_path.as_posix():
            return None
        raise MembershipConflictError(
            f"Workload {pipeline_id!r} is already registered in "
            f"{catalog_path.as_posix()} with manifest {registered!r}; expected "
            f"{manifest_ref!r}. Resolve the existing entry manually."
        )
    desired = _insert_workload_entry(current, pipeline_id, manifest_ref)
    _self_check(desired, catalog_path, pipeline_id, manifest_ref)
    return MembershipPatch(
        catalog_path=catalog_path,
        expected=current.encode("utf-8"),
        desired=desired.encode("utf-8"),
    )


def _domain_catalog_glob(layout: ProjectLayout) -> Path:
    system_root = layout.system_root or _DEFAULT_SYSTEM_ROOT
    return Path(system_root) / "config" / "domains"


def _list_domain_catalog_paths(root: Path, layout: ProjectLayout) -> tuple[Path, ...]:
    catalog_dir = root / _domain_catalog_glob(layout)
    if not catalog_dir.is_dir():
        return ()
    return tuple(_domain_catalog_glob(layout) / path.name for path in sorted(catalog_dir.glob("*.yaml")))


def _scan_cross_file_registration(
    root: Path,
    *,
    layout: ProjectLayout,
    pipeline_id: str,
    pipeline_path: Path,
) -> _CrossFileRegistration | None:
    expected = pipeline_path.as_posix()
    for catalog_path in _list_domain_catalog_paths(root, layout):
        current = read_catalog_text(repo_root=root, path=catalog_path)
        if current is None:
            continue
        payload = _catalog_payload(current, catalog_path)
        registered = _registered_manifest(payload, pipeline_id)
        if registered is None:
            continue
        matches = _resolved_manifest_path(root, catalog_path, registered) == expected
        return _CrossFileRegistration(
            catalog_path=catalog_path,
            manifest=registered,
            matches=matches,
        )
    return None


def _resolved_manifest_path(repo_root: Path, catalog_path: Path, manifest: str) -> str:
    catalog_file = (repo_root / catalog_path).resolve()
    resolved = (catalog_file.parent / manifest).resolve()
    try:
        return resolved.relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        return manifest


def _validate_catalog_domain(payload: dict[str, object], catalog_path: Path, expected_domain: str) -> None:
    domain = payload.get("domain")
    if domain is not None and domain != expected_domain:
        raise MembershipConflictError(
            f"Domain catalog {catalog_path.as_posix()} declares domain {domain!r}; expected {expected_domain!r}."
        )


def _membership_failure(message: str) -> SelfServiceResult:
    return SelfServiceResult(
        passed=False,
        errors=(
            dpone_error(
                MEMBERSHIP_CONFLICT_CODE,
                message,
                stage="init_pipeline",
                fixes=[manual_fix("register_domain_catalog_membership")],
                docs_url=error_docs_url(MEMBERSHIP_CONFLICT_CODE),
            ),
        ),
        exit_code=4,
    )


def _membership_failure_plan(
    plan: ScaffoldApplyPlan,
    applier: ScaffoldApplier | None,
    failure: SelfServiceResult,
) -> tuple[ScaffoldApplyPlan, SelfServiceResult]:
    if applier is not None and plan.created_files:
        plan = applier.compensate(plan)
    enriched = SelfServiceResult(
        passed=failure.passed,
        changes=plan.changes,
        errors=failure.errors,
        details={
            **(failure.details or {}),
            "rollback_journal": plan.rollback_journal,
            "rollback_issues": list(plan.rollback_issues),
            "recovery_artifacts": list(plan.recovery_artifacts),
            "recovery_required": plan.recovery_required,
        },
        exit_code=failure.exit_code,
    )
    return plan, enriched


__all__ = [
    "MEMBERSHIP_CONFLICT_CODE",
    "MembershipConflictError",
    "MembershipPatch",
    "domain_catalog_path",
    "membership_catalog_snippet",
    "plan_membership_patch",
    "preflight_domain_membership",
    "register_domain_membership",
]
