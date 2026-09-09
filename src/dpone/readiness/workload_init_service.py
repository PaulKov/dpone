"""Plan-first GitOps workload scaffolding for declarative Airflow."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.readiness.workload_init_catalog_merge import CatalogPatchReceipt


from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal

from dpone.manifest.loader import ManifestLoaderRouter
from dpone.readiness.airflow_scaffold_apply import ScaffoldApplier, ScaffoldFile
from dpone.readiness.airflow_self_service_models import SelfServiceResult
from dpone.readiness.self_service_error_catalog import errors_from_workload_conflicts, manifest_validation_error
from dpone.readiness.workload_init_catalog_merge import (
    apply_catalog_patch,
    plan_domain_catalog_patch,
    plan_ownership_patch,
    rollback_catalog_patch,
)
from dpone.readiness.workload_init_result import (
    merge_workload_init_changes as _merge_changes,
)
from dpone.readiness.workload_init_result import (
    validation_summary as _validation_summary,
)
from dpone.readiness.workload_init_result import (
    workload_init_details as _details,
)
from dpone.readiness.workload_init_templates import (
    DEFAULT_SCHEDULE,
    DEFAULT_TIMEZONE,
    batch_manifest_payload,
    catalog_manifest_payload,
    dag_declaration,
    docs_stub,
    sql_stub,
    workload_init_options,
)
from dpone.readiness.workload_init_unit_of_work import (
    ReceiptScaffoldFileSystem,
    preflight_workload_set,
    recovery_artifacts_from_changes,
    rollback_workload_init,
    scaffold_apply_failure_receipt,
    unit_of_work_details,
    validate_generated_manifest,
)

LayoutKind = Literal["batch", "catalog"]
DEFAULT_WORKLOAD_SET = "dpone_workloads/gitops/gitops.yaml"
DEFAULT_DOMAIN_DIR = "dpone_workloads/gitops/domains"
DEFAULT_OWNERSHIP = "ownership.yaml"


@dataclass(frozen=True)
class WorkloadInitRequest:
    workload_ref: str
    source: str
    sink: str
    strategy: str
    layout: LayoutKind = "batch"
    domain: str | None = None
    dag_id: str | None = None
    schedule: str | None = DEFAULT_SCHEDULE
    owner: str = "data_platform"
    timezone: str = DEFAULT_TIMEZONE
    workload_set: str = DEFAULT_WORKLOAD_SET
    apply: bool = False


class WorkloadInitService:
    """Generate GitOps workload artifacts with plan-first idempotency."""

    def __init__(self, *, repo_root: str | Path, manifest_loader: ManifestLoaderRouter | None = None) -> None:
        self._root = Path(repo_root)
        self._loader = manifest_loader or ManifestLoaderRouter()

    def init(self, request: WorkloadInitRequest) -> SelfServiceResult:
        domain, workload_id = _parse_workload_ref(request.workload_ref, domain_override=request.domain)
        options = workload_init_options(source=request.source, sink=request.sink, strategy=request.strategy)
        options["domain"] = domain
        dag_id = request.dag_id or f"DAG__{domain}__{workload_id}__sync"
        dag_spec = dag_declaration(
            dag_id=dag_id,
            workload_id=workload_id,
            schedule=request.schedule,
            owner=request.owner,
            timezone=request.timezone,
        )

        manifest_rel = f"workloads/{domain}/dpone/manifests/{workload_id}.yaml"
        sql_rel = f"workloads/{domain}/dpone/sql/{workload_id}.sql"
        docs_rel = f"workloads/{domain}/dpone/docs/{workload_id}.md"
        domain_rel = f"{DEFAULT_DOMAIN_DIR}/{domain}.yaml"
        manifest_ref = _manifest_ref_from_domain(domain_rel, manifest_rel)
        sql_repo_relative = "../sql/" + f"{workload_id}.sql"

        manifest_payload = (
            batch_manifest_payload(workload_id=workload_id, options=options, sql_repo_relative=sql_repo_relative)
            if request.layout == "batch"
            else catalog_manifest_payload(workload_id=workload_id, options=options, sql_repo_relative=sql_repo_relative)
        )

        scaffold_files: tuple[ScaffoldFile, ...] = (
            ScaffoldFile.yaml(Path(manifest_rel), manifest_payload),
            ScaffoldFile(path=Path(sql_rel), text=sql_stub(workload_id=workload_id, options=options)),
            ScaffoldFile(
                path=Path(docs_rel), text=docs_stub(domain=domain, workload_id=workload_id, owner=request.owner)
            ),
        )
        scaffold_filesystem = ReceiptScaffoldFileSystem(self._root)
        workload_set_file, workload_set_conflict = preflight_workload_set(
            scaffold_filesystem,
            Path(request.workload_set),
        )
        if workload_set_file is not None:
            scaffold_files = (workload_set_file, *scaffold_files)
        applier = ScaffoldApplier(self._root, filesystem=scaffold_filesystem)
        scaffold_plan = applier.plan(scaffold_files)
        if workload_set_conflict is not None:
            scaffold_plan = replace(
                scaffold_plan,
                changes=(workload_set_conflict, *scaffold_plan.changes),
            )
        domain_plan = plan_domain_catalog_patch(
            repo_root=self._root,
            path=Path(domain_rel),
            domain=domain,
            workload_id=workload_id,
            manifest_ref=manifest_ref,
            dag_id=dag_id,
            dag_declaration=dag_spec,
        )
        ownership_plan = plan_ownership_patch(
            repo_root=self._root,
            path=Path(DEFAULT_OWNERSHIP),
            domain=domain,
            workload_id=workload_id,
            owner=request.owner,
        )
        changes = _merge_changes(scaffold_plan.changes, domain_plan, ownership_plan, repo_root=self._root)
        has_conflict = any(change.action == "conflict" for change in changes)
        conflict_errors = errors_from_workload_conflicts(changes, domain=domain, workload_id=workload_id)
        if has_conflict or not request.apply:
            details = _details(
                request=request,
                domain=domain,
                workload_id=workload_id,
                dag_id=dag_id,
                manifest_rel=manifest_rel,
                validation=_validation_summary(None),
            )
            return SelfServiceResult(
                passed=not has_conflict,
                changes=changes,
                errors=conflict_errors,
                details=details,
            )

        validation = validate_generated_manifest(
            self._loader,
            scaffold_files,
            manifest_rel=manifest_rel,
        )
        if validation.get("passed") is not True:
            return SelfServiceResult(
                passed=False,
                changes=changes,
                errors=(
                    manifest_validation_error(
                        message=str(validation.get("message", "validation failed")),
                        path=manifest_rel,
                        domain=domain,
                        workload_id=workload_id,
                    ),
                ),
                details=_details(
                    request=request,
                    domain=domain,
                    workload_id=workload_id,
                    dag_id=dag_id,
                    manifest_rel=manifest_rel,
                    validation=_validation_summary(validation),
                    rollback_journal=scaffold_plan.rollback_journal,
                    unit_of_work=unit_of_work_details("not_started"),
                ),
            )

        try:
            scaffold_apply = applier.apply(scaffold_files)
        except OSError as exc:
            rollback = rollback_workload_init(
                catalog_receipts=(),
                scaffold_filesystem=scaffold_filesystem,
                repo_root=self._root,
                catalog_rollback=rollback_catalog_patch,
                initial_error=exc,
            )
            scaffold_failure = scaffold_apply_failure_receipt(
                exc,
                scaffold_plan,
                rollback_issues=rollback.issues,
                recovery_artifacts=rollback.recovery_artifacts,
            )
            changes = _merge_changes(
                scaffold_failure.changes,
                domain_plan,
                ownership_plan,
                repo_root=self._root,
            )
            return SelfServiceResult(
                passed=False,
                changes=changes,
                errors=errors_from_workload_conflicts(changes, domain=domain, workload_id=workload_id),
                details=_details(
                    request=request,
                    domain=domain,
                    workload_id=workload_id,
                    dag_id=dag_id,
                    manifest_rel=manifest_rel,
                    validation=_validation_summary(validation),
                    rollback_journal=scaffold_failure.rollback_journal,
                    unit_of_work=unit_of_work_details(
                        rollback.status,
                        recovery_artifacts=rollback.recovery_artifacts,
                        issues=rollback.issues,
                    ),
                ),
            )
        if scaffold_apply.has_conflict:
            rollback = rollback_workload_init(
                catalog_receipts=(),
                scaffold_filesystem=scaffold_filesystem,
                repo_root=self._root,
                catalog_rollback=rollback_catalog_patch,
                known_issues=scaffold_apply.rollback_issues,
                known_recovery_artifacts=tuple(
                    dict.fromkeys(
                        (
                            *scaffold_apply.recovery_artifacts,
                            *recovery_artifacts_from_changes(scaffold_apply.changes),
                        )
                    )
                ),
            )
            changes = _merge_changes(
                scaffold_apply.changes,
                domain_plan,
                ownership_plan,
                repo_root=self._root,
            )
            return SelfServiceResult(
                passed=False,
                changes=changes,
                errors=errors_from_workload_conflicts(changes, domain=domain, workload_id=workload_id),
                details=_details(
                    request=request,
                    domain=domain,
                    workload_id=workload_id,
                    dag_id=dag_id,
                    manifest_rel=manifest_rel,
                    validation=_validation_summary(validation),
                    rollback_journal=scaffold_apply.rollback_journal,
                    unit_of_work=unit_of_work_details(
                        rollback.status,
                        recovery_artifacts=rollback.recovery_artifacts,
                        issues=rollback.issues,
                    ),
                ),
            )

        catalog_plans = [domain_plan, ownership_plan]
        receipts: list[CatalogPatchReceipt] = []
        failed_plan = domain_plan
        try:
            for plan in catalog_plans:
                failed_plan = plan
                receipt = apply_catalog_patch(plan, repo_root=self._root)
                if receipt is not None:
                    receipts.append(receipt)
        except OSError as exc:
            rollback = rollback_workload_init(
                catalog_receipts=receipts,
                scaffold_filesystem=scaffold_filesystem,
                repo_root=self._root,
                catalog_rollback=rollback_catalog_patch,
                initial_error=exc,
            )
            reason = str(exc)
            if rollback.issues:
                reason = f"{reason} Workload init rollback requires recovery: {'; '.join(rollback.issues)}"
            catalog_plans[catalog_plans.index(failed_plan)] = replace(
                failed_plan,
                action="conflict",
                reason=reason,
            )
            changes = _merge_changes(
                scaffold_apply.changes,
                catalog_plans[0],
                catalog_plans[1],
                repo_root=self._root,
            )
            return SelfServiceResult(
                passed=False,
                changes=changes,
                errors=errors_from_workload_conflicts(changes, domain=domain, workload_id=workload_id),
                details=_details(
                    request=request,
                    domain=domain,
                    workload_id=workload_id,
                    dag_id=dag_id,
                    manifest_rel=manifest_rel,
                    validation=_validation_summary(validation),
                    rollback_journal=scaffold_apply.rollback_journal,
                    unit_of_work=unit_of_work_details(
                        rollback.status,
                        recovery_artifacts=rollback.recovery_artifacts,
                        issues=rollback.issues,
                    ),
                ),
            )

        changes = _merge_changes(
            scaffold_apply.changes,
            domain_plan,
            ownership_plan,
            repo_root=self._root,
        )
        details = _details(
            request=request,
            domain=domain,
            workload_id=workload_id,
            dag_id=dag_id,
            manifest_rel=manifest_rel,
            validation=validation,
            rollback_journal=scaffold_apply.rollback_journal,
            unit_of_work=unit_of_work_details("committed"),
        )
        return SelfServiceResult(passed=True, changes=changes, details=details)


def _parse_workload_ref(workload_ref: str, *, domain_override: str | None) -> tuple[str, str]:
    ref = workload_ref.strip().strip("/")
    if not ref:
        raise ValueError("workload reference must be DOMAIN/WORKLOAD_ID")
    if "/" not in ref:
        if domain_override:
            return domain_override.strip(), ref
        raise ValueError("workload reference must be DOMAIN/WORKLOAD_ID or pass --domain")
    domain, workload_id = ref.split("/", 1)
    if domain_override and domain_override != domain:
        raise ValueError(f"--domain {domain_override!r} conflicts with reference domain {domain!r}")
    if not domain or not workload_id:
        raise ValueError("workload reference must be DOMAIN/WORKLOAD_ID")
    return domain, workload_id


def _manifest_ref_from_domain(domain_rel: str, manifest_rel: str) -> str:
    depth = len(Path(domain_rel).parent.parts)
    prefix = "/".join([".."] * depth)
    return f"{prefix}/{manifest_rel}" if prefix else manifest_rel


__all__ = ["DEFAULT_WORKLOAD_SET", "WorkloadInitRequest", "WorkloadInitService"]
