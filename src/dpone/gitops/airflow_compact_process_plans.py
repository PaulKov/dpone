"""Build selector-indexed process plans for one compact Airflow pack."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.backfill.mapping import AirflowBackfillMappingViolation, build_airflow_mapping_plan
from dpone.gitops.airflow_compact_errors import (
    AirflowConnectionProjectionClosureError,
    CompactConnectionProjectionError,
    CompactProcessPlanError,
    close_connection_projection,
    required_runtime_connection_refs,
)
from dpone.gitops.airflow_compact_runtime import compact_runtime_command
from dpone.gitops.airflow_compact_steps import compact_pack_steps, compact_process_steps
from dpone.gitops.airflow_pack_models import GitOpsAirflowPackStep
from dpone.gitops.airflow_process_identity import resolve_airflow_process_identities
from dpone.gitops.airflow_retry_authority import certify_airflow_retry_authority
from dpone.gitops.airflow_step_visibility import (
    AirflowStepVisibilityError,
    estimate_visible_tasks,
    resolve_step_visibility,
    separate_airflow_hook_count,
)
from dpone.gitops.workload_catalog_models import (
    GitOpsWorkloadCatalogIssue,
    GitOpsWorkloadDefinition,
)
from dpone.governance.hooks import HookValidationError
from dpone.manifest.errors import LegacySingleManifestConfigurationError, ManifestConfigurationError
from dpone.manifest.loader import ManifestLoaderRouter
from dpone.manifest.models import ProcessSpec

DEFAULT_PROCESS_PLAN_KEY = "__default__"


@dataclass(frozen=True, slots=True)
class GitOpsAirflowProcessPlan:
    """Static commands and internal steps for one selected process."""

    selector: str | None
    node_id: str
    process_name: str
    visibility: str
    task_group: str | None
    estimated_visible_tasks: int
    depends_on_process_selectors: tuple[str, ...]
    steps: tuple[GitOpsAirflowPackStep, ...]
    inline_runtime_command: str
    expanded_runtime_command: str
    mapping_plan: dict[str, Any]

    @property
    def key(self) -> str:
        return self.selector or DEFAULT_PROCESS_PLAN_KEY

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "selector": self.selector,
            "dag_node": {
                "node_id": self.node_id,
                "process_name": self.process_name,
                "visibility": self.visibility,
                "task_group": self.task_group,
                "estimated_visible_tasks": self.estimated_visible_tasks,
                "depends_on_process_selectors": list(self.depends_on_process_selectors),
            },
            "runtime_commands": {
                "inline": self.inline_runtime_command,
                "expanded": self.expanded_runtime_command,
            },
            "mapping_plan": dict(self.mapping_plan),
            "steps": [step.to_jsonable() for step in self.steps],
        }


@dataclass(frozen=True, slots=True)
class GitOpsAirflowProcessProjection:
    """Pack-level process plans plus their legacy compatibility projection."""

    plans: tuple[GitOpsAirflowProcessPlan, ...]
    steps: tuple[GitOpsAirflowPackStep, ...]
    runtime_command: str
    required_connection_refs: tuple[str, ...] | None = None
    retry_authority: dict[str, Any] | None = None
    warnings: tuple[GitOpsWorkloadCatalogIssue, ...] = ()
    blockers: tuple[GitOpsWorkloadCatalogIssue, ...] = ()

    def project_connections(self, projection: Mapping[str, Any]) -> dict[str, Any]:
        """Close exact dependencies while preserving unresolved compatibility paths."""

        if self.required_connection_refs is None:
            return dict(projection)
        try:
            return close_connection_projection(projection, required_refs=self.required_connection_refs)
        except AirflowConnectionProjectionClosureError as exc:
            raise CompactConnectionProjectionError(exc.code, str(exc)) from exc


def resolve_compact_process_projection(
    *,
    workload: GitOpsWorkloadDefinition,
    runtime_manifest_path: str,
    runtime_manifest_kind: str,
    repo_root: Path | None,
    output_path: str,
) -> GitOpsAirflowProcessProjection:
    """Build selector plans and one backward-compatible top-level pack view."""

    plans: tuple[GitOpsAirflowProcessPlan, ...] = ()
    required_connection_refs: tuple[str, ...] | None = None
    warnings: list[GitOpsWorkloadCatalogIssue] = []
    blockers: list[GitOpsWorkloadCatalogIssue] = []
    retry_authority: dict[str, Any] | None = None
    if repo_root is not None and runtime_manifest_kind != "unmaterialized_manifest":
        try:
            compiled = _compile_compact_process_plans(
                workload=workload,
                runtime_manifest_path=runtime_manifest_path,
                repo_root=repo_root,
                output_path=output_path,
            )
            plans = compiled.plans
            required_connection_refs = compiled.required_connection_refs
            retry_authority = compiled.retry_authority
        except LegacySingleManifestConfigurationError:
            warnings.append(
                _process_plan_issue(
                    workload,
                    code="DPONE_AIRFLOW_PROCESS_PLAN_COMPATIBILITY_FALLBACK",
                    message=(
                        "Legacy single-manifest metadata could not produce selector plans; "
                        "the pack retains its legacy unselected execution contract."
                    ),
                )
            )
        except (
            AirflowStepVisibilityError,
            CompactProcessPlanError,
            HookValidationError,
            ManifestConfigurationError,
        ) as exc:
            blockers.append(
                _process_plan_issue(
                    workload,
                    code=getattr(exc, "code", "DPONE_AIRFLOW_PROCESS_PLAN_INVALID"),
                    message=str(exc),
                )
            )
    legacy_plan = plans[0] if len(plans) == 1 else None
    runtime_command = legacy_plan.expanded_runtime_command if legacy_plan is not None else ""
    if legacy_plan is None and runtime_manifest_kind != "unmaterialized_manifest":
        runtime_command = compact_runtime_command(
            workload_id=workload.workload_id,
            manifest=runtime_manifest_path,
            skip_separate_hooks=not plans,
        )
    steps = (
        legacy_plan.steps
        if legacy_plan is not None
        else compact_pack_steps(workload=workload, repo_root=repo_root, output_path=output_path)
    )
    return GitOpsAirflowProcessProjection(
        plans=plans,
        steps=steps,
        runtime_command=runtime_command,
        required_connection_refs=required_connection_refs,
        retry_authority=retry_authority,
        warnings=tuple(warnings),
        blockers=tuple(blockers),
    )


def build_compact_process_plans(
    *,
    workload: GitOpsWorkloadDefinition,
    runtime_manifest_path: str,
    repo_root: Path,
    output_path: str,
    manifest_loader: ManifestLoaderRouter | None = None,
) -> tuple[GitOpsAirflowProcessPlan, ...]:
    """Compile all workload processes into deterministic selector plans."""

    return _compile_compact_process_plans(
        workload=workload,
        runtime_manifest_path=runtime_manifest_path,
        repo_root=repo_root,
        output_path=output_path,
        manifest_loader=manifest_loader,
    ).plans


@dataclass(frozen=True, slots=True)
class _CompiledProcessPlans:
    plans: tuple[GitOpsAirflowProcessPlan, ...]
    required_connection_refs: tuple[str, ...]
    retry_authority: dict[str, Any] | None


def _compile_compact_process_plans(
    *,
    workload: GitOpsWorkloadDefinition,
    runtime_manifest_path: str,
    repo_root: Path,
    output_path: str,
    manifest_loader: ManifestLoaderRouter | None = None,
) -> _CompiledProcessPlans:
    """Load once, then derive plans and their exact connection dependency set."""

    manifest_path = (repo_root / workload.manifest).resolve(strict=False)
    try:
        manifest_path.relative_to(repo_root.resolve(strict=False))
    except ValueError as exc:
        raise CompactProcessPlanError(
            "DPONE_AIRFLOW_PROCESS_PLAN_PATH_INVALID",
            "Workload manifest is outside the repository root.",
        ) from exc
    loaded = (manifest_loader or ManifestLoaderRouter()).load(manifest_path, metadata_only=True)
    if not loaded.processes:
        raise CompactProcessPlanError(
            "DPONE_AIRFLOW_PROCESS_PLAN_EMPTY",
            "A runnable workload must contain at least one process.",
        )
    identities = resolve_airflow_process_identities(loaded, workload_id=workload.workload_id)
    if any(identity.selector_required and identity.process.selector is None for identity in identities):
        raise CompactProcessPlanError(
            "DPONE_AIRFLOW_NODE_SELECTOR_MISSING",
            "Every compiled flow or batch process requires a selector.",
        )
    identity_keys = [identity.process.selector or DEFAULT_PROCESS_PLAN_KEY for identity in identities]
    if len(identity_keys) != len(set(identity_keys)):
        raise CompactProcessPlanError(
            "DPONE_AIRFLOW_PROCESS_PLAN_SELECTOR_DUPLICATE",
            "Compiled process selectors must be unique inside one workload pack.",
        )
    plans = tuple(
        _build_process_plan(
            workload=workload,
            process=identity.process,
            node_id=identity.node_id,
            runtime_manifest_path=runtime_manifest_path,
            repo_root=repo_root,
            output_path=output_path,
        )
        for identity in identities
    )
    return _CompiledProcessPlans(
        plans=tuple(sorted(plans, key=lambda plan: plan.key)),
        required_connection_refs=required_runtime_connection_refs(loaded.processes),
        retry_authority=certify_airflow_retry_authority(process.raw_config for process in loaded.processes),
    )


def process_plans_jsonable(plans: tuple[GitOpsAirflowProcessPlan, ...]) -> dict[str, dict[str, Any]]:
    return {plan.key: plan.to_jsonable() for plan in plans}


def compact_pack_kpo_field(plans: tuple[GitOpsAirflowProcessPlan, ...]) -> str:
    """Return the wire field that makes mapped packs fail closed on old providers."""

    return (
        "mapped_kpo_kwargs"
        if any(plan.mapping_plan.get("mode") in {"visible", "summary"} for plan in plans)
        else "kpo_kwargs"
    )


def _build_process_plan(
    *,
    workload: GitOpsWorkloadDefinition,
    process: ProcessSpec,
    node_id: str,
    runtime_manifest_path: str,
    repo_root: Path,
    output_path: str,
) -> GitOpsAirflowProcessPlan:
    steps = compact_process_steps(
        workload=workload,
        process=process,
        repo_root=repo_root,
        output_path=output_path,
    )
    visibility = resolve_step_visibility(process.raw_config)
    try:
        mapping_plan = build_airflow_mapping_plan(
            process.config.load_config,
            _mapping_policy(workload=workload, process=process),
        ).to_jsonable()
    except AirflowBackfillMappingViolation as exc:
        raise CompactProcessPlanError(exc.code, str(exc)) from exc
    return GitOpsAirflowProcessPlan(
        selector=process.selector,
        node_id=node_id,
        process_name=process.name,
        visibility=visibility,
        task_group=process.task_group,
        estimated_visible_tasks=estimate_visible_tasks(
            visibility=visibility,
            separate_hook_count=separate_airflow_hook_count(process.raw_config),
        ),
        depends_on_process_selectors=_internal_dependency_selectors(process),
        steps=steps,
        inline_runtime_command=compact_runtime_command(
            workload_id=workload.workload_id,
            manifest=runtime_manifest_path,
            selector=process.selector,
            skip_separate_hooks=False,
        ),
        expanded_runtime_command=compact_runtime_command(
            workload_id=workload.workload_id,
            manifest=runtime_manifest_path,
            selector=process.selector,
            skip_separate_hooks=True,
        ),
        mapping_plan=mapping_plan,
    )


def _mapping_policy(*, workload: GitOpsWorkloadDefinition, process: ProcessSpec) -> dict[str, Any] | None:
    airflow = workload.effective_config.get("airflow")
    if isinstance(airflow, dict) and "mapping" in airflow:
        mapping = airflow.get("mapping")
        if not isinstance(mapping, dict):
            raise CompactProcessPlanError(
                "DPONE_AIRFLOW_MAPPING_POLICY_INVALID",
                "workload airflow.mapping must be a mapping",
            )
        return dict(mapping)
    gitops = process.raw_config.get("gitops")
    if not isinstance(gitops, dict):
        return None
    process_airflow = gitops.get("airflow")
    if not isinstance(process_airflow, dict) or "mapping" not in process_airflow:
        return None
    mapping = process_airflow.get("mapping")
    if not isinstance(mapping, dict):
        raise CompactProcessPlanError(
            "DPONE_AIRFLOW_MAPPING_POLICY_INVALID",
            "manifest gitops.airflow.mapping must be a mapping",
        )
    return dict(mapping)


def _internal_dependency_selectors(process: ProcessSpec) -> tuple[str, ...]:
    selectors: set[str] = set()
    process_path = process.config_path.resolve(strict=False)
    for dependency in tuple(getattr(process.config, "dependencies", ()) or ()):
        raw_path = str(getattr(dependency, "path", "") or "")
        if raw_path.startswith("#"):
            selector = raw_path[1:].strip()
        else:
            file_part, separator, selector = raw_path.partition("#")
            if not separator or Path(file_part).resolve(strict=False) != process_path:
                continue
            selector = selector.strip()
        if selector:
            selectors.add(selector)
    return tuple(sorted(selectors))


def _process_plan_issue(
    workload: GitOpsWorkloadDefinition,
    *,
    code: str,
    message: str,
) -> GitOpsWorkloadCatalogIssue:
    return GitOpsWorkloadCatalogIssue(
        code=code,
        message=message,
        path=workload.manifest,
        source="dpone gitops airflow pack",
    )


__all__ = [
    "DEFAULT_PROCESS_PLAN_KEY",
    "CompactConnectionProjectionError",
    "CompactProcessPlanError",
    "GitOpsAirflowProcessPlan",
    "GitOpsAirflowProcessProjection",
    "build_compact_process_plans",
    "process_plans_jsonable",
    "resolve_compact_process_projection",
]
