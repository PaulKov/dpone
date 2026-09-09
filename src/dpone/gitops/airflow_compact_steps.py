"""Build compact Airflow pack step metadata."""

from __future__ import annotations

import shlex
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

from dpone.gitops.airflow_pack_models import (
    GitOpsAirflowHookRuntimeCommand,
    GitOpsAirflowPackStep,
)
from dpone.gitops.workload_catalog_models import GitOpsWorkloadDefinition
from dpone.governance.hooks import HookGraph, HookValidationError

if TYPE_CHECKING:
    from dpone.manifest.models import ProcessSpec


def compact_pack_steps(
    *,
    workload: GitOpsWorkloadDefinition,
    repo_root: Path | None,
    output_path: str,
) -> tuple[GitOpsAirflowPackStep, ...]:
    """Return visible preparation, runtime and outcome steps for one pack."""

    hook_steps = _hook_steps(workload=workload, repo_root=repo_root)
    return (
        *hook_steps,
        GitOpsAirflowPackStep(
            name="dpone_runtime",
            phase="runtime",
            command=f"build_dpone_gitops_task_group_from_pack({output_path})",
            required=True,
            credential_required=False,
            produces=("runtime_evidence", "xcom_summary"),
            reason="compact_pack_runtime",
            depends_on=_terminal_step_names(hook_steps),
        ),
        GitOpsAirflowPackStep(
            name="outcome_gate",
            phase="evidence",
            command="validate compact pack runtime outcome",
            required=True,
            credential_required=False,
            produces=(),
            reason="compact_pack_outcome_gate",
            depends_on=("dpone_runtime",),
        ),
    )


def compact_process_steps(
    *,
    workload: GitOpsWorkloadDefinition,
    process: ProcessSpec,
    repo_root: Path,
    output_path: str,
) -> tuple[GitOpsAirflowPackStep, ...]:
    """Build one selector-scoped static step graph for a compact pack."""

    graph = _hook_graph_for_process(process, repo_root=repo_root)
    hook_steps = _hook_steps_from_graph(
        graph,
        manifest=workload.manifest,
        selector=process.selector,
    )
    return (
        *hook_steps,
        GitOpsAirflowPackStep(
            name="dpone_runtime",
            phase="runtime",
            command=f"build_dpone_gitops_task_group_from_pack({output_path})",
            required=True,
            credential_required=False,
            produces=("runtime_evidence", "xcom_summary"),
            reason="compact_pack_runtime",
            depends_on=_terminal_step_names(hook_steps),
        ),
        GitOpsAirflowPackStep(
            name="outcome_gate",
            phase="evidence",
            command="validate compact pack runtime outcome",
            required=True,
            credential_required=False,
            produces=(),
            reason="compact_pack_outcome_gate",
            depends_on=("dpone_runtime",),
        ),
    )


def _hook_steps(
    *,
    workload: GitOpsWorkloadDefinition,
    repo_root: Path | None,
) -> tuple[GitOpsAirflowPackStep, ...]:
    graph = _hook_graph_for_manifest(workload.manifest, repo_root=repo_root)
    return _hook_steps_from_graph(graph, manifest=workload.manifest, selector=None)


def _hook_steps_from_graph(
    graph: HookGraph | None,
    *,
    manifest: str,
    selector: str | None,
) -> tuple[GitOpsAirflowPackStep, ...]:
    if graph is None:
        return ()
    steps: list[GitOpsAirflowPackStep] = []
    for action in graph.ordered_phase("pre_hook"):
        if action.execution.airflow != "separate_task":
            continue
        name = _hook_step_name("pre_hook", action.id)
        argv = (
            "dpone",
            "hooks",
            "execute",
            manifest,
            "--phase",
            "pre_hook",
            "--hook-id",
            action.id,
            *((("--selector", selector)) if selector is not None else ()),
        )
        steps.append(
            GitOpsAirflowPackStep(
                name=name,
                phase="pre_hook",
                command=shlex.join(argv),
                runtime_command=GitOpsAirflowHookRuntimeCommand(
                    hook_id=action.id,
                    process_selector=selector,
                    argv=argv,
                ),
                required=True,
                credential_required=True,
                produces=("hook_evidence",),
                reason=f"hook:{action.kind}",
                depends_on=tuple(_hook_step_name("pre_hook", dependency) for dependency in action.depends_on),
            )
        )
    return tuple(steps)


def _hook_graph_for_process(process: ProcessSpec, *, repo_root: Path) -> HookGraph:
    source = _mapping(process.raw_config.get("source"))
    options = _mapping(source.get("options"))
    return HookGraph.from_config(
        options.get("hooks"),
        manifest_dir=str(process.config_path.parent),
        repo_root=str(repo_root),
    )


def _hook_graph_for_manifest(manifest: str, *, repo_root: Path | None) -> HookGraph | None:
    if repo_root is None:
        return None
    path = (repo_root / manifest).resolve()
    if not path.exists():
        return None
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError:
        return None
    if not isinstance(payload, dict):
        return None
    source = _mapping(payload.get("source"))
    options = _mapping(source.get("options"))
    try:
        return HookGraph.from_config(
            options.get("hooks"),
            manifest_dir=str(path.parent),
            repo_root=str(repo_root),
        )
    except HookValidationError:
        return None


def _hook_step_name(phase: str, hook_id: str) -> str:
    suffix = "".join(char if char.isalnum() else "_" for char in hook_id.strip()).strip("_").lower()
    return f"{phase}_{suffix or 'hook'}"


def _terminal_step_names(steps: tuple[GitOpsAirflowPackStep, ...]) -> tuple[str, ...]:
    dependencies = {dependency for step in steps for dependency in step.depends_on}
    return tuple(step.name for step in steps if step.name not in dependencies)


def _mapping(value: object) -> dict[str, object]:
    return dict(value) if isinstance(value, dict) else {}


__all__ = ["compact_pack_steps", "compact_process_steps"]
