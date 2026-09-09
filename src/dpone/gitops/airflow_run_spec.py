from __future__ import annotations

import re
import shlex
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from dpone.gitops.airflow_runtime_models import (
    GitOpsAirflowRunSpec,
    GitOpsAirflowRunSpecEntry,
    GitOpsAirflowRunSpecStep,
)
from dpone.gitops.models import GitOpsIssue
from dpone.governance.hooks import HookGraph, HookValidationError


class GitOpsAirflowRunSpecBuilder:
    """Build deterministic runtime contracts from an already verified GitOps bundle."""

    def build(
        self,
        *,
        bundle_path: str,
        bundle: Mapping[str, Any],
        image: str,
        image_digest: str | None,
        worktree: str,
        evidence_output: str,
        require_attestation: bool,
    ) -> GitOpsAirflowRunSpec:
        entries = _entries(bundle)
        blockers = _entry_blockers(entries)
        steps = _steps(
            bundle_path=bundle_path,
            bundle=bundle,
            entries=entries,
            worktree=worktree,
            require_attestation=require_attestation,
        )
        return GitOpsAirflowRunSpec(
            bundle_path=bundle_path,
            bundle_digest=_bundle_digest(bundle),
            image=image,
            image_digest=image_digest,
            worktree=worktree,
            evidence_output=evidence_output,
            entries=entries,
            steps=steps,
            blockers=blockers,
        )


def _entries(bundle: Mapping[str, Any]) -> tuple[GitOpsAirflowRunSpecEntry, ...]:
    raw_entries = bundle.get("entries")
    if not isinstance(raw_entries, list):
        return ()
    entries: list[GitOpsAirflowRunSpecEntry] = []
    for raw_entry in raw_entries:
        if not isinstance(raw_entry, Mapping):
            continue
        manifest = str(raw_entry.get("manifest") or "").strip()
        plan_path = str(raw_entry.get("plan_path") or "").strip()
        verify_path = str(raw_entry.get("verify_path") or "").strip()
        if not manifest and not plan_path:
            continue
        entries.append(
            GitOpsAirflowRunSpecEntry(
                manifest=manifest,
                plan_path=plan_path,
                verify_path=verify_path,
                passed=bool(raw_entry.get("passed", False)),
            )
        )
    return tuple(entries)


def _steps(
    *,
    bundle_path: str,
    bundle: Mapping[str, Any],
    entries: tuple[GitOpsAirflowRunSpecEntry, ...],
    worktree: str,
    require_attestation: bool,
) -> tuple[GitOpsAirflowRunSpecStep, ...]:
    steps: list[GitOpsAirflowRunSpecStep] = [
        GitOpsAirflowRunSpecStep(
            name="bundle_verify",
            kind="bundle_verify",
            command=_bundle_verify_command(bundle_path=bundle_path, require_attestation=require_attestation),
        )
    ]
    verify_lock = bool(_mapping(bundle.get("policy")).get("verify_lock"))
    for entry in entries:
        suffix = _step_suffix(entry.manifest or entry.plan_path)
        if entry.plan_path:
            verify_step_name = f"gitops_verify_{suffix}"
            steps.append(
                GitOpsAirflowRunSpecStep(
                    name=verify_step_name,
                    kind="gitops_verify",
                    command=_verify_command(plan_path=entry.plan_path, worktree=worktree, verify_lock=verify_lock),
                    manifest=entry.manifest or None,
                )
            )
        if entry.manifest:
            hook_steps = _hook_steps(entry.manifest, worktree=worktree)
            steps.extend(hook_steps)
            steps.append(
                GitOpsAirflowRunSpecStep(
                    name=f"dpone_run_{suffix}",
                    kind="dpone_run",
                    command=f"dpone run {shlex.quote(entry.manifest)}",
                    manifest=entry.manifest,
                    depends_on=_terminal_hook_step_names(hook_steps),
                )
            )
    return tuple(steps)


def _entry_blockers(entries: tuple[GitOpsAirflowRunSpecEntry, ...]) -> tuple[GitOpsIssue, ...]:
    return tuple(
        GitOpsIssue(
            code="bundle_entry_failed",
            message="GitOps bundle entry is not passed and cannot be added to an Airflow runtime contract",
            path=entry.manifest or entry.plan_path,
            source="dpone gitops airflow run-spec",
        )
        for entry in entries
        if not entry.passed
    )


def _bundle_verify_command(*, bundle_path: str, require_attestation: bool) -> str:
    command = f"dpone gitops bundle verify {shlex.quote(bundle_path)}"
    if require_attestation:
        command += " --require-attestation"
    return command


def _verify_command(*, plan_path: str, worktree: str, verify_lock: bool) -> str:
    command = f"dpone gitops verify {shlex.quote(plan_path)} --worktree {shlex.quote(worktree)}"
    if verify_lock:
        command += " --verify-lock"
    return command


def _bundle_digest(bundle: Mapping[str, Any]) -> str | None:
    attestation = _mapping(bundle.get("attestation"))
    digest = str(attestation.get("bundle_digest") or "").strip()
    if not digest:
        return None
    return digest if digest.startswith("sha256:") else f"sha256:{digest}"


def _step_suffix(value: str) -> str:
    suffix = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip()).strip("_").lower()
    return suffix or "entry"


def _hook_steps(manifest: str, *, worktree: str) -> tuple[GitOpsAirflowRunSpecStep, ...]:
    graph = _hook_graph_for_manifest(manifest, worktree=worktree)
    if graph is None:
        return ()
    steps: list[GitOpsAirflowRunSpecStep] = []
    for action in graph.ordered_phase("pre_hook"):
        if action.execution.airflow != "separate_task":
            continue
        step_name = _hook_step_name("pre_hook", action.id)
        steps.append(
            GitOpsAirflowRunSpecStep(
                name=step_name,
                kind=action.kind,
                command=(
                    f"dpone hooks execute {shlex.quote(manifest)} --phase pre_hook --hook-id {shlex.quote(action.id)}"
                ),
                manifest=manifest,
                depends_on=tuple(_hook_step_name("pre_hook", dependency) for dependency in action.depends_on),
            )
        )
    return tuple(steps)


def _hook_graph_for_manifest(manifest: str, *, worktree: str) -> HookGraph | None:
    path = Path(worktree) / manifest
    if not path.exists():
        return None
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError:
        return None
    if not isinstance(payload, Mapping):
        return None
    source = _mapping(payload.get("source"))
    options = _mapping(source.get("options"))
    try:
        return HookGraph.from_config(
            options.get("hooks"),
            manifest_dir=str(path.parent),
            repo_root=str(Path(worktree)),
        )
    except HookValidationError:
        return None


def _hook_step_name(phase: str, hook_id: str) -> str:
    return f"{phase}_{_step_suffix(hook_id)}"


def _terminal_hook_step_names(steps: tuple[GitOpsAirflowRunSpecStep, ...]) -> tuple[str, ...]:
    if not steps:
        return ()
    dependencies = {dependency for step in steps for dependency in step.depends_on}
    terminals = tuple(step.name for step in steps if step.name not in dependencies)
    return terminals or (steps[-1].name,)


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


__all__ = ["GitOpsAirflowRunSpecBuilder"]
