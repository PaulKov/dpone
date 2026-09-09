"""Services for manifest-defined hook graph execution."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.governance.hooks import HookPhaseEvidence


import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from dpone.governance.hooks import HookExecutionContext, HookGraph, HookGraphRunner, HookStepEvidence
from dpone.governance.sql_hooks import SqlHookProvider
from dpone.manifest.errors import ManifestConfigurationError
from dpone.manifest.loader import ManifestLoaderRouter


@dataclass(frozen=True, slots=True)
class HookExecuteReport:
    manifest: str
    phase: str
    hook_id: str | None
    status: str
    steps: tuple[HookStepEvidence, ...]
    process: str | None = None
    selector: str | None = None
    kind: str = "dpone.hooks.execute.v1"

    @property
    def passed(self) -> bool:
        return self.status in {"planned", "succeeded"}

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "manifest": self.manifest,
            "process": self.process,
            "selector": self.selector,
            "phase": self.phase,
            "hook_id": self.hook_id,
            "status": self.status,
            "steps": [
                {
                    "id": step.id,
                    "phase": step.phase,
                    "kind": step.kind,
                    "type": step.type,
                    "status": step.status,
                    "lineage": {
                        "inputs": list(step.lineage.inputs),
                        "outputs": list(step.lineage.outputs),
                    },
                    "result": dict(step.result),
                    "error_message": step.error_message,
                }
                for step in self.steps
            ],
        }

    def to_text(self) -> str:
        lines = [
            "dpone hooks execute",
            f"- manifest: {self.manifest}",
            f"- process: {self.process or ''}",
            f"- phase: {self.phase}",
            f"- status: {self.status}",
        ]
        lines.extend(f"- hook: {step.id} ({step.kind}/{step.type}) -> {step.status}" for step in self.steps)
        return "\n".join(lines) + "\n"


class HookExecuteService:
    """Execute or plan one manifest hook phase/action."""

    def execute(
        self,
        *,
        path: str | Path,
        registry_paths: Sequence[str | os.PathLike[str]] = (),
        selector: str | None,
        phase: str,
        hook_id: str | None,
        dry_run: bool,
        repo_root: str | Path | None = None,
    ) -> HookExecuteReport:
        manifest_path = Path(path)
        manifest = ManifestLoaderRouter(registry_paths=_registry_paths(registry_paths)).load(
            manifest_path,
            metadata_only=dry_run,
        )
        spec = _resolve_single_process(manifest, selector=selector)
        graph = HookGraph.from_config(
            _hooks_config(spec.raw_config),
            manifest_dir=str(manifest_path.parent),
            repo_root=str(Path(repo_root or Path.cwd())),
        )
        selected = _select_steps(graph, phase=phase, hook_id=hook_id)
        if dry_run:
            return HookExecuteReport(
                manifest=str(manifest_path),
                process=spec.name,
                selector=spec.selector,
                phase=phase,
                hook_id=hook_id,
                status="planned",
                steps=tuple(_planned_step(action, phase) for action in selected),
            )

        spec.config.ensure_runtime_bindings()
        runner = HookGraphRunner(
            providers={"sql": SqlHookProvider(_connectors(spec.config))},
        )
        evidence = _run_selected(runner, graph, phase=phase, hook_id=hook_id, process_name=spec.name)
        return HookExecuteReport(
            manifest=str(manifest_path),
            process=spec.name,
            selector=spec.selector,
            phase=phase,
            hook_id=hook_id,
            status="succeeded" if evidence.passed else "failed",
            steps=evidence.steps,
        )


def _hooks_config(raw_config: Mapping[str, Any]) -> object:
    source = raw_config.get("source")
    if not isinstance(source, dict):
        return None
    options = source.get("options")
    if not isinstance(options, dict):
        return None
    return options.get("hooks")


def _registry_paths(raw: Sequence[str | os.PathLike[str]]) -> tuple[Path, ...]:
    paths: list[Path] = []
    seen: set[str] = set()
    for item in raw:
        path = Path(item)
        key = str(path.resolve(strict=False))
        if key in seen:
            continue
        seen.add(key)
        paths.append(path)
    return tuple(paths)


def _resolve_single_process(manifest: Any, *, selector: str | None) -> Any:
    if selector:
        for spec in manifest.processes:
            if spec.selector == selector or spec.name == selector:
                return spec
        available = [process.selector or process.name for process in manifest.processes]
        raise ManifestConfigurationError(f"Selector {selector!r} not found in {manifest.path}. Available: {available}")
    if len(manifest.processes) != 1:
        raise ManifestConfigurationError(
            f"Manifest {manifest.path} contains {len(manifest.processes)} processes. Use --selector."
        )
    return manifest.processes[0]


def _select_steps(graph: HookGraph, *, phase: str, hook_id: str | None):
    ordered = graph.ordered_phase(phase)
    if hook_id is None:
        return ordered
    selected = tuple(action for action in ordered if action.id == hook_id)
    if not selected:
        raise ManifestConfigurationError(f"unknown hook id {hook_id!r} in {phase}")
    return selected


def _planned_step(action: Any, phase: str) -> HookStepEvidence:
    return HookStepEvidence(
        id=action.id,
        phase=phase,
        kind=action.kind,
        type=action.type,
        status="planned",
        result={"sql_hash": action.sql_hash},
        lineage=action.lineage,
    )


def _run_selected(
    runner: HookGraphRunner,
    graph: HookGraph,
    *,
    phase: str,
    hook_id: str | None,
    process_name: str,
) -> HookPhaseEvidence:
    if hook_id is None:
        return runner.run_phase(
            graph,
            phase,
            HookExecutionContext(run_id=process_name, load_id=process_name, process_name=process_name),
        )
    selected = tuple(replace(action, depends_on=()) for action in graph.phase(phase) if action.id == hook_id)
    filtered = HookGraph(
        pre_hook=selected if phase == "pre_hook" else (), post_hook=selected if phase == "post_hook" else ()
    )
    return runner.run_phase(
        filtered,
        phase,
        HookExecutionContext(run_id=process_name, load_id=process_name, process_name=process_name),
    )


def _connectors(config: Any) -> dict[str, Any]:
    source = getattr(config, "source_obj", None)
    sink = getattr(config, "sink_obj", None)
    return {
        "source": getattr(source, "connector", source),
        "sink": getattr(sink, "connector", sink),
    }
