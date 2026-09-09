"""Build visible Airflow KPO tasks from dpone GitOps run-spec steps."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone_airflow_pack.runtime_adapter import DEFAULT_ARTIFACT_DIR, load_dpone_kpo_kwargs

RUN_SPEC_FILE = "run-spec.json"


def build_dpone_gitops_step_tasks_from_artifacts(
    *,
    dag: Any,
    artifact_dir: str | Path = DEFAULT_ARTIFACT_DIR,
    labels: Mapping[str, Any] | None = None,
    annotations: Mapping[str, Any] | None = None,
    operator_overrides: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one visible KubernetesPodOperator per run-spec step."""

    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

    root = Path(artifact_dir)
    steps = _load_run_spec_steps(root)
    tasks: dict[str, Any] = {}
    for step in steps:
        name = str(step.get("name") or "").strip()
        command = str(step.get("command") or "").strip()
        if not name or not command:
            continue
        kwargs = _load_step_kpo_kwargs(
            root,
            step_name=name,
            command=command,
            do_xcom_push=str(step.get("kind") or "") == "dpone_run",
            labels=labels,
            annotations=annotations,
            operator_overrides=operator_overrides,
        )
        tasks[name] = KubernetesPodOperator(dag=dag, **kwargs)
    _wire_step_dependencies(tasks=tasks, steps=steps)
    return tasks


def _load_run_spec_steps(root: Path) -> list[Mapping[str, Any]]:
    payload = json.loads((root / RUN_SPEC_FILE).read_text(encoding="utf-8"))
    steps = payload.get("steps")
    return [step for step in steps if isinstance(step, Mapping)] if isinstance(steps, list) else []


def _load_step_kpo_kwargs(
    root: Path,
    *,
    step_name: str,
    command: str,
    do_xcom_push: bool,
    labels: Mapping[str, Any] | None,
    annotations: Mapping[str, Any] | None,
    operator_overrides: Mapping[str, Any] | None,
) -> dict[str, Any]:
    kwargs = load_dpone_kpo_kwargs(
        root,
        task_id=step_name,
        name=step_name,
        labels=labels,
        annotations=annotations,
        operator_overrides=operator_overrides,
    )
    kwargs["cmds"] = ["/bin/sh", "-ec"]
    kwargs["arguments"] = [command]
    kwargs["do_xcom_push"] = do_xcom_push
    return kwargs


def _wire_step_dependencies(*, tasks: Mapping[str, Any], steps: list[Mapping[str, Any]]) -> None:
    for step in steps:
        task = tasks.get(str(step.get("name") or ""))
        dependencies = step.get("depends_on")
        if task is None or not isinstance(dependencies, list):
            continue
        for dependency in dependencies:
            upstream = tasks.get(str(dependency))
            if upstream is not None:
                _chain(upstream, task)


def _chain(upstream: Any, downstream: Any) -> None:
    upstream >> downstream
