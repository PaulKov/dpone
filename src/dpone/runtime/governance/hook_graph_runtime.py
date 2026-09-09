"""Load-config helpers and Airflow hook-graph runtime adjustments."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import replace
from typing import Any

from dpone.governance.hooks import HookGraph


def hooks_config(load_config: Any) -> object:
    options = getattr(load_config, "options", None)
    if not isinstance(options, Mapping):
        return None
    return options.get("hooks")


def manifest_dir(load_config: Any) -> str | None:
    options = getattr(load_config, "options", None)
    if not isinstance(options, Mapping):
        return None
    raw = options.get("manifest_dir")
    return str(raw) if raw else None


def repo_root(load_config: Any) -> str | None:
    options = getattr(load_config, "options", None)
    if not isinstance(options, Mapping):
        return None
    raw = options.get("repo_root")
    return str(raw) if raw else None


def quality_config(load_config: Any) -> object:
    options = getattr(load_config, "options", None)
    if not isinstance(options, Mapping):
        return None
    return options.get("quality")


def connectors(*, source: Any, sink: Any) -> dict[str, Any]:
    return {
        "source": getattr(source, "connector", source),
        "sink": getattr(sink, "connector", sink),
    }


def filter_airflow_separate_hooks(graph: HookGraph, *, phase: str) -> HookGraph:
    if phase != "pre_hook" or os.getenv("DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS") != "1":
        return graph
    skipped = {action.id for action in graph.pre_hook if action.execution.airflow == "separate_task"}
    if not skipped:
        return graph
    remaining = tuple(
        replace(action, depends_on=tuple(dependency for dependency in action.depends_on if dependency not in skipped))
        for action in graph.pre_hook
        if action.id not in skipped
    )
    return HookGraph(pre_hook=remaining, post_hook=graph.post_hook)
