"""Shared pack-task wiring helpers for compact GitOps packs."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from dpone_airflow_pack.init_fetch_pod_guard import validate_strict_operator_overrides
from dpone_airflow_pack.mapped_tasks import reject_mapping_owned_overrides
from dpone_airflow_pack.node_materialization import PackNodeMaterialization
from dpone_airflow_pack.pack_task_runtime import mapping, sequence

_CONTRACT_OWNED_FIELDS = frozenset(
    {"cmds", "arguments", "image", "namespace", "full_pod_spec", "do_xcom_push", "task_group"}
)


def pod_spec_for_step(pod_spec: Mapping[str, Any], *, step_name: str) -> dict[str, Any]:
    payload = deepcopy(dict(pod_spec))
    metadata = payload.setdefault("metadata", {})
    if isinstance(metadata, dict):
        metadata["name"] = str(metadata.get("name") or "dpone").rstrip("-") + "-" + step_name.replace("_", "-")
    return payload


def safe_overrides(
    overrides: Mapping[str, Any] | None,
    *,
    node_scoped: bool = False,
    mapped: bool = False,
    strict: bool = False,
) -> dict[str, Any]:
    clean = dict(overrides or {})
    if strict:
        return validate_strict_operator_overrides(clean)
    contract_owned = _CONTRACT_OWNED_FIELDS | ({"task_id"} if node_scoped else set())
    forbidden = sorted(contract_owned.intersection(clean))
    if forbidden:
        raise ValueError(f"operator_overrides cannot replace compact pack-owned fields: {', '.join(forbidden)}")
    if mapped:
        reject_mapping_owned_overrides(clean)
    return clean


def inline_required_status(
    *,
    pack: Mapping[str, Any],
    node: PackNodeMaterialization | None,
) -> str | None:
    if node is None or node.visibility != "inline":
        return None
    outcome = mapping(pack.get("outcome_gate"))
    return str(outcome.get("required_status") or "passed")


def pack_steps(pack: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    return tuple(step for step in sequence(pack.get("steps")) if isinstance(step, Mapping))


def chain_pack_steps(pack: Mapping[str, Any], tasks: Mapping[str, Any]) -> None:
    for step in pack_steps(pack):
        step_name = str(step.get("name") or "")
        if step_name not in tasks:
            continue
        for dependency in sequence(step.get("depends_on")):
            dependency_name = str(dependency)
            if dependency_name in tasks:
                chain(tasks[dependency_name], tasks[step_name])


def terminal_hook_task_ids(pack: Mapping[str, Any]) -> tuple[str, ...]:
    hook_steps = tuple(step for step in pack_steps(pack) if step.get("phase") == "pre_hook")
    dependencies = {str(dependency) for step in hook_steps for dependency in sequence(step.get("depends_on"))}
    return tuple(str(step["name"]) for step in hook_steps if str(step.get("name")) not in dependencies)


def chain(upstream: Any, downstream: Any) -> None:
    upstream >> downstream


__all__ = [
    "chain",
    "chain_pack_steps",
    "inline_required_status",
    "pack_steps",
    "pod_spec_for_step",
    "safe_overrides",
    "terminal_hook_task_ids",
]
