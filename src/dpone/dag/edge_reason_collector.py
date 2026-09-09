"""Reason collection helpers for DAG direct-edge explanations."""

from __future__ import annotations

from typing import Any

from dpone.dag.edge_resolver import build_dependency_indexes, resolve_declared_dependency
from dpone.dag.yaml_types import ProcessNode


def collect_edge_reasons(
    ctx: Any,
    *,
    upstream: ProcessNode,
    downstream: ProcessNode,
    max_triggers: int,
    reason_cls: type[Any],
) -> list[Any]:
    reasons: list[Any] = []

    indexes = ctx.indexes or build_dependency_indexes(ctx.nodes)

    # 1) group-to-group expansion: a trigger in any task of downstream.task_group
    if upstream.task_group and downstream.task_group:
        key = (str(upstream.task_group), str(downstream.task_group))
        triggers = ctx.group_triggers.get(key) or []
        if triggers:
            reasons.append(
                reason_cls(
                    kind="group_to_group",
                    description=(
                        f"Group '{key[1]}' depends on group '{key[0]}', so every task in '{key[0]}' is upstream of every task in '{key[1]}'"
                    ),
                    evidence={
                        "upstream_group": key[0],
                        "downstream_group": key[1],
                        "trigger_count": len(triggers),
                        "triggers": [t.to_jsonable() for t in list(triggers)[:max_triggers]],
                        "triggers_truncated": len(triggers) > max_triggers,
                    },
                )
            )

    # 2) direct dependency declarations on the downstream task itself
    for dep_index, dep in enumerate(downstream.dependencies or []):
        resolution = resolve_declared_dependency(downstream, dep, dep_index=dep_index, indexes=indexes)
        if not resolution.matches_edge(upstream_name=upstream.name, downstream_name=downstream.name):
            continue

        if resolution.kind == "group_to_task":
            reasons.append(
                reason_cls(
                    kind="group_to_task",
                    description=(
                        f"Task '{downstream.name}' depends on group '{resolution.group}', so it depends on every task in that group"
                    ),
                    evidence={
                        "depends_on_index": dep_index,
                        "group": resolution.group,
                        "alias": resolution.alias,
                    },
                )
            )
        elif resolution.kind == "depends_on_task_id":
            reasons.append(
                reason_cls(
                    kind="depends_on_task_id",
                    description=f"Task '{downstream.name}' directly depends on task_id '{upstream.name}'",
                    evidence={"depends_on_index": dep_index, "task_id": upstream.name},
                )
            )
        elif resolution.kind == "depends_on_group_string":
            reasons.append(
                reason_cls(
                    kind="depends_on_group_string",
                    description=(
                        f"Task '{downstream.name}' depends on group '{resolution.detail.get('group')}' referenced as a string in depends_on.path"
                    ),
                    evidence={
                        "depends_on_index": dep_index,
                        "group": resolution.detail.get("group"),
                        "match_count": resolution.detail.get("match_count"),
                    },
                )
            )
        elif resolution.kind == "depends_on_selector":
            reasons.append(
                reason_cls(
                    kind="depends_on_selector",
                    description=(
                        f"Task '{downstream.name}' depends on selector '{resolution.detail.get('selector')}' in file '{resolution.detail.get('file')}'"
                    ),
                    evidence={
                        "depends_on_index": dep_index,
                        "file": resolution.detail.get("file"),
                        "selector": resolution.detail.get("selector"),
                        "match_count": resolution.detail.get("match_count"),
                    },
                )
            )
        elif resolution.kind == "depends_on_file_all":
            reasons.append(
                reason_cls(
                    kind="depends_on_file_all",
                    description=(
                        f"Task '{downstream.name}' depends on file '{resolution.detail.get('file')}' without selector, so it waits for all tasks from that file"
                    ),
                    evidence={
                        "depends_on_index": dep_index,
                        "file": resolution.detail.get("file"),
                        "match_count": resolution.detail.get("match_count"),
                    },
                )
            )
        elif resolution.kind == "depends_on_stem_fallback":
            reasons.append(
                reason_cls(
                    kind="depends_on_stem_fallback",
                    description=(
                        f"Task '{downstream.name}' resolved depends_on.path '{resolution.raw_path}' by legacy stem fallback '{resolution.detail.get('stem')}'"
                    ),
                    evidence={
                        "depends_on_index": dep_index,
                        "stem": resolution.detail.get("stem"),
                        "match_count": resolution.detail.get("match_count"),
                    },
                )
            )
        elif resolution.kind == "unresolved":
            # Unresolved declarations do not create edges; should never reach here.
            continue
        else:
            reasons.append(
                reason_cls(
                    kind=resolution.kind or "depends_on_other",
                    description=(
                        f"Task '{downstream.name}' depends on '{resolution.raw_path}' which matched upstream '{upstream.name}'"
                    ),
                    evidence={"depends_on_index": dep_index, **dict(resolution.detail)},
                )
            )

    return _dedupe_reasons(reasons)


def _dedupe_reasons(reasons: list[Any]) -> list[Any]:
    seen: set[tuple[str, str]] = set()
    out: list[Any] = []
    for reason in reasons:
        key = (reason.kind, reason.description)
        if key in seen:
            continue
        seen.add(key)
        out.append(reason)
    return out


__all__ = ["collect_edge_reasons"]
