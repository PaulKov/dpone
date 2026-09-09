from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml

from dpone.output import dumps_yaml
from dpone.output_table import render_table

HR = "-" * 80
HR2 = "=" * 80


def compact_yaml(value: Any) -> str:
    """Make values diff-friendly in a single line.

    Used by "end-to-end" explain commands where we want to show the exact
    depends_on item but keep output compact.
    """

    if value is None:
        return "null"
    if isinstance(value, str | int | float | bool):
        return json.dumps(value, ensure_ascii=False)

    s = yaml.safe_dump(value, sort_keys=True, allow_unicode=True).strip()
    # Collapse to one line
    return " ".join(line.strip() for line in s.splitlines())


def _node_line(prefix: str, key: str, value: Any) -> str:
    return f"{prefix}{key}: {value}" if value is not None and value != "" else ""


def render_dag_banner(*, root: Path, base_path: Path, task_count: int, extra: Sequence[str] | None = None) -> str:
    lines = [
        f"DAG root: {root}",
        f"Base dir: {base_path}",
        f"Tasks:    {task_count}",
    ]
    if extra:
        for x in extra:
            if x:
                lines.append(str(x))
    return "\n".join(lines)


def render_node_block(title: str, node: Mapping[str, Any]) -> str:
    """Render a node dict as printed by explain_* helpers."""

    lines: list[str] = [f"\n{title}:"]
    lines.append(f"  name:      {node.get('name')}")
    lines.append(f"  ref:       {node.get('ref')}")

    if node.get("task_group"):
        lines.append(f"  task_group:{node.get('task_group')}")
    if node.get("source"):
        lines.append(f"  source:    {node.get('source')}")
    if node.get("sink"):
        lines.append(f"  sink:      {node.get('sink')}")

    return "\n".join(lines)


def render_task_block(title: str, task: Mapping[str, Any]) -> str:
    # alias for older naming
    return render_node_block(title, task)


def render_warnings(warnings: Iterable[str]) -> str:
    out = []
    for w in warnings or ():
        out.append(f"WARN: {w}")
    return "\n".join(out)


# ---------------- reasons ----------------


def _reason_kind(reason: Any) -> str:
    if isinstance(reason, dict):
        return str(reason.get("kind") or "")
    return str(getattr(reason, "kind", ""))


def _reason_description(reason: Any) -> str:
    if isinstance(reason, dict):
        return str(reason.get("description") or "")
    return str(getattr(reason, "description", ""))


def _reason_evidence(reason: Any) -> dict:
    if isinstance(reason, dict):
        ev = reason.get("evidence")
        return ev if isinstance(ev, dict) else {}
    ev = getattr(reason, "evidence", None)
    return ev if isinstance(ev, dict) else {}


def render_reasons_section(
    reasons: Sequence[Any],
    *,
    title: str = "Reasons",
    include_evidence: bool = True,
) -> str:
    if not reasons:
        return ""

    lines: list[str] = [f"\n{title}:"]
    rows = [[_reason_kind(r), _reason_description(r)] for r in reasons]
    lines.append(render_table(["kind", "description"], rows))

    if include_evidence:
        for r in reasons:
            ev = _reason_evidence(r)
            if not ev:
                continue
            kind = _reason_kind(r)
            lines.append(f"\n[{kind}] evidence:")
            lines.append(dumps_yaml(ev, sort_keys=True).rstrip())

    return "\n".join(lines)


def render_reasons_compact(reasons: Sequence[Any]) -> str:
    if not reasons:
        return "(no attributed reasons)"
    lines = [f"- {_reason_kind(r)}: {_reason_description(r)}" for r in reasons]
    return "\n".join(lines)


def render_table_section(title: str, headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    lines: list[str] = [f"\n{title}:"]
    lines.append(render_table(list(headers), [list(r) for r in rows]))
    return "\n".join(lines)


def render_yaml_block(title: str, obj: Any) -> str:
    lines: list[str] = [f"\n{title}:"]
    lines.append(dumps_yaml(obj, sort_keys=True).rstrip())
    return "\n".join(lines)
