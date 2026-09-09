"""Rendering helpers for route conformance reports."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path


def json_text(payload: Mapping[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def write_json(path: str | Path, payload: Mapping[str, object]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json_text(payload), encoding="utf-8")


def write_report(
    output_dir: str | Path,
    json_path: str | Path,
    markdown_path: str | Path,
    payload: Mapping[str, object],
    markdown: str,
) -> None:
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    Path(json_path).write_text(json_text(payload), encoding="utf-8")
    Path(markdown_path).write_text(markdown, encoding="utf-8")


def report_markdown(
    *,
    title: str,
    identity: str,
    passed: bool,
    status: str,
    score: float,
    rows: Sequence[str],
    blockers: Sequence[str],
    warnings: Sequence[str],
    next_actions: Sequence[str],
) -> str:
    lines = [
        f"# {title}",
        "",
        f"- Identity: `{identity}`",
        f"- Passed: `{passed}`",
        f"- Status: `{status}`",
        f"- Score: `{score}`",
        "",
        "## Evidence",
        "",
    ]
    lines.extend(rows or ["- no evidence rows"])
    lines.extend(["", "## Blockers", ""])
    lines.extend(f"- `{item}`" for item in blockers) if blockers else lines.append("- none")
    lines.extend(["", "## Warnings", ""])
    lines.extend(f"- `{item}`" for item in warnings) if warnings else lines.append("- none")
    lines.extend(["", "## Runbook", ""])
    lines.extend(f"- {item}" for item in next_actions) if next_actions else lines.append(
        "- Keep the evidence immutable."
    )
    lines.append("")
    return "\n".join(lines)
