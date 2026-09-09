"""Rendering helpers for schema planning CLI commands."""

from __future__ import annotations

import json
from collections.abc import Iterable


def render_infer_text(payload: dict) -> str:
    lines = ["dpone schema infer", f"- enforcement: {payload.get('schema_contract', {}).get('enforcement')}"]
    columns = payload.get("columns", {})
    if isinstance(columns, dict):
        for name, column in columns.items():
            if isinstance(column, dict):
                lines.append(
                    f"- {name}: {column.get('logical_type')} confidence={column.get('confidence')} "
                    f"source={column.get('decision_source')}"
                )
    return "\n".join(lines) + "\n"


def render_infer_md(payload: dict) -> str:
    return f"# dpone schema infer\n\n```json\n{json.dumps(payload, indent=2, ensure_ascii=False)}\n```\n"


def render_physical_text(payload: dict) -> str:
    return "dpone schema physical-plan\n" + "\n".join(str(item) for item in payload.get("ddl", [])) + "\n"


def render_physical_md(payload: dict) -> str:
    return (
        "# dpone schema physical-plan\n\n```sql\n" + "\n".join(str(item) for item in payload.get("ddl", [])) + "\n```\n"
    )


def render_physical_diff_text(payload: dict) -> str:
    lines = [
        "dpone schema physical-diff",
        f"- table: {payload.get('table')}",
        f"- mode: {payload.get('mode')}",
        f"- has_drift: {payload.get('has_drift')}",
    ]
    _append_issue_lines(lines, "blockers", payload.get("blockers", []))
    _append_issue_lines(lines, "warnings", payload.get("warnings", []))
    ddl = payload.get("ddl", [])
    if ddl:
        lines.append("- ddl:")
        lines.extend(f"  {item}" for item in ddl)
    return "\n".join(lines) + "\n"


def render_physical_diff_md(payload: dict) -> str:
    return f"# dpone schema physical-diff\n\n```json\n{json.dumps(payload, indent=2, ensure_ascii=False)}\n```\n"


def render_schema_explain_text(payload: dict) -> str:
    lines = [
        f"dpone schema explain: {payload.get('source_system')} -> {payload.get('sink_system')}",
        f"- breaking_changes: {payload.get('has_breaking_changes')}",
    ]
    for entry in payload.get("type_decisions", []):
        if isinstance(entry, dict):
            lines.append(
                f"- {entry.get('column')}: {entry.get('source_type')} -> {entry.get('target_type')} "
                f"expected={entry.get('matrix_expected_target_type')} "
                f"category={entry.get('matrix_decision_category')} "
                f"source={entry.get('matrix_decision_source')} "
                f"matrix_match={entry.get('matrix_matches_target')}"
            )
    return "\n".join(lines) + "\n"


def render_schema_explain_md(payload: dict) -> str:
    lines = [
        f"# dpone schema explain: {payload.get('source_system')} -> {payload.get('sink_system')}",
        "",
        f"- Breaking changes: `{payload.get('has_breaking_changes')}`",
        "",
        "| Column | Source type | Target type | Expected target | Category | Decision source | Matrix match | Diagnostic |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for entry in payload.get("type_decisions", []):
        if isinstance(entry, dict):
            lines.append(
                f"| `{entry.get('column')}` | `{entry.get('source_type')}` | `{entry.get('target_type')}` | "
                f"`{entry.get('matrix_expected_target_type')}` | `{entry.get('matrix_decision_category')}` | "
                f"`{entry.get('matrix_decision_source')}` | `{entry.get('matrix_matches_target')}` | "
                f"{entry.get('diagnostic')} |"
            )
    return "\n".join(lines) + "\n"


def render_type_matrix_text(payload: dict) -> str:
    lines = [
        f"dpone schema type-matrix: {payload.get('source')} -> {payload.get('sink')}",
        f"- profile: {payload.get('profile')}",
    ]
    for entry in payload.get("entries", []):
        if isinstance(entry, dict):
            label = (
                f"{entry.get('column')}:{entry.get('source_type')}" if entry.get("column") else entry.get("source_type")
            )
            lines.append(
                f"- {label} -> {entry.get('target_type')} "
                f"transport={entry.get('native_transport')} compatible={entry.get('schema_evolution_compatible')} "
                f"category={entry.get('decision_category')} source={entry.get('decision_source')}"
            )
    return "\n".join(lines) + "\n"


def render_type_matrix_md(payload: dict) -> str:
    lines = [
        f"# dpone schema type-matrix: {payload.get('source')} -> {payload.get('sink')}",
        "",
        f"- Profile: `{payload.get('profile')}`",
        f"- Runbook: `{payload.get('runbook')}`",
        "",
        "| Column | Source type | Canonical type | Target type | Native transport | Decision | Source | Schema evolution compatible | Notes |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for entry in payload.get("entries", []):
        if isinstance(entry, dict):
            notes = entry.get("warning") or entry.get("reason") or ""
            lines.append(
                f"| `{entry.get('column') or ''}` | `{entry.get('source_type')}` | `{entry.get('canonical_type')}` | "
                f"`{entry.get('target_type')}` | `{entry.get('native_transport')}` | "
                f"`{entry.get('decision_category')}` | `{entry.get('decision_source')}` | "
                f"`{entry.get('schema_evolution_compatible')}` | {notes} |"
            )
    return "\n".join(lines) + "\n"


def _append_issue_lines(lines: list[str], label: str, values: object) -> None:
    if not values:
        return
    lines.append(f"- {label}:")
    if isinstance(values, Iterable) and not isinstance(values, str | bytes):
        lines.extend(f"  - {item}" for item in values)
    else:
        lines.append(f"  - {values}")


__all__ = [
    "render_infer_md",
    "render_infer_text",
    "render_physical_diff_md",
    "render_physical_diff_text",
    "render_physical_md",
    "render_physical_text",
    "render_schema_explain_md",
    "render_schema_explain_text",
    "render_type_matrix_md",
    "render_type_matrix_text",
]
