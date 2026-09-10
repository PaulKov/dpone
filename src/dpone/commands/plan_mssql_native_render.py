"""Render the bounded native plan without generic snapshot assumptions."""

from __future__ import annotations

from typing import Any


def render_mssql_native(plan: dict[str, Any], *, markdown: bool = False) -> list[str]:
    """Expose authored limits and pending composition in text and Markdown."""
    if not plan:
        return []
    fields = {key: value for key, value in plan.items() if key not in {"limits", "publication_scope"}}
    fields.update(plan["limits"])
    fields["publication_scope"] = ", ".join(f"{key}={value}" for key, value in plan["publication_scope"].items())
    quote = "`" if markdown else ""
    lines = ["", "## Bounded MSSQL native transfer", ""] if markdown else []
    for key, value in fields.items():
        rendered = ", ".join(str(item) for item in value) if isinstance(value, list) else str(value)
        lines.append(f"- {key}: {quote}{rendered}{quote}")
    return lines
