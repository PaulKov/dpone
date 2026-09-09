"""Feature parity Markdown sections."""

from __future__ import annotations

from typing import Any

from tools.oss_benchmark.payload_utils import format_int as _format_int


def render_feature_parity_section(matrix: dict[str, Any]) -> str:
    if not matrix:
        return ""
    tools = list(matrix.get("tools") or [])
    dimensions = list(matrix.get("dimensions") or [])
    summary = matrix.get("summary") or {}
    entries = _feature_entries_by_key(matrix)
    lines = [
        "",
        "## Feature Parity Matrix",
        "",
        "This matrix compares public product-surface capabilities. It is evidence-bounded: closed-core tools are included as feature comparators, while source-code quality metrics remain limited to code-comparable repositories.",
        "",
        "![Feature parity coverage](assets/oss-feature-parity.svg)",
        "",
        "| Tool | Feature score | Band | Code comparable? | Comparator note |",
        "|---|---:|---|---|---|",
    ]
    for tool in tools:
        slug = tool.get("slug", "")
        item = summary.get(slug, {})
        lines.append(
            "| "
            f"{tool.get('name', slug)} | "
            f"{_format_int(item.get('score'))} | "
            f"{item.get('band', 'n/a')} | "
            f"{'yes' if item.get('code_comparable') else 'no'} | "
            f"{item.get('comparator_note', tool.get('comparator_note', 'n/a'))} |"
        )
    lines.extend(["", "### Capability coverage", ""])
    lines.append("| Capability | " + " | ".join(tool.get("name", tool.get("slug", "")) for tool in tools) + " |")
    lines.append("|---|" + "|".join("---" for _ in tools) + "|")
    for dimension in dimensions:
        row = [str(dimension.get("label", dimension.get("slug", "")))]
        for tool in tools:
            row.append(_feature_badge(entries.get((tool.get("slug", ""), dimension.get("slug", "")), {})))
        lines.append("| " + " | ".join(row) + " |")
    lines.extend(["", "### Feature evidence", ""])
    for tool in tools:
        slug = tool.get("slug", "")
        evidence = "; ".join(
            _feature_evidence(entry, idx) for idx, entry in enumerate(_top_feature_entries(matrix, slug))
        )
        lines.append(f"- **{tool.get('name', slug)}:** {evidence}.")
    lines.extend(["", "### Feature source index", ""])
    for source in _feature_source_index(matrix):
        lines.append(f"- [{source}]({_feature_source_href(source)})")
    lines.extend(
        ["", "Feature parity sources are stored in raw evidence under `feature_parity.entries[].sources`.", ""]
    )
    return "\n".join(lines)


def _feature_entries_by_key(matrix: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    return {(str(entry.get("tool", "")), str(entry.get("dimension", ""))): entry for entry in matrix.get("entries", [])}


def _feature_badge(rating: dict[str, Any]) -> str:
    level = str(rating.get("level", "n/a"))
    label = {
        "native": "native",
        "managed": "managed",
        "strong": "strong",
        "supported": "supported",
        "partial": "partial",
        "external": "external",
        "not_detected": "not detected",
    }.get(level, level)
    return f"`{label}`"


def _feature_evidence(entry: dict[str, Any], index: int) -> str:
    source = (entry.get("sources") or [""])[0]
    return (
        f"{entry.get('dimension', 'capability')} `{entry.get('level', 'n/a')}` "
        f"([{index + 1}]({_feature_source_href(source)}))"
    )


def _top_feature_entries(matrix: dict[str, Any], tool_slug: str) -> list[dict[str, Any]]:
    levels = matrix.get("levels") or {}
    entries = [entry for entry in matrix.get("entries", []) if entry.get("tool") == tool_slug]
    return sorted(entries, key=lambda entry: (-int(levels.get(entry.get("level"), 0)), entry.get("dimension", "")))[:3]


def _feature_source_index(matrix: dict[str, Any]) -> list[str]:
    sources: list[str] = []
    for entry in matrix.get("entries", []):
        for source in entry.get("sources", []):
            if source not in sources:
                sources.append(source)
    priority = (
        "docs/cdc.md",
        "docs/certification-suite.md",
        "https://docs.airbyte.com/platform/understanding-airbyte/cdc",
        "https://dlthub.com/docs/general-usage/schema-evolution",
        "https://docs.pentaho.com/pdia-data-integration/pipeline-designer/working-with-transformations",
        "https://hop.apache.org/",
        "https://fivetran.com/docs/core-concepts/features",
        "https://www.informatica.com/products/data-governance/cloud-data-governance-and-catalog.html",
    )
    ordered = [source for source in priority if source in sources]
    ordered.extend(source for source in sources if source not in ordered)
    return ordered[:12]


def _feature_source_href(source: str) -> str:
    if source.startswith("docs/"):
        return f"../{source.removeprefix('docs/')}"
    return source
