"""Evidence artifact writing for nested normalization certification."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class NestedEvidenceArtifact:
    json_path: Path
    markdown_path: Path


def write_nested_artifact(output_dir: Path, stem: str, payload: dict[str, Any]) -> NestedEvidenceArtifact:
    """Write deterministic JSON and Markdown evidence for one nested check."""

    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
    markdown_path.write_text(_markdown(stem, payload), encoding="utf-8")
    return NestedEvidenceArtifact(json_path=json_path, markdown_path=markdown_path)


def _markdown(title: str, payload: dict[str, Any]) -> str:
    lines = [f"# {title.replace('_', ' ').title()}", "", f"Status: **{payload.get('status')}**", ""]
    checks = payload.get("checks")
    if isinstance(checks, dict):
        lines.extend(["| Check | Result |", "|---|---|"])
        lines.extend(f"| `{name}` | {result} |" for name, result in checks.items())
        lines.append("")
    lines.append("```json")
    lines.append(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    lines.append("```")
    return "\n".join(lines) + "\n"


__all__ = ["NestedEvidenceArtifact", "write_nested_artifact"]
