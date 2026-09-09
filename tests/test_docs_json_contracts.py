"""Ensure every fenced block advertised as JSON is executable JSON."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def _json_blocks(path: Path) -> tuple[tuple[int, str], ...]:
    blocks: list[tuple[int, str]] = []
    current: list[str] | None = None
    start = 0
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if current is None and line == "```json":
            current = []
            start = line_number + 1
        elif current is not None and line == "```":
            blocks.append((start, "\n".join(current)))
            current = None
        elif current is not None:
            current.append(line)
    return tuple(blocks)


def test_all_json_fences_are_valid_json() -> None:
    failures: list[str] = []
    for path in sorted(DOCS.rglob("*.md")):
        for line_number, block in _json_blocks(path):
            try:
                json.loads(block)
            except json.JSONDecodeError as exc:
                relative = path.relative_to(ROOT)
                failures.append(f"{relative}:{line_number + exc.lineno - 1}: {exc.msg}")

    assert failures == []
