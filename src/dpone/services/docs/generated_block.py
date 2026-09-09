from __future__ import annotations

from pathlib import Path


def replace_generated_block(text: str, block: str, *, start_marker: str, end_marker: str) -> str:
    if start_marker not in text or end_marker not in text:
        raise ValueError(f"Generated block markers not found: {start_marker} / {end_marker}")
    before, rest = text.split(start_marker, 1)
    _, after = rest.split(end_marker, 1)
    return before.rstrip() + "\n\n" + block.rstrip() + "\n" + after.lstrip("\n")


def sync_generated_doc(
    doc_path: Path,
    *,
    rendered_block: str,
    start_marker: str,
    end_marker: str,
) -> tuple[bool, str]:
    original = doc_path.read_text(encoding="utf-8")
    updated = replace_generated_block(
        original,
        rendered_block,
        start_marker=start_marker,
        end_marker=end_marker,
    )
    changed = updated != original
    if changed:
        doc_path.write_text(updated, encoding="utf-8")
    return changed, updated


def is_generated_doc_in_sync(
    doc_path: Path,
    *,
    rendered_block: str,
    start_marker: str,
    end_marker: str,
) -> bool:
    current = doc_path.read_text(encoding="utf-8")
    expected = replace_generated_block(
        current,
        rendered_block,
        start_marker=start_marker,
        end_marker=end_marker,
    )
    return current == expected
