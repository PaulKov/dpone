"""Shared nested path normalization helpers."""

from __future__ import annotations


def normalize_path(path: str) -> str:
    """Normalize a user path or a tiny JSONPath subset to a dot path."""

    cleaned = str(path or "").strip()
    if cleaned.startswith("$."):
        cleaned = cleaned[2:]
    elif cleaned == "$":
        cleaned = ""
    cleaned = cleaned.replace("[*]", "").replace("[]", "")
    while ".." in cleaned:
        cleaned = cleaned.replace("..", ".")
    return cleaned.strip(".")
