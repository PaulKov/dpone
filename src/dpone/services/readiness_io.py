from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml


def load_manifest(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError("manifest must be a mapping")
    return raw


def load_rows(path: str | None) -> list[dict[str, Any]]:
    if not path:
        return []
    text = Path(path).read_text(encoding="utf-8").strip()
    if not text:
        return []
    if text.startswith("["):
        return [dict(item) for item in json.loads(text)]
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def load_actual_physical(path: str) -> dict[str, Any]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("actual physical design file must contain an object")
    actual = raw.get("actual", raw)
    if not isinstance(actual, dict):
        raise ValueError("actual physical design must be an object")
    return actual
