"""Backward-compatible v1 module-size baseline Python API."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ModuleSizeBaselineEntry:
    path: str
    lines: int
    owner: str
    reason: str
    target: str


def load_module_size_baseline(path: Path) -> tuple[ModuleSizeBaselineEntry, ...]:
    """Load the historical permissive v1 baseline API without v2 policy semantics."""

    if not path.exists():
        return ()
    payload = json.loads(path.read_text(encoding="utf-8"))
    entries = payload.get("entries", []) if isinstance(payload, dict) else []
    if not isinstance(entries, list):
        raise ValueError(f"Module size baseline entries must be a list: {path}")
    return tuple(
        ModuleSizeBaselineEntry(
            path=str(entry["path"]),
            lines=int(entry["lines"]),
            owner=str(entry["owner"]),
            reason=str(entry["reason"]),
            target=str(entry["target"]),
        )
        for entry in entries
    )


def write_module_size_baseline(path: Path, entries: tuple[ModuleSizeBaselineEntry, ...]) -> None:
    """Write the historical deterministic v1 JSON representation."""

    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "entries": [
            {
                "path": entry.path,
                "lines": entry.lines,
                "owner": entry.owner,
                "reason": entry.reason,
                "target": entry.target,
            }
            for entry in entries
        ],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


__all__ = ["ModuleSizeBaselineEntry", "load_module_size_baseline", "write_module_size_baseline"]
