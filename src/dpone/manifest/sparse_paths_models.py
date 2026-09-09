from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class SparsePathEntry:
    path: str
    kind: str
    source: str
    required: bool
    exists: bool
    is_dir: bool
    reason: str

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "kind": self.kind,
            "source": self.source,
            "required": self.required,
            "exists": self.exists,
            "is_dir": self.is_dir,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class SparsePathIssue:
    code: str
    message: str
    path: str
    source: str

    def to_jsonable(self) -> dict[str, str]:
        return {
            "code": self.code,
            "message": self.message,
            "path": self.path,
            "source": self.source,
        }


@dataclass(frozen=True, slots=True)
class SparsePathReport:
    manifest: str
    workload_root: str
    entries: tuple[SparsePathEntry, ...]
    warnings: tuple[SparsePathIssue, ...] = ()
    blockers: tuple[SparsePathIssue, ...] = ()
    kind: str = "manifest.sparse_paths"

    @property
    def passed(self) -> bool:
        return not self.blockers

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "manifest": self.manifest,
            "workload_root": self.workload_root,
            "entries": [entry.to_jsonable() for entry in self.entries],
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


class SparsePathReportBuilder:
    def __init__(self, *, manifest: str, workload_root: str) -> None:
        self.manifest = manifest
        self.workload_root = workload_root
        self._entries: list[SparsePathEntry] = []
        self._entry_paths: set[str] = set()
        self._warnings: list[SparsePathIssue] = []
        self._blockers: list[SparsePathIssue] = []

    def add_entry(self, entry: SparsePathEntry) -> None:
        if entry.path in self._entry_paths:
            return
        self._entry_paths.add(entry.path)
        self._entries.append(entry)

    def add_warning(self, *, code: str, message: str, path: str | Path, source: str) -> None:
        self._warnings.append(SparsePathIssue(code=code, message=message, path=str(path), source=source))

    def add_blocker(self, *, code: str, message: str, path: str | Path, source: str) -> None:
        self._blockers.append(SparsePathIssue(code=code, message=message, path=str(path), source=source))

    def to_report(self) -> SparsePathReport:
        return SparsePathReport(
            manifest=self.manifest,
            workload_root=self.workload_root,
            entries=tuple(self._entries),
            warnings=tuple(self._warnings),
            blockers=tuple(self._blockers),
        )


__all__ = [
    "SparsePathEntry",
    "SparsePathIssue",
    "SparsePathReport",
    "SparsePathReportBuilder",
]
