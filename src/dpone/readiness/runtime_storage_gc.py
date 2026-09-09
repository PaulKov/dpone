from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import time
from typing import Any


@dataclass(frozen=True, slots=True)
class StorageGcResult:
    work_dir: str
    dry_run: bool
    candidate_count: int
    deleted_count: int
    candidates: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "runtime.storage_gc",
            "work_dir": self.work_dir,
            "dry_run": self.dry_run,
            "candidate_count": self.candidate_count,
            "deleted_count": self.deleted_count,
            "candidates": list(self.candidates),
        }


class StorageGcService:
    """Safe local cleanup for runtime work/debug files."""

    def collect(self, *, work_dir: str | Path, older_than_seconds: int = 0, dry_run: bool = True) -> StorageGcResult:
        root = Path(work_dir)
        candidates = tuple(self._candidates(root=root, older_than_seconds=max(0, int(older_than_seconds))))
        deleted = 0
        if not dry_run:
            for path in candidates:
                try:
                    path.unlink()
                    deleted += 1
                except OSError:
                    continue
            self._prune_empty_dirs(root)
        return StorageGcResult(
            work_dir=str(root),
            dry_run=dry_run,
            candidate_count=len(candidates),
            deleted_count=deleted,
            candidates=tuple(str(path) for path in candidates),
        )

    def _candidates(self, *, root: Path, older_than_seconds: int) -> tuple[Path, ...]:
        if not root.exists():
            return ()
        threshold = time() - older_than_seconds
        values: list[Path] = []
        for path in root.rglob("*"):
            if path.is_file() and _is_runtime_gc_path(path) and path.stat().st_mtime <= threshold:
                values.append(path)
        return tuple(sorted(values))

    def _prune_empty_dirs(self, root: Path) -> None:
        if not root.exists():
            return
        for path in sorted((item for item in root.rglob("*") if item.is_dir()), reverse=True):
            try:
                path.rmdir()
            except OSError:
                continue


def _is_runtime_gc_path(path: Path) -> bool:
    return bool({"transfer", "leases", "debug"}.intersection(path.parts))


__all__ = ["StorageGcResult", "StorageGcService"]
