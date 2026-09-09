from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


class SparsePathValidationError(ValueError):
    """Raised when a manifest sparse path is unsafe for repo checkout."""


@dataclass(frozen=True, slots=True)
class ResolvedSparsePath:
    path: str
    exists: bool
    is_dir: bool


class SparsePathPolicy:
    """Path safety and repo-relative rendering for sparse checkout entries."""

    def __init__(self, *, repo_root: Path, workload_root: Path) -> None:
        self.repo_root = repo_root.resolve(strict=False)
        self.workload_root = workload_root.resolve(strict=False)
        self._ensure_within(self.workload_root, self.repo_root, source="workload_root")

    def resolve_user_path(self, raw: object, *, source: str, base_dir: Path | None = None) -> Path:
        text = self._validate_relative_text(raw, source=source)
        anchor = (base_dir or self.repo_root).resolve(strict=False)
        resolved = (anchor / text).resolve(strict=False)
        self._ensure_within(resolved, self.repo_root, source=source)
        self._ensure_within(resolved, self.workload_root, source=source)
        return resolved

    def entry_for_user_path(
        self,
        raw: object,
        *,
        kind: str,
        source: str,
        required: bool,
        reason: str,
        base_dir: Path | None = None,
        is_dir: bool | None = None,
    ) -> ResolvedSparsePath:
        path = self.resolve_user_path(raw, source=source, base_dir=base_dir)
        declared_dir = bool(is_dir) if is_dir is not None else str(raw).strip().endswith(("/", "\\"))
        return self.entry_for_resolved_path(
            path,
            kind=kind,
            source=source,
            required=required,
            reason=reason,
            is_dir=declared_dir or None,
        )

    def entry_for_resolved_path(
        self,
        path: Path,
        *,
        kind: str,
        source: str,
        required: bool,
        reason: str,
        is_dir: bool | None = None,
    ) -> ResolvedSparsePath:
        _ = (kind, required, reason)
        resolved = path.resolve(strict=False)
        self._ensure_within(resolved, self.repo_root, source=source)
        self._ensure_within(resolved, self.workload_root, source=source)
        exists = resolved.exists()
        directory = bool(is_dir) if is_dir is not None else (resolved.is_dir() if exists else False)
        return ResolvedSparsePath(
            path=self.repo_relative(resolved, is_dir=directory),
            exists=exists,
            is_dir=directory,
        )

    def repo_relative(self, path: Path, *, is_dir: bool = False) -> str:
        rel = path.resolve(strict=False).relative_to(self.repo_root).as_posix()
        if is_dir and rel != "." and not rel.endswith("/"):
            return f"{rel}/"
        return rel

    @staticmethod
    def _validate_relative_text(raw: object, *, source: str) -> str:
        text = str(raw or "").strip()
        if not text:
            raise SparsePathValidationError(f"{source} must not be empty")
        candidate = Path(text)
        if candidate.is_absolute():
            raise SparsePathValidationError(f"{source} must be relative, got absolute path: {text}")
        parts = candidate.parts
        if any(part == ".." for part in parts):
            raise SparsePathValidationError(f"{source} must not contain '..': {text}")
        if any(not str(part).strip() for part in parts):
            raise SparsePathValidationError(f"{source} contains an empty path segment: {text}")
        return text

    @staticmethod
    def _ensure_within(path: Path, root: Path, *, source: str) -> None:
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise SparsePathValidationError(f"{source} must stay under the configured workload root") from exc


__all__ = ["ResolvedSparsePath", "SparsePathPolicy", "SparsePathValidationError"]
