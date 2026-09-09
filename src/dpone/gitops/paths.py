from __future__ import annotations

from pathlib import Path

from dpone.gitops.path_policy import GitOpsPathValidationError, safe_relative_path


def confined_repo_path(repo_root: Path, value: str | Path, *, source: str) -> tuple[Path, Path]:
    """Return a safe relative path and its symlink-aware repository destination."""

    relative = safe_relative_path(value, source=source)
    try:
        root = repo_root.resolve(strict=False)
        destination = (root / relative).resolve(strict=False)
    except (OSError, RuntimeError) as exc:
        raise GitOpsPathValidationError(f"{source} could not be resolved safely") from exc
    try:
        destination.relative_to(root)
    except ValueError as exc:
        raise GitOpsPathValidationError(f"{source} must resolve below the repository root") from exc
    return relative, destination


def confined_repo_file_path(repo_root: Path, value: str | Path, *, source: str) -> tuple[Path, Path]:
    """Return a confined file destination whose existing path chain is writable."""

    relative, destination = confined_repo_path(repo_root, value, source=source)
    root = repo_root.resolve(strict=False)
    if destination.exists() and destination.is_dir():
        raise GitOpsPathValidationError(f"{source} must be a file path, not a directory")
    for parent in destination.parents:
        if parent == root:
            break
        if parent.exists() and not parent.is_dir():
            raise GitOpsPathValidationError(f"{source} parent must be a directory: {parent.relative_to(root)}")
    return relative, destination


__all__ = [
    "GitOpsPathValidationError",
    "confined_repo_file_path",
    "confined_repo_path",
    "safe_relative_path",
]


def compact_report_output_is_safe(repo_root: Path, raw_output: object, *, pack_root: Path, cache_root: Path) -> bool:
    """A console mirror cannot overwrite a captured input or immutable cache tree."""
    if not raw_output:
        return True
    try:
        _, destination = confined_repo_file_path(repo_root, str(raw_output), source="--output")
        if destination.exists() and destination.stat().st_nlink > 1:
            return False
        return all(not destination.is_relative_to(root.resolve()) for root in (pack_root, cache_root))
    except (ValueError, OSError, RuntimeError):
        return False
