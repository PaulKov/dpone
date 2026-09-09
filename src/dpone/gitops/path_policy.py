from __future__ import annotations

from pathlib import Path


class GitOpsPathValidationError(ValueError):
    """Raised when a GitOps path is unsafe under the repository root."""


def safe_relative_path(raw: object, *, source: str) -> Path:
    text = str(raw or "").strip()
    if text == ".":
        return Path(".")
    if not text:
        raise GitOpsPathValidationError(f"{source} must not be empty")
    candidate = Path(text.rstrip("/\\"))
    if candidate.is_absolute():
        raise GitOpsPathValidationError(f"{source} must be relative")
    if any(part == ".." for part in candidate.parts):
        raise GitOpsPathValidationError(f"{source} must not contain '..'")
    return candidate


__all__ = ["GitOpsPathValidationError", "safe_relative_path"]
