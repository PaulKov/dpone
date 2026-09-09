"""Capability contract for project-scoped authoring serialization."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path

AuthoringLockFactory = Callable[[Path], AbstractContextManager[None]]


class ProjectAuthoringLockError(RuntimeError):
    """The configured project authoring lock could not be acquired safely."""

    code = "DPONE_PROJECT_AUTHORING_LOCK_FAILED"


__all__ = ["AuthoringLockFactory", "ProjectAuthoringLockError"]
