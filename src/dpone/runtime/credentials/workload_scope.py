"""Workload-lifetime credential resolution scope."""

from __future__ import annotations

from threading import RLock
from typing import Any, Protocol


class CredentialResolver(Protocol):
    """Resolve one logical connection reference."""

    def resolve(self, connection_ref: str) -> Any:
        """Return one runtime connection resolution."""


class WorkloadScopedCredentialResolver:
    """Resolve each connection ref once and retain it only for one workload."""

    def __init__(self, delegate: CredentialResolver) -> None:
        self._delegate = delegate
        self._resolved: dict[str, Any] = {}
        self._lock = RLock()

    def resolve(self, connection_ref: str) -> Any:
        normalized = str(connection_ref or "").strip()
        if not normalized:
            raise ValueError("connection_ref is required")
        with self._lock:
            if normalized in self._resolved:
                return self._resolved[normalized]
            resolved = self._delegate.resolve(normalized)
            self._resolved[normalized] = resolved
            return resolved


__all__ = ["CredentialResolver", "WorkloadScopedCredentialResolver"]
