"""Ports used to validate producer evidence before cache-status publication."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class AirflowCacheStatusDocument:
    payload: dict[str, Any]
    sha256: str


class AirflowCacheStatusStorageError(RuntimeError):
    def __init__(
        self,
        code: str,
        *,
        state_may_have_changed: bool = False,
        target_sha256: str | None = None,
    ) -> None:
        self.code = code
        self.state_may_have_changed = state_may_have_changed
        self.target_sha256 = target_sha256
        super().__init__(code)


class AirflowCacheStatusFileTransaction(Protocol):
    def read_source(self, name: str, *, max_bytes: int) -> AirflowCacheStatusDocument: ...

    def require_safe_destination(self, name: str) -> None: ...

    def commit(self, name: str, payload: Mapping[str, object]) -> str: ...

    def write_diagnostic(self, name: str, payload: Mapping[str, object]) -> None: ...

    def remove_and_sync(self, name: str) -> None: ...


class AirflowCacheStatusFileStorage(Protocol):
    def transaction(
        self,
        status_root: Path,
        *,
        lock_timeout_seconds: float,
    ) -> AbstractContextManager[AirflowCacheStatusFileTransaction]: ...


class AirflowCacheStatusSchemaValidator(Protocol):
    """Validate one evidence payload without coupling services to GitOps."""

    def supports(self, schema: str) -> bool: ...

    def has_violations(self, payload: Mapping[str, object], *, expected_schema: str) -> bool: ...


__all__ = [
    "AirflowCacheStatusDocument",
    "AirflowCacheStatusFileStorage",
    "AirflowCacheStatusFileTransaction",
    "AirflowCacheStatusSchemaValidator",
    "AirflowCacheStatusStorageError",
]
