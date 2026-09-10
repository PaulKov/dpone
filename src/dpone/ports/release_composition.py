"""Capabilities required to admit and publish a composed immutable release."""

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, Protocol

from dpone.ports.dbt_release_files import VerifiedWorkspaceReleaseCapture


class CompositionNativeSourceReader(VerifiedWorkspaceReleaseCapture, Protocol):
    """Return complete native source observations after confined verification."""

    def read(self, compiled_root: Path, *, expected_release_id: str) -> Any: ...


class CompositionIntegrity(Protocol):
    """Produce and independently verify bounded exact tree subjects."""

    def write(self, compiled_root: Path) -> object: ...

    def verify(self, compiled_root: Path) -> object: ...

    def require_capture_budget(self, sizes: Iterable[int]) -> None: ...


class CompositionPublisher(Protocol):
    """Atomically create-or-compare; preserve visible durability uncertainty."""

    def __call__(self, destination: Path, files: Mapping[str, bytes]) -> str: ...
