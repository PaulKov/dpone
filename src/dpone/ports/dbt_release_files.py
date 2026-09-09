"""Bounded acquisition capability shared by release, evidence and mirror readers."""

from collections.abc import Mapping
from pathlib import Path
from typing import Protocol


class ConfinedReleaseFileReader(Protocol):
    """Read one bounded no-follow file relative to an explicit release root."""

    def __call__(self, root: Path, relative_path: str, *, max_bytes: int) -> bytes: ...


class VerifiedWorkspaceReleaseCapture(Protocol):
    """Capture complete verified bytes bound to the caller's authorized release.

    Implementations must preserve exact release bytes (including provenance),
    reject incomplete/unbounded sources and finish cleanup before returning.
    This is offline source verification, never permission to activate or write SQL.
    """

    def capture_verified_files(
        self, root: Path, *, release_payload: bytes, expected_release_id: str
    ) -> Mapping[str, bytes]: ...
