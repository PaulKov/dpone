"""Content lifecycle for one journaled, bot-owned dbt audit mirror subtree."""

from pathlib import Path
from typing import Protocol


class DbtProdMirrorContent(Protocol):
    """Stage and verify a complete subtree, never mutate an active destination."""

    def stage(self, destination: Path) -> None:
        """Create and verify the whole content in an absent staging destination."""
        ...

    def verify(self, destination: Path) -> None:
        """Reject incomplete, unexpected or changed content."""
        ...

    def matches(self, destination: Path) -> bool:
        """Return false for absent/drifted content; reject an unsafe root."""
        ...


class DbtProdMirrorOwnership(Protocol):
    """Authorize already-confined destinations while the transaction lock is held."""

    def require_owned_or_absent(
        self, *, repository_root: Path, mirror: Path, snapshot: Path, descriptor: Path
    ) -> None: ...


__all__ = ["DbtProdMirrorContent", "DbtProdMirrorOwnership"]
