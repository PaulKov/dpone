"""Artifact identity verifier for external ClickHouse publication."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


class ClickHouseExternalArtifactVerifier:
    """Delegate exact artifact revalidation without exposing storage details."""

    def __init__(self, *, revalidate: Callable[[Any], None]) -> None:
        self._revalidate = revalidate

    def revalidate(self, artifact: Any) -> None:
        artifact.validate()
        self._revalidate(artifact)


__all__ = ["ClickHouseExternalArtifactVerifier"]
