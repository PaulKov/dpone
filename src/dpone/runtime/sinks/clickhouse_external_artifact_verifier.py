"""Artifact identity verifier for external ClickHouse publication."""

from __future__ import annotations

from collections.abc import Callable

from dpone.ports.clickhouse_external_replication import ArtifactIdentity


class ClickHouseExternalArtifactVerifier:
    """Delegate exact artifact revalidation without exposing storage details."""

    def __init__(self, *, revalidate: Callable[[ArtifactIdentity], None]) -> None:
        self._revalidate = revalidate

    def revalidate(self, artifact: ArtifactIdentity) -> None:
        artifact.validate()
        self._revalidate(artifact)


__all__ = ["ClickHouseExternalArtifactVerifier"]
