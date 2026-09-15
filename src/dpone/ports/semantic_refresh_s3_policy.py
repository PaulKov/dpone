"""Compatibility import for the shared S3 capability policy."""

from dpone.ports.versioned_artifact_store import S3VersionedArtifactPolicy as SemanticRefreshS3ArtifactStorePolicy

__all__ = ["SemanticRefreshS3ArtifactStorePolicy"]
