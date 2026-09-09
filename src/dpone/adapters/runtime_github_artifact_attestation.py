"""Artifact-registry bridge for offline GitHub runtime attestations."""

from __future__ import annotations

import tempfile
from pathlib import Path

from dpone.adapters.github_artifact_attestation import (
    GitHubArtifactAttestationError,
    GitHubArtifactAttestationVerifier,
)
from dpone.contracts.runtime_artifact_attestation import (
    MAX_ATTESTATION_BUNDLE_BYTES,
    GitHubArtifactAttestationPolicy,
    RuntimeArtifactAttestationError,
    RuntimeArtifactAttestationSubject,
    runtime_attestation_bundle_key,
)
from dpone.ports.artifact_registry import (
    ArtifactRegistryError,
    ArtifactRegistryObjectNotFound,
    ArtifactRegistryReader,
    ArtifactRegistryReadLimitExceeded,
)


class RegistryGitHubArtifactAttestationVerifier:
    """Fetch one immutable bundle and verify the exact staged release-set."""

    def __init__(
        self,
        *,
        registry: ArtifactRegistryReader,
        policy: GitHubArtifactAttestationPolicy,
        verifier: GitHubArtifactAttestationVerifier | None = None,
    ) -> None:
        self._registry = registry
        self._policy = policy
        self._verifier = verifier or GitHubArtifactAttestationVerifier()

    def verify(
        self,
        *,
        subject: RuntimeArtifactAttestationSubject,
    ) -> None:
        key = runtime_attestation_bundle_key(
            release_id=subject.release_id,
            release_set_sha256=subject.subject_sha256,
        )
        metadata = self._stat_bundle(key)
        if metadata.size_bytes <= 0:
            raise RuntimeArtifactAttestationError(
                "DPONE_ARTIFACT_ATTESTATION_BUNDLE_INVALID",
                "runtime attestation bundle is empty",
            )
        if metadata.size_bytes > MAX_ATTESTATION_BUNDLE_BYTES:
            raise RuntimeArtifactAttestationError(
                "DPONE_ARTIFACT_ATTESTATION_BUNDLE_TOO_LARGE",
                "runtime attestation bundle exceeds its byte limit",
            )
        with tempfile.TemporaryDirectory(prefix="dpone-runtime-attestation-") as directory:
            bundle_path = Path(directory) / "github.sigstore.jsonl"
            self._download_bundle(key, bundle_path)
            try:
                self._verifier.verify_files(
                    subject_path=subject.subject_path,
                    bundle_path=bundle_path,
                    policy=self._policy,
                    expected_subject_sha256=subject.subject_sha256,
                )
            except GitHubArtifactAttestationError as exc:
                raise RuntimeArtifactAttestationError(exc.code, str(exc)) from exc

    def _stat_bundle(self, key):
        try:
            return self._registry.stat(key)
        except ArtifactRegistryObjectNotFound as exc:
            raise RuntimeArtifactAttestationError(
                "DPONE_ARTIFACT_ATTESTATION_BUNDLE_NOT_FOUND",
                "runtime attestation bundle was not found",
            ) from exc
        except ArtifactRegistryError as exc:
            raise RuntimeArtifactAttestationError(
                "DPONE_ARTIFACT_REGISTRY_UNAVAILABLE",
                "runtime attestation registry metadata is unavailable",
            ) from exc

    def _download_bundle(self, key, destination: Path) -> None:
        try:
            self._registry.download_file(
                key,
                destination,
                max_bytes=MAX_ATTESTATION_BUNDLE_BYTES,
            )
        except ArtifactRegistryObjectNotFound as exc:
            raise RuntimeArtifactAttestationError(
                "DPONE_ARTIFACT_ATTESTATION_BUNDLE_NOT_FOUND",
                "runtime attestation bundle was not found",
            ) from exc
        except ArtifactRegistryReadLimitExceeded as exc:
            raise RuntimeArtifactAttestationError(
                "DPONE_ARTIFACT_ATTESTATION_BUNDLE_TOO_LARGE",
                "runtime attestation bundle exceeds its byte limit",
            ) from exc
        except ArtifactRegistryError as exc:
            raise RuntimeArtifactAttestationError(
                "DPONE_ARTIFACT_REGISTRY_UNAVAILABLE",
                "runtime attestation bundle download is unavailable",
            ) from exc


__all__ = ["RegistryGitHubArtifactAttestationVerifier"]
