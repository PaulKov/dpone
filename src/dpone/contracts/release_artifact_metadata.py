"""Pure release identity and artifact locator/checksum metadata admission."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from pathlib import PurePosixPath
from typing import Any

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest, is_sha256_digest, release_id
from dpone.contracts.airflow_release_artifacts import ReleaseArtifactLocatorError, release_artifact_path


class ReleaseArtifactMetadataError(ValueError):
    """A metadata violation; adapters retain their own public error and path context."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class ArtifactChecksumPolicy(Enum):
    """Existing adapter spelling rules, including legacy projection coercion."""

    CANONICAL_TEXT = "canonical_text"
    LEGACY_TEXT = "legacy_text"
    LEGACY_VALUE = "legacy_value"


@dataclass(frozen=True, slots=True)
class ReleaseArtifactPin:
    """A relative confined locator and checksum with the original accepted spelling."""

    path: PurePosixPath
    sha256: str


def parse_release_artifact_pin(
    descriptor: Mapping[str, Any], *, checksum_policy: ArtifactChecksumPolicy
) -> ReleaseArtifactPin:
    """Admit the path before interpreting its checksum, without acquiring bytes.

    Projection historically coerces the checksum to text; publication requires a
    string value. The explicit policies retain both contracts without weakening
    canonical lowercase checks or changing accepted legacy checksum spelling.
    """
    try:
        path = release_artifact_path(descriptor)
    except ReleaseArtifactLocatorError as exc:
        raise ReleaseArtifactMetadataError("DPONE_RELEASE_ARTIFACT_PATH_INVALID", str(exc)) from exc
    digest = descriptor.get("sha256")
    if checksum_policy in {ArtifactChecksumPolicy.CANONICAL_TEXT, ArtifactChecksumPolicy.LEGACY_TEXT}:
        digest = str(digest or "")
    elif checksum_policy is not ArtifactChecksumPolicy.LEGACY_VALUE:
        raise TypeError("unsupported release artifact checksum policy")
    valid = (
        is_canonical_sha256_digest(digest)
        if checksum_policy is ArtifactChecksumPolicy.CANONICAL_TEXT
        else is_sha256_digest(digest)
    )
    if not valid:
        raise ReleaseArtifactMetadataError("DPONE_DEPLOYMENT_DIGEST_INVALID", "release artifact sha256 is invalid")
    return ReleaseArtifactPin(path, str(digest))


def require_release_metadata_identity(release: Mapping[str, Any], *, requested_release_id: str) -> None:
    """Check envelope, claimed digest, content identity and requested identity in order."""
    if release.get("schema") not in {"dpone.release-set.v1", "dpone.release-set.v2", "dpone.release-set.v3"}:
        raise ReleaseArtifactMetadataError("DPONE_RELEASE_SCHEMA_INVALID", "release-set schema is invalid")
    claimed = release.get("release_id")
    if not is_canonical_sha256_digest(claimed):
        raise ReleaseArtifactMetadataError(
            "DPONE_RELEASE_ID_INVALID", "release-set identity must be a canonical sha256 digest"
        )
    computed = release_id(release)
    if claimed != computed:
        raise ReleaseArtifactMetadataError(
            "DPONE_RELEASE_FINGERPRINT_MISMATCH", "release-set content does not match its claimed identity"
        )
    if requested_release_id != computed:
        raise ReleaseArtifactMetadataError(
            "DPONE_RELEASE_ID_MISMATCH", "requested release identity does not match release-set content"
        )
