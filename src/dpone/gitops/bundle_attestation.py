from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class BundleArtifactDigest:
    path: str
    sha256: str
    bytes: int


@dataclass(frozen=True, slots=True)
class BundleAttestationPayload:
    artifacts: tuple[BundleArtifactDigest, ...]
    bundle_digest: str
    provenance: dict[str, Any]


class GitOpsBundleAttestationBuilder:
    """Builds deterministic digest evidence for bundle handoff artifacts."""

    def build(
        self,
        *,
        repo_root: Path,
        artifact_paths: tuple[str, ...],
        provenance: dict[str, Any],
    ) -> BundleAttestationPayload:
        artifacts = tuple(_digest_artifact(repo_root=repo_root, rel_path=path) for path in artifact_paths)
        bundle_digest = _bundle_digest(artifacts)
        return BundleAttestationPayload(
            artifacts=artifacts,
            bundle_digest=bundle_digest,
            provenance=provenance,
        )


def _digest_artifact(*, repo_root: Path, rel_path: str) -> BundleArtifactDigest:
    data = (repo_root / rel_path).read_bytes()
    return BundleArtifactDigest(
        path=rel_path,
        sha256=hashlib.sha256(data).hexdigest(),
        bytes=len(data),
    )


def _bundle_digest(artifacts: tuple[BundleArtifactDigest, ...]) -> str:
    payload = [
        {
            "path": artifact.path,
            "sha256": artifact.sha256,
            "bytes": artifact.bytes,
        }
        for artifact in artifacts
    ]
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


__all__ = ["BundleArtifactDigest", "BundleAttestationPayload", "GitOpsBundleAttestationBuilder"]
