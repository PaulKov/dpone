"""Artifact checksum helpers for native transfer resume guardrails."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any


class ArtifactChecksumService:
    """Compute deterministic checksums for file-backed artifacts."""

    def checksum(self, artifact: Any) -> str:
        partitions = tuple(getattr(artifact, "partitions", None) or ())
        if partitions:
            digest = hashlib.sha256()
            for partition in partitions:
                digest.update(self.checksum(partition).removeprefix("sha256:").encode("utf-8"))
            return f"sha256:{digest.hexdigest()}"

        file_path = getattr(artifact, "file_path", None)
        if not file_path:
            raise ValueError(f"Artifact {type(artifact).__name__} does not expose a file_path.")
        return f"sha256:{_sha256_file(Path(file_path))}"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
