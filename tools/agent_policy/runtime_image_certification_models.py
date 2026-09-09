"""Typed inputs and outputs for runtime image certification."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, NamedTuple


@dataclass(frozen=True)
class CertificationInputs:
    image: str
    version: str
    release_tag: str
    source_commit_sha: str
    source_ref: str
    source_repository: str
    signer_workflow: str
    digest: str
    platform: str
    run_id: str
    run_attempt: str
    dockerfile: Path
    dockerignore: Path
    context_root: Path
    context_manifest_output: Path
    sbom: Path
    provenance_verification: Path
    sbom_verification: Path
    checks_root: Path


class CertificationArtifacts(NamedTuple):
    certification: dict[str, Any]
    context_manifest: dict[str, Any]


__all__ = ["CertificationArtifacts", "CertificationInputs"]
