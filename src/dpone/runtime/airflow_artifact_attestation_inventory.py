"""Publication policy for the detached runtime release attestation bundle."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from dpone.contracts.runtime_artifact_attestation import (
    MAX_ATTESTATION_BUNDLE_BYTES as MAX_ATTESTATION_BUNDLE_BYTES,
)
from dpone.contracts.runtime_artifact_attestation import runtime_attestation_bundle_key
from dpone.runtime.airflow_artifact_delivery_models import PublishRequest


@dataclass(frozen=True, slots=True)
class AttestationPublicationSpec:
    key: PurePosixPath
    path: Path
    source_root: Path


def attestation_publication_spec(
    request: PublishRequest,
    *,
    release_schema: str,
    release_set_sha256: str,
) -> AttestationPublicationSpec | None:
    path = request.attestation_bundle_path
    if path is None:
        return None
    return AttestationPublicationSpec(
        key=runtime_attestation_bundle_key(
            release_id=request.release_id,
            release_set_sha256=release_set_sha256,
        ),
        path=path,
        source_root=path.parent,
    )


__all__ = [
    "AttestationPublicationSpec",
    "MAX_ATTESTATION_BUNDLE_BYTES",
    "attestation_publication_spec",
]
