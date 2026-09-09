"""Dependency-neutral contracts for the PyPI prepublication boundary."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Final

EXPECTED_PACKAGES: Final = frozenset(
    {"apache-airflow-providers-dpone", "dpone", "dpone-airflow-pack", "dpone-native-accel"}
)
PYPI_INDEX_URL: Final = "https://pypi.org"
WORKFLOW_PATH: Final = ".github/workflows/release.yml"


class PrepublicationGateError(RuntimeError):
    """A stable fail-closed PyPI prepublication blocker."""


def fail(
    code: str,
    *,
    package: str | None = None,
    filename: str | None = None,
) -> PrepublicationGateError:
    """Build a bounded stable blocker without logging an untrusted filename."""

    filename_ref = (
        f" filename_ref=sha256:{hashlib.sha256(filename.encode('utf-8', 'surrogatepass')).hexdigest()[:16]}"
        if filename
        else ""
    )
    details = "".join(value for value in (f": package={package}" if package else "", filename_ref))
    return PrepublicationGateError(f"{code}{details}")


@dataclass(frozen=True, slots=True, order=True)
class Candidate:
    """One immutable candidate identity accepted from the build receipt."""

    filename: str
    package: str
    version: str
    artifact_type: str
    sha256: str
    size_bytes: int

    def to_receipt(self, classification: str) -> dict[str, str | int]:
        return {
            "artifact_type": self.artifact_type,
            "classification": classification,
            "filename": self.filename,
            "package": self.package,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "version": self.version,
        }


@dataclass(frozen=True, slots=True)
class PublicationContext:
    """Provider-bound identity of the exact release workflow attempt."""

    repository: str
    commit_sha: str
    release: str
    workflow_path: str
    workflow_run_id: int
    workflow_run_attempt: int

    def to_payload(self) -> dict[str, str | int]:
        return {
            "commit_sha": self.commit_sha,
            "release": self.release,
            "repository": self.repository,
            "workflow_path": self.workflow_path,
            "workflow_run_attempt": self.workflow_run_attempt,
            "workflow_run_id": self.workflow_run_id,
        }


@dataclass(frozen=True, slots=True, order=True)
class PublicObservation:
    """One exact-version PyPI endpoint observation."""

    package: str
    endpoint_url: str
    release_state: str
    published_filenames: tuple[str, ...]

    def to_payload(self) -> dict[str, object]:
        return {
            "endpoint_url": self.endpoint_url,
            "package": self.package,
            "published_filenames": list(self.published_filenames),
            "release_state": self.release_state,
        }


def canonical_bytes(payload: object) -> bytes:
    """Serialize one receipt projection deterministically."""

    return (json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True) + "\n").encode("ascii")


@dataclass(frozen=True, slots=True)
class PrepublicationReport:
    """Deterministic receipt proving the safe upload subset."""

    expected_version: str
    context: PublicationContext
    candidate_inventory_sha256: str
    classified: tuple[tuple[Candidate, str], ...]
    observations: tuple[PublicObservation, ...]

    def to_payload(self) -> dict[str, object]:
        artifacts = [candidate.to_receipt(classification) for candidate, classification in self.classified]
        observations = [item.to_payload() for item in self.observations]
        existing = sum(item["classification"] == "ALREADY_PUBLISHED_EXACT" for item in artifacts)
        count = len(artifacts)
        mode = "fresh" if existing == 0 else "idempotent" if existing == count else "resume"
        observation_digest = hashlib.sha256(
            canonical_bytes({"artifacts": artifacts, "observations": observations})
        ).hexdigest()
        return {
            "artifacts": artifacts,
            "blockers": [],
            "candidate_inventory_sha256": self.candidate_inventory_sha256,
            "decision": "GO",
            "expected_version": self.expected_version,
            "index_url": PYPI_INDEX_URL,
            "observations": observations,
            "publication": self.context.to_payload(),
            "public_observation_sha256": observation_digest,
            "schema_version": 1,
            "status": "PASS",
            "summary": {
                "candidate_count": count,
                "existing_exact_count": existing,
                "pending_upload_count": count - existing,
                "publication_mode": mode,
            },
        }


def blocked_payload(
    expected_version: str,
    blocker: str,
    *,
    context: PublicationContext | None,
) -> dict[str, object]:
    """Build the same closed receipt envelope for a blocked attempt."""

    return {
        "artifacts": [],
        "blockers": [blocker],
        "candidate_inventory_sha256": None,
        "decision": "NO-GO",
        "expected_version": expected_version,
        "index_url": PYPI_INDEX_URL,
        "observations": [],
        "publication": context.to_payload() if context else None,
        "public_observation_sha256": None,
        "schema_version": 1,
        "status": "FAIL",
        "summary": {
            "candidate_count": 0,
            "existing_exact_count": 0,
            "pending_upload_count": 0,
            "publication_mode": "blocked",
        },
    }
