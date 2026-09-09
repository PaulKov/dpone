"""Dependency-neutral data contracts for the local PyPI candidate inventory."""

from __future__ import annotations

from dataclasses import dataclass, field

DEFAULT_MAX_ENTRIES = 64
DEFAULT_MAX_FILE_BYTES = 128 * 1024 * 1024
DEFAULT_MAX_TOTAL_BYTES = 512 * 1024 * 1024


@dataclass(frozen=True, slots=True, order=True)
class CandidateArtifact:
    """Filename and digest passed to the public PyPI identity check."""

    filename: str
    sha256: str


@dataclass(frozen=True, slots=True)
class CandidateInventoryLimits:
    """Resource limits applied to every complete candidate scan."""

    max_entries: int = DEFAULT_MAX_ENTRIES
    max_file_bytes: int = DEFAULT_MAX_FILE_BYTES
    max_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES


@dataclass(frozen=True, slots=True, order=True)
class DistributionRelease:
    """One distribution version and its complete local candidate artifacts."""

    package: str
    version: str
    candidate_artifacts: tuple[CandidateArtifact, ...] = ()


@dataclass(frozen=True, slots=True, order=True)
class CandidateInventoryArtifact:
    """One parsed, regular candidate archive observed by a bounded scan."""

    filename: str
    package: str
    version: str
    artifact_type: str
    sha256: str
    size_bytes: int = 0
    source_identity: tuple[int, ...] = field(default=(), compare=False, repr=False)

    @property
    def exact_identity(self) -> tuple[str, int, str]:
        """Return the public immutable handoff identity."""

        return self.filename, self.size_bytes, self.sha256

    def to_payload(self) -> dict[str, str | int]:
        """Render only stable release evidence, excluding filesystem metadata."""

        return {
            "artifact_type": self.artifact_type,
            "filename": self.filename,
            "package": self.package,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "version": self.version,
        }


@dataclass(frozen=True, slots=True)
class CandidateInventoryReport:
    """Closed, deterministic pre-publish candidate inventory."""

    expected_version: str
    entry_count: int
    releases: tuple[DistributionRelease, ...]
    artifacts: tuple[CandidateInventoryArtifact, ...]
    blockers: tuple[str, ...]
    expected_artifact_count: int = 8
    expected_distribution_count: int = 4
    source_identity: tuple[int, ...] = field(default=(), compare=False, repr=False)

    @property
    def status(self) -> str:
        return "passed" if not self.blockers else "failed"

    @property
    def decision(self) -> str:
        return "GO" if not self.blockers else "NO-GO"

    def to_payload(self) -> dict[str, object]:
        """Return the deterministic machine-readable inventory evidence."""

        return {
            "artifacts": [artifact.to_payload() for artifact in self.artifacts],
            "blockers": list(self.blockers),
            "decision": self.decision,
            "expected_version": self.expected_version,
            "schema_version": 1,
            "status": self.status,
            "summary": {
                "artifact_count": len(self.artifacts),
                "distribution_count": len(self.releases),
                "expected_artifact_count": self.expected_artifact_count,
                "expected_distribution_count": self.expected_distribution_count,
            },
        }
