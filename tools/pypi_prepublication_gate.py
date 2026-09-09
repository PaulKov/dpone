#!/usr/bin/env python3
"""Fail closed unless public PyPI state is an exact candidate subset."""

from __future__ import annotations

import argparse
import math
import os
import re
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

if __package__:
    from .pypi_prepublication_contract import (
        EXPECTED_PACKAGES,
        WORKFLOW_PATH,
        Candidate,
        PrepublicationGateError,
        PrepublicationReport,
        PublicationContext,
        PublicObservation,
        blocked_payload,
        canonical_bytes,
        fail,
    )
    from .pypi_prepublication_pypi import fetch_pypi_version, public_files, version_url
    from .pypi_prepublication_source import load_candidate_inventory, verify_local_candidates
else:
    from pypi_prepublication_contract import (
        EXPECTED_PACKAGES,
        WORKFLOW_PATH,
        Candidate,
        PrepublicationGateError,
        PrepublicationReport,
        PublicationContext,
        PublicObservation,
        blocked_payload,
        canonical_bytes,
        fail,
    )
    from pypi_prepublication_pypi import fetch_pypi_version, public_files, version_url
    from pypi_prepublication_source import (
        load_candidate_inventory,
        verify_local_candidates,
    )

__all__ = (
    "Candidate",
    "PrepublicationGateError",
    "PrepublicationReport",
    "PublicationContext",
    "PublicObservation",
    "canonical_bytes",
    "evaluate_prepublication",
    "fetch_pypi_version",
    "load_candidate_inventory",
    "main",
    "observe_stable_prepublication",
    "publication_context",
)

_COMMIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_REPOSITORY = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
VersionFetcher = Callable[[str, str], Mapping[str, Any] | None]
MAX_STABILITY_ATTEMPTS = 100


@dataclass(frozen=True, slots=True)
class StabilityEvidence:
    """Bounded evidence that the complete public subset stopped changing."""

    attempts: int
    required_stable_observations: int
    stable_count: int
    started_at: str
    ended_at: str
    elapsed_seconds: float
    public_observation_sha256: str | None
    public_observation_sha256_timeline: tuple[str, ...]

    def to_payload(self) -> dict[str, object]:
        return {
            "attempts": self.attempts,
            "elapsed_seconds": round(self.elapsed_seconds, 6),
            "ended_at": self.ended_at,
            "public_observation_sha256": self.public_observation_sha256,
            "public_observation_sha256_timeline": list(self.public_observation_sha256_timeline),
            "required_stable_observations": self.required_stable_observations,
            "stable_count": self.stable_count,
            "started_at": self.started_at,
        }


class PrepublicationStabilityError(PrepublicationGateError):
    """PyPI never exposed one stable exact candidate subset in the bound."""

    def __init__(self, evidence: StabilityEvidence, last_safe_report: PrepublicationReport | None) -> None:
        super().__init__("PYPI_PREPUBLICATION_PUBLIC_STATE_UNSTABLE")
        self.evidence = evidence
        self.last_safe_report = last_safe_report


def _positive_decimal(value: str) -> int | None:
    if not value.isascii() or not value.isdecimal() or value.startswith("0"):
        return None
    parsed = int(value)
    return parsed if parsed > 0 else None


def publication_context(
    *,
    repository: str,
    commit_sha: str,
    release: str,
    workflow_path: str,
    workflow_run_id: str,
    workflow_run_attempt: str,
    expected_version: str,
    environment: Mapping[str, str],
) -> PublicationContext:
    """Validate CLI identity against immutable GitHub-provided environment."""

    run_id = _positive_decimal(workflow_run_id)
    run_attempt = _positive_decimal(workflow_run_attempt)
    if (
        _REPOSITORY.fullmatch(repository) is None
        or _COMMIT_SHA.fullmatch(commit_sha) is None
        or _VERSION.fullmatch(expected_version) is None
        or release != f"v{expected_version}"
        or workflow_path != WORKFLOW_PATH
        or run_id is None
        or run_attempt is None
    ):
        raise fail("PYPI_PREPUBLICATION_CONTEXT_INVALID")
    expected_environment = {
        "GITHUB_EVENT_NAME": "push",
        "GITHUB_REF": f"refs/tags/{release}",
        "GITHUB_REF_NAME": release,
        "GITHUB_REF_TYPE": "tag",
        "GITHUB_REPOSITORY": repository,
        "GITHUB_RUN_ATTEMPT": workflow_run_attempt,
        "GITHUB_RUN_ID": workflow_run_id,
        "GITHUB_SHA": commit_sha,
        "GITHUB_WORKFLOW_REF": f"{repository}/{workflow_path}@refs/tags/{release}",
    }
    if any(environment.get(key) != value for key, value in expected_environment.items()):
        raise fail("PYPI_PREPUBLICATION_GITHUB_CONTEXT_MISMATCH")
    return PublicationContext(repository, commit_sha, release, workflow_path, run_id, run_attempt)


def evaluate_prepublication(
    inventory_path: Path,
    dist_dir: Path,
    *,
    expected_version: str,
    context: PublicationContext,
    fetcher: VersionFetcher = fetch_pypi_version,
) -> PrepublicationReport:
    """Prove public files are absent or an exact immutable candidate subset."""

    candidates, inventory_sha256 = load_candidate_inventory(inventory_path, expected_version=expected_version)
    verify_local_candidates(dist_dir, candidates)
    classifications = {candidate.filename: "PENDING_UPLOAD" for candidate in candidates}
    observations: list[PublicObservation] = []
    for package in sorted(EXPECTED_PACKAGES):
        payload = fetcher(package, expected_version)
        if payload is None:
            observations.append(
                PublicObservation(package, version_url(package, expected_version), "VERSION_ABSENT", ())
            )
            continue
        expected = {candidate.filename: candidate for candidate in candidates if candidate.package == package}
        rows = public_files(payload, package=package, version=expected_version)
        for row in rows:
            filename = str(row["filename"])
            candidate = expected.get(filename)
            if candidate is None:
                raise fail("PYPI_PREPUBLICATION_PUBLIC_FILE_UNEXPECTED", package=package, filename=filename)
            digests = row.get("digests")
            public_size = row.get("size")
            if not isinstance(digests, dict) or type(public_size) is not int or public_size != candidate.size_bytes:
                raise fail("PYPI_PREPUBLICATION_PUBLIC_SIZE_INVALID", package=package, filename=filename)
            if digests.get("sha256") != candidate.sha256:
                raise fail("PYPI_PREPUBLICATION_PUBLIC_DIGEST_MISMATCH", package=package, filename=filename)
            if row.get("yanked") is not False:
                raise fail("PYPI_PREPUBLICATION_PUBLIC_YANKED_OR_UNKNOWN", package=package, filename=filename)
            classifications[filename] = "ALREADY_PUBLISHED_EXACT"
        observations.append(
            PublicObservation(
                package,
                version_url(package, expected_version),
                "EXACT_CANDIDATE_SUBSET",
                tuple(sorted(str(row["filename"]) for row in rows)),
            )
        )
    classified = tuple((candidate, classifications[candidate.filename]) for candidate in candidates)
    return PrepublicationReport(expected_version, context, inventory_sha256, classified, tuple(observations))


def observe_stable_prepublication(
    inventory_path: Path,
    dist_dir: Path,
    *,
    expected_version: str,
    context: PublicationContext,
    required_stable_observations: int = 2,
    timeout_seconds: float = 300.0,
    poll_interval_seconds: float = 15.0,
    fetcher: VersionFetcher = fetch_pypi_version,
    clock: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
    utcnow: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> tuple[PrepublicationReport, StabilityEvidence]:
    """Require repeated identical exact-subset reads before failure reconciliation.

    PyPI may accept only part of one multi-project upload before the publisher
    exits, while its JSON/CDN surface converges asynchronously.  This observer
    accepts every intermediate state only when it is itself an exact candidate
    subset, resets stability whenever that complete subset changes, and fails
    closed after a bounded wait.
    """

    if (
        isinstance(required_stable_observations, bool)
        or not isinstance(required_stable_observations, int)
        or required_stable_observations < 2
        or required_stable_observations > MAX_STABILITY_ATTEMPTS
        or not math.isfinite(timeout_seconds)
        or timeout_seconds < 0
        or not math.isfinite(poll_interval_seconds)
        or poll_interval_seconds <= 0
    ):
        raise fail("PYPI_PREPUBLICATION_STABILITY_POLICY_INVALID")
    started_at = _aware_utc(utcnow()).isoformat()
    started = clock()
    prior_digest: str | None = None
    stable_count = 0
    attempts = 0
    digest_timeline: list[str] = []
    report: PrepublicationReport | None = None
    while attempts < MAX_STABILITY_ATTEMPTS:
        report = evaluate_prepublication(
            inventory_path,
            dist_dir,
            expected_version=expected_version,
            context=context,
            fetcher=fetcher,
        )
        attempts += 1
        digest = str(report.to_payload()["public_observation_sha256"])
        digest_timeline.append(digest)
        stable_count = stable_count + 1 if digest == prior_digest else 1
        prior_digest = digest
        if stable_count >= required_stable_observations:
            return report, _stability_evidence(
                attempts=attempts,
                required=required_stable_observations,
                stable_count=stable_count,
                started_at=started_at,
                started=started,
                clock=clock,
                utcnow=utcnow,
                digest=digest,
                digest_timeline=tuple(digest_timeline),
            )
        elapsed = max(0.0, clock() - started)
        remaining = timeout_seconds - elapsed
        if remaining <= 0:
            break
        sleeper(min(poll_interval_seconds, remaining))
    raise PrepublicationStabilityError(
        _stability_evidence(
            attempts=attempts,
            required=required_stable_observations,
            stable_count=stable_count,
            started_at=started_at,
            started=started,
            clock=clock,
            utcnow=utcnow,
            digest=prior_digest,
            digest_timeline=tuple(digest_timeline),
        ),
        report,
    )


def _stability_evidence(
    *,
    attempts: int,
    required: int,
    stable_count: int,
    started_at: str,
    started: float,
    clock: Callable[[], float],
    utcnow: Callable[[], datetime],
    digest: str | None,
    digest_timeline: tuple[str, ...],
) -> StabilityEvidence:
    return StabilityEvidence(
        attempts=attempts,
        required_stable_observations=required,
        stable_count=stable_count,
        started_at=started_at,
        ended_at=_aware_utc(utcnow()).isoformat(),
        elapsed_seconds=max(0.0, clock() - started),
        public_observation_sha256=digest,
        public_observation_sha256_timeline=digest_timeline,
    )


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise fail("PYPI_PREPUBLICATION_STABILITY_CLOCK_INVALID")
    return value.astimezone(UTC)


def _write_receipt(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(canonical_bytes(payload))
    os.replace(temporary, path)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prove safe PyPI prepublication state for exact candidates.")
    parser.add_argument("--candidate-inventory", required=True, type=Path)
    parser.add_argument("--dist-dir", default=Path("dist"), type=Path)
    parser.add_argument("--expected-version", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--release", required=True)
    parser.add_argument("--workflow-path", required=True)
    parser.add_argument("--workflow-run-id", required=True)
    parser.add_argument("--workflow-run-attempt", required=True)
    parser.add_argument("--output", required=True, type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    context: PublicationContext | None = None
    try:
        context = publication_context(
            repository=arguments.repository,
            commit_sha=arguments.commit_sha,
            release=arguments.release,
            workflow_path=arguments.workflow_path,
            workflow_run_id=arguments.workflow_run_id,
            workflow_run_attempt=arguments.workflow_run_attempt,
            expected_version=arguments.expected_version,
            environment=os.environ,
        )
        payload = evaluate_prepublication(
            arguments.candidate_inventory,
            arguments.dist_dir,
            expected_version=arguments.expected_version,
            context=context,
        ).to_payload()
    except PrepublicationGateError as exc:
        payload = blocked_payload(arguments.expected_version, str(exc), context=context)
        _write_receipt(arguments.output, payload)
        sys.stderr.write(f"{exc}\n")
        return 1
    _write_receipt(arguments.output, payload)
    sys.stdout.buffer.write(canonical_bytes(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
