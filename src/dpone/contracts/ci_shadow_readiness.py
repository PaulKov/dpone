"""Dormant, default-deny CI-shadow readiness decision algebra.

This internal module deliberately has no publication or authority surface.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum

_SHA = re.compile(r"^[0-9a-f]{40}$")


class ReadinessStatus(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    UNVERIFIED = "UNVERIFIED"


class ReadinessCode(StrEnum):
    AUTHENTICATED = "AUTHENTICATED"
    SOURCE_FAILURE = "SOURCE_FAILURE"
    PROVENANCE_UNVERIFIED = "PROVENANCE_UNVERIFIED"


@dataclass(frozen=True, slots=True)
class ReadinessSubject:
    """Exact immutable subject accepted by the dormant evaluator."""

    commit_sha: str

    def __post_init__(self) -> None:
        if not _SHA.fullmatch(self.commit_sha):
            raise ValueError("readiness subject must be a lowercase 40-character commit SHA")


@dataclass(frozen=True, slots=True)
class ProvenanceFacts:
    """Typed facts supplied by a future authenticated verifier boundary."""

    authenticated: bool
    terminal_source_failure: bool = False


@dataclass(frozen=True, slots=True)
class ReadinessDecision:
    """Internal decision value; it does not grant any external authority."""

    subject: ReadinessSubject
    status: ReadinessStatus
    code: ReadinessCode


def evaluate_readiness(subject: ReadinessSubject, facts: ProvenanceFacts) -> ReadinessDecision:
    """Apply the closed default-deny decision table without I/O or clock reads."""

    if not facts.authenticated:
        return ReadinessDecision(subject, ReadinessStatus.UNVERIFIED, ReadinessCode.PROVENANCE_UNVERIFIED)
    if facts.terminal_source_failure:
        return ReadinessDecision(subject, ReadinessStatus.FAIL, ReadinessCode.SOURCE_FAILURE)
    return ReadinessDecision(subject, ReadinessStatus.PASS, ReadinessCode.AUTHENTICATED)
