"""Typed evidence and decision models for runtime image promotion."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from typing import Any


class ManifestState(str, Enum):  # noqa: UP042 - mypy project target remains Python 3.10
    PRESENT = "present"
    ABSENT = "absent"
    ERROR = "error"


class AliasDecision(str, Enum):  # noqa: UP042 - mypy project target remains Python 3.10
    CREATED = "CREATED"
    NOOP_SAME = "NOOP_SAME"
    ADVANCED = "ADVANCED"
    PRESERVED_NEWER = "PRESERVED_NEWER"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True)
class ManifestObservation:
    state: ManifestState
    digest: str | None = None
    error_code: str | None = None
    version: str | None = None


@dataclass(frozen=True)
class TagDecision:
    decision: AliasDecision
    write_required: bool
    blocker_code: str | None = None


@dataclass(frozen=True)
class CertificationIdentity:
    image: str
    version: str
    digest: str
    source_commit_sha: str
    source_ref: str


@dataclass(frozen=True)
class Certification:
    _payload: dict[str, Any]
    image: str
    digest: str
    version: str
    source_commit_sha: str

    def to_payload(self) -> dict[str, Any]:
        return deepcopy(self._payload)

    @property
    def schema(self) -> str:
        return str(self._payload["schema"])


@dataclass(frozen=True)
class PublicationReceipt:
    _payload: dict[str, Any]
    status: str
    outcome: str

    def to_payload(self) -> dict[str, Any]:
        return deepcopy(self._payload)


__all__ = [
    "AliasDecision",
    "Certification",
    "CertificationIdentity",
    "ManifestObservation",
    "ManifestState",
    "PublicationReceipt",
    "TagDecision",
]
