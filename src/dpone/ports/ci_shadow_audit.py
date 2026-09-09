"""Ports for bounded, read-only PR Gate shadow identity observations."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol, runtime_checkable


class CiShadowAuditProviderError(RuntimeError):
    """A read-only provider observation could not be acquired safely."""


@dataclass(frozen=True)
class CiShadowAuditDeadline:
    """One monotonic wall-time bound shared by a complete PR observation."""

    clock: Callable[[], float]
    expires_at: float

    def remaining_seconds(self) -> float:
        """Return positive remaining time or fail closed at the deadline."""

        remaining = self.expires_at - self.clock()
        if remaining <= 0:
            raise CiShadowAuditProviderError("shadow audit observation deadline expired")
        return remaining


class CiShadowAuditProvider(Protocol):
    """Read-only provider API used by the trusted default-branch auditor."""

    def get_workflow_run(self, run_id: int) -> Mapping[str, object]:
        """Return the exact immutable producer-run metadata or raise.

        The incoming ``workflow_run`` payload starts an audit but is not by
        itself sufficient evidence.  Implementations must acquire this record
        through the provider's exact run endpoint before claims are trusted.
        """

    def list_attempt_jobs(self, run_id: int, attempt: int) -> Sequence[Mapping[str, object]]:
        """Return the complete, exact-attempt job population or raise."""

    def get_git_tree(self, commit_sha: str) -> Mapping[str, Mapping[str, object]]:
        """Return one complete immutable Git tree keyed by confined path."""

    def get_git_blob(self, blob_sha: str) -> bytes:
        """Return one bounded immutable Git blob's raw bytes or raise."""

    def list_open_pull_requests(self) -> Sequence[Mapping[str, object]]:
        """Return one complete, freshly paginated open-PR set or raise."""

    def get_pull_request(self, number: int) -> Mapping[str, object]:
        """Return the current exact pull-request record or raise."""

    def resolve_pull_merge_ref(self, number: int) -> str:
        """Resolve the GitHub-owned ``refs/pull/N/merge`` reference or raise."""

    def get_commit_parents(self, sha: str) -> Sequence[str]:
        """Return the ordered immutable parent SHA list for ``sha`` or raise."""


@runtime_checkable
class CiShadowAuditDeadlineProvider(Protocol):
    """Optional capability for enforcing a deadline inside provider dispatch."""

    def list_open_pull_requests_bounded(self, *, deadline: CiShadowAuditDeadline) -> Sequence[Mapping[str, object]]: ...

    def get_pull_request_bounded(self, number: int, *, deadline: CiShadowAuditDeadline) -> Mapping[str, object]: ...

    def resolve_pull_merge_ref_bounded(self, number: int, *, deadline: CiShadowAuditDeadline) -> str: ...

    def get_commit_parents_bounded(self, sha: str, *, deadline: CiShadowAuditDeadline) -> Sequence[str]: ...
