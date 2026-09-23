"""Process-local, one-shot authority for same-fence attempt continuation."""

from __future__ import annotations

import os
from threading import Lock

from dpone.contracts.bounded_window import WindowLease
from dpone.contracts.mssql_tds_directory import TdsDirectorySnapshot
from dpone.contracts.mssql_tds_worker import TdsAttemptSnapshot

_ERROR = "mssql_native.tds_suspension_invalid"
_ISSUER = object()


class TdsAttemptResumeClaim:
    """Consumed suspension claim; each journal half can be opened exactly once."""

    __slots__ = ("_directory", "_lifecycle", "_lock", "_pid", "_used")

    def __init__(
        self,
        lifecycle: TdsAttemptSnapshot,
        directory: TdsDirectorySnapshot,
        issuer: object,
    ) -> None:
        if issuer is not _ISSUER:
            raise ValueError(_ERROR)
        self._lifecycle = lifecycle
        self._directory = directory
        self._pid = os.getpid()
        self._lock = Lock()
        self._used: set[str] = set()

    @property
    def observations(self) -> tuple[TdsAttemptSnapshot, TdsDirectorySnapshot]:
        """Expose immutable binding facts; writer authority remains in this claim."""
        return self._lifecycle, self._directory

    def lifecycle(self, lease: WindowLease) -> TdsAttemptSnapshot:
        return self._take("lifecycle", lease, self._lifecycle)

    def directory(self, lease: WindowLease) -> TdsDirectorySnapshot:
        return self._take("directory", lease, self._directory)

    def _take(self, kind: str, lease: WindowLease, snapshot):
        with self._lock:
            if os.getpid() != self._pid or kind in self._used:
                raise ValueError(_ERROR)
            owner = self._lifecycle.state.ownership
            if (lease.target_id, lease.owner, lease.fence) != (
                self._lifecycle.state.identity.target_key,
                owner.owner,
                owner.fence,
            ):
                raise ValueError(_ERROR)
            self._used.add(kind)
            return snapshot


class TdsAttemptSuspension:
    """Non-serializable capability issued only after both local actors close."""

    __slots__ = ("_claim", "_lock", "_pid")

    def __init__(self, claim: TdsAttemptResumeClaim, issuer: object) -> None:
        if issuer is not _ISSUER:
            raise ValueError(_ERROR)
        self._claim: TdsAttemptResumeClaim | None = claim
        self._pid = os.getpid()
        self._lock = Lock()

    def consume(self) -> TdsAttemptResumeClaim:
        with self._lock:
            if os.getpid() != self._pid or self._claim is None:
                raise ValueError(_ERROR)
            claim, self._claim = self._claim, None
            return claim


def _issue_tds_attempt_suspension(
    lifecycle: TdsAttemptSnapshot,
    directory: TdsDirectorySnapshot,
) -> TdsAttemptSuspension:
    if (
        type(lifecycle) is not TdsAttemptSnapshot
        or type(directory) is not TdsDirectorySnapshot
        or lifecycle.state.identity != directory.state.parent
        or lifecycle.state.ownership != directory.ownership
    ):
        raise ValueError(_ERROR)
    return TdsAttemptSuspension(TdsAttemptResumeClaim(lifecycle, directory, _ISSUER), _ISSUER)


__all__ = ("TdsAttemptResumeClaim", "TdsAttemptSuspension")
