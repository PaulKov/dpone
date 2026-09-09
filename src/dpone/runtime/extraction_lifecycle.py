"""Truthful source-extraction timing and artifact terminal contracts.

The immutable values in this module are evidence.  A small, thread-safe
authority owns their state transitions so lazy artifacts never have to mutate
a receipt or invent an extraction-completion timestamp before their iterator
or generator has actually been exhausted.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from threading import RLock

_UTC = timezone.utc  # noqa: UP017 - package type checking includes Python 3.11 stubs.

ExtractionClock = Callable[[], datetime]


class ArtifactTerminalOutcome(str, Enum):  # noqa: UP042 - stable typing with Python 3.10 mypy stubs.
    """Terminal ownership decision issued by the top-level load boundary."""

    SUCCESS = "success"
    ABORT = "abort"
    RETAIN_COMMIT_UNKNOWN = "retain_commit_unknown"


class ArtifactCleanupDisposition(str, Enum):  # noqa: UP042 - stable typing with Python 3.10 mypy stubs.
    """Sink-neutral classification carried by failures to the top-level owner."""

    CLEANUP_SAFE = "cleanup_safe"
    COMMITTED_SECONDARY_ONLY = "committed_secondary_only"
    PRESERVE_EVIDENCE = "preserve_staging_evidence"


@dataclass(frozen=True, slots=True)
class ExtractionLifecycleReceipt:
    """Immutable extraction-window evidence with optional native snapshot proof.

    ``extraction_started_at`` is the earliest boundary observed by the stated
    ``clock_authority``; it is available for every supported source.  A source
    may additionally publish ``snapshot_acquired_at`` and
    ``snapshot_authority`` only when a vendor/session primitive actually
    established that snapshot.  Generic orchestration must never promote its
    invocation timestamp to snapshot evidence.

    ``extraction_completed_at`` remains ``None`` while a lazy artifact is still
    readable.  Completion means all source rows/bytes have been consumed and
    their artifact row/byte receipt has been published; it does not mean that
    the target transaction has committed.
    """

    extraction_started_at: datetime
    clock_authority: str
    extraction_completed_at: datetime | None = None
    snapshot_acquired_at: datetime | None = None
    snapshot_authority: str | None = None
    source_token: str | None = None

    def __post_init__(self) -> None:
        _require_utc(self.extraction_started_at, field="extraction_started_at")
        _require_authority(self.clock_authority, field="clock_authority")
        completed = self.extraction_completed_at
        if completed is not None:
            _require_utc(completed, field="extraction_completed_at")
            if completed < self.extraction_started_at:
                raise ValueError("extraction_lifecycle.completed_before_start")
        snapshot = self.snapshot_acquired_at
        if snapshot is not None:
            _require_utc(snapshot, field="snapshot_acquired_at")
            if snapshot < self.extraction_started_at:
                raise ValueError("extraction_lifecycle.snapshot_before_start")
            if completed is not None and snapshot > completed:
                raise ValueError("extraction_lifecycle.snapshot_after_completion")
            _require_authority(self.snapshot_authority, field="snapshot_authority")
        elif self.snapshot_authority is not None:
            raise ValueError("extraction_lifecycle.snapshot_authority_without_snapshot")
        token = self.source_token
        if token is not None and not token.strip():
            raise ValueError("extraction_lifecycle.source_token_empty")
        if token is not None and snapshot is None:
            raise ValueError("extraction_lifecycle.source_token_without_snapshot")

    @property
    def complete(self) -> bool:
        """Whether source consumption has truthfully completed."""

        return self.extraction_completed_at is not None


@dataclass(frozen=True, slots=True)
class ArtifactTerminalReceipt:
    """Immutable evidence for one idempotent artifact terminal decision."""

    outcome: ArtifactTerminalOutcome
    cleanup_attempted: bool
    cleanup_succeeded: bool
    cleanup_error_code: str | None = None

    def to_dict(self) -> dict[str, object]:
        """Return a stable redaction-safe runtime evidence payload."""

        return {
            "outcome": self.outcome.value,
            "cleanup_attempted": self.cleanup_attempted,
            "cleanup_succeeded": self.cleanup_succeeded,
            "cleanup_error_code": self.cleanup_error_code,
        }


class ArtifactTerminalAuthority:
    """Freeze one terminal decision across every view of one owned resource."""

    def __init__(self) -> None:
        self._lock = RLock()
        self._receipt: ArtifactTerminalReceipt | None = None

    @property
    def receipt(self) -> ArtifactTerminalReceipt | None:
        with self._lock:
            return self._receipt

    def terminate(
        self,
        outcome: ArtifactTerminalOutcome,
        *,
        release: Callable[[ArtifactTerminalOutcome], None],
        should_release: bool,
    ) -> ArtifactTerminalReceipt:
        """Apply the first decision and receipt any best-effort release failure."""

        with self._lock:
            if self._receipt is not None:
                return self._receipt
            error_code = None
            succeeded = True
            if should_release:
                try:
                    release(outcome)
                except BaseException as exc:
                    succeeded = False
                    error_code = f"{type(exc).__module__}.{type(exc).__qualname__}"
            self._receipt = ArtifactTerminalReceipt(
                outcome=outcome,
                cleanup_attempted=should_release,
                cleanup_succeeded=succeeded,
                cleanup_error_code=error_code,
            )
            return self._receipt


class ExtractionLifecycleStateError(RuntimeError):
    """Raised when a caller attempts an impossible lifecycle transition."""


class ExtractionLifecycleAuthority:
    """Atomically issue immutable receipts for eager and lazy extraction.

    A newly-created authority is intentionally *pending*.  Lazy source code
    calls :meth:`acquire` at its real read/materialization boundary.  A native
    source calls :meth:`acquire_snapshot` after its vendor snapshot exists.
    Readers may inspect :attr:`receipt` at any time without observing a
    partially-mutated value.
    """

    def __init__(self, *, clock: ExtractionClock | None = None) -> None:
        self._clock = clock or _utc_now
        self._lock = RLock()
        self._receipt: ExtractionLifecycleReceipt | None = None

    @property
    def receipt(self) -> ExtractionLifecycleReceipt | None:
        """Return the current frozen receipt, or ``None`` before acquisition."""

        with self._lock:
            return self._receipt

    def acquire(
        self,
        *,
        started_at: datetime | None = None,
        clock_authority: str = "dpone.orchestrator.utc",
    ) -> ExtractionLifecycleReceipt:
        """Record a truthful extraction-window start exactly once."""

        with self._lock:
            if self._receipt is not None:
                return self._receipt
            receipt = ExtractionLifecycleReceipt(
                extraction_started_at=_as_utc(started_at or self._clock()),
                clock_authority=clock_authority,
            )
            self._receipt = receipt
            return receipt

    def acquire_snapshot(
        self,
        *,
        snapshot_authority: str,
        source_token: str | None = None,
        acquired_at: datetime | None = None,
        clock_authority: str = "dpone.orchestrator.utc",
    ) -> ExtractionLifecycleReceipt:
        """Record one proven native snapshot and extraction start atomically."""

        with self._lock:
            if self._receipt is not None:
                current = self._receipt
                if current.snapshot_authority != snapshot_authority or current.source_token != source_token:
                    raise ExtractionLifecycleStateError("extraction_lifecycle.snapshot_changed")
                return current
            boundary = _as_utc(acquired_at or self._clock())
            receipt = ExtractionLifecycleReceipt(
                extraction_started_at=boundary,
                clock_authority=clock_authority,
                snapshot_acquired_at=boundary,
                snapshot_authority=snapshot_authority,
                source_token=source_token,
            )
            self._receipt = receipt
            return receipt

    def complete(self, *, completed_at: datetime | None = None) -> ExtractionLifecycleReceipt:
        """Replace an acquired in-progress receipt with its completed value."""

        with self._lock:
            current = self._receipt
            if current is None:
                raise ExtractionLifecycleStateError("extraction_lifecycle.extraction_not_started")
            if current.complete:
                return current
            completed = ExtractionLifecycleReceipt(
                extraction_started_at=current.extraction_started_at,
                clock_authority=current.clock_authority,
                extraction_completed_at=_as_utc(completed_at or self._clock()),
                snapshot_acquired_at=current.snapshot_acquired_at,
                snapshot_authority=current.snapshot_authority,
                source_token=current.source_token,
            )
            self._receipt = completed
            return completed

    def require_completed(self) -> ExtractionLifecycleReceipt:
        """Return complete evidence or fail closed with a stable diagnostic."""

        with self._lock:
            receipt = self._receipt
            if receipt is None:
                raise ExtractionLifecycleStateError("extraction_lifecycle.extraction_not_started")
            if not receipt.complete:
                raise ExtractionLifecycleStateError("extraction_lifecycle.extraction_not_completed")
            return receipt

    def require_acquired(self) -> ExtractionLifecycleReceipt:
        """Return the stable extraction boundary whether open or completed.

        This receipt is suitable for immutable planning only.  Callers that
        publish target evidence must use :meth:`require_completed` instead.
        """

        with self._lock:
            receipt = self._receipt
            if receipt is None:
                raise ExtractionLifecycleStateError("extraction_lifecycle.extraction_not_started")
            return receipt

    def require_in_progress(self) -> ExtractionLifecycleReceipt:
        """Return acquired, incomplete evidence for an externally owned read.

        A component accepting an authority from its caller must not infer
        transaction ownership from a mutable receipt.  This guard lets that
        component prove the caller has already acquired the extraction
        boundary while preventing reuse after the caller completed it.
        """

        with self._lock:
            receipt = self._receipt
            if receipt is None:
                raise ExtractionLifecycleStateError("extraction_lifecycle.extraction_not_started")
            if receipt.complete:
                raise ExtractionLifecycleStateError("extraction_lifecycle.extraction_already_completed")
            return receipt


def _utc_now() -> datetime:
    return datetime.now(_UTC)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("extraction_lifecycle.timestamp_must_be_timezone_aware")
    return value.astimezone(_UTC)


def _require_utc(value: datetime, *, field: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"extraction_lifecycle.{field}_must_be_timezone_aware")
    if value.utcoffset() != _UTC.utcoffset(value):
        raise ValueError(f"extraction_lifecycle.{field}_must_be_utc")


def _require_authority(value: str | None, *, field: str) -> None:
    if value is None or not value.strip() or len(value) > 128:
        raise ValueError(f"extraction_lifecycle.{field}_invalid")


__all__ = [
    "ArtifactCleanupDisposition",
    "ArtifactTerminalAuthority",
    "ArtifactTerminalOutcome",
    "ArtifactTerminalReceipt",
    "ExtractionClock",
    "ExtractionLifecycleAuthority",
    "ExtractionLifecycleReceipt",
    "ExtractionLifecycleStateError",
]
