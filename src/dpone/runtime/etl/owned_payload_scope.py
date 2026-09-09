"""Top-level ownership boundary for source extraction payloads.

The scope is acquired immediately after ``Source.extract`` returns and is the
only runtime component allowed to issue a terminal outcome for source-owned
artifacts.  Sink implementations retain ownership of their own staging and
temporary target resources only.
"""

from __future__ import annotations

from dataclasses import dataclass
from threading import RLock
from typing import Any

from dpone.runtime.extraction_lifecycle import (
    ArtifactCleanupDisposition,
    ArtifactTerminalAuthority,
    ArtifactTerminalOutcome,
    ArtifactTerminalReceipt,
    ExtractionLifecycleAuthority,
    ExtractionLifecycleReceipt,
    ExtractionLifecycleStateError,
)


class OwnedPayloadReleaseError(RuntimeError):
    """One or more source resources could not honor the terminal outcome."""


class _OwnedArtifactRegistry:
    """Thread-safe set of every source-resource view created during preparation."""

    def __init__(self, lifecycle: ExtractionLifecycleAuthority | None) -> None:
        self._lifecycle = lifecycle
        self._lock = RLock()
        self._artifacts: list[object] = []
        self._identities: set[int] = set()
        self._terminal_outcome: ArtifactTerminalOutcome | None = None

    def bind(self, artifact: object, *, initial: bool = False) -> None:
        """Register a view and bind/prove the same extraction lifecycle."""

        with self._lock:
            identity = id(artifact)
            if identity in self._identities:
                return
            if self._terminal_outcome is not None:
                raise ValueError("owned_payload_scope.already_terminated")
            _bind_or_validate_lifecycle(
                artifact,
                self._lifecycle,
                initial=initial,
            )
            self._identities.add(identity)
            self._artifacts.append(artifact)

    def release(self, outcome: ArtifactTerminalOutcome) -> None:
        """Freeze the registry and terminalize every registered physical view."""

        with self._lock:
            if self._terminal_outcome is None:
                self._terminal_outcome = outcome
            artifacts = tuple(self._artifacts)
        errors: list[BaseException] = []
        for artifact in artifacts:
            try:
                _terminate_artifact(artifact, outcome)
            except BaseException as exc:
                errors.append(exc)
        if errors:
            raise OwnedPayloadReleaseError("owned_payload.registered_release_failed") from errors[0]


class _TargetCommitAuthority:
    """Publish the first authoritative sink result before secondary runtime work."""

    def __init__(self) -> None:
        self._lock = RLock()
        self._result: Any | None = None

    @property
    def result(self) -> Any | None:
        with self._lock:
            return self._result

    def publish(self, load_result: Any) -> Any:
        with self._lock:
            if self._result is None:
                self._result = load_result
            return self._result


@dataclass(frozen=True, slots=True)
class OwnedPayloadScope:
    """One-way terminal owner for a single top-level extraction result."""

    artifact: object
    extraction_lifecycle: ExtractionLifecycleAuthority | None
    _terminal_authority: ArtifactTerminalAuthority
    _registry: _OwnedArtifactRegistry
    _target_commit_authority: _TargetCommitAuthority

    @classmethod
    def from_extract_result(cls, extract_result: object) -> OwnedPayloadScope:
        """Acquire ownership without consuming or mutating the source artifact."""

        artifact = getattr(extract_result, "artifact")
        lifecycle = getattr(extract_result, "extraction_lifecycle", None)
        if lifecycle is None:
            lifecycle = getattr(artifact, "extraction_lifecycle", None)
        if lifecycle is not None and not isinstance(lifecycle, ExtractionLifecycleAuthority):
            raise TypeError("owned_payload_scope.extraction_lifecycle_invalid")
        registry = _OwnedArtifactRegistry(lifecycle)
        registry.bind(artifact, initial=True)
        return cls(
            artifact=artifact,
            extraction_lifecycle=lifecycle,
            _terminal_authority=ArtifactTerminalAuthority(),
            _registry=registry,
            _target_commit_authority=_TargetCommitAuthority(),
        )

    def bind(self, artifact: object) -> None:
        """Register a transformed artifact view before it can reach a sink."""

        self._registry.bind(artifact)

    @property
    def terminal_receipt(self) -> ArtifactTerminalReceipt | None:
        """Return the immutable first terminal decision, if any."""

        return self._terminal_authority.receipt

    @property
    def target_commit_result(self) -> Any | None:
        """Return the sink result captured at the authoritative commit callback."""

        return self._target_commit_authority.result

    def require_completed_extraction(self) -> ExtractionLifecycleReceipt:
        """Return complete source timing evidence for durable target receipts."""

        authority = self.extraction_lifecycle
        if authority is None:
            raise ExtractionLifecycleStateError("extraction_lifecycle.authority_missing")
        return authority.require_completed()

    def require_acquired_extraction(self) -> ExtractionLifecycleReceipt:
        """Return the stable extraction start without claiming source completion.

        Planning may use this immutable boundary while an externally-owned
        snapshot lease remains open.  Durable target receipts must continue to
        use :meth:`require_completed_extraction`.
        """

        authority = self.extraction_lifecycle
        if authority is None:
            raise ExtractionLifecycleStateError("extraction_lifecycle.authority_missing")
        return authority.require_acquired()

    def success(self) -> ArtifactTerminalReceipt:
        """Release source resources after an authoritative target commit/replay."""

        return self.terminate(ArtifactTerminalOutcome.SUCCESS)

    def abort(self) -> ArtifactTerminalReceipt:
        """Roll back/close the source and delete attempt-local evidence."""

        return self.terminate(ArtifactTerminalOutcome.ABORT)

    def retain_commit_unknown(self) -> ArtifactTerminalReceipt:
        """Retain durable bytes but close all read-only source sessions."""

        return self.terminate(ArtifactTerminalOutcome.RETAIN_COMMIT_UNKNOWN)

    def terminate_for_error(self, error: BaseException) -> ArtifactTerminalReceipt:
        """Map a typed transaction disposition to the exact source outcome."""

        return self.terminate(_terminal_outcome_for_error(error))

    def mark_target_committed(self, load_result: Any) -> Any:
        """Freeze authoritative target evidence before secondary work.

        Resource terminalization remains owned by the top-level processor after
        checkpoints, quality evidence, and receipt publication reach their
        respective terminal boundary.
        """

        return self._target_commit_authority.publish(load_result)

    def terminate(self, outcome: ArtifactTerminalOutcome) -> ArtifactTerminalReceipt:
        """Issue one idempotent decision; cleanup failures remain receipt evidence."""

        return self._terminal_authority.terminate(
            outcome,
            release=self._registry.release,
            should_release=True,
        )


def _terminal_outcome_for_error(error: BaseException) -> ArtifactTerminalOutcome:
    typed = getattr(error, "artifact_terminal_outcome", None)
    if isinstance(typed, ArtifactTerminalOutcome):
        return typed
    disposition = getattr(error, "cleanup_disposition", None)
    value = getattr(disposition, "value", disposition)
    try:
        classified = ArtifactCleanupDisposition(value)
    except (TypeError, ValueError):
        return ArtifactTerminalOutcome.ABORT
    if classified is ArtifactCleanupDisposition.COMMITTED_SECONDARY_ONLY:
        return ArtifactTerminalOutcome.SUCCESS
    if classified is ArtifactCleanupDisposition.PRESERVE_EVIDENCE:
        return ArtifactTerminalOutcome.RETAIN_COMMIT_UNKNOWN
    return ArtifactTerminalOutcome.ABORT


def _terminate_artifact(artifact: object, outcome: ArtifactTerminalOutcome) -> None:
    terminate = getattr(artifact, "terminate", None)
    if callable(terminate):
        receipt = terminate(outcome)
        if getattr(receipt, "cleanup_succeeded", True) is False:
            raise OwnedPayloadReleaseError(
                str(getattr(receipt, "cleanup_error_code", None) or "owned_payload.release_failed")
            )
        return

    children = _owned_children(artifact)
    if children:
        errors: list[BaseException] = []
        for child in children:
            try:
                _terminate_artifact(child, outcome)
            except BaseException as exc:
                errors.append(exc)
        if errors:
            raise OwnedPayloadReleaseError("owned_payload.child_release_failed") from errors[0]
        return

    if outcome is ArtifactTerminalOutcome.RETAIN_COMMIT_UNKNOWN:
        return
    cleanup = getattr(artifact, "cleanup", None)
    if callable(cleanup):
        cleanup()


def _owned_children(artifact: object) -> tuple[object, ...]:
    """Resolve known composite extraction contracts without sink coupling."""

    delta = getattr(artifact, "delta_artifact", None)
    keys = getattr(artifact, "key_artifact", None)
    if delta is not None and keys is not None:
        return (delta, keys)
    return ()


def _bind_or_validate_lifecycle(
    artifact: object,
    lifecycle: ExtractionLifecycleAuthority | None,
    *,
    initial: bool,
) -> None:
    if lifecycle is None:
        return
    current = getattr(artifact, "extraction_lifecycle", None)
    if current is lifecycle:
        return
    if current is not None:
        raise ValueError("owned_payload_scope.extraction_lifecycle_changed")
    bind = getattr(artifact, "bind_extraction_lifecycle", None)
    if callable(bind):
        bind(lifecycle)
        return
    receipt = lifecycle.receipt
    snapshot_token = getattr(artifact, "snapshot_token", None)
    if receipt is not None and snapshot_token == receipt.source_token:
        return
    if not initial:
        raise ValueError("owned_payload_scope.extraction_lifecycle_unprovable")


__all__ = ["OwnedPayloadReleaseError", "OwnedPayloadScope"]
