"""Protected workspace witness operations and complete immutable readbacks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from dpone.contracts.dbt_workspace_channel import WorkspaceChannel, WorkspaceHandoverError
from dpone.contracts.dbt_workspace_handover import MAX_CHANNEL_REVISION, WorkspaceHandoverClaim
from dpone.contracts.dbt_workspace_lifecycle import DbtWorkspaceLifecycleReadback
from dpone.contracts.dbt_workspace_registration_baseline import WorkspaceAdoptedCurrent

if TYPE_CHECKING:
    from dpone.contracts.dbt_workspace_activation import DbtWorkspaceActivationRequest


@dataclass(frozen=True, slots=True)
class WorkspaceStoredOccurrence:
    """Reconstructible snapshot plus fresh protected original ownership readback."""

    snapshot: WorkspaceHandoverClaim | WorkspaceAdoptedCurrent
    request: DbtWorkspaceActivationRequest
    lifecycle: DbtWorkspaceLifecycleReadback

    def __post_init__(self) -> None:
        try:
            if not isinstance(self.lifecycle, DbtWorkspaceLifecycleReadback):
                raise ValueError
            self.lifecycle.require_request(self.request)
            if isinstance(self.snapshot, WorkspaceAdoptedCurrent):
                self.snapshot.__post_init__()
                if self.snapshot.request != self.request or self.snapshot.guard_epochs != self.lifecycle.guards:
                    raise ValueError
            elif isinstance(self.snapshot, WorkspaceHandoverClaim):
                self.snapshot.require_request(self.request)
            else:
                raise ValueError
        except (ValueError, TypeError, AttributeError):
            raise WorkspaceHandoverError("stored_occurrence") from None


@dataclass(frozen=True, slots=True)
class WorkspaceChannelReadback:
    """One coherent database snapshot; local pointer state is deliberately absent."""

    channel: WorkspaceChannel
    revision: int
    current: WorkspaceStoredOccurrence | None
    pending: WorkspaceHandoverClaim | None
    pending_occurrence: WorkspaceStoredOccurrence | None

    def __post_init__(self) -> None:
        try:
            if (
                not isinstance(self.channel, WorkspaceChannel)
                or type(self.revision) is not int
                or not 0 <= self.revision <= MAX_CHANNEL_REVISION
            ):
                raise ValueError
            self.channel.__post_init__()
            completed_revision = 0
            if self.current is not None:
                self.current.__post_init__()
                snapshot = self.current.snapshot
                if isinstance(snapshot, WorkspaceHandoverClaim):
                    if snapshot.channel != self.channel:
                        raise ValueError
                    completed_revision = snapshot.claim_revision + 1
                else:
                    snapshot.require_channel(self.channel)
            if self.pending is None:
                if self.pending_occurrence is not None or self.revision != completed_revision:
                    raise ValueError
                if self.current is not None and self.current.lifecycle.state != "ACTIVE":
                    raise ValueError
            else:
                self._require_pending(completed_revision)
        except (ValueError, TypeError, AttributeError):
            raise WorkspaceHandoverError("channel_readback") from None

    def _require_pending(self, completed_revision: int) -> None:
        claim = self.pending
        if not isinstance(claim, WorkspaceHandoverClaim):
            raise ValueError
        claim.__post_init__()
        if (
            claim.channel != self.channel
            or self.revision != claim.claim_revision
            or claim.expected_channel_revision != completed_revision
        ):
            raise ValueError
        previous = (claim.predecessor_activation_id, claim.predecessor_deployment_id, claim.predecessor_request_sha256)
        expected = (
            (None, None, None)
            if self.current is None
            else (
                self.current.request.activation_id,
                self.current.request.deployment_id,
                self.current.request.request_sha256,
            )
        )
        if previous != expected or (self.current is not None and self.current.lifecycle.state == "PREPARED"):
            raise ValueError
        if self.pending_occurrence is not None:
            self.pending_occurrence.__post_init__()
            if (
                self.pending_occurrence.snapshot != claim
                or self.pending_occurrence.lifecycle.state != "PREPARED"
                or (self.current is not None and self.current.lifecycle.state != "RETIRED")
            ):
                raise ValueError


class WorkspaceHandoverStorePort(Protocol):
    """Atomic gateway, not a mutable local journal or a source of desired content.

    Every mutation returns fresh exact protected readback. Unknown acknowledgement
    requires a new connection/read of the same claim, never a replacement identity.
    Each transition validates complete historical ownership and native predicates in
    its SQL transaction. Registration is a separate privileged capability.
    """

    def read_channel(self, channel: WorkspaceChannel) -> WorkspaceChannelReadback:
        """Fail closed if unregistered or immutable/channel authority differs."""
        ...

    def claim(self, claim: WorkspaceHandoverClaim, *, expected_revision: int) -> WorkspaceChannelReadback:
        """CAS current/pending/revision or read the identical already-saved claim."""
        ...

    def begin_retirement(self, claim: WorkspaceHandoverClaim) -> WorkspaceChannelReadback:
        """Close new predecessor attempts while preserving existing exact epochs."""
        ...

    def finalize_retirement(self, claim: WorkspaceHandoverClaim) -> WorkspaceChannelReadback:
        """Release only after terminal receipts and connector quiescence prove closure."""
        ...

    def prepare_successor(
        self, claim: WorkspaceHandoverClaim, request: DbtWorkspaceActivationRequest
    ) -> WorkspaceChannelReadback:
        """Persist original request and PREPARED atomically; retries use saved request."""
        ...

    def complete(self, claim: WorkspaceHandoverClaim) -> WorkspaceChannelReadback:
        """After local pointer commit, atomically persist ACTIVE and channel current."""
        ...
