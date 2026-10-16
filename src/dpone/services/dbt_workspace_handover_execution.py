"""Bounded application executor for the protected workspace handover saga.

No local journal is authoritative. All SQL mutations are injected atomic store
operations; external verification and artifact I/O never share their transaction.
Gateway refusals and unknown acknowledgements propagate without automatic retry.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from dpone.contracts.dbt_workspace_channel import WorkspaceChannel, WorkspaceHandoverError
from dpone.contracts.dbt_workspace_handover import WorkspaceHandoverClaim
from dpone.ports.dbt_workspace_handover import WorkspaceChannelReadback, WorkspaceHandoverStorePort
from dpone.ports.dbt_workspace_handover_execution import VerifiedWorkspaceDesired, WorkspaceHandoverExecutionPort
from dpone.services.dbt_workspace_handover import WorkspaceHandoverAction, plan_workspace_handover


class WorkspaceHandoverCycleStatus(StrEnum):
    """Bounded outcomes; exceptions retain their specific operational error code."""

    CONVERGED = "CONVERGED"
    CONTINUATION_REQUIRED = "CONTINUATION_REQUIRED"


@dataclass(frozen=True, slots=True)
class WorkspaceHandoverCycleResult:
    """Last-observed convergence, not a cross-store linearizability guarantee."""

    status: WorkspaceHandoverCycleStatus
    channel_revision: int
    activation_id: str | None

    @property
    def converged(self) -> bool:
        return self.status is WorkspaceHandoverCycleStatus.CONVERGED


class WorkspaceHandoverExecutor:
    """Progress at most two transitions, including a previously pending claim.

    Every iteration rereads protected authority. Contention consumes the bounded
    step budget; unchanged mutation readback yields continuation immediately.
    Store/driver errors (including COMMIT_UNKNOWN and WAITING_ATTEMPTS) escape
    unchanged. A later existing watcher cycle resolves them by fresh exact read.
    """

    def __init__(
        self, *, channel: WorkspaceChannel, store: WorkspaceHandoverStorePort, driver: WorkspaceHandoverExecutionPort
    ) -> None:
        channel.__post_init__()
        self._channel = channel
        self._store = store
        self._driver = driver

    def run_cycle(self) -> WorkspaceHandoverCycleResult:
        progressed: set[str] = set()
        readback = self._read()
        for _ in range(24):
            self._verify(readback)
            occurrence = readback.pending_occurrence if readback.pending is not None else readback.current
            matches = occurrence is not None and self._driver.local_matches(occurrence)
            restore_current = readback.current is not None and not matches
            latest = (
                None if readback.pending is not None or restore_current else self._driver.verified_latest(self._channel)
            )
            if latest is not None:
                latest.__post_init__()
            action = plan_workspace_handover(
                readback, latest=None if latest is None else latest.desired, local_matches=matches
            )
            if action is WorkspaceHandoverAction.CONVERGED:
                fresh = self._read()
                if fresh == readback and fresh.current is not None and self._driver.local_matches(fresh.current):
                    return self._result(fresh, WorkspaceHandoverCycleStatus.CONVERGED)
                readback = fresh
                continue
            if action in {WorkspaceHandoverAction.REPLICATE_CURRENT, WorkspaceHandoverAction.REPLICATE_SUCCESSOR}:
                if occurrence is None:
                    raise WorkspaceHandoverError("missing_replica_occurrence")
                if readback.pending is not None and not self._admit_progress(progressed, readback.pending):
                    break
                self._driver.replicate(occurrence)
                readback = self._read()
                continue
            if action is WorkspaceHandoverAction.CLAIM_LATEST:
                if latest is None:
                    raise WorkspaceHandoverError("missing_remote_observation")
                claim = self._driver.build_claim(readback, latest)
                self._require_claim(readback, latest, claim)
                self._driver.verify_snapshot(self._channel, claim)
                if not self._admit_progress(progressed, claim):
                    break
                if not self._driver.remote_revision_matches(latest):
                    readback = self._read()
                    continue
                updated = self._store.claim(claim, expected_revision=readback.revision)
            else:
                pending = readback.pending
                if pending is None:
                    raise WorkspaceHandoverError("missing_protected_claim")
                if not self._admit_progress(progressed, pending):
                    break
                updated = self._advance(action, readback, pending)
            self._validate_readback(updated)
            fresh = self._read()
            if fresh == readback and updated == readback:
                return self._result(fresh, WorkspaceHandoverCycleStatus.CONTINUATION_REQUIRED)
            readback = fresh
        return self._result(readback, WorkspaceHandoverCycleStatus.CONTINUATION_REQUIRED)

    def _advance(
        self, action: WorkspaceHandoverAction, readback: WorkspaceChannelReadback, claim: WorkspaceHandoverClaim
    ) -> WorkspaceChannelReadback:
        if action is WorkspaceHandoverAction.BEGIN_RETIREMENT:
            return self._store.begin_retirement(claim)
        if action is WorkspaceHandoverAction.FINALIZE_RETIREMENT:
            return self._store.finalize_retirement(claim)
        if action is WorkspaceHandoverAction.PREPARE_SUCCESSOR:
            if readback.current is not None and readback.current.lifecycle.state != "RETIRED":
                raise WorkspaceHandoverError("predecessor_not_retired")
            request = self._driver.observe_successor(claim)
            claim.require_request(request)
            return self._store.prepare_successor(claim, request)
        if action is WorkspaceHandoverAction.COMPLETE:
            return self._store.complete(claim)
        raise WorkspaceHandoverError("unexpected_handover_action")

    def _read(self) -> WorkspaceChannelReadback:
        readback = self._store.read_channel(self._channel)
        self._validate_readback(readback)
        return readback

    def _validate_readback(self, readback: WorkspaceChannelReadback) -> None:
        if not isinstance(readback, WorkspaceChannelReadback) or readback.channel != self._channel:
            raise WorkspaceHandoverError("store_channel_mismatch")
        readback.__post_init__()

    def _verify(self, readback: WorkspaceChannelReadback) -> None:
        if readback.current is not None:
            self._driver.verify_snapshot(self._channel, readback.current.snapshot)
        if readback.pending is not None:
            self._driver.verify_snapshot(self._channel, readback.pending)

    @staticmethod
    def _admit_progress(progressed: set[str], claim: WorkspaceHandoverClaim) -> bool:
        identity = claim.successor_activation_id
        if identity not in progressed and len(progressed) == 2:
            return False
        progressed.add(identity)
        return True

    @staticmethod
    def _require_claim(
        readback: WorkspaceChannelReadback, latest: VerifiedWorkspaceDesired, claim: WorkspaceHandoverClaim
    ) -> None:
        if not isinstance(claim, WorkspaceHandoverClaim):
            raise WorkspaceHandoverError("claim_proposal")
        claim.__post_init__()
        previous = readback.current
        expected_previous = (
            (None, None, None)
            if previous is None
            else (previous.request.activation_id, previous.request.deployment_id, previous.request.request_sha256)
        )
        if (
            claim.channel != readback.channel
            or claim.expected_channel_revision != readback.revision
            or claim.desired_state_json != latest.desired_json
            or claim.observed_remote_revision != latest.revision
            or (
                claim.predecessor_activation_id,
                claim.predecessor_deployment_id,
                claim.predecessor_request_sha256,
            )
            != expected_previous
        ):
            raise WorkspaceHandoverError("claim_proposal")

    @staticmethod
    def _result(
        readback: WorkspaceChannelReadback, status: WorkspaceHandoverCycleStatus
    ) -> WorkspaceHandoverCycleResult:
        return WorkspaceHandoverCycleResult(
            status, readback.revision, None if readback.current is None else readback.current.request.activation_id
        )
