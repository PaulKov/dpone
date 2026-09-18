"""Typed production ports behind the external-replication runtime seam."""

from __future__ import annotations

import secrets
from collections.abc import Callable, Mapping
from dataclasses import replace
from typing import Any, NoReturn

from dpone.ports.clickhouse_external_replication import (
    ExternalArtifactReceipt,
    ExternalArtifactSourcePort,
    ExternalAuthorityMutationResult,
    ExternalAuthorityMutationStatus,
    ExternalAuthorityPhase,
    ExternalAuthorityPort,
    ExternalAuthorityRecord,
    ExternalClusterDdlPort,
    ExternalDispatchPermit,
    ExternalMemberRecord,
    ExternalMemberStageState,
    ExternalPublicationError,
    ExternalReplicaStagingPort,
    ExternalTopologyCatalogPort,
    MemberPublicationState,
    QueueEntry,
    QueueState,
    VersionedExternalAuthorityRecord,
    classify_member_publication,
    derive_target_key,
)
from dpone.runtime.sinks.clickhouse_external_replication_state import (
    candidate_observation as _candidate,
)
from dpone.runtime.sinks.clickhouse_external_replication_state import (
    complete_candidate as _complete,
)
from dpone.runtime.sinks.clickhouse_external_replication_state import desired_generation as _desired
from dpone.runtime.sinks.clickhouse_external_replication_state import equivalent_state as _equivalent
from dpone.runtime.sinks.clickhouse_external_replication_state import generation_shape as _shape
from dpone.runtime.sinks.clickhouse_external_replication_state import has_predecessor as _has_predecessor
from dpone.runtime.sinks.clickhouse_external_replication_state import (
    member as _member,
)
from dpone.runtime.sinks.clickhouse_external_replication_state import (
    member_ids as _ids,
)
from dpone.runtime.sinks.clickhouse_external_replication_state import (
    record_from_state,
    require_queue_entry,
    state_from_versioned,
)
from dpone.runtime.sinks.clickhouse_external_replication_state import (
    replace_member as _replace_member,
)


def _state(current: VersionedExternalAuthorityRecord) -> dict[str, Any]:
    return state_from_versioned(current)


def _target_key(adapter: ClickHouseExternalReplicationServiceAdapter) -> str:
    return derive_target_key(adapter._cluster, adapter._database, adapter._target)


def _inventory(adapter: ClickHouseExternalReplicationServiceAdapter) -> str:
    if adapter._inventory_digest is None:
        _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_INVENTORY_INVALID")
    return adapter._inventory_digest


def _take_permit(
    adapter: ClickHouseExternalReplicationServiceAdapter, operation_id: str, action: str
) -> ExternalDispatchPermit:
    permit = adapter._permits.pop((operation_id, action), None)
    if permit is None:
        _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CAS_UNKNOWN")
    return permit


def _error(code: str, record: ExternalAuthorityRecord | None = None) -> NoReturn:
    evidence: dict[str, object] = {"evidence_scope": "runtime", "member_ids": []}
    if record is not None:
        evidence.update(
            target_key=record.target_key,
            operation_id=record.operation_id,
            phase=record.phase.value,
            member_ids=list(_ids(record)),
        )
    raise ExternalPublicationError(code, evidence=evidence)


class ClickHouseExternalReplicationServiceAdapter:
    """Translate the runtime coordinator into fenced typed port operations."""

    def __init__(
        self,
        *,
        topology: ExternalTopologyCatalogPort,
        authority: ExternalAuthorityPort,
        staging: ExternalReplicaStagingPort,
        ddl: ExternalClusterDdlPort,
        cluster: str,
        database: str,
        target: str,
        token_factory: Callable[[], str] | None = None,
        evidence_scope: str = "mocked_in_process",
    ) -> None:
        self._topology_port = topology
        self._authority = authority
        self._staging = staging
        self._ddl = ddl
        self._cluster = cluster
        self._database = database
        self._target = target
        self._token = token_factory or (lambda: secrets.token_hex(16))
        self.evidence_scope = evidence_scope
        self._inventory_digest: str | None = None
        self._permits: dict[tuple[str, str], ExternalDispatchPermit] = {}

    def inventory(self, cluster: str) -> tuple[str, ...]:
        if cluster != self._cluster:
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_INVENTORY_DRIFT")
        topology = self._topology_port.inventory(cluster)
        topology.validate()
        digest = getattr(self._topology_port, "inventory_digest", topology.digest)
        self._inventory_digest = str(digest)
        return tuple(member.member_id for member in topology.ordered_members)

    def inventory_digest(self) -> str:
        return _inventory(self)

    def read_authority(self, target_key: str) -> Mapping[str, Any] | None:
        current = self._authority.read_versioned(target_key)
        return None if current is None else _state(current)

    def compare_and_swap_authority(
        self, target_key: str, expected_version: int | None, desired: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        current = self._authority.read_versioned(target_key)
        if current is None:
            if expected_version is not None:
                _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CAS_CONFLICT")
            return self._verified(self._authority.create_if_absent(self._adopt(self._record(desired, None))), None)
        if current.version != expected_version:
            if _equivalent(_state(current), desired):
                return _state(current)
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CAS_CONFLICT", current.record)
        record = self._record(desired, current.record)
        if record.phase is ExternalAuthorityPhase.LOCKED and record.operation_id != current.record.operation_id:
            record = self._adopt(record)
        action: str | None = None
        if record.phase is ExternalAuthorityPhase.PUBLICATION_DISPATCHING:
            if set(self._publication_states(record).values()) != {MemberPublicationState.PENDING}:
                _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", record)
            action = "publication"
            record = replace(record, publication_correlation_token=self._token())
            record = replace(
                record,
                publication_query_digest=self._ddl.publication_query_digest(record, cluster=self._cluster),
            )
        elif record.phase is ExternalAuthorityPhase.COMMITTED:
            entry, members = self._require_publication(record)
            record = replace(record, publication_entry=entry.entry, members=members)
        elif record.phase is ExternalAuthorityPhase.CLEANUP_DISPATCHING and _has_predecessor(record):
            action = "cleanup"
            record = replace(record, cleanup_correlation_token=self._token())
            record = replace(
                record,
                cleanup_query_digest=self._ddl.cleanup_query_digest(record, cluster=self._cluster),
            )
        elif record.phase is ExternalAuthorityPhase.COMPLETED:
            cleanup_entry, members = self._require_cleanup(record)
            record = replace(
                record,
                cleanup_entry=None if cleanup_entry is None else cleanup_entry.entry,
                members=members,
            )
        return self._verified(self._authority.compare_and_swap(current, record), action)

    def observe_candidate(self, member_id: str, candidate_name: str) -> Mapping[str, Any]:
        record = self._member_current(member_id).record
        if record.candidate != candidate_name:
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", record)
        return _candidate(self._staging.observe(member_id, record), record)

    def stage_member_once(
        self,
        member_id: str,
        *,
        operation_id: str,
        candidate_name: str,
        artifact: ExternalArtifactReceipt,
        source: ExternalArtifactSourcePort,
    ) -> Mapping[str, Any]:
        current = self._member_current(member_id)
        record = current.record
        if (
            record.operation_id != operation_id
            or record.candidate != candidate_name
            or record.artifact != artifact.identity
            or source.identity != artifact.identity
            or record.artifact_binding_id != source.binding_id
        ):
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_ARTIFACT_CHANGED", record)
        source.revalidate(artifact.identity)
        source.open_replay()
        observed = self._staging.observe(member_id, record)
        member = _member(record, member_id)
        if observed.candidate is None:
            created = self._staging.create_candidate(member_id, record)
            member = replace(member, stage_state=ExternalMemberStageState.CANDIDATE_BOUND, candidate=created)
            current = self._typed_cas(current, replace(record, members=_replace_member(record, member)))
            record = current.record
        elif member.candidate != observed.candidate:
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", record)
        error: Exception | None = None
        try:
            self._staging.load_candidate(member_id, record, source)
        except Exception as caught:  # reconcile a possibly-lost load reply
            error = caught
        observed = self._staging.observe(member_id, record)
        if _complete(observed.candidate, record.artifact):
            ready = replace(
                _member(record, member_id),
                stage_state=ExternalMemberStageState.READY,
                candidate=observed.candidate,
            )
            self._typed_cas(current, replace(record, members=_replace_member(record, ready)))
            return _candidate(observed, record)
        if error is not None:
            raise error
        return _candidate(observed, record)

    def drop_owned_candidate(self, member_id: str, *, candidate_uuid: str) -> None:
        record = self._member_current(member_id).record
        expected = _member(record, member_id).candidate
        if expected is None or expected.uuid != candidate_uuid:
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", record)
        self._staging.drop_candidate(member_id, record, expected)
        if self._staging.observe(member_id, record).candidate is not None:
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_STAGING_INCOMPLETE", record)

    def dispatch_publication_once(self, *, operation_id: str, candidate_name: str, member_ids: tuple[str, ...]) -> None:
        record = self._current(operation_id).record
        if record.candidate != candidate_name or _ids(record) != member_ids:
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", record)
        if set(self._publication_states(record).values()) != {MemberPublicationState.PENDING}:
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", record)
        self._ddl.dispatch_publication(record, _take_permit(self, operation_id, "publication"), cluster=self._cluster)

    def observe_publication(self, operation_id: str) -> Mapping[str, str]:
        record = self._current(operation_id).record
        entry = require_queue_entry(self._ddl, self._cluster, record, cleanup=False, fail=_error)
        states = self._publication_states(record)
        queue = entry.state_for(_ids(record))
        if MemberPublicationState.UNKNOWN in states.values() or queue is QueueState.UNKNOWN:
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_DDL_UNKNOWN", record)
        if queue in {QueueState.TERMINAL_SUCCESS, QueueState.TERMINAL_FAILURE} and len(set(states.values())) > 1:
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_PUBLICATION_PARTIAL_TERMINAL", record)
        return {
            member_id: "desired" if state is MemberPublicationState.COMMITTED else "predecessor"
            for member_id, state in states.items()
        }

    def dispatch_cleanup_once(self, *, operation_id: str, member_ids: tuple[str, ...]) -> None:
        record = self._current(operation_id).record
        if _ids(record) != member_ids:
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_INVENTORY_DRIFT", record)
        if _has_predecessor(record):
            if set(self._cleanup_states(record).values()) != {True}:
                _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_UNKNOWN", record)
            self._ddl.drop_predecessor(record, _take_permit(self, operation_id, "cleanup"), cluster=self._cluster)

    def observe_cleanup(self, operation_id: str) -> Mapping[str, bool]:
        record = self._current(operation_id).record
        if (
            record.phase is ExternalAuthorityPhase.CLEANUP_DISPATCHING
            and _has_predecessor(record)
            and require_queue_entry(self._ddl, self._cluster, record, cleanup=True, fail=_error).state_for(_ids(record))
            is QueueState.UNKNOWN
        ):
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_UNKNOWN", record)
        return self._cleanup_states(record)

    def _record(self, state: Mapping[str, Any], base: ExternalAuthorityRecord | None) -> ExternalAuthorityRecord:
        return record_from_state(
            state,
            base,
            database=self._database,
            target=self._target,
            inventory_digest=_inventory(self),
        )

    def _adopt(self, record: ExternalAuthorityRecord) -> ExternalAuthorityRecord:
        observed = tuple(self._staging.observe(member.member_id, record) for member in record.members)
        if any(value.candidate is not None for value in observed):
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", record)
        targets = tuple(value.target for value in observed)
        mixed_presence = any(value is None for value in targets) and any(value is not None for value in targets)
        if mixed_presence or len({_shape(value) for value in targets if value is not None}) > 1:
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_BASELINE_DIVERGED", record)
        return replace(
            record,
            members=tuple(replace(member, predecessor=value) for member, value in zip(record.members, targets)),
        )

    def _require_publication(
        self, record: ExternalAuthorityRecord
    ) -> tuple[QueueEntry, tuple[ExternalMemberRecord, ...]]:
        entry = require_queue_entry(self._ddl, self._cluster, record, cleanup=False, fail=_error)
        if entry.state_for(_ids(record)) not in {QueueState.TERMINAL_SUCCESS, QueueState.TERMINAL_FAILURE}:
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_PUBLICATION_IN_PROGRESS", record)
        states = self._publication_states(record)
        if set(states.values()) != {MemberPublicationState.COMMITTED}:
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_PUBLICATION_PARTIAL_TERMINAL", record)
        members = tuple(replace(member, publication_state=states[member.member_id]) for member in record.members)
        return entry, members

    def _require_cleanup(
        self, record: ExternalAuthorityRecord
    ) -> tuple[QueueEntry | None, tuple[ExternalMemberRecord, ...]]:
        entry = (
            require_queue_entry(self._ddl, self._cluster, record, cleanup=True, fail=_error)
            if _has_predecessor(record)
            else None
        )
        if entry is not None and entry.state_for(_ids(record)) not in {
            QueueState.TERMINAL_SUCCESS,
            QueueState.TERMINAL_FAILURE,
        }:
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_IN_PROGRESS", record)
        if any(self._cleanup_states(record).values()):
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_IN_PROGRESS", record)
        return entry, tuple(replace(member, cleanup_complete=True) for member in record.members)

    def _publication_states(self, record: ExternalAuthorityRecord) -> dict[str, MemberPublicationState]:
        return {
            member.member_id: classify_member_publication(
                self._staging.observe(member.member_id, record),
                desired=_desired(member),
                predecessor=member.predecessor,
            )
            for member in record.members
        }

    def _cleanup_states(self, record: ExternalAuthorityRecord) -> dict[str, bool]:
        result: dict[str, bool] = {}
        for member_id, state in self._publication_states(record).items():
            if state not in {MemberPublicationState.COMMITTED, MemberPublicationState.CLEANUP_PENDING}:
                _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_UNKNOWN", record)
            result[member_id] = (
                _member(record, member_id).predecessor is not None and state is MemberPublicationState.COMMITTED
            )
        return result

    def _typed_cas(
        self, current: VersionedExternalAuthorityRecord, desired: ExternalAuthorityRecord
    ) -> VersionedExternalAuthorityRecord:
        return self._require_verified(self._authority.compare_and_swap(current, desired), None)

    def _verified(self, result: ExternalAuthorityMutationResult, action: str | None) -> Mapping[str, Any]:
        return _state(self._require_verified(result, action))

    def _require_verified(
        self, result: ExternalAuthorityMutationResult, action: str | None
    ) -> VersionedExternalAuthorityRecord:
        if result.status is not ExternalAuthorityMutationStatus.VERIFIED or result.observed is None:
            suffix = "CAS_CONFLICT" if result.status is ExternalAuthorityMutationStatus.CONFLICT else "CAS_UNKNOWN"
            _error(f"DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_{suffix}")
        if action is not None:
            if result.permit is None:
                _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CAS_UNKNOWN", result.observed.record)
            self._permits[(result.observed.record.operation_id, action)] = result.permit
        return result.observed

    def _current(self, operation_id: str) -> VersionedExternalAuthorityRecord:
        current = self._authority.read_versioned(_target_key(self))
        if current is None or current.record.operation_id != operation_id:
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_AUTHORITY_CONFLICT")
        return current

    def _member_current(self, member_id: str) -> VersionedExternalAuthorityRecord:
        current = self._authority.read_versioned(_target_key(self))
        if current is None or member_id not in _ids(current.record):
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_AUTHORITY_CONFLICT")
        return current


__all__ = ["ClickHouseExternalReplicationServiceAdapter"]
