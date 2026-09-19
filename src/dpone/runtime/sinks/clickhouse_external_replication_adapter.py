"""Typed production ports behind the external-replication runtime seam."""

from __future__ import annotations

import secrets
from collections.abc import Callable, Mapping
from dataclasses import replace
from typing import Any
from uuid import uuid4

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
    ExternalMemberStageState,
    ExternalReplicaStagingPort,
    ExternalTopologyCatalogPort,
    MemberPublicationState,
    VersionedExternalAuthorityRecord,
    derive_target_key,
)
from dpone.runtime.sinks import clickhouse_external_replication_adapter_ddl as ddl_ops
from dpone.runtime.sinks.clickhouse_external_replication_state import (
    candidate_observation as _candidate,
)
from dpone.runtime.sinks.clickhouse_external_replication_state import (
    complete_candidate as _complete,
)
from dpone.runtime.sinks.clickhouse_external_replication_state import equivalent_state as _equivalent
from dpone.runtime.sinks.clickhouse_external_replication_state import fail_external as _error
from dpone.runtime.sinks.clickhouse_external_replication_state import generation_shape as _shape
from dpone.runtime.sinks.clickhouse_external_replication_state import has_predecessor as _has_predecessor
from dpone.runtime.sinks.clickhouse_external_replication_state import member as _member
from dpone.runtime.sinks.clickhouse_external_replication_state import (
    member_ids as _ids,
)
from dpone.runtime.sinks.clickhouse_external_replication_state import (
    record_from_state,
    require_stage_request,
    reusable_candidate_observation,
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
        evidence_status: str = "UNVERIFIED",
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
        self.evidence_status = evidence_status
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
            if set(ddl_ops.publication_states(self, record).values()) != {MemberPublicationState.PENDING}:
                _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", record)
            action = "publication"
            record = replace(record, publication_correlation_token=self._token())
            record = replace(
                record,
                publication_query_digest=self._ddl.publication_query_digest(record, cluster=self._cluster),
            )
        elif record.phase is ExternalAuthorityPhase.COMMITTED:
            entry, members = ddl_ops.require_publication(self, record)
            record = replace(record, publication_entry=entry.entry, members=members)
        elif record.phase is ExternalAuthorityPhase.CLEANUP_DISPATCHING and _has_predecessor(record):
            action = "cleanup"
            record = replace(record, cleanup_correlation_token=self._token())
            record = replace(
                record,
                cleanup_query_digest=self._ddl.cleanup_query_digest(record, cluster=self._cluster),
            )
        elif record.phase is ExternalAuthorityPhase.COMPLETED:
            cleanup_entry, members = ddl_ops.require_cleanup(self, record)
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
        require_stage_request(
            record,
            operation_id=operation_id,
            candidate_name=candidate_name,
            artifact=artifact,
            source=source,
            fail=_error,
        )
        source.revalidate(artifact.identity)
        observed = self._staging.observe(member_id, record)
        member = _member(record, member_id)
        reusable = reusable_candidate_observation(observed, member, record, fail=_error)
        if reusable is not None:
            return reusable
        if record.phase is not ExternalAuthorityPhase.STAGING:
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_STAGING_INCOMPLETE", record)
        created_here = False
        if observed.candidate is None:
            intent = member.candidate_uuid_intent or str(uuid4())
            if member.candidate_uuid_intent is None:
                member = replace(
                    member,
                    stage_state=ExternalMemberStageState.CREATE_INTENT,
                    candidate_uuid_intent=intent,
                )
                current = self._typed_cas(current, replace(record, members=_replace_member(record, member)))
                record = current.record
            created = self._staging.create_candidate(member_id, record, expected_uuid=intent)
            member = replace(
                member,
                stage_state=ExternalMemberStageState.CANDIDATE_BOUND,
                candidate_uuid_intent=intent,
                candidate=created,
            )
            current = self._typed_cas(current, replace(record, members=_replace_member(record, member)))
            record = current.record
            created_here = True
        elif member.candidate is None and member.candidate_uuid_intent == observed.candidate.uuid:
            member = replace(
                member,
                stage_state=ExternalMemberStageState.CANDIDATE_BOUND,
                candidate=observed.candidate,
            )
            current = self._typed_cas(current, replace(record, members=_replace_member(record, member)))
            record = current.record
            created_here = True
        elif member.candidate != observed.candidate:
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", record)
        if not created_here:
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_STAGING_INCOMPLETE", record)
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
        intent = _member(record, member_id).candidate_uuid_intent
        if expected is None and intent == candidate_uuid:
            observed = self._staging.observe(member_id, record).candidate
            expected = observed if observed is not None and observed.uuid == intent else None
        if expected is None or expected.uuid != candidate_uuid:
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", record)
        self._staging.drop_candidate(member_id, record, expected)
        if self._staging.observe(member_id, record).candidate is not None:
            _error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_STAGING_INCOMPLETE", record)

    def dispatch_publication_once(self, *, operation_id: str, candidate_name: str, member_ids: tuple[str, ...]) -> None:
        ddl_ops.dispatch_publication_once(
            self, operation_id=operation_id, candidate_name=candidate_name, member_ids=member_ids
        )

    def observe_publication(self, operation_id: str) -> Mapping[str, str]:
        return ddl_ops.observe_publication(self, operation_id)

    def dispatch_cleanup_once(self, *, operation_id: str, member_ids: tuple[str, ...]) -> None:
        ddl_ops.dispatch_cleanup_once(self, operation_id=operation_id, member_ids=member_ids)

    def observe_cleanup(self, operation_id: str) -> Mapping[str, bool]:
        return ddl_ops.observe_cleanup(self, operation_id)

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
