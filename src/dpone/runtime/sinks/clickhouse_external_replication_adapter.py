"""Typed production ports behind the external-replication runtime seam."""

from __future__ import annotations

import secrets
from collections.abc import Callable, Mapping
from dataclasses import replace
from typing import Any, NoReturn, Protocol

from dpone.contracts.clickhouse_cluster_publication import QueueEntry, QueueState
from dpone.contracts.clickhouse_external_replication import (
    ArtifactIdentity,
    ExternalArtifactReceipt,
    ExternalAuthorityPhase,
    ExternalAuthorityRecord,
    ExternalMemberRecord,
    ExternalMemberStageState,
    ExternalPublicationError,
    MemberGenerationObservation,
    MemberPublicationState,
    PhysicalGeneration,
    classify_member_publication,
    derive_target_key,
)
from dpone.ports.clickhouse_external_replication import (
    ExternalAuthorityMutationResult,
    ExternalAuthorityMutationStatus,
    ExternalAuthorityPort,
    ExternalClusterDdlPort,
    ExternalDispatchPermit,
    ExternalReplicaStagingPort,
    ExternalTopologyCatalogPort,
    VersionedExternalAuthorityRecord,
)


class ExternalArtifactSourcePort(Protocol):
    """Invocation-scoped immutable artifact and its explicit load capability."""

    @property
    def identity(self) -> ArtifactIdentity: ...

    def revalidate(self, expected: ArtifactIdentity) -> None: ...

    def load_candidate(
        self,
        staging: ExternalReplicaStagingPort,
        member_id: str,
        record: ExternalAuthorityRecord,
    ) -> None: ...


class ClickHouseExternalReplicationServiceAdapter:
    """Translate the runtime coordinator into fenced typed port operations."""

    def __init__(
        self,
        *,
        topology: ExternalTopologyCatalogPort,
        authority: ExternalAuthorityPort,
        source: ExternalArtifactSourcePort,
        staging: ExternalReplicaStagingPort,
        ddl: ExternalClusterDdlPort,
        cluster: str,
        database: str,
        target: str,
        token_factory: Callable[[], str] | None = None,
    ) -> None:
        self._topology_port = topology
        self._authority = authority
        self._source = source
        self._staging = staging
        self._ddl = ddl
        self._cluster = cluster
        self._database = database
        self._target = target
        self._token = token_factory or (lambda: secrets.token_hex(16))
        self._inventory_digest: str | None = None
        self._permits: dict[tuple[str, str], ExternalDispatchPermit] = {}

    def inventory(self, cluster: str) -> tuple[str, ...]:
        if cluster != self._cluster:
            self._error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_INVENTORY_DRIFT")
        topology = self._topology_port.inventory(cluster)
        topology.validate()
        self._inventory_digest = topology.digest
        return tuple(member.member_id for member in topology.ordered_members)

    def read_authority(self, target_key: str) -> Mapping[str, Any] | None:
        current = self._authority.read_versioned(target_key)
        return None if current is None else self._state(current)

    def compare_and_swap_authority(
        self, target_key: str, expected_version: int | None, desired: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        current = self._authority.read_versioned(target_key)
        if current is None:
            if expected_version is not None:
                self._error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CAS_CONFLICT")
            return self._verified(self._authority.create_if_absent(self._adopt(self._record(desired, None))), None)
        if current.version != expected_version:
            if _equivalent(self._state(current), desired):
                return self._state(current)
            self._error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CAS_CONFLICT", current.record)
        record = self._record(desired, current.record)
        action: str | None = None
        if record.phase is ExternalAuthorityPhase.PUBLICATION_DISPATCHING:
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
            self._error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", record)
        return self._candidate(self._staging.observe(member_id, record), record)

    def stage_member_once(
        self,
        member_id: str,
        *,
        operation_id: str,
        candidate_name: str,
        artifact: ExternalArtifactReceipt,
    ) -> Mapping[str, Any]:
        current = self._member_current(member_id)
        record = current.record
        if (
            record.operation_id != operation_id
            or record.candidate != candidate_name
            or record.artifact != artifact.identity
            or self._source.identity != artifact.identity
        ):
            self._error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_ARTIFACT_CHANGED", record)
        self._source.revalidate(artifact.identity)
        observed = self._staging.observe(member_id, record)
        member = _member(record, member_id)
        if observed.candidate is None:
            created = self._staging.create_candidate(member_id, record)
            member = replace(member, stage_state=ExternalMemberStageState.CANDIDATE_BOUND, candidate=created)
            current = self._typed_cas(current, replace(record, members=_replace_member(record, member)))
            record = current.record
        elif member.candidate != observed.candidate:
            self._error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", record)
        error: Exception | None = None
        try:
            self._source.load_candidate(self._staging, member_id, record)
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
            return self._candidate(observed, record)
        if error is not None:
            raise error
        return self._candidate(observed, record)

    def drop_owned_candidate(self, member_id: str, *, candidate_uuid: str) -> None:
        record = self._member_current(member_id).record
        expected = _member(record, member_id).candidate
        if expected is None or expected.uuid != candidate_uuid:
            self._error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", record)
        self._staging.drop_candidate(member_id, record, expected)
        if self._staging.observe(member_id, record).candidate is not None:
            self._error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_STAGING_INCOMPLETE", record)

    def dispatch_publication_once(self, *, operation_id: str, candidate_name: str, member_ids: tuple[str, ...]) -> None:
        record = self._current(operation_id).record
        if record.candidate != candidate_name or _ids(record) != member_ids:
            self._error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", record)
        self._ddl.dispatch_publication(record, self._take_permit(operation_id, "publication"), cluster=self._cluster)

    def observe_publication(self, operation_id: str) -> Mapping[str, str]:
        record = self._current(operation_id).record
        entry = self._entry(record, cleanup=False)
        states = self._publication_states(record)
        queue = entry.state_for(_ids(record))
        if MemberPublicationState.UNKNOWN in states.values() or queue is QueueState.UNKNOWN:
            self._error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_DDL_UNKNOWN", record)
        if queue in {QueueState.TERMINAL_SUCCESS, QueueState.TERMINAL_FAILURE} and len(set(states.values())) > 1:
            self._error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_PUBLICATION_PARTIAL_TERMINAL", record)
        return {
            member_id: "desired" if state is MemberPublicationState.COMMITTED else "predecessor"
            for member_id, state in states.items()
        }

    def dispatch_cleanup_once(self, *, operation_id: str, member_ids: tuple[str, ...]) -> None:
        record = self._current(operation_id).record
        if _ids(record) != member_ids:
            self._error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_INVENTORY_DRIFT", record)
        if _has_predecessor(record):
            self._ddl.drop_predecessor(record, self._take_permit(operation_id, "cleanup"), cluster=self._cluster)

    def observe_cleanup(self, operation_id: str) -> Mapping[str, bool]:
        record = self._current(operation_id).record
        if _has_predecessor(record) and self._entry(record, cleanup=True).state_for(_ids(record)) is QueueState.UNKNOWN:
            self._error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_UNKNOWN", record)
        return self._cleanup_states(record)

    def _record(self, state: Mapping[str, Any], base: ExternalAuthorityRecord | None) -> ExternalAuthorityRecord:
        artifact = self._artifact(state)
        members = tuple(self._state_member(state, member_id, base) for member_id in state["member_ids"])
        error = next(
            (
                f"external_generation_diverged:{member.member_id}"
                for member in members
                if state["member_states"][member.member_id]["state"] == "DIVERGED"
            ),
            None,
        )
        record = ExternalAuthorityRecord(
            target_key=str(state["target_key"]),
            operation_id=str(state["operation_id"]),
            fence_token=str(state["fence_token"]),
            phase=ExternalAuthorityPhase(str(state["phase"])),
            dispatch_epoch=int(state["dispatch_epoch"]),
            inventory_digest=self._inventory(),
            plan_digest=str(state["plan_digest"]),
            database=self._database,
            target=self._target,
            candidate=str(state["candidate_name"]),
            members=members,
            artifact=artifact,
            generation_id=None if artifact is None else str(state["generation_id"]),
            publication_correlation_token=_field(base, "publication_correlation_token"),
            publication_entry=_field(base, "publication_entry"),
            publication_query_digest=_field(base, "publication_query_digest"),
            cleanup_correlation_token=_field(base, "cleanup_correlation_token"),
            cleanup_entry=_field(base, "cleanup_entry"),
            cleanup_query_digest=_field(base, "cleanup_query_digest"),
            error_code=error,
        )
        record.validate()
        return record

    def _state_member(
        self, state: Mapping[str, Any], member_id: str, base: ExternalAuthorityRecord | None
    ) -> ExternalMemberRecord:
        prior = None if base is None else _member(base, member_id)
        raw = state["member_states"][member_id]
        status = raw["state"]
        stage = ExternalMemberStageState.CANDIDATE_BOUND
        if status == "READY":
            stage = ExternalMemberStageState.READY
        elif status == "PENDING":
            stage = ExternalMemberStageState.PENDING
        candidate = None if prior is None else prior.candidate
        uuid = raw.get("candidate_uuid")
        if uuid is None and status in {"PENDING", "LOADING"}:
            candidate = None
        elif uuid is not None and (candidate is None or candidate.uuid != uuid):
            self._error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", base)
        return ExternalMemberRecord(
            member_id=member_id,
            stage_state=stage,
            publication_state=MemberPublicationState.UNKNOWN if prior is None else prior.publication_state,
            cleanup_complete=False if prior is None else prior.cleanup_complete,
            predecessor=None if prior is None else prior.predecessor,
            candidate=candidate,
        )

    def _artifact(self, state: Mapping[str, Any]) -> ArtifactIdentity | None:
        if "artifact_sha256" not in state:
            return None
        expected = self._source.identity
        projected = (
            str(state["artifact_sha256"]),
            int(state["artifact_row_count"]),
            str(state["artifact_schema_sha256"]),
            str(state["artifact_content_sha256"]),
        )
        actual = (expected.sha256, expected.row_count, expected.schema_digest, expected.wire_digest)
        if projected != actual:
            self._error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_ARTIFACT_CHANGED")
        return expected

    def _adopt(self, record: ExternalAuthorityRecord) -> ExternalAuthorityRecord:
        observed = tuple(self._staging.observe(member.member_id, record) for member in record.members)
        if any(value.candidate is not None for value in observed):
            self._error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", record)
        targets = tuple(value.target for value in observed)
        mixed_presence = any(value is None for value in targets) and any(value is not None for value in targets)
        if mixed_presence or len({_shape(value) for value in targets if value is not None}) > 1:
            self._error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_BASELINE_DIVERGED", record)
        return replace(
            record,
            members=tuple(replace(member, predecessor=value) for member, value in zip(record.members, targets)),
        )

    def _require_publication(
        self, record: ExternalAuthorityRecord
    ) -> tuple[QueueEntry, tuple[ExternalMemberRecord, ...]]:
        entry = self._entry(record, cleanup=False)
        if entry.state_for(_ids(record)) not in {QueueState.TERMINAL_SUCCESS, QueueState.TERMINAL_FAILURE}:
            self._error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_PUBLICATION_IN_PROGRESS", record)
        states = self._publication_states(record)
        if set(states.values()) != {MemberPublicationState.COMMITTED}:
            self._error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_PUBLICATION_PARTIAL_TERMINAL", record)
        members = tuple(replace(member, publication_state=states[member.member_id]) for member in record.members)
        return entry, members

    def _require_cleanup(
        self, record: ExternalAuthorityRecord
    ) -> tuple[QueueEntry | None, tuple[ExternalMemberRecord, ...]]:
        entry = self._entry(record, cleanup=True) if _has_predecessor(record) else None
        if entry is not None and entry.state_for(_ids(record)) not in {
            QueueState.TERMINAL_SUCCESS,
            QueueState.TERMINAL_FAILURE,
        }:
            self._error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_IN_PROGRESS", record)
        if any(self._cleanup_states(record).values()):
            self._error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_IN_PROGRESS", record)
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
                self._error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_UNKNOWN", record)
            result[member_id] = state is MemberPublicationState.COMMITTED
        return result

    def _entry(self, record: ExternalAuthorityRecord, *, cleanup: bool) -> QueueEntry:
        entry_id = record.cleanup_entry if cleanup else record.publication_entry
        token = record.cleanup_correlation_token if cleanup else record.publication_correlation_token
        digest = record.cleanup_query_digest if cleanup else record.publication_query_digest
        code = f"DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_{'CLEANUP_' if cleanup else 'DDL_'}UNKNOWN"
        if not token or not digest:
            self._error(code, record)
        entries = (
            (() if (entry := self._ddl.read_entry(self._cluster, entry_id)) is None else (entry,))
            if entry_id
            else self._ddl.find_entries(self._cluster, token)
        )
        if len(entries) != 1 or entries[0].correlation_token != token or entries[0].query_digest != digest:
            self._error(code, record)
        return entries[0]

    def _typed_cas(
        self, current: VersionedExternalAuthorityRecord, desired: ExternalAuthorityRecord
    ) -> VersionedExternalAuthorityRecord:
        return self._require_verified(self._authority.compare_and_swap(current, desired), None)

    def _verified(self, result: ExternalAuthorityMutationResult, action: str | None) -> Mapping[str, Any]:
        return self._state(self._require_verified(result, action))

    def _require_verified(
        self, result: ExternalAuthorityMutationResult, action: str | None
    ) -> VersionedExternalAuthorityRecord:
        if result.status is not ExternalAuthorityMutationStatus.VERIFIED or result.observed is None:
            suffix = "CAS_CONFLICT" if result.status is ExternalAuthorityMutationStatus.CONFLICT else "CAS_UNKNOWN"
            self._error(f"DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_{suffix}")
        if action is not None:
            if result.permit is None:
                self._error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CAS_UNKNOWN", result.observed.record)
            self._permits[(result.observed.record.operation_id, action)] = result.permit
        return result.observed

    def _state(self, current: VersionedExternalAuthorityRecord) -> dict[str, Any]:
        record = current.record
        states = {}
        for member in record.members:
            status = member.stage_state.value.upper()
            if member.stage_state is ExternalMemberStageState.CANDIDATE_BOUND:
                diverged = record.error_code == f"external_generation_diverged:{member.member_id}"
                status = "DIVERGED" if diverged else "AMBIGUOUS"
            states[member.member_id] = {
                "state": status,
                "candidate_uuid": None if member.candidate is None else member.candidate.uuid,
            }
        state: dict[str, Any] = {
            "target_key": record.target_key,
            "operation_id": record.operation_id,
            "fence_token": record.fence_token,
            "phase": record.phase.value,
            "dispatch_epoch": record.dispatch_epoch,
            "inventory_digest": record.inventory_digest,
            "plan_digest": record.plan_digest,
            "candidate_name": record.candidate,
            "member_ids": _ids(record),
            "member_states": states,
            "version": current.version,
        }
        if record.artifact is not None:
            state.update(
                artifact_sha256=record.artifact.sha256,
                artifact_schema_sha256=record.artifact.schema_digest,
                artifact_content_sha256=record.artifact.wire_digest,
                artifact_row_count=record.artifact.row_count,
                generation_id=record.generation_id,
            )
        return state

    @staticmethod
    def _candidate(observed: MemberGenerationObservation, record: ExternalAuthorityRecord) -> Mapping[str, Any]:
        candidate = observed.candidate
        if candidate is None:
            return {"exists": False, "member_id": observed.member_id}
        return {
            "exists": True,
            "member_id": observed.member_id,
            "operation_id": record.operation_id,
            "candidate_name": record.candidate,
            "candidate_uuid": candidate.uuid,
            "schema_sha256": candidate.schema_digest,
            "content_sha256": candidate.content_digest,
            "row_count": candidate.row_count,
        }

    def _current(self, operation_id: str) -> VersionedExternalAuthorityRecord:
        current = self._authority.read_versioned(self._target_key())
        if current is None or current.record.operation_id != operation_id:
            self._error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_AUTHORITY_CONFLICT")
        return current

    def _member_current(self, member_id: str) -> VersionedExternalAuthorityRecord:
        current = self._authority.read_versioned(self._target_key())
        if current is None or member_id not in _ids(current.record):
            self._error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_AUTHORITY_CONFLICT")
        return current

    def _take_permit(self, operation_id: str, action: str) -> ExternalDispatchPermit:
        permit = self._permits.pop((operation_id, action), None)
        if permit is None:
            self._error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CAS_UNKNOWN")
        return permit

    def _target_key(self) -> str:
        return derive_target_key(self._cluster, self._database, self._target)

    def _inventory(self) -> str:
        if self._inventory_digest is None:
            self._error("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_INVENTORY_INVALID")
        return self._inventory_digest

    @staticmethod
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


def _field(record: ExternalAuthorityRecord | None, name: str) -> str | None:
    return None if record is None else getattr(record, name)


def _member(record: ExternalAuthorityRecord, member_id: str) -> ExternalMemberRecord:
    return next(member for member in record.members if member.member_id == member_id)


def _replace_member(record: ExternalAuthorityRecord, desired: ExternalMemberRecord) -> tuple[ExternalMemberRecord, ...]:
    return tuple(desired if member.member_id == desired.member_id else member for member in record.members)


def _ids(record: ExternalAuthorityRecord) -> tuple[str, ...]:
    return tuple(member.member_id for member in record.members)


def _desired(member: ExternalMemberRecord) -> PhysicalGeneration:
    if member.candidate is None:
        raise ExternalPublicationError("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED")
    return member.candidate


def _complete(candidate: PhysicalGeneration | None, artifact: ArtifactIdentity | None) -> bool:
    return (
        candidate is not None
        and artifact is not None
        and (
            candidate.schema_digest,
            candidate.content_digest,
            candidate.row_count,
        )
        == (artifact.schema_digest, artifact.wire_digest, artifact.row_count)
    )


def _shape(value: PhysicalGeneration | None) -> tuple[Any, ...] | None:
    return None if value is None else (value.engine_full, value.schema_digest, value.content_digest, value.row_count)


def _has_predecessor(record: ExternalAuthorityRecord) -> bool:
    return any(member.predecessor is not None for member in record.members)


def _equivalent(current: Mapping[str, Any], desired: Mapping[str, Any]) -> bool:
    return {key: value for key, value in current.items() if key != "version"} == {
        key: value for key, value in desired.items() if key != "version"
    }


__all__ = ["ClickHouseExternalReplicationServiceAdapter", "ExternalArtifactSourcePort"]
