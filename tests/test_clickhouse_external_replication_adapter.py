from __future__ import annotations

from dataclasses import replace

import pytest

from dpone.contracts.clickhouse_cluster_publication import QueueEntry, QueueHostResult
from dpone.contracts.clickhouse_external_replication import (
    ExternalArtifactReceipt,
    ExternalAuthorityPhase,
    ExternalMember,
    ExternalPublicationError,
    ExternalPublicationRequest,
    ExternalTopology,
    MemberGenerationObservation,
    PhysicalGeneration,
    ReplicationMode,
)
from dpone.ports.clickhouse_external_replication import (
    ExternalAuthorityMutationResult,
    ExternalAuthorityMutationStatus,
    ExternalDispatchPermit,
    VersionedExternalAuthorityRecord,
)
from dpone.runtime.sinks.clickhouse_external_replication_adapter import (
    ClickHouseExternalReplicationServiceAdapter,
)
from dpone.runtime.sinks.clickhouse_external_replication_runtime import ClickHouseExternalReplicationRuntime


def _digest(value: str) -> str:
    return (value * 64)[:64]


class _Topology:
    def __init__(self) -> None:
        self.value = ExternalTopology(
            cluster="analytics_cluster",
            replication_mode=ReplicationMode.EXTERNAL,
            members=tuple(
                ExternalMember.create(shard_num=1, replica_num=replica, internal_replication=False)
                for replica in (1, 2)
            ),
        )

    def inventory(self, cluster: str) -> ExternalTopology:
        assert cluster == self.value.cluster
        return self.value


class _Authority:
    def __init__(self) -> None:
        self.current: VersionedExternalAuthorityRecord | None = None
        self.unknown_dispatch = False
        self.lose_ready_cas_once = False
        self.phases: list[str] = []

    def read_versioned(self, target_key: str) -> VersionedExternalAuthorityRecord | None:
        if self.current is not None:
            assert self.current.record.target_key == target_key
        return self.current

    def create_if_absent(self, record):
        if self.current is not None:
            return ExternalAuthorityMutationResult(ExternalAuthorityMutationStatus.CONFLICT, self.current)
        self.current = VersionedExternalAuthorityRecord(record, 0)
        return ExternalAuthorityMutationResult(ExternalAuthorityMutationStatus.VERIFIED, self.current)

    def compare_and_swap(self, current, desired):
        assert self.current == current
        if self.unknown_dispatch and desired.phase.value == "PUBLICATION_DISPATCHING":
            return ExternalAuthorityMutationResult(ExternalAuthorityMutationStatus.OUTCOME_UNKNOWN)
        if self.lose_ready_cas_once and any(member.stage_state.value == "ready" for member in desired.members):
            self.lose_ready_cas_once = False
            return ExternalAuthorityMutationResult(ExternalAuthorityMutationStatus.OUTCOME_UNKNOWN)
        self.current = VersionedExternalAuthorityRecord(desired, current.version + 1)
        self.phases.append(desired.phase.value)
        permit = None
        if desired.phase.value in {"PUBLICATION_DISPATCHING", "CLEANUP_DISPATCHING"}:
            permit = ExternalDispatchPermit(
                desired.target_key,
                desired.operation_id,
                desired.fence_token,
                desired.dispatch_epoch,
            )
        return ExternalAuthorityMutationResult(ExternalAuthorityMutationStatus.VERIFIED, self.current, permit)


class _Artifact:
    def __init__(self) -> None:
        self.calls = 0
        self.identity = _request().artifact.identity
        self.binding_id = "artifact-v1"

    def revalidate(self, artifact) -> None:
        artifact.validate()
        assert artifact == self.identity
        self.calls += 1

    def open_replay(self) -> object:
        return object()


class _Staging:
    def __init__(self, members: tuple[str, ...]) -> None:
        self.values = {
            member: MemberGenerationObservation(member, _generation(f"old-{index}", _digest("d")), None)
            for index, member in enumerate(members, 1)
        }
        self.loads = dict.fromkeys(members, 0)
        self.drops = dict.fromkeys(members, 0)
        self.crash_after_create_once = False

    def observe(self, member_id, record):
        del record
        return self.values[member_id]

    def create_candidate(self, member_id, record, *, expected_uuid):
        candidate = replace(_generation("intent", _digest("0"), rows=0), uuid=expected_uuid)
        self.values[member_id] = replace(self.values[member_id], candidate=candidate)
        if self.crash_after_create_once:
            self.crash_after_create_once = False
            raise _ProcessDeath
        return candidate

    def load_candidate(self, member_id, record, source):
        source.revalidate(record.artifact)
        source.open_replay()
        self.loads[member_id] += 1
        current = self.values[member_id]
        assert current.candidate is not None and record.artifact is not None
        self.values[member_id] = replace(
            current,
            candidate=replace(
                current.candidate,
                schema_digest=record.artifact.schema_digest,
                content_digest=record.artifact.wire_digest,
                row_count=record.artifact.row_count,
            ),
        )

    def drop_candidate(self, member_id, record, expected):
        del record
        assert self.values[member_id].candidate == expected
        self.drops[member_id] += 1
        self.values[member_id] = replace(self.values[member_id], candidate=None)


class _Ddl:
    def __init__(self, staging: _Staging) -> None:
        self.staging = staging
        self.entries: list[QueueEntry] = []
        self.publication_calls = 0
        self.cleanup_calls = 0
        self.lose_publication_reply = False

    def publication_query_digest(self, record, *, cluster):
        del record, cluster
        return _digest("e")

    def cleanup_query_digest(self, record, *, cluster):
        del record, cluster
        return _digest("f")

    def dispatch_publication(self, record, permit, *, cluster):
        del permit, cluster
        self.publication_calls += 1
        for member, observation in tuple(self.staging.values.items()):
            self.staging.values[member] = MemberGenerationObservation(member, observation.candidate, observation.target)
        self.entries.append(
            _entry("publication", record.publication_correlation_token, record.publication_query_digest)
        )
        if self.lose_publication_reply:
            raise TimeoutError("reply lost")

    def find_entries(self, cluster, correlation_token):
        del cluster
        return tuple(entry for entry in self.entries if entry.correlation_token == correlation_token)

    def read_entry(self, cluster, entry):
        del cluster
        return next((value for value in self.entries if value.entry == entry), None)

    def drop_predecessor(self, record, permit, *, cluster):
        del permit, cluster
        self.cleanup_calls += 1
        for member, observation in tuple(self.staging.values.items()):
            self.staging.values[member] = replace(observation, candidate=None)
        self.entries.append(_entry("cleanup", record.cleanup_correlation_token, record.cleanup_query_digest))


class _ProcessDeath(BaseException):
    pass


def _generation(label: str, content: str, *, rows: int = 2) -> PhysicalGeneration:
    return PhysicalGeneration(
        uuid=f"uuid-{label}",
        engine_full="MergeTree() ORDER BY tuple()",
        schema_digest=_digest("c"),
        content_digest=content,
        row_count=rows,
    )


def _entry(name: str, token: str | None, query_digest: str | None) -> QueueEntry:
    assert token is not None and query_digest is not None
    return QueueEntry(
        entry=f"{name}-{token}",
        query_digest=query_digest,
        correlation_token=token,
        hosts=tuple(QueueHostResult(member, "Finished", 0, "") for member in _members()),
    )


def _members() -> tuple[str, ...]:
    return tuple(member.member_id for member in _Topology().value.ordered_members)


def _request(scheduler_invocation: str = "scheduled-run") -> ExternalPublicationRequest:
    return ExternalPublicationRequest(
        cluster="analytics_cluster",
        database="analytics",
        target="target_table",
        scheduler_invocation=scheduler_invocation,
        plan_sha256=_digest("f"),
        artifact=ExternalArtifactReceipt(
            artifact_id="artifact-v1",
            sha256=_digest("a"),
            byte_size=128,
            row_count=2,
            schema_sha256=_digest("c"),
            content_sha256=_digest("b"),
            replayable=True,
        ),
    )


def _runtime(*, lose_reply: bool = False, unknown_dispatch: bool = False, absent_target: bool = False):
    topology, authority, artifact = _Topology(), _Authority(), _Artifact()
    staging = _Staging(_members())
    if absent_target:
        staging.values = {member: MemberGenerationObservation(member, None, None) for member in _members()}
    ddl = _Ddl(staging)
    ddl.lose_publication_reply = lose_reply
    authority.unknown_dispatch = unknown_dispatch
    service = ClickHouseExternalReplicationServiceAdapter(
        topology=topology,
        authority=authority,
        staging=staging,
        ddl=ddl,
        cluster="analytics_cluster",
        database="analytics",
        target="target_table",
    )
    return (
        ClickHouseExternalReplicationRuntime(service=service, artifact_source=artifact),
        authority,
        artifact,
        staging,
        ddl,
    )


def test_adapter_runs_exact_typed_authority_and_queue_protocol() -> None:
    runtime, authority, artifact, staging, ddl = _runtime(lose_reply=True)

    receipt = runtime.run(_request())
    replay = runtime.run(_request())

    assert receipt.phase == "COMPLETED"
    assert replay.to_dict() == receipt.to_dict()
    assert authority.current is not None and authority.current.record.phase.value == "COMPLETED"
    assert set(staging.loads.values()) == {1}
    assert artifact.calls >= 2
    assert ddl.publication_calls == 1
    assert ddl.cleanup_calls == 1
    assert "analytics" not in str(receipt.to_dict())


def test_process_death_after_create_recovers_from_prebound_uuid() -> None:
    runtime, authority, _, staging, ddl = _runtime()
    staging.crash_after_create_once = True

    with pytest.raises(_ProcessDeath):
        runtime.stage(_request())

    assert authority.current is not None
    creating = next(
        member for member in authority.current.record.members if member.stage_state.value == "create_intent"
    )
    observed = staging.values[creating.member_id].candidate
    assert observed is not None
    assert creating.candidate_uuid_intent == observed.uuid

    service = ClickHouseExternalReplicationServiceAdapter(
        topology=_Topology(),
        authority=authority,
        staging=staging,
        ddl=ddl,
        cluster="analytics_cluster",
        database="analytics",
        target="target_table",
    )
    fresh = ClickHouseExternalReplicationRuntime(service=service, artifact_source=_Artifact())
    receipt = fresh.run(_request())

    assert receipt.phase == "COMPLETED"
    assert staging.drops[creating.member_id] == 0
    assert staging.loads[creating.member_id] == 1


def test_adapter_never_dispatches_without_verified_cas_permit() -> None:
    runtime, _, _, _, ddl = _runtime(unknown_dispatch=True)

    with pytest.raises(ExternalPublicationError, match="AUTHORITY_CONFLICT"):
        runtime.run(_request())

    assert ddl.publication_calls == 0
    assert ddl.cleanup_calls == 0


@pytest.mark.parametrize("replaced_slot", ["target", "candidate"])
def test_adapter_reproves_bound_generations_before_publication_dispatch(replaced_slot: str) -> None:
    runtime, authority, _, staging, ddl = _runtime()
    request = _request()
    runtime.stage(request)
    member_id = _members()[0]
    observed = staging.values[member_id]
    staging.values[member_id] = replace(
        observed,
        **{replaced_slot: _generation(f"foreign-{replaced_slot}", _digest("9"))},
    )

    with pytest.raises(ExternalPublicationError, match="AUTHORITY_CONFLICT"):
        runtime.publish(request)

    assert ddl.publication_calls == 0
    assert authority.current is not None
    assert authority.current.record.phase is ExternalAuthorityPhase.STAGED


def test_adapter_absent_target_completes_without_cleanup_dispatch() -> None:
    runtime, authority, _, _, ddl = _runtime(absent_target=True)

    receipt = runtime.run(_request())

    assert receipt.phase == "COMPLETED"
    assert authority.current is not None and authority.current.record.phase is ExternalAuthorityPhase.COMPLETED
    assert ddl.publication_calls == 1
    assert ddl.cleanup_calls == 0
    assert "CLEANUP_DISPATCHING" not in authority.phases


def test_adapter_rejects_foreign_predecessor_before_cleanup_dispatch() -> None:
    runtime, _, _, staging, ddl = _runtime()
    request = _request()
    runtime.stage(request)
    runtime.publish(request)
    member_id = _members()[0]
    observed = staging.values[member_id]
    staging.values[member_id] = replace(observed, candidate=_generation("foreign", _digest("9")))

    with pytest.raises(ExternalPublicationError, match="CLEANUP_UNKNOWN"):
        runtime.cleanup(request)

    assert ddl.cleanup_calls == 0


def test_adapter_freshly_adopts_target_for_each_completed_operation() -> None:
    runtime, authority, _, staging, ddl = _runtime()

    first = runtime.run(_request("scheduled-run-1"))
    second = runtime.run(_request("scheduled-run-2"))

    assert first.phase == second.phase == "COMPLETED"
    assert first.operation_id != second.operation_id
    assert authority.current is not None and authority.current.record.operation_id == second.operation_id
    assert set(staging.loads.values()) == {2}
    assert ddl.publication_calls == 2
    assert ddl.cleanup_calls == 2


def test_adapter_freshly_adopts_target_after_initially_absent_operation() -> None:
    runtime, _, _, staging, ddl = _runtime(absent_target=True)

    first = runtime.run(_request("scheduled-run-1"))
    second = runtime.run(_request("scheduled-run-2"))

    assert first.phase == second.phase == "COMPLETED"
    assert set(staging.loads.values()) == {2}
    assert ddl.publication_calls == 2
    assert ddl.cleanup_calls == 1


def test_fresh_adapter_initializes_inventory_before_cleanup() -> None:
    runtime, authority, _, staging, ddl = _runtime()
    request = _request()
    runtime.stage(request)
    runtime.publish(request)
    topology = _Topology()
    fresh_service = ClickHouseExternalReplicationServiceAdapter(
        topology=topology,
        authority=authority,
        staging=staging,
        ddl=ddl,
        cluster=request.cluster,
        database=request.database,
        target=request.target,
    )

    completed = ClickHouseExternalReplicationRuntime(service=fresh_service).cleanup(request)

    assert completed.phase == "COMPLETED"
    assert ddl.cleanup_calls == 1


def test_cleanup_rejects_topology_drift_before_dispatch() -> None:
    runtime, authority, _, staging, ddl = _runtime()
    request = _request()
    runtime.stage(request)
    runtime.publish(request)
    topology = _Topology()
    topology.value = replace(
        topology.value,
        members=(
            *topology.value.members,
            ExternalMember.create(shard_num=1, replica_num=3, internal_replication=False),
        ),
    )
    fresh_service = ClickHouseExternalReplicationServiceAdapter(
        topology=topology,
        authority=authority,
        staging=staging,
        ddl=ddl,
        cluster=request.cluster,
        database=request.database,
        target=request.target,
    )

    with pytest.raises(ExternalPublicationError, match="INVENTORY_DRIFT"):
        ClickHouseExternalReplicationRuntime(service=fresh_service).cleanup(request)

    assert ddl.cleanup_calls == 0


def test_fresh_adapter_initializes_inventory_before_abort() -> None:
    runtime, authority, _, staging, ddl = _runtime()
    request = _request()
    runtime.stage(request)
    fresh_service = ClickHouseExternalReplicationServiceAdapter(
        topology=_Topology(),
        authority=authority,
        staging=staging,
        ddl=ddl,
        cluster=request.cluster,
        database=request.database,
        target=request.target,
    )

    ClickHouseExternalReplicationRuntime(service=fresh_service).abort(request)

    assert authority.current is not None
    assert authority.current.record.phase is ExternalAuthorityPhase.ABORTED
    assert set(staging.drops.values()) == {1}


def test_late_same_operation_worker_never_reloads_ready_candidate() -> None:
    runtime, authority, artifact, staging, _ = _runtime()
    request = _request()
    runtime.stage(request)
    service = runtime._service
    assert authority.current is not None

    observed = service.stage_member_once(
        _members()[0],
        operation_id=request.operation_id,
        candidate_name=authority.current.record.candidate,
        artifact=request.artifact,
        source=artifact,
    )

    assert observed["row_count"] == request.artifact.row_count
    assert set(staging.loads.values()) == {1}


def test_lost_ready_cas_recovers_complete_physical_generation() -> None:
    runtime, authority, _, _, _ = _runtime()
    authority.lose_ready_cas_once = True

    staged = runtime.stage(_request())

    assert staged.phase == "STAGED"
    assert authority.current is not None
    for member in authority.current.record.members:
        assert member.stage_state is not None
        assert member.candidate is not None
        assert member.candidate.content_digest == _request().artifact.content_sha256
        assert member.candidate.row_count == _request().artifact.row_count
