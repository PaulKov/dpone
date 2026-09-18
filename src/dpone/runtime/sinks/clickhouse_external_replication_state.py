"""Codec between typed external authority and the runtime state machine."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, NoReturn

from dpone.ports.clickhouse_external_replication import (
    ArtifactIdentity,
    ExternalArtifactReceipt,
    ExternalArtifactSourcePort,
    ExternalAuthorityPhase,
    ExternalAuthorityRecord,
    ExternalClusterDdlPort,
    ExternalMemberRecord,
    ExternalMemberStageState,
    ExternalPublicationError,
    MemberGenerationObservation,
    MemberPublicationState,
    PhysicalGeneration,
    QueueEntry,
    VersionedExternalAuthorityRecord,
)

Fail = Callable[[str, ExternalAuthorityRecord | None], NoReturn]


def require_queue_entry(
    ddl: ExternalClusterDdlPort,
    cluster: str,
    record: ExternalAuthorityRecord,
    *,
    cleanup: bool,
    fail: Fail,
) -> QueueEntry:
    """Resolve the queue entry bound to the authority token and digest."""

    entry_id = record.cleanup_entry if cleanup else record.publication_entry
    token = record.cleanup_correlation_token if cleanup else record.publication_correlation_token
    digest = record.cleanup_query_digest if cleanup else record.publication_query_digest
    code = f"DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_{'CLEANUP_' if cleanup else 'DDL_'}UNKNOWN"
    if not token or not digest:
        fail(code, record)
    entries = (
        (() if (entry := ddl.read_entry(cluster, entry_id)) is None else (entry,))
        if entry_id
        else ddl.find_entries(cluster, token)
    )
    if len(entries) != 1 or entries[0].correlation_token != token or entries[0].query_digest != digest:
        fail(code, record)
    return entries[0]


def record_from_state(
    state: Mapping[str, Any],
    base: ExternalAuthorityRecord | None,
    *,
    database: str,
    target: str,
    inventory_digest: str,
) -> ExternalAuthorityRecord:
    if str(state["inventory_digest"]) != inventory_digest:
        fail_external("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_INVENTORY_DRIFT", base)
    artifact = _artifact(state)
    same_operation = base is not None and base.operation_id == str(state["operation_id"])
    operation_base = base if same_operation else None
    members = tuple(_state_member(state, member_id, operation_base) for member_id in state["member_ids"])
    error = next(
        (
            f"external_generation_diverged:{item.member_id}"
            for item in members
            if state["member_states"][item.member_id]["state"] == "DIVERGED"
        ),
        None,
    )
    record = ExternalAuthorityRecord(
        target_key=str(state["target_key"]),
        operation_id=str(state["operation_id"]),
        fence_token=str(state["fence_token"]),
        phase=ExternalAuthorityPhase(str(state["phase"])),
        dispatch_epoch=int(state["dispatch_epoch"]),
        inventory_digest=str(state["inventory_digest"]),
        plan_digest=str(state["plan_digest"]),
        database=database,
        target=target,
        candidate=str(state["candidate_name"]),
        members=members,
        artifact=artifact,
        artifact_binding_id=None if artifact is None else str(state["artifact_binding_id"]),
        generation_id=None if artifact is None else str(state["generation_id"]),
        publication_correlation_token=_field(operation_base, "publication_correlation_token"),
        publication_entry=_field(operation_base, "publication_entry"),
        publication_query_digest=_field(operation_base, "publication_query_digest"),
        cleanup_correlation_token=_field(operation_base, "cleanup_correlation_token"),
        cleanup_entry=_field(operation_base, "cleanup_entry"),
        cleanup_query_digest=_field(operation_base, "cleanup_query_digest"),
        error_code=error,
    )
    record.validate()
    return record


def state_from_versioned(current: VersionedExternalAuthorityRecord) -> dict[str, Any]:
    record = current.record
    states: dict[str, dict[str, Any]] = {}
    for item in record.members:
        status = item.stage_state.value.upper()
        if item.stage_state is ExternalMemberStageState.CANDIDATE_BOUND:
            diverged = record.error_code == f"external_generation_diverged:{item.member_id}"
            status = "DIVERGED" if diverged else "AMBIGUOUS"
        states[item.member_id] = {
            "state": status,
            **_generation_state(item.candidate),
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
        "member_ids": member_ids(record),
        "member_states": states,
        "version": current.version,
    }
    if record.artifact is not None:
        state.update(
            artifact_sha256=record.artifact.sha256,
            artifact_schema_sha256=record.artifact.schema_digest,
            artifact_content_sha256=record.artifact.wire_digest,
            artifact_row_count=record.artifact.row_count,
            artifact_byte_size=record.artifact.byte_size,
            artifact_binding_id=record.artifact_binding_id,
            generation_id=record.generation_id,
        )
    return state


def member(record: ExternalAuthorityRecord, member_id: str) -> ExternalMemberRecord:
    return next(item for item in record.members if item.member_id == member_id)


def replace_member(record: ExternalAuthorityRecord, desired: ExternalMemberRecord) -> tuple[ExternalMemberRecord, ...]:
    return tuple(desired if item.member_id == desired.member_id else item for item in record.members)


def member_ids(record: ExternalAuthorityRecord) -> tuple[str, ...]:
    return tuple(item.member_id for item in record.members)


def complete_candidate(candidate: PhysicalGeneration | None, artifact: ArtifactIdentity | None) -> bool:
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


def require_replayable_artifact(artifact: ExternalArtifactReceipt, fail: Any, member_ids: tuple[str, ...]) -> None:
    if not artifact.replayable or artifact.byte_size < 0 or artifact.row_count < 0:
        fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_ARTIFACT_UNSUPPORTED", member_ids=member_ids)


def desired_generation(member_record: ExternalMemberRecord) -> PhysicalGeneration:
    if member_record.candidate is None:
        raise ExternalPublicationError("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED")
    return member_record.candidate


def generation_shape(value: PhysicalGeneration | None) -> tuple[Any, ...] | None:
    return None if value is None else (value.engine_full, value.schema_digest, value.content_digest, value.row_count)


def has_predecessor(record: ExternalAuthorityRecord) -> bool:
    return any(member_record.predecessor is not None for member_record in record.members)


def equivalent_state(current: Mapping[str, Any], desired: Mapping[str, Any]) -> bool:
    return {key: value for key, value in current.items() if key != "version"} == {
        key: value for key, value in desired.items() if key != "version"
    }


def candidate_name(target: str, operation_id: str) -> str:
    return f"{target[:96]}__dpone_ext_{operation_id[:20]}"


def without_version(state: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in state.items() if key != "version"}


def owned_observation(observation: Mapping[str, Any], state: Mapping[str, Any]) -> bool:
    return (
        observation.get("operation_id") == state["operation_id"]
        and observation.get("candidate_name") == state["candidate_name"]
    )


def matches_generation(observation: Mapping[str, Any], state: Mapping[str, Any]) -> bool:
    return bool(observation.get("exists")) and (
        observation.get("operation_id") == state["operation_id"]
        and observation.get("candidate_name") == state["candidate_name"]
        and observation.get("schema_sha256") == state["artifact_schema_sha256"]
        and observation.get("content_sha256") == state["artifact_content_sha256"]
        and observation.get("row_count") == state["artifact_row_count"]
    )


def require_stage_request(
    record: ExternalAuthorityRecord,
    *,
    operation_id: str,
    candidate_name: str,
    artifact: ExternalArtifactReceipt,
    source: ExternalArtifactSourcePort,
    fail: Any,
) -> None:
    if (
        record.operation_id != operation_id
        or record.candidate != candidate_name
        or record.artifact != artifact.identity
        or source.identity != artifact.identity
        or record.artifact_binding_id != source.binding_id
    ):
        fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_ARTIFACT_CHANGED", record)


def reusable_candidate_observation(
    observed: MemberGenerationObservation,
    member_record: ExternalMemberRecord,
    record: ExternalAuthorityRecord,
    *,
    fail: Any,
) -> Mapping[str, Any] | None:
    if not complete_candidate(observed.candidate, record.artifact):
        return None
    if (
        member_record.candidate is None
        or observed.candidate is None
        or member_record.candidate.uuid != observed.candidate.uuid
    ):
        fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", record)
    return candidate_observation(observed, record)


def candidate_observation(observed: MemberGenerationObservation, record: ExternalAuthorityRecord) -> Mapping[str, Any]:
    candidate = observed.candidate
    if candidate is None:
        return {"exists": False, "member_id": observed.member_id}
    return {
        "exists": True,
        "member_id": observed.member_id,
        "operation_id": record.operation_id,
        "candidate_name": record.candidate,
        "candidate_uuid": candidate.uuid,
        "engine_full": candidate.engine_full,
        "schema_sha256": candidate.schema_digest,
        "content_sha256": candidate.content_digest,
        "row_count": candidate.row_count,
    }


def _state_member(
    state: Mapping[str, Any], member_id: str, base: ExternalAuthorityRecord | None
) -> ExternalMemberRecord:
    prior = None if base is None else member(base, member_id)
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
    elif uuid is not None:
        encoded = _generation_from_state(raw)
        if encoded is not None:
            if candidate is not None and candidate.uuid != encoded.uuid:
                fail_external("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", base)
            candidate = encoded
        elif candidate is None or candidate.uuid != uuid:
            fail_external("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", base)
    return ExternalMemberRecord(
        member_id=member_id,
        stage_state=stage,
        publication_state=MemberPublicationState.UNKNOWN if prior is None else prior.publication_state,
        cleanup_complete=False if prior is None else prior.cleanup_complete,
        predecessor=None if prior is None else prior.predecessor,
        candidate=candidate,
    )


def _generation_state(candidate: PhysicalGeneration | None) -> dict[str, Any]:
    if candidate is None:
        return {"candidate_uuid": None}
    return {
        "candidate_uuid": candidate.uuid,
        "candidate_engine_full": candidate.engine_full,
        "candidate_schema_sha256": candidate.schema_digest,
        "candidate_content_sha256": candidate.content_digest,
        "candidate_row_count": candidate.row_count,
    }


def _generation_from_state(raw: Mapping[str, Any]) -> PhysicalGeneration | None:
    fields = (
        "candidate_engine_full",
        "candidate_schema_sha256",
        "candidate_content_sha256",
        "candidate_row_count",
    )
    if not all(raw.get(field) is not None for field in fields):
        return None
    candidate = PhysicalGeneration(
        uuid=str(raw["candidate_uuid"]),
        engine_full=str(raw["candidate_engine_full"]),
        schema_digest=str(raw["candidate_schema_sha256"]),
        content_digest=str(raw["candidate_content_sha256"]),
        row_count=int(raw["candidate_row_count"]),
    )
    candidate.validate()
    return candidate


def _artifact(state: Mapping[str, Any]) -> ArtifactIdentity | None:
    if "artifact_sha256" not in state:
        return None
    artifact = ArtifactIdentity(
        sha256=str(state["artifact_sha256"]),
        byte_size=int(state["artifact_byte_size"]),
        row_count=int(state["artifact_row_count"]),
        schema_digest=str(state["artifact_schema_sha256"]),
        wire_digest=str(state["artifact_content_sha256"]),
    )
    artifact.validate()
    return artifact


def _field(record: ExternalAuthorityRecord | None, name: str) -> str | None:
    return None if record is None else getattr(record, name)


def fail_external(code: str, record: ExternalAuthorityRecord | None = None) -> NoReturn:
    evidence: dict[str, object] = {"evidence_scope": "runtime", "member_ids": []}
    if record is not None:
        evidence.update(
            target_key=record.target_key,
            operation_id=record.operation_id,
            phase=record.phase.value,
            member_ids=list(member_ids(record)),
        )
    raise ExternalPublicationError(code, evidence=evidence)


__all__ = [
    "complete_candidate",
    "candidate_name",
    "require_replayable_artifact",
    "candidate_observation",
    "desired_generation",
    "equivalent_state",
    "fail_external",
    "generation_shape",
    "has_predecessor",
    "member",
    "member_ids",
    "matches_generation",
    "record_from_state",
    "require_stage_request",
    "reusable_candidate_observation",
    "replace_member",
    "state_from_versioned",
    "owned_observation",
    "without_version",
]
