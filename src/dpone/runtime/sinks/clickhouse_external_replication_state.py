"""Codec between typed external authority and the runtime state machine."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.ports.clickhouse_external_replication import (
    ArtifactIdentity,
    ExternalAuthorityPhase,
    ExternalAuthorityRecord,
    ExternalMemberRecord,
    ExternalMemberStageState,
    ExternalPublicationError,
    MemberPublicationState,
    PhysicalGeneration,
    VersionedExternalAuthorityRecord,
)


def record_from_state(
    state: Mapping[str, Any],
    base: ExternalAuthorityRecord | None,
    *,
    database: str,
    target: str,
    inventory_digest: str,
) -> ExternalAuthorityRecord:
    if str(state["inventory_digest"]) != inventory_digest:
        _fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_INVENTORY_DRIFT", base)
    artifact = _artifact(state)
    members = tuple(_state_member(state, member_id, base) for member_id in state["member_ids"])
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
            "candidate_uuid": None if item.candidate is None else item.candidate.uuid,
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
    elif uuid is not None and (candidate is None or candidate.uuid != uuid):
        _fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", base)
    return ExternalMemberRecord(
        member_id=member_id,
        stage_state=stage,
        publication_state=MemberPublicationState.UNKNOWN if prior is None else prior.publication_state,
        cleanup_complete=False if prior is None else prior.cleanup_complete,
        predecessor=None if prior is None else prior.predecessor,
        candidate=candidate,
    )


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


def _fail(code: str, record: ExternalAuthorityRecord | None) -> None:
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
    "member",
    "member_ids",
    "record_from_state",
    "replace_member",
    "state_from_versioned",
]
