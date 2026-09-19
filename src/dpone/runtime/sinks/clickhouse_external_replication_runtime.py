"""Recoverable coordinator for externally replicated ClickHouse publication."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, NoReturn

from dpone.ports.clickhouse_external_replication import (
    ExternalArtifactReceipt,
    ExternalArtifactSourcePort,
    ExternalPublicationError,
    ExternalPublicationRequest,
    ExternalReplicationReceipt,
    ExternalReplicationRuntimeService,
    derive_generation_id,
    derive_operation_id,
    derive_target_key,
    digest_payload,
)
from dpone.runtime.sinks import clickhouse_external_replication_authority as authority_ops
from dpone.runtime.sinks import (
    clickhouse_external_replication_phases as phase_ops,
)
from dpone.runtime.sinks import clickhouse_external_replication_runtime_support as runtime_support


class ClickHouseExternalReplicationRuntime:
    """Order durable effects so retries never append or redispatch blindly."""

    def __init__(
        self,
        *,
        service: ExternalReplicationRuntimeService,
        artifact_source: ExternalArtifactSourcePort | None = None,
    ) -> None:
        self._service = service
        self._artifact_source = artifact_source

    def run(self, request: ExternalPublicationRequest) -> ExternalReplicationReceipt:
        self.stage(request)
        self.publish(request)
        return self.cleanup(request)

    def stage(self, request: ExternalPublicationRequest) -> ExternalReplicationReceipt:
        phase_ops.validate_request(request, self._fail)
        state = self.prepare(
            cluster=request.cluster,
            database=request.database,
            target=request.target,
            scheduler_invocation=request.scheduler_invocation,
            plan_sha256=request.plan_sha256,
        )
        source = self._require_source()
        phase_ops.require_replayable_artifact(request.artifact, self._fail, ())
        source.revalidate(request.artifact.identity)
        if source.binding_id != request.artifact.artifact_id or source.identity != request.artifact.identity:
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_ARTIFACT_CHANGED")
        members = tuple(sorted(self._service.inventory(request.cluster)))
        if len(members) < 2 or len(set(members)) != len(members):
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_INVENTORY_INVALID", member_ids=members)
        phase_ops.require_replayable_artifact(request.artifact, self._fail, members)
        if state["phase"] == "COMPLETED":
            return self._receipt(state)
        state = self._stage(request, state)
        return self._receipt(state)

    def prepare(
        self,
        *,
        cluster: str,
        database: str,
        target: str,
        scheduler_invocation: str,
        plan_sha256: str,
    ) -> dict[str, Any]:
        """Acquire or resume target authority before source extraction."""

        if not all((cluster, database, target, scheduler_invocation)):
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_REQUEST_INVALID")
        target_key = derive_target_key(cluster, database, target)
        operation_id = derive_operation_id(
            scheduler_invocation=scheduler_invocation,
            target_key=target_key,
            normalized_plan_digest=plan_sha256,
        )
        members = tuple(sorted(self._service.inventory(cluster)))
        if len(members) < 2 or len(set(members)) != len(members):
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_INVENTORY_INVALID", member_ids=members)
        return self._lock_values(
            target_key=target_key,
            operation_id=operation_id,
            target=target,
            plan_sha256=plan_sha256,
            members=members,
        )

    def abort_prepared(
        self,
        *,
        cluster: str,
        database: str,
        target: str,
        scheduler_invocation: str,
        plan_sha256: str,
    ) -> None:
        """Close an exact pre-artifact lock without touching ClickHouse data."""

        target_key = derive_target_key(cluster, database, target)
        operation_id = derive_operation_id(
            scheduler_invocation=scheduler_invocation,
            target_key=target_key,
            normalized_plan_digest=plan_sha256,
        )
        authority_ops.abort_prepared(
            target_key=target_key,
            operation_id=operation_id,
            read=self._read,
            cas=self._cas,
            fail=self._fail,
            without_version=phase_ops.without_version,
        )

    def publish(self, request: ExternalPublicationRequest) -> ExternalReplicationReceipt:
        """Publish a previously staged operation and finish exact cleanup."""

        request.validate()
        state = self._read(request.target_key)
        if state is None or state.get("operation_id") != request.operation_id:
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_AUTHORITY_CONFLICT", state=state)
        members = tuple(sorted(self._service.inventory(request.cluster)))
        self._require_same_inputs(state, request, members)
        if state["phase"] == "COMPLETED":
            return self._receipt(state)
        if state["phase"] in {"LOCKED", "STAGING"}:
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_STAGING_INCOMPLETE", state=state)
        state = phase_ops.publish_phase(state, service=self._service, cas=self._cas, fail=self._fail)
        return self._receipt(state)

    def validate_staged(
        self,
        request: ExternalPublicationRequest,
        expected: ExternalReplicationReceipt,
    ) -> ExternalReplicationReceipt:
        """Re-prove the exact all-member barrier immediately before publication."""

        members = tuple(sorted(self._service.inventory(request.cluster)))
        state = self._read(request.target_key)
        if state is None or state.get("operation_id") != request.operation_id:
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_AUTHORITY_CONFLICT", state=state)
        self._require_same_inputs(state, request, members)
        if state.get("phase") != "STAGED" or int(state["version"]) != expected.authority_version:
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", state=state)
        for member_id in members:
            observed = self._service.observe_candidate(member_id, state["candidate_name"])
            bound = state["member_states"][member_id]
            if (
                bound.get("state") != "READY"
                or not phase_ops.matches_generation(observed, state)
                or observed.get("candidate_uuid") != bound.get("candidate_uuid")
            ):
                self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", state=state)
        return self._receipt(state)

    def cleanup(self, request: ExternalPublicationRequest) -> ExternalReplicationReceipt:
        """Finish separately fenced cleanup after publication is committed."""

        request.validate()
        state = self._read(request.target_key)
        if state is None or state.get("operation_id") != request.operation_id:
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_AUTHORITY_CONFLICT", state=state)
        members = tuple(sorted(self._service.inventory(request.cluster)))
        self._require_same_inputs(state, request, members)
        if state["phase"] == "COMPLETED":
            return self._receipt(state)
        if state["phase"] not in {"COMMITTED", "CLEANUP_DISPATCHING"}:
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_IN_PROGRESS", state=state)
        return self._receipt(phase_ops.cleanup_phase(state, service=self._service, cas=self._cas, fail=self._fail))

    def abort(self, request: ExternalPublicationRequest) -> None:
        """Remove exact owned unpublished candidates and close the operation."""

        request.validate()
        state = self._read(request.target_key)
        if state is None or state.get("operation_id") != request.operation_id:
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_AUTHORITY_CONFLICT", state=state)
        members = tuple(sorted(self._service.inventory(request.cluster)))
        self._require_same_inputs(state, request, members)
        if state["phase"] == "ABORTED":
            return
        if state["phase"] not in {"LOCKED", "STAGING", "STAGED"}:
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_UNKNOWN", state=state)
        for member_id in state["member_ids"]:
            observed = dict(self._service.observe_candidate(member_id, state["candidate_name"]))
            if not observed.get("exists"):
                continue
            if not phase_ops.owned_observation(observed, state):
                self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", state=state)
            candidate_uuid = str(observed.get("candidate_uuid") or "")
            self._service.drop_owned_candidate(member_id, candidate_uuid=candidate_uuid)
            if self._service.observe_candidate(member_id, state["candidate_name"]).get("exists"):
                self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_UNKNOWN", state=state)
        self._cas(state, {**phase_ops.without_version(state), "phase": "ABORTED"})

    def _receipt(self, state: dict[str, Any]) -> ExternalReplicationReceipt:
        return runtime_support.receipt(self._service, state, ExternalReplicationReceipt.from_state)

    def _lock_values(
        self,
        *,
        target_key: str,
        operation_id: str,
        target: str,
        plan_sha256: str,
        members: tuple[str, ...],
    ) -> dict[str, Any]:
        return authority_ops.acquire_lock(
            target_key=target_key,
            operation_id=operation_id,
            candidate_name=phase_ops.candidate_name(target, operation_id),
            plan_sha256=plan_sha256,
            members=members,
            inventory_digest=runtime_support.inventory_digest(self._service, members, digest_payload),
            read=self._read,
            cas=self._cas,
            fail=self._fail,
        )

    def _stage(self, request: ExternalPublicationRequest, state: dict[str, Any]) -> dict[str, Any]:
        if state["phase"] == "LOCKED":
            artifact = request.artifact
            state = self._cas(
                state,
                {
                    **phase_ops.without_version(state),
                    "phase": "STAGING",
                    "artifact_sha256": artifact.sha256,
                    "artifact_binding_id": self._require_source().binding_id,
                    "artifact_byte_size": artifact.byte_size,
                    "artifact_schema_sha256": artifact.schema_sha256,
                    "artifact_content_sha256": artifact.content_sha256,
                    "artifact_row_count": artifact.row_count,
                    "generation_id": derive_generation_id(
                        operation_id=state["operation_id"],
                        artifact_sha256=artifact.sha256,
                        schema_digest=artifact.schema_sha256,
                        row_count=artifact.row_count,
                    ),
                },
            )
        if state["phase"] not in {"STAGING", "STAGED", "PUBLICATION_DISPATCHING", "COMMITTED", "CLEANUP_DISPATCHING"}:
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_STATE_INVALID", state=state)
        if state["phase"] != "STAGING":
            return state
        for member_id in state["member_ids"]:
            state = self._stage_member(request.artifact, state, member_id)
        if not all(value["state"] == "READY" for value in state["member_states"].values()):
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_STAGING_INCOMPLETE", state=state)
        return self._cas(state, {**phase_ops.without_version(state), "phase": "STAGED"})

    def _stage_member(self, artifact: ExternalArtifactReceipt, state: dict[str, Any], member_id: str) -> dict[str, Any]:
        source = self._require_source()
        source.revalidate(artifact.identity)
        member = dict(state["member_states"][member_id])
        observation = dict(self._service.observe_candidate(member_id, state["candidate_name"]))
        if member["state"] == "READY":
            if phase_ops.matches_generation(observation, state) and observation.get("candidate_uuid") == member.get(
                "candidate_uuid"
            ):
                return state
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", state=state)
        if observation.get("exists"):
            if phase_ops.matches_generation(observation, state):
                return self._mark_member(state, member_id, "READY", observation)
            if member["state"] not in {"CREATE_INTENT", "LOADING", "AMBIGUOUS"} or not phase_ops.owned_observation(
                observation, state
            ):
                self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", state=state)
            expected_uuid = str(observation.get("candidate_uuid") or "")
            if member.get("candidate_uuid") not in {None, expected_uuid}:
                self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", state=state)
            if member["state"] == "CREATE_INTENT" and member.get("candidate_uuid") == expected_uuid:
                state = self._mark_member(state, member_id, "LOADING", observation)
            else:
                self._service.drop_owned_candidate(member_id, candidate_uuid=expected_uuid)
                if self._service.observe_candidate(member_id, state["candidate_name"]).get("exists"):
                    self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_STAGING_INCOMPLETE", state=state)
        if state["member_states"][member_id]["state"] != "LOADING":
            state = self._mark_member(state, member_id, "LOADING", None)
        try:
            result = dict(
                self._service.stage_member_once(
                    member_id,
                    operation_id=state["operation_id"],
                    candidate_name=state["candidate_name"],
                    artifact=artifact,
                    source=source,
                )
            )
        except Exception:
            observed = dict(self._service.observe_candidate(member_id, state["candidate_name"]))
            latest = self._read(str(state["target_key"]))
            if latest is None or latest.get("operation_id") != state["operation_id"]:
                self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_AUTHORITY_CONFLICT", state=state)
            state = latest
            if phase_ops.matches_generation(observed, state):
                return self._mark_member(state, member_id, "READY", observed)
            state = self._mark_member(state, member_id, "AMBIGUOUS", observed if observed.get("exists") else None)
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_STAGING_INCOMPLETE", state=state)
        if not phase_ops.matches_generation(result, state):
            state = self._mark_member(state, member_id, "DIVERGED", result)
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", state=state)
        return self._mark_member(state, member_id, "READY", result)

    def _mark_member(
        self,
        state: dict[str, Any],
        member_id: str,
        status: str,
        candidate: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        members = {key: dict(value) for key, value in state["member_states"].items()}
        members[member_id] = {"state": status, **phase_ops.candidate_state(candidate)}
        return self._cas(state, {**phase_ops.without_version(state), "member_states": members})

    def _cas(self, current: dict[str, Any] | None, desired: Mapping[str, Any]) -> dict[str, Any]:
        return runtime_support.compare_and_swap(
            self._service,
            current,
            desired,
            self._fail,
            ExternalPublicationError,
        )

    def _read(self, target_key: str) -> dict[str, Any] | None:
        value = self._service.read_authority(target_key)
        return None if value is None else dict(value)

    def _require_same_inputs(
        self, state: Mapping[str, Any], request: ExternalPublicationRequest, members: tuple[str, ...]
    ) -> None:
        binding = (
            None
            if self._artifact_source is None or "artifact_binding_id" not in state
            else self._artifact_source.binding_id
        )
        phase_ops.require_same_inputs(
            state,
            request,
            members,
            runtime_support.inventory_digest(self._service, members, digest_payload),
            binding,
            self._fail,
        )

    def _require_source(self) -> ExternalArtifactSourcePort:
        if self._artifact_source is None:
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_ARTIFACT_UNSUPPORTED")
        return self._artifact_source

    def _fail(
        self,
        code: str,
        *,
        state: Mapping[str, Any] | None = None,
        member_ids: tuple[str, ...] = (),
    ) -> NoReturn:
        runtime_support.fail(
            self._service,
            code,
            state=state,
            member_ids=member_ids,
            error_factory=ExternalPublicationError,
        )


__all__ = ["ClickHouseExternalReplicationRuntime", "ExternalReplicationRuntimeService"]
