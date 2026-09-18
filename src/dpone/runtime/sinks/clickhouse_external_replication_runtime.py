"""Recoverable coordinator for externally replicated ClickHouse publication."""

from __future__ import annotations

import secrets
from collections.abc import Mapping
from typing import Any, NoReturn, Protocol

from dpone.ports.clickhouse_external_replication import (
    ExternalArtifactReceipt,
    ExternalArtifactSourcePort,
    ExternalContractError,
    ExternalPublicationError,
    ExternalPublicationRequest,
    derive_generation_id,
    derive_operation_id,
    derive_target_key,
    digest_payload,
)
from dpone.runtime.sinks.clickhouse_external_replication_receipt import ExternalReplicationReceipt
from dpone.runtime.sinks.clickhouse_external_replication_state import candidate_name as _candidate_name
from dpone.runtime.sinks.clickhouse_external_replication_state import matches_generation as _matches
from dpone.runtime.sinks.clickhouse_external_replication_state import owned_observation as _owned
from dpone.runtime.sinks.clickhouse_external_replication_state import without_version as _without_version


class ExternalReplicationRuntimeService(Protocol):
    """Durable effects required by the pure runtime coordinator."""

    def inventory(self, cluster: str) -> tuple[str, ...]: ...
    def inventory_digest(self) -> str: ...
    def read_authority(self, target_key: str) -> Mapping[str, Any] | None: ...
    def compare_and_swap_authority(
        self, target_key: str, expected_version: int | None, desired: Mapping[str, Any]
    ) -> Mapping[str, Any]: ...
    def observe_candidate(self, member_id: str, candidate_name: str) -> Mapping[str, Any]: ...
    def stage_member_once(
        self,
        member_id: str,
        *,
        operation_id: str,
        candidate_name: str,
        artifact: ExternalArtifactReceipt,
        source: ExternalArtifactSourcePort,
    ) -> Mapping[str, Any]: ...
    def drop_owned_candidate(self, member_id: str, *, candidate_uuid: str) -> None: ...
    def dispatch_publication_once(
        self, *, operation_id: str, candidate_name: str, member_ids: tuple[str, ...]
    ) -> None: ...
    def observe_publication(self, operation_id: str) -> Mapping[str, str]: ...
    def dispatch_cleanup_once(self, *, operation_id: str, member_ids: tuple[str, ...]) -> None: ...
    def observe_cleanup(self, operation_id: str) -> Mapping[str, bool]: ...


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
        """Resume one deterministic operation through staging, publish, and cleanup."""

        self.stage(request)
        self.publish(request)
        return self.cleanup(request)

    def stage(self, request: ExternalPublicationRequest) -> ExternalReplicationReceipt:
        """Fence and stage every member without mutating the published target."""

        try:
            request.validate()
        except ExternalContractError as error:
            self._fail(f"DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_{error.code}")
        state = self.prepare(
            cluster=request.cluster,
            database=request.database,
            target=request.target,
            scheduler_invocation=request.scheduler_invocation,
            plan_sha256=request.plan_sha256,
        )
        source = self._require_source()
        self._require_artifact(request.artifact, member_ids=())
        source.revalidate(request.artifact.identity)
        if source.binding_id != request.artifact.artifact_id or source.identity != request.artifact.identity:
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_ARTIFACT_CHANGED")
        members = tuple(sorted(self._service.inventory(request.cluster)))
        if len(members) < 2 or len(set(members)) != len(members):
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_INVENTORY_INVALID", member_ids=members)
        self._require_artifact(request.artifact, member_ids=members)
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
        state = self._publish(state)
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
                or not _matches(observed, state)
                or observed.get("candidate_uuid") != bound.get("candidate_uuid")
            ):
                self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", state=state)
        return self._receipt(state)

    def cleanup(self, request: ExternalPublicationRequest) -> ExternalReplicationReceipt:
        """Finish separately fenced cleanup after publication is committed."""

        state = self._read(request.target_key)
        if state is None or state.get("operation_id") != request.operation_id:
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_AUTHORITY_CONFLICT", state=state)
        if state["phase"] == "COMPLETED":
            return self._receipt(state)
        if state["phase"] not in {"COMMITTED", "CLEANUP_DISPATCHING"}:
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_IN_PROGRESS", state=state)
        return self._receipt(self._cleanup(state))

    def abort(self, request: ExternalPublicationRequest) -> None:
        """Remove exact owned unpublished candidates and close the operation."""

        state = self._read(request.target_key)
        if state is None or state.get("operation_id") != request.operation_id:
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_AUTHORITY_CONFLICT", state=state)
        if state["phase"] == "ABORTED":
            return
        if state["phase"] not in {"LOCKED", "STAGING", "STAGED"}:
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_UNKNOWN", state=state)
        for member_id in state["member_ids"]:
            observed = dict(self._service.observe_candidate(member_id, state["candidate_name"]))
            if not observed.get("exists"):
                continue
            if not _owned(observed, state):
                self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", state=state)
            candidate_uuid = str(observed.get("candidate_uuid") or "")
            self._service.drop_owned_candidate(member_id, candidate_uuid=candidate_uuid)
            if self._service.observe_candidate(member_id, state["candidate_name"]).get("exists"):
                self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_UNKNOWN", state=state)
        self._cas(state, {**_without_version(state), "phase": "ABORTED"})

    def _receipt(self, state: dict[str, Any]) -> ExternalReplicationReceipt:
        scope = str(getattr(self._service, "evidence_scope", "local_synthetic"))
        return ExternalReplicationReceipt.from_state(state, evidence_scope=scope)

    def _lock_values(
        self,
        *,
        target_key: str,
        operation_id: str,
        target: str,
        plan_sha256: str,
        members: tuple[str, ...],
    ) -> dict[str, Any]:
        current = self._read(target_key)
        if current is not None and current.get("operation_id") == operation_id:
            if (
                tuple(current["member_ids"]) != members
                or current["plan_digest"] != plan_sha256
                or current["inventory_digest"] != self._inventory_digest(members)
            ):
                self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_INVENTORY_DRIFT", state=current)
            return current
        if current is not None and current.get("phase") not in {"COMPLETED", "ABORTED"}:
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_AUTHORITY_CONFLICT", state=current)
        desired = {
            "target_key": target_key,
            "operation_id": operation_id,
            "fence_token": secrets.token_hex(16),
            "phase": "LOCKED",
            "dispatch_epoch": 0 if current is None else int(current["dispatch_epoch"]) + 1,
            "inventory_digest": self._inventory_digest(members),
            "plan_digest": plan_sha256,
            "candidate_name": _candidate_name(target, operation_id),
            "member_ids": members,
            "member_states": {member: {"state": "PENDING"} for member in members},
        }
        return self._cas(current, desired)

    def _inventory_digest(self, members: tuple[str, ...]) -> str:
        provider = getattr(self._service, "inventory_digest", None)
        if callable(provider):
            return str(provider())
        return digest_payload({"member_ids": members})

    def _stage(self, request: ExternalPublicationRequest, state: dict[str, Any]) -> dict[str, Any]:
        if state["phase"] == "LOCKED":
            artifact = request.artifact
            state = self._cas(
                state,
                {
                    **_without_version(state),
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
        return self._cas(state, {**_without_version(state), "phase": "STAGED"})

    def _stage_member(self, artifact: ExternalArtifactReceipt, state: dict[str, Any], member_id: str) -> dict[str, Any]:
        source = self._require_source()
        source.revalidate(artifact.identity)
        member = dict(state["member_states"][member_id])
        observation = dict(self._service.observe_candidate(member_id, state["candidate_name"]))
        if member["state"] == "READY":
            if _matches(observation, state) and observation.get("candidate_uuid") == member.get("candidate_uuid"):
                return state
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", state=state)
        if observation.get("exists"):
            if _matches(observation, state):
                return self._mark_member(state, member_id, "READY", str(observation["candidate_uuid"]))
            if member["state"] not in {"LOADING", "AMBIGUOUS"} or not _owned(observation, state):
                self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", state=state)
            expected_uuid = str(observation.get("candidate_uuid") or "")
            if member.get("candidate_uuid") not in {None, expected_uuid}:
                self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", state=state)
            self._service.drop_owned_candidate(member_id, candidate_uuid=expected_uuid)
            if self._service.observe_candidate(member_id, state["candidate_name"]).get("exists"):
                self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_STAGING_INCOMPLETE", state=state)
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
            if _matches(observed, state):
                return self._mark_member(state, member_id, "READY", str(observed["candidate_uuid"]))
            candidate_uuid = str(observed.get("candidate_uuid") or "") or None
            state = self._mark_member(state, member_id, "AMBIGUOUS", candidate_uuid)
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_STAGING_INCOMPLETE", state=state)
        if not _matches(result, state):
            state = self._mark_member(state, member_id, "DIVERGED", str(result.get("candidate_uuid") or ""))
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED", state=state)
        return self._mark_member(state, member_id, "READY", str(result["candidate_uuid"]))

    def _publish(self, state: dict[str, Any]) -> dict[str, Any]:
        if state["phase"] == "STAGED":
            state = self._cas(
                state,
                {
                    **_without_version(state),
                    "phase": "PUBLICATION_DISPATCHING",
                    "dispatch_epoch": int(state["dispatch_epoch"]) + 1,
                },
            )
            try:
                self._service.dispatch_publication_once(
                    operation_id=state["operation_id"],
                    candidate_name=state["candidate_name"],
                    member_ids=tuple(state["member_ids"]),
                )
            except Exception:
                pass
        if state["phase"] == "PUBLICATION_DISPATCHING":
            observed = self._service.observe_publication(state["operation_id"])
            if set(observed) != set(state["member_ids"]) or set(observed.values()) != {"desired"}:
                self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_PUBLICATION_IN_PROGRESS", state=state)
            state = self._cas(state, {**_without_version(state), "phase": "COMMITTED"})
        return state

    def _cleanup(self, state: dict[str, Any]) -> dict[str, Any]:
        if state["phase"] == "COMMITTED":
            state = self._cas(
                state,
                {
                    **_without_version(state),
                    "phase": "CLEANUP_DISPATCHING",
                    "dispatch_epoch": int(state["dispatch_epoch"]) + 1,
                },
            )
            try:
                self._service.dispatch_cleanup_once(
                    operation_id=state["operation_id"], member_ids=tuple(state["member_ids"])
                )
            except Exception:
                pass
        if state["phase"] == "CLEANUP_DISPATCHING":
            observed = self._service.observe_cleanup(state["operation_id"])
            if set(observed) != set(state["member_ids"]) or any(observed.values()):
                self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_IN_PROGRESS", state=state)
            state = self._cas(state, {**_without_version(state), "phase": "COMPLETED"})
        if state["phase"] != "COMPLETED":
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_STATE_INVALID", state=state)
        return state

    def _mark_member(
        self, state: dict[str, Any], member_id: str, status: str, candidate_uuid: str | None
    ) -> dict[str, Any]:
        members = {key: dict(value) for key, value in state["member_states"].items()}
        members[member_id] = {"state": status, "candidate_uuid": candidate_uuid}
        return self._cas(state, {**_without_version(state), "member_states": members})

    def _cas(self, current: dict[str, Any] | None, desired: Mapping[str, Any]) -> dict[str, Any]:
        target_key = str(desired["target_key"])
        version = None if current is None else int(current["version"])
        try:
            return dict(self._service.compare_and_swap_authority(target_key, version, desired))
        except Exception:
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_AUTHORITY_CONFLICT", state=current)

    def _read(self, target_key: str) -> dict[str, Any] | None:
        value = self._service.read_authority(target_key)
        return None if value is None else dict(value)

    def _require_same_inputs(
        self, state: Mapping[str, Any], request: ExternalPublicationRequest, members: tuple[str, ...]
    ) -> None:
        if tuple(state["member_ids"]) != members or state["plan_digest"] != request.plan_sha256:
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_INVENTORY_DRIFT", state=state)
        if state["inventory_digest"] != self._inventory_digest(members):
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_INVENTORY_DRIFT", state=state)
        if "artifact_sha256" in state and state["artifact_sha256"] != request.artifact.sha256:
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_ARTIFACT_CHANGED", state=state)
        if (
            self._artifact_source is not None
            and "artifact_binding_id" in state
            and state["artifact_binding_id"] != self._artifact_source.binding_id
        ):
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_ARTIFACT_CHANGED", state=state)

    def _require_source(self) -> ExternalArtifactSourcePort:
        if self._artifact_source is None:
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_ARTIFACT_UNSUPPORTED")
        return self._artifact_source

    def _require_artifact(self, artifact: ExternalArtifactReceipt, *, member_ids: tuple[str, ...]) -> None:
        if not artifact.replayable or artifact.byte_size < 0 or artifact.row_count < 0:
            self._fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_ARTIFACT_UNSUPPORTED", member_ids=member_ids)

    @staticmethod
    def _fail(
        code: str,
        *,
        state: Mapping[str, Any] | None = None,
        member_ids: tuple[str, ...] = (),
    ) -> NoReturn:
        evidence: dict[str, object] = {
            "evidence_scope": "local_synthetic",
            "member_ids": list(member_ids or tuple(state.get("member_ids", ())) if state else member_ids),
        }
        if state is not None:
            evidence.update(
                target_key=state.get("target_key"),
                operation_id=state.get("operation_id"),
                phase=state.get("phase"),
            )
        raise ExternalPublicationError(code, evidence=evidence)


__all__ = ["ClickHouseExternalReplicationRuntime", "ExternalReplicationRuntimeService"]
