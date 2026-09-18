"""Synthetic end-to-end contract for external ClickHouse publication.

These tests intentionally define the runtime/port seam before the production
implementation.  The fake keeps rows in memory, but models durable authority,
member-local candidates, one-shot publication, and separately fenced cleanup.
"""

from __future__ import annotations

import copy
import json
from collections.abc import Mapping
from typing import Any

import pytest

from dpone.contracts.clickhouse_external_replication import (
    ExternalArtifactReceipt,
    ExternalPublicationError,
    ExternalPublicationRequest,
)
from dpone.runtime.sinks.clickhouse_external_replication_runtime import (
    ClickHouseExternalReplicationRuntime,
)

_ROWS = ((1, "alpha"), (2, "beta"), (3, "gamma"))
_ARTIFACT_SHA256 = "a" * 64
_CONTENT_SHA256 = "b" * 64
_SCHEMA_SHA256 = "c" * 64


class _SyntheticArtifactSource:
    binding_id = "artifact-v1"

    def revalidate(self, artifact: ExternalArtifactReceipt) -> None:
        assert artifact.artifact_id == self.binding_id

    def open_replay(self) -> object:
        return object()


class _SyntheticExternalService:
    """Narrow stateful fake for the external service port expected by runtime."""

    def __init__(self) -> None:
        self.members = ("member-a", "member-b")
        self.authority: dict[str, dict[str, Any]] = {}
        self.candidates: dict[str, dict[str, Any] | None] = dict.fromkeys(self.members)
        self.targets = {member: "predecessor-generation" for member in self.members}
        self.target_rows = {member: ((0, "old"),) for member in self.members}
        self.predecessors_present = dict.fromkeys(self.members, True)
        self.stage_calls = dict.fromkeys(self.members, 0)
        self.stage_effects = dict.fromkeys(self.members, 0)
        self.drop_candidate_calls = dict.fromkeys(self.members, 0)
        self.publish_calls = 0
        self.cleanup_calls = 0
        self.publish_dispatched = False
        self.cleanup_dispatched = False
        self._lost_stage_ack_member: str | None = None
        self._partial_stage_member: str | None = None
        self._partial_recovery_enabled = True
        self._divergent_member: str | None = None
        self._interrupt_cleanup = False

    def inventory(self, cluster: str) -> tuple[str, ...]:
        assert cluster == "analytics_cluster"
        return self.members

    def read_authority(self, target_key: str) -> Mapping[str, Any] | None:
        value = self.authority.get(target_key)
        return copy.deepcopy(value) if value is not None else None

    def compare_and_swap_authority(
        self,
        target_key: str,
        expected_version: int | None,
        desired: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        current = self.authority.get(target_key)
        current_version = None if current is None else int(current["version"])
        if current_version != expected_version:
            raise RuntimeError("authority_conflict")
        stored = copy.deepcopy(dict(desired))
        stored["version"] = 0 if current_version is None else current_version + 1
        self.authority[target_key] = stored
        return copy.deepcopy(stored)

    def observe_candidate(self, member_id: str, candidate_name: str) -> Mapping[str, Any]:
        assert member_id in self.members
        value = self.candidates[member_id]
        if value is None:
            return {"exists": False, "member_id": member_id}
        assert value["candidate_name"] == candidate_name
        return copy.deepcopy(value)

    def stage_member_once(
        self,
        member_id: str,
        *,
        operation_id: str,
        candidate_name: str,
        artifact: ExternalArtifactReceipt,
        source: _SyntheticArtifactSource,
    ) -> Mapping[str, Any]:
        """Apply one direct member-local load; this method never retries itself."""

        self.stage_calls[member_id] += 1
        source.revalidate(artifact)
        if self.candidates[member_id] is not None:
            raise AssertionError("runtime attempted to append to an existing candidate")
        rows = _ROWS
        content_sha256 = artifact.content_sha256
        if member_id == self._partial_stage_member:
            rows = _ROWS[:1]
            content_sha256 = "d" * 64
        elif member_id == self._divergent_member:
            content_sha256 = "e" * 64
        candidate = {
            "exists": True,
            "member_id": member_id,
            "operation_id": operation_id,
            "candidate_name": candidate_name,
            "candidate_uuid": f"uuid-{member_id}-{self.stage_effects[member_id] + 1}",
            "schema_sha256": artifact.schema_sha256,
            "content_sha256": content_sha256,
            "row_count": len(rows),
            "rows": rows,
        }
        self.candidates[member_id] = candidate
        self.stage_effects[member_id] += 1
        if member_id == self._partial_stage_member:
            self._partial_stage_member = None
            raise TimeoutError("synthetic_partial_stage")
        if member_id == self._lost_stage_ack_member:
            self._lost_stage_ack_member = None
            raise TimeoutError("synthetic_lost_stage_ack")
        return copy.deepcopy(candidate)

    def drop_owned_candidate(self, member_id: str, *, candidate_uuid: str) -> None:
        candidate = self.candidates[member_id]
        assert candidate is not None
        assert candidate["candidate_uuid"] == candidate_uuid
        if not self._partial_recovery_enabled:
            raise TimeoutError("synthetic_recovery_deferred")
        self.drop_candidate_calls[member_id] += 1
        self.candidates[member_id] = None

    def dispatch_publication_once(
        self,
        *,
        operation_id: str,
        candidate_name: str,
        member_ids: tuple[str, ...],
    ) -> None:
        assert member_ids == self.members
        assert all(self.candidates[member] is not None for member in member_ids)
        if self.publish_dispatched:
            raise AssertionError("publication was blindly redispatched")
        self.publish_calls += 1
        self.publish_dispatched = True
        for member in member_ids:
            candidate = self.candidates[member]
            assert candidate is not None
            assert candidate["operation_id"] == operation_id
            assert candidate["candidate_name"] == candidate_name
            self.targets[member] = operation_id
            self.target_rows[member] = candidate["rows"]
            self.candidates[member] = {
                "exists": True,
                "member_id": member,
                "operation_id": operation_id,
                "candidate_name": candidate_name,
                "candidate_uuid": f"predecessor-{member}",
                "schema_sha256": _SCHEMA_SHA256,
                "content_sha256": "0" * 64,
                "row_count": 1,
                "rows": ((0, "old"),),
            }

    def observe_publication(self, operation_id: str) -> Mapping[str, str]:
        return {
            member: ("desired" if value == operation_id else "predecessor") for member, value in self.targets.items()
        }

    def dispatch_cleanup_once(self, *, operation_id: str, member_ids: tuple[str, ...]) -> None:
        assert self.publish_dispatched
        if self.cleanup_dispatched:
            raise AssertionError("cleanup was blindly redispatched")
        self.cleanup_calls += 1
        self.cleanup_dispatched = True
        if self._interrupt_cleanup:
            self.predecessors_present[member_ids[0]] = False
            self.candidates[member_ids[0]] = None
            raise TimeoutError("synthetic_cleanup_interrupted")
        self.predecessors_present = dict.fromkeys(member_ids, False)
        self.candidates = dict.fromkeys(member_ids)

    def observe_cleanup(self, operation_id: str) -> Mapping[str, bool]:
        assert operation_id in self.targets.values()
        return dict(self.predecessors_present)

    def lose_next_stage_ack(self, member_id: str) -> None:
        self._lost_stage_ack_member = member_id

    def interrupt_next_stage(self, member_id: str) -> None:
        self._partial_stage_member = member_id
        self._partial_recovery_enabled = False

    def allow_partial_stage_recovery(self) -> None:
        self._partial_recovery_enabled = True

    def diverge_next_stage(self, member_id: str) -> None:
        self._divergent_member = member_id

    def interrupt_cleanup(self) -> None:
        self._interrupt_cleanup = True

    def finish_original_cleanup(self) -> None:
        assert self.cleanup_dispatched
        self.predecessors_present = dict.fromkeys(self.members, False)
        self.candidates = dict.fromkeys(self.members)

    def effect_counts(self) -> tuple[Any, ...]:
        return (
            dict(self.stage_calls),
            dict(self.stage_effects),
            dict(self.drop_candidate_calls),
            self.publish_calls,
            self.cleanup_calls,
        )


def _artifact() -> ExternalArtifactReceipt:
    return ExternalArtifactReceipt(
        artifact_id="artifact-v1",
        sha256=_ARTIFACT_SHA256,
        byte_size=128,
        row_count=len(_ROWS),
        schema_sha256=_SCHEMA_SHA256,
        content_sha256=_CONTENT_SHA256,
        replayable=True,
    )


def _request() -> ExternalPublicationRequest:
    return ExternalPublicationRequest(
        cluster="analytics_cluster",
        database="analytics",
        target="target_table",
        scheduler_invocation="synthetic-run-1",
        plan_sha256="f" * 64,
        artifact=_artifact(),
    )


def _runtime(service: _SyntheticExternalService) -> ClickHouseExternalReplicationRuntime:
    return ClickHouseExternalReplicationRuntime(service=service, artifact_source=_SyntheticArtifactSource())


def _assert_completed(receipt: Any, service: _SyntheticExternalService) -> None:
    assert receipt.phase == "COMPLETED"
    assert receipt.replication_mode == "external"
    assert tuple(receipt.member_ids) == service.members
    assert receipt.evidence_status == "PASS"
    assert receipt.evidence_scope == "local_synthetic"
    assert all(service.targets[member] == receipt.operation_id for member in service.members)
    assert all(service.predecessors_present[member] is False for member in service.members)


def test_external_runtime_stages_and_publishes_complete_generation_on_every_member() -> None:
    service = _SyntheticExternalService()

    receipt = _runtime(service).run(_request())

    _assert_completed(receipt, service)
    assert service.stage_calls == {"member-a": 1, "member-b": 1}
    assert service.stage_effects == {"member-a": 1, "member-b": 1}
    assert service.publish_calls == 1
    assert service.cleanup_calls == 1
    assert all(rows == _ROWS for rows in service.target_rows.values())
    assert all(candidate is None for candidate in service.candidates.values())


def test_lost_member_stage_ack_is_reconciled_without_duplicate_insert() -> None:
    service = _SyntheticExternalService()
    service.lose_next_stage_ack("member-a")

    receipt = _runtime(service).run(_request())

    _assert_completed(receipt, service)
    assert service.stage_calls["member-a"] == 1
    assert service.stage_effects["member-a"] == 1
    assert service.target_rows["member-a"] == _ROWS


def test_partial_member_stage_blocks_then_retry_rebuilds_exact_candidate() -> None:
    service = _SyntheticExternalService()
    runtime = _runtime(service)
    service.interrupt_next_stage("member-b")

    with pytest.raises(ExternalPublicationError) as raised:
        runtime.run(_request())

    assert raised.value.code == "DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_STAGING_INCOMPLETE"
    assert service.publish_calls == 0
    assert service.cleanup_calls == 0
    assert service.stage_effects == {"member-a": 1, "member-b": 1}

    service.allow_partial_stage_recovery()
    receipt = runtime.run(_request())

    _assert_completed(receipt, service)
    assert service.stage_effects == {"member-a": 1, "member-b": 2}
    assert service.drop_candidate_calls == {"member-a": 0, "member-b": 1}


def test_divergent_member_digest_blocks_publication_and_preserves_candidates() -> None:
    service = _SyntheticExternalService()
    service.diverge_next_stage("member-b")

    with pytest.raises(ExternalPublicationError) as raised:
        _runtime(service).run(_request())

    assert raised.value.code == "DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_GENERATION_DIVERGED"
    assert service.publish_calls == 0
    assert all(candidate is not None for candidate in service.candidates.values())


def test_completed_same_operation_replay_has_zero_mutation_effects() -> None:
    service = _SyntheticExternalService()
    runtime = _runtime(service)
    first = runtime.run(_request())
    before = service.effect_counts()

    replay = runtime.run(_request())

    assert replay.to_dict() == first.to_dict()
    assert service.effect_counts() == before


def test_cleanup_interruption_resumes_original_cleanup_without_redispatch() -> None:
    service = _SyntheticExternalService()
    runtime = _runtime(service)
    service.interrupt_cleanup()

    with pytest.raises(ExternalPublicationError) as raised:
        runtime.run(_request())

    assert raised.value.code == "DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_IN_PROGRESS"
    assert service.publish_calls == 1
    assert service.cleanup_calls == 1
    assert tuple(service.predecessors_present.values()) == (False, True)

    service.finish_original_cleanup()
    receipt = runtime.run(_request())

    _assert_completed(receipt, service)
    assert service.publish_calls == 1
    assert service.cleanup_calls == 1


def test_failure_evidence_is_redacted_and_contains_only_opaque_member_ids() -> None:
    service = _SyntheticExternalService()
    service.diverge_next_stage("member-b")

    with pytest.raises(ExternalPublicationError) as raised:
        _runtime(service).run(_request())

    rendered = json.dumps(raised.value.evidence, sort_keys=True)
    assert set(raised.value.evidence["member_ids"]) == {"member-a", "member-b"}
    assert raised.value.evidence["evidence_scope"] == "local_synthetic"
    for forbidden in (
        "SENSITIVE_ENDPOINT",
        "SENSITIVE_SECRET",
        "SENSITIVE_VALUE",
        "alpha",
        "beta",
        "gamma",
        "artifact-v1",
    ):
        assert forbidden not in rendered
        assert forbidden not in str(raised.value)
