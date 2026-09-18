from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any

import pytest

from dpone.contracts.clickhouse_external_replication import (
    ExternalArtifactReceipt,
    ExternalPublicationError,
    ExternalPublicationRequest,
)
from dpone.runtime.sinks.clickhouse_external_replication_runtime import ClickHouseExternalReplicationRuntime


class _Service:
    def __init__(self) -> None:
        self.members = ("member-a", "member-b")
        self.authority: dict[str, Any] | None = None
        self.candidates: dict[str, dict[str, Any] | None] = dict.fromkeys(self.members)
        self.targets = dict.fromkeys(self.members, "predecessor")
        self.predecessors = dict.fromkeys(self.members, True)
        self.stage_calls = dict.fromkeys(self.members, 0)
        self.drop_calls = dict.fromkeys(self.members, 0)
        self.publish_calls = 0
        self.cleanup_calls = 0
        self.partial_member: str | None = None
        self.cleanup_interrupted = False

    def inventory(self, cluster: str) -> tuple[str, ...]:
        assert cluster == "analytics_cluster"
        return self.members

    def read_authority(self, target_key: str) -> Mapping[str, Any] | None:
        del target_key
        return copy.deepcopy(self.authority)

    def compare_and_swap_authority(
        self, target_key: str, expected_version: int | None, desired: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        del target_key
        actual = None if self.authority is None else self.authority["version"]
        if actual != expected_version:
            raise RuntimeError("authority_conflict")
        self.authority = {**copy.deepcopy(dict(desired)), "version": 0 if actual is None else actual + 1}
        return copy.deepcopy(self.authority)

    def observe_candidate(self, member_id: str, candidate_name: str) -> Mapping[str, Any]:
        candidate = self.candidates[member_id]
        if candidate is None:
            return {"exists": False, "member_id": member_id}
        assert candidate["candidate_name"] == candidate_name
        return copy.deepcopy(candidate)

    def stage_member_once(self, member_id: str, **values: Any) -> Mapping[str, Any]:
        assert self.candidates[member_id] is None
        self.stage_calls[member_id] += 1
        artifact = values["artifact"]
        candidate = {
            "exists": True,
            "member_id": member_id,
            "operation_id": values["operation_id"],
            "candidate_name": values["candidate_name"],
            "candidate_uuid": f"uuid-{member_id}-{self.stage_calls[member_id]}",
            "schema_sha256": artifact.schema_sha256,
            "content_sha256": artifact.content_sha256,
            "row_count": artifact.row_count,
        }
        self.candidates[member_id] = candidate
        if member_id == self.partial_member:
            self.partial_member = None
            candidate["content_sha256"] = "d" * 64
            raise TimeoutError("partial")
        return copy.deepcopy(candidate)

    def drop_owned_candidate(self, member_id: str, *, candidate_uuid: str) -> None:
        assert self.candidates[member_id]["candidate_uuid"] == candidate_uuid  # type: ignore[index]
        self.drop_calls[member_id] += 1
        self.candidates[member_id] = None

    def dispatch_publication_once(self, **values: Any) -> None:
        self.publish_calls += 1
        for member in values["member_ids"]:
            self.targets[member] = values["operation_id"]

    def observe_publication(self, operation_id: str) -> Mapping[str, str]:
        return {member: "desired" if value == operation_id else "predecessor" for member, value in self.targets.items()}

    def dispatch_cleanup_once(self, **values: Any) -> None:
        self.cleanup_calls += 1
        members = values["member_ids"]
        self.predecessors[members[0]] = False
        if self.cleanup_interrupted:
            raise TimeoutError("cleanup")
        self.predecessors = dict.fromkeys(members, False)

    def observe_cleanup(self, operation_id: str) -> Mapping[str, bool]:
        del operation_id
        return dict(self.predecessors)


def _request() -> ExternalPublicationRequest:
    return ExternalPublicationRequest(
        cluster="analytics_cluster",
        database="analytics",
        target="target_table",
        scheduler_invocation="scheduled-run",
        plan_sha256="f" * 64,
        artifact=ExternalArtifactReceipt(
            artifact_id="artifact-v1",
            sha256="a" * 64,
            byte_size=128,
            row_count=2,
            schema_sha256="b" * 64,
            content_sha256="c" * 64,
            replayable=True,
        ),
    )


def test_completed_operation_replays_without_mutation() -> None:
    service = _Service()
    runtime = ClickHouseExternalReplicationRuntime(service=service)

    first = runtime.run(_request())
    effects = (dict(service.stage_calls), service.publish_calls, service.cleanup_calls)
    replay = runtime.run(_request())

    assert first.to_dict() == replay.to_dict()
    assert replay.phase == "COMPLETED"
    assert effects == (service.stage_calls, service.publish_calls, service.cleanup_calls)


def test_ambiguous_partial_candidate_is_replaced_before_retry() -> None:
    service = _Service()
    service.partial_member = "member-b"
    runtime = ClickHouseExternalReplicationRuntime(service=service)

    with pytest.raises(ExternalPublicationError, match="STAGING_INCOMPLETE"):
        runtime.run(_request())

    receipt = runtime.run(_request())
    assert receipt.phase == "COMPLETED"
    assert service.stage_calls["member-b"] == 2
    assert service.drop_calls["member-b"] == 1
    assert service.publish_calls == 1


def test_cleanup_resume_observes_original_dispatch_without_repeating_it() -> None:
    service = _Service()
    service.cleanup_interrupted = True
    runtime = ClickHouseExternalReplicationRuntime(service=service)

    with pytest.raises(ExternalPublicationError, match="CLEANUP_IN_PROGRESS"):
        runtime.run(_request())
    assert service.cleanup_calls == 1

    service.predecessors = dict.fromkeys(service.members, False)
    receipt = runtime.run(_request())

    assert receipt.phase == "COMPLETED"
    assert service.cleanup_calls == 1
