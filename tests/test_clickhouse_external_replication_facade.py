from __future__ import annotations

import copy
from collections.abc import Mapping
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import SOURCE_BYTE_BUDGET_OPTION, LoadStrategy
from dpone.contracts.clickhouse_external_replication import ArtifactIdentity, ExternalPublicationError
from dpone.runtime.sinks.clickhouse_external_replication_facade import (
    ClickHouseExternalReplicationFacade,
)
from dpone.runtime.sinks.clickhouse_external_replication_runtime import (
    ClickHouseExternalReplicationRuntime,
)
from dpone.runtime.sinks.clickhouse_full_refresh_publication import SCHEDULER_IDENTITY_OPTION


class _ArtifactSource:
    binding_id = "artifact-v1"
    identity = ArtifactIdentity(
        sha256="a" * 64,
        byte_size=128,
        row_count=2,
        schema_digest="b" * 64,
        wire_digest="c" * 64,
    )

    def revalidate(self, expected: ArtifactIdentity) -> None:
        assert expected == self.identity

    def open_replay(self) -> object:
        return object()


class _Service:
    evidence_scope = "local_synthetic"

    def __init__(self) -> None:
        self.members = ("member-a", "member-b")
        self.authority: dict[str, Any] | None = None
        self.candidates: dict[str, dict[str, Any] | None] = dict.fromkeys(self.members)
        self.targets = dict.fromkeys(self.members, "predecessor")
        self.predecessors = dict.fromkeys(self.members, True)
        self.stage_calls = 0
        self.publish_calls = 0
        self.cleanup_calls = 0

    def inventory(self, cluster: str) -> tuple[str, ...]:
        assert cluster == "analytics_cluster"
        return self.members

    def inventory_digest(self) -> str:
        return "d" * 64

    def read_authority(self, target_key: str) -> Mapping[str, Any] | None:
        del target_key
        return copy.deepcopy(self.authority)

    def compare_and_swap_authority(
        self, target_key: str, expected_version: int | None, desired: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        del target_key
        actual = None if self.authority is None else self.authority["version"]
        assert actual == expected_version
        self.authority = {**copy.deepcopy(dict(desired)), "version": 0 if actual is None else actual + 1}
        return copy.deepcopy(self.authority)

    def observe_candidate(self, member_id: str, candidate_name: str) -> Mapping[str, Any]:
        candidate = self.candidates[member_id]
        if candidate is None:
            return {"exists": False, "member_id": member_id}
        assert candidate["candidate_name"] == candidate_name
        return copy.deepcopy(candidate)

    def stage_member_once(self, member_id: str, **values: Any) -> Mapping[str, Any]:
        self.stage_calls += 1
        artifact = values["artifact"]
        candidate = {
            "exists": True,
            "member_id": member_id,
            "operation_id": values["operation_id"],
            "candidate_name": values["candidate_name"],
            "candidate_uuid": f"uuid-{member_id}",
            "schema_sha256": artifact.schema_sha256,
            "content_sha256": artifact.content_sha256,
            "row_count": artifact.row_count,
        }
        self.candidates[member_id] = candidate
        return copy.deepcopy(candidate)

    def drop_owned_candidate(self, member_id: str, *, candidate_uuid: str) -> None:
        assert self.candidates[member_id]["candidate_uuid"] == candidate_uuid  # type: ignore[index]
        self.candidates[member_id] = None

    def dispatch_publication_once(self, **values: Any) -> None:
        self.publish_calls += 1
        for member in values["member_ids"]:
            self.targets[member] = values["operation_id"]

    def observe_publication(self, operation_id: str) -> Mapping[str, str]:
        return {member: "desired" if value == operation_id else "predecessor" for member, value in self.targets.items()}

    def dispatch_cleanup_once(self, **values: Any) -> None:
        self.cleanup_calls += 1
        self.predecessors = dict.fromkeys(values["member_ids"], False)

    def observe_cleanup(self, operation_id: str) -> Mapping[str, bool]:
        del operation_id
        return dict(self.predecessors)


@dataclass
class _Fixture:
    service: _Service
    facade: ClickHouseExternalReplicationFacade
    source_calls: int = 0


def _fixture() -> _Fixture:
    service = _Service()
    holder = _Fixture(service, None)  # type: ignore[arg-type]

    def source_factory(load_config: Any, payload: Any) -> _ArtifactSource:
        assert load_config.target_table == "target_table"
        assert payload.artifact.file_path == "/synthetic/only-used-by-factory"
        assert service.authority is not None and service.authority["phase"] == "LOCKED"
        holder.source_calls += 1
        return _ArtifactSource()

    holder.facade = ClickHouseExternalReplicationFacade(
        service_factory=lambda cluster, database, target, **_: service,
        artifact_source_factory=source_factory,
        runtime_factory=ClickHouseExternalReplicationRuntime,
    )
    return holder


def _config(**option_updates: Any) -> LoadConfig:
    options = {
        SOURCE_BYTE_BUDGET_OPTION: 1024,
        SCHEDULER_IDENTITY_OPTION: "scheduled-run",
        "lineage": False,
        "physical_design": {
            "storage": {
                "clickhouse": {
                    "engine": "MergeTree",
                    "cluster": {
                        "name": "analytics_cluster",
                        "ddl_scope": "cluster",
                        "replication_mode": "external",
                    },
                }
            }
        },
    }
    options.update(option_updates)
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="source",
        source_table="source_table",
        target_schema="analytics",
        target_table="target_table",
        load_strategy=LoadStrategy.FULL_REFRESH,
        staging_schema="analytics",
        options=options,
    )


def _payload(*, codec: object | None = None) -> Any:
    artifact = SimpleNamespace(file_path="/synthetic/only-used-by-factory", bulk_text_codec=codec)
    return SimpleNamespace(artifact=artifact, schema=(("id", "int"),))


def test_facade_fences_before_artifact_factory_and_runs_governed_lifecycle() -> None:
    fixture = _fixture()
    config = fixture.facade.prepare_admission(_config())

    assert fixture.service.authority is not None
    assert fixture.service.authority["phase"] == "LOCKED"
    assert fixture.source_calls == 0
    assert fixture.facade.replay_result(config) is None

    context = fixture.facade.stage(config, _payload())
    validation = fixture.facade.validate(context)
    committed = fixture.facade.publish(context, validation)
    completed = fixture.facade.cleanup(context)

    assert context.staged_receipt.phase == "STAGED"
    assert committed.phase == "COMMITTED"
    assert completed.phase == "COMPLETED"
    assert fixture.service.stage_calls == 2
    assert fixture.service.publish_calls == 1
    assert fixture.service.cleanup_calls == 1
    assert "file_path" not in repr(context)


def test_direct_stage_acquires_authority_before_artifact_factory() -> None:
    fixture = _fixture()

    context = fixture.facade.stage(_config(), _payload())

    assert fixture.service.authority is not None
    assert context.staged_receipt.phase == "STAGED"
    assert fixture.source_calls == 1


def test_prepare_completed_operation_exposes_idempotent_replay_without_source() -> None:
    fixture = _fixture()
    config = fixture.facade.prepare_admission(_config())
    context = fixture.facade.stage(config, _payload())
    fixture.facade.publish(context, fixture.facade.validate(context))
    completed = fixture.facade.cleanup(context)

    replay_config = fixture.facade.prepare_admission(_config())

    replay = fixture.facade.replay_result(replay_config)
    assert replay is not None
    assert replay.commit_receipt_id == completed.operation_id
    assert replay.inserted_rows == 2
    assert fixture.source_calls == 1


def test_validation_is_bound_to_exact_staged_authority() -> None:
    fixture = _fixture()
    config = fixture.facade.prepare_admission(_config())
    context = fixture.facade.stage(config, _payload())
    validation = fixture.facade.validate(context)
    wrong = type(validation)(validation.operation_id, validation.generation_id, validation.authority_version + 1)

    with pytest.raises(ExternalPublicationError, match="VALIDATION_INVALID"):
        fixture.facade.publish(context, wrong)

    assert fixture.service.publish_calls == 0


def test_validation_rejects_authority_or_inventory_drift() -> None:
    fixture = _fixture()
    config = fixture.facade.prepare_admission(_config())
    context = fixture.facade.stage(config, _payload())
    assert fixture.service.authority is not None
    fixture.service.authority["version"] += 1

    with pytest.raises(ExternalPublicationError, match="GENERATION_DIVERGED"):
        fixture.facade.validate(context)

    assert fixture.service.publish_calls == 0


def test_external_mode_is_explicit_and_unsafe_transformations_fail_closed() -> None:
    fixture = _fixture()
    assert fixture.facade.is_enabled(_config()) is True
    assert fixture.facade.is_enabled(_config(physical_design={})) is False

    with pytest.raises(ExternalPublicationError, match="TRANSFORMATION_UNSUPPORTED"):
        fixture.facade.stage(_config(lineage=True), _payload())
    with pytest.raises(ExternalPublicationError, match="TRANSFORMATION_UNSUPPORTED"):
        fixture.facade.stage(
            _config(), _payload(codec=SimpleNamespace(clickhouse_decode_expression=lambda value: value))
        )

    assert fixture.source_calls == 0
    assert fixture.service.stage_calls == 0


def test_abort_removes_only_unpublished_owned_candidates() -> None:
    fixture = _fixture()
    config = fixture.facade.prepare_admission(_config())
    context = fixture.facade.stage(config, _payload())

    fixture.facade.abort(context)

    assert fixture.service.authority is not None
    assert fixture.service.authority["phase"] == "ABORTED"
    assert fixture.service.candidates == dict.fromkeys(fixture.service.members)
