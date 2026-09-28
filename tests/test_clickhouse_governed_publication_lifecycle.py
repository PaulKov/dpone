"""Compose governance, staged finalization and publication without live I/O."""

from __future__ import annotations

from dataclasses import replace

import pytest

from dpone.governance.hooks import InMemoryLoadStepAuditStorage
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.governance.finalization import LoadGovernanceFinalizationCoordinator
from dpone.runtime.governance.ports import StagedLoadHandle
from dpone.runtime.governance.service import LoadGovernanceService
from dpone.runtime.normalization.staged_mutation import NestedPackageStagedMutation
from dpone.runtime.sinks.clickhouse_cluster_full_refresh_publication import (
    ClickHouseClusterFullRefreshPublicationService,
)
from dpone.runtime.sinks.clickhouse_sink import ClickHouseSink
from dpone.runtime.sinks.clickhouse_staged_load import ClickHouseStagedLoadService
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sources.base import ExtractResult
from tests.test_clickhouse_cluster_full_refresh_publication import (
    _Authority,
    _Bootstrap,
    _Catalog,
    _config,
    _Ddl,
)
from tests.test_clickhouse_production_finalize import FakeSink
from tests.test_native_load_governance_finalization import _load_record


class _RangeQualityOwner:
    def __init__(self) -> None:
        self.receipt: dict[str, object] | None = None
        self.range_execution_evidence = _MutableRangeEvidence()

    def mark_range_governed_quality_passed(self, *, config: object, quality_receipt: dict[str, object]) -> None:
        del config
        self.receipt = quality_receipt
        self.range_execution_evidence.outcome_status = "quality_passed"

    def mark_range_publication_confirmed(self, *, result: object, config: object) -> None:
        del result, config
        self.range_execution_evidence.outcome_status = "published"

    def mark_range_cleanup_succeeded(self) -> None:
        self.range_execution_evidence.outcome_status = "succeeded"


class _MutableRangeEvidence:
    def __init__(self) -> None:
        self.outcome_status = "staged"

    def to_dict(self) -> dict[str, object]:
        return {"schema_version": "dpone.columnar_range_execution.v1", "outcome_status": self.outcome_status}


@pytest.mark.parametrize("coordinator", ["governance", "nested"])
def test_composed_publication_cleanup_preserves_receipt_and_fenced_ownership(coordinator: str) -> None:
    """Finalization metadata must reach real cleanup, never generic name-drop."""

    catalog, authority = _Catalog(), _Authority()
    ddl = _Ddl(catalog)
    publication = ClickHouseClusterFullRefreshPublicationService(catalog, lambda database: authority, ddl, _Bootstrap())
    config = _config()
    candidate = replace(config, target_table="candidate")
    handle = StagedLoadHandle(
        staging_config=candidate,
        finalization_config=candidate,
        payload_schema=(("id", "Int64"),),
        staged_rows=2,
    )
    backend = FakeSink()
    generic_drops: list[str] = []
    backend._swap_table_into_target = lambda target, staged: publication.publish(target, staged, staged_rows=2)
    backend._cleanup_full_refresh_publication = publication.cleanup
    backend._drop_table = lambda table, _config: generic_drops.append(table)
    facade = object.__new__(ClickHouseSink)
    facade._staged_load = ClickHouseStagedLoadService(backend)
    facade.stage_payload = lambda _config, _payload: handle
    payload = LoadPayload(artifact=InMemoryRowsArtifact([{"id": 1}, {"id": 2}]), schema=[("id", "Int64")])

    if coordinator == "governance":
        result = LoadGovernanceFinalizationCoordinator().load(
            sink=facade,
            load_config=config,
            payload=payload,
            extract_result=ExtractResult(artifact=payload.artifact, schema=payload.schema),
            load_record=_load_record(),
        )
    else:
        mutation = NestedPackageStagedMutation(facade)
        mutation.stage(config, payload)
        result = mutation.finalize_all()[0]

    assert result.total_rows == 2
    assert result.commit_receipt_id is not None
    assert authority.current is not None
    assert authority.current.record.phase.value == "COMPLETED"
    assert authority.current.record.cleanup_entry == "query-1"
    assert ddl.dispatches == 1
    assert ddl.cleanup_dispatches == 1
    assert generic_drops == []
    assert handle.metadata == {}  # Original caller data remains isolated.


def test_governance_coordinator_threads_authoritative_quality_receipt_to_range_owner() -> None:
    config = _config()
    candidate = replace(config, target_table="candidate")
    owner = _RangeQualityOwner()
    handle = StagedLoadHandle(
        staging_config=candidate,
        finalization_config=candidate,
        payload_schema=(("id", "Int64"),),
        staged_rows=2,
        sink_state=owner,
    )
    backend = FakeSink()
    facade = object.__new__(ClickHouseSink)
    facade._staged_load = ClickHouseStagedLoadService(backend)
    facade.stage_payload = lambda _config, _payload: handle
    payload = LoadPayload(artifact=InMemoryRowsArtifact([{"id": 1}, {"id": 2}]), schema=[("id", "Int64")])

    audit = InMemoryLoadStepAuditStorage()
    LoadGovernanceFinalizationCoordinator(LoadGovernanceService(audit_storage=audit)).load(
        sink=facade,
        load_config=config,
        payload=payload,
        extract_result=ExtractResult(artifact=payload.artifact, schema=payload.schema),
        load_record=_load_record(),
    )

    assert owner.receipt is not None
    assert owner.receipt["boundary"] == "pre_commit"
    report = owner.receipt["report"]
    assert isinstance(report, dict)
    assert report["passed"] is True
    finalized = next(record for record in audit.records if record.step_id == "finalized")
    terminal = next(record for record in audit.records if record.step_id == "range_evidence_terminal")
    assert finalized.details["range_execution"]["outcome_status"] == "published"
    assert terminal.details["range_execution"]["outcome_status"] == "succeeded"
