"""Compose governance, staged finalization and publication without live I/O."""

from __future__ import annotations

from dataclasses import replace

import pytest

from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.governance.finalization import LoadGovernanceFinalizationCoordinator
from dpone.runtime.governance.ports import StagedLoadHandle
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
