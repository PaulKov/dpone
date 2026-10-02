"""A fresh preparation may reuse a retired slot, never its unpublished intent."""

from dataclasses import replace

import pytest

from dpone.contracts.clickhouse_cluster_publication import AuthorityPhase as Phase
from dpone.contracts.clickhouse_cluster_publication import VersionedAuthorityRecord
from dpone.runtime.state.mssql_publication_envelope import require_transition
from tests.test_mssql_publication_authority import record


def retired():
    return VersionedAuthorityRecord(replace(record(), phase=Phase.RETIRED_UNPUBLISHED), 1)


def fresh():
    before = record()
    return replace(
        before,
        operation_id="fresh-operation",
        fence_token="fresh-fence",
        candidate="example_fresh_candidate",
        desired=replace(
            before.desired,
            uuid="fresh-generation",
            engine_full="ReplicatedMergeTree('/fresh','r')",
            keeper_path="/fresh",
        ),
        plan_digest="9" * 64,
        dispatch_epoch=1,
    )


@pytest.mark.parametrize("first_publication", [False, True])
def test_fresh_preparation_after_retirement_does_not_grant_dispatch(first_publication):
    before, desired = retired(), fresh()
    if first_publication:
        before = replace(before, record=replace(before.record, predecessor=None))
        desired = replace(desired, predecessor=None)
    assert require_transition(before, desired) is False


@pytest.mark.parametrize(
    "changes",
    [
        {"operation_id": "operation"},
        {"fence_token": "fence"},
        {"fence_token": ""},
        {"candidate": "example_candidate"},
        {"candidate": "example"},
        {"candidate": ""},
        {"desired": record().desired},
        {"desired": record().predecessor},
        {"desired": replace(record().desired, uuid="fresh-generation")},
        {"desired": replace(record().predecessor, uuid="fresh-generation")},
        {"predecessor": None},
        {"predecessor": record().desired},
        {"inventory_digest": "1" * 64},
        {"database": "different"},
        {"target": "different"},
        {"target_key": "1" * 64},
        {"dispatch_epoch": 0},
        {"dispatch_epoch": 2},
        {"ddl_correlation_token": "old-intent"},
        {"ddl_query_digest": "1" * 64},
        {"ddl_entry": "old-entry"},
        {"cleanup_correlation_token": "old-cleanup"},
        {"cleanup_query_digest": "1" * 64},
        {"cleanup_entry": "old-entry"},
        {"quality_reader": "old-reader"},
        {"phase": Phase.DISPATCHING},
        {"phase": Phase.COMMITTED},
        {"phase": Phase.COMPLETED},
    ],
)
def test_retirement_cannot_be_reused_or_treated_as_a_published_generation(changes):
    with pytest.raises(ValueError):
        require_transition(retired(), replace(fresh(), **changes))


def test_retirement_origin_can_only_be_initial_revision_one():
    with pytest.raises(ValueError):
        require_transition(replace(retired(), version=2), fresh())


def test_retirement_cannot_carry_a_previous_dispatch_epoch():
    before = replace(retired(), record=replace(retired().record, dispatch_epoch=2))
    with pytest.raises(ValueError):
        require_transition(before, replace(fresh(), dispatch_epoch=3))


def test_retirement_with_inherited_quality_cannot_be_used_for_a_fresh_operation():
    before = replace(
        retired(),
        record=replace(
            retired().record,
            schema_version="dpone.clickhouse.cluster-full-refresh.v2",
            quality_evidence="historical-capsule",
        ),
    )
    with pytest.raises(ValueError):
        require_transition(before, fresh())
