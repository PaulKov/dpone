"""Root verification applies inside mutation, even when admission is bypassed."""

from dataclasses import replace

import pytest

from dpone.contracts.clickhouse_cluster_publication import AuthorityMutationStatus as Status
from dpone.contracts.clickhouse_cluster_publication import AuthorityPhase as Phase
from dpone.contracts.clickhouse_cluster_publication import VersionedAuthorityRecord
from tests.test_mssql_publication_authority import WRITE_ID, Transport, authority, mutation_receipt
from tests.test_mssql_publication_retirement import retired_receipt, retired_record
from tests.test_publication_retirement import plan


def preparation(before):
    return replace(
        before,
        operation_id="fresh-operation",
        fence_token="fresh-fence",
        phase=Phase.PREPARED,
        candidate="fresh_candidate",
        desired=replace(before.desired, uuid="fresh-uuid", keeper_path="/fresh"),
        dispatch_epoch=before.dispatch_epoch + 1,
        ddl_entry=None,
        ddl_correlation_token=None,
        ddl_query_digest=None,
    )


def test_exact_fresh_cas_preserves_retired_root_and_grants_no_dispatch():
    original = plan()
    current = VersionedAuthorityRecord(retired_record(original), 1)
    desired = preparation(current.record)
    written = replace(desired, authority_write_id=WRITE_ID.hex)
    row = mutation_receipt(written, 2, root=retired_receipt(original, won=0))
    transport = Transport([row])
    result = authority(transport).compare_and_swap(current, desired)
    assert result.status is Status.VERIFIED
    assert result.observed == VersionedAuthorityRecord(written, 2)
    assert result.permit is None
    assert len(transport.calls) == 1


@pytest.mark.parametrize("root_column", [1, 2, 3, 5, 8, 9, 10, 11])
def test_mutation_with_unverifiable_retired_root_has_no_success_or_permission(root_column):
    original = plan()
    current = VersionedAuthorityRecord(retired_record(original), 1)
    desired = preparation(current.record)
    root = list(retired_receipt(original, won=0))
    root[root_column] = None
    row = mutation_receipt(desired, 2, root=tuple(root))
    result = authority(Transport([row])).compare_and_swap(current, desired)
    assert result.status is Status.OUTCOME_UNKNOWN
    assert result.permit is None


def test_direct_cas_cannot_reuse_retired_id_after_later_completion():
    original = plan()
    retired = retired_record(original)
    completed = replace(preparation(retired), phase=Phase.COMPLETED, ddl_entry="query-later")
    desired = replace(preparation(completed), operation_id=retired.operation_id)
    row = mutation_receipt(desired, 5, root=retired_receipt(original, won=0))
    result = authority(Transport([row])).compare_and_swap(VersionedAuthorityRecord(completed, 4), desired)
    assert result.status is Status.OUTCOME_UNKNOWN
    assert result.permit is None


@pytest.mark.parametrize("failure", ["after_output", "commit"])
def test_fresh_cas_lost_ack_is_unknown_and_never_retried(failure):
    original = plan()
    current = VersionedAuthorityRecord(retired_record(original), 1)
    desired = preparation(current.record)
    row = mutation_receipt(desired, 2, root=retired_receipt(original, won=0))
    transport = Transport([row], failure=failure)
    result = authority(transport).compare_and_swap(current, desired)
    assert result.status is Status.OUTCOME_UNKNOWN
    assert result.permit is None
    assert len(transport.calls) == 1
