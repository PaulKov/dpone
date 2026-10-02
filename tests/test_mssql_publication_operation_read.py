"""Immutable root history admits new work but never revives a retired ID."""

from dataclasses import replace

import pytest

from dpone.contracts.clickhouse_cluster_publication import AuthorityPhase as Phase
from dpone.contracts.clickhouse_cluster_publication import ClusterPublicationError, VersionedAuthorityRecord
from tests.test_mssql_publication_authority import WRITE_ID, Transport, authority, receipt, record
from tests.test_mssql_publication_retirement import retired_receipt, retired_record
from tests.test_publication_retirement import plan


def history(phase):
    original = plan()
    root = retired_record(original)
    root_row = retired_receipt(original, won=0)
    if phase is Phase.RETIRED_UNPUBLISHED:
        return root, 1, root_row + root_row
    current = replace(
        root,
        phase=phase,
        operation_id="later-operation",
        fence_token="later-fence",
        dispatch_epoch=1 if phase is Phase.PREPARED else 2,
        candidate="later-candidate",
        desired=replace(root.desired, uuid="later-generation", keeper_path="/later"),
        ddl_entry=None if phase is Phase.PREPARED else "query-later",
    )
    return current, 2, receipt(current, 2, won=0) + root_row


@pytest.mark.parametrize("phase", [Phase.RETIRED_UNPUBLISHED, Phase.PREPARED, Phase.COMMITTED, Phase.COMPLETED])
def test_retired_id_stays_rejected_after_later_publications(phase):
    current, _, row = history(phase)
    transport = Transport([row])
    with pytest.raises(ClusterPublicationError, match="RETIRED_OPERATION"):
        authority(transport).read_for_operation(current.target_key, plan().original.record.operation_id)
    assert len(transport.calls) == 1
    assert "UPDATE" not in transport.calls[0][0]
    assert "INSERT" not in transport.calls[0][0]


@pytest.mark.parametrize("phase", [Phase.RETIRED_UNPUBLISHED, Phase.PREPARED, Phase.COMMITTED, Phase.COMPLETED])
def test_fresh_id_observes_current_state_without_a_dispatch_permit(phase):
    current, version, row = history(phase)
    observed = authority(Transport([row])).read_for_operation(current.target_key, "fresh-id")
    assert observed == VersionedAuthorityRecord(current, version)


@pytest.mark.parametrize(
    "root_column,value",
    [
        (1, 2),
        (2, b"{}"),
        (3, b"x" * 32),
        (4, "invalid-write-id"),
        (5, b"x" * 32),
        (6, 0),
        (7, 0),
        (8, "native"),
        (9, b"untrusted"),
        (10, b"x" * 32),
        (11, "another-operation"),
        (12, "COMPLETED"),
    ],
)
def test_root_corruption_is_not_treated_as_permission(root_column, value):
    current, _, raw = history(Phase.COMPLETED)
    row = list(raw)
    row[13 + root_column] = value
    with pytest.raises(ClusterPublicationError, match="READ_UNKNOWN"):
        authority(Transport([tuple(row)])).read_for_operation(current.target_key, "fresh-id")


@pytest.mark.parametrize("root", [(), (None,) * 13])
def test_missing_root_is_not_ordinary_absence(root):
    current, _, row = history(Phase.COMPLETED)
    with pytest.raises(ClusterPublicationError, match="READ_UNKNOWN"):
        authority(Transport([row[:13] + root])).read_for_operation(current.target_key, "fresh-id")


def test_native_root_allows_normal_operation_admission():
    prepared = replace(record(), authority_write_id=WRITE_ID.hex)
    row = receipt(prepared, 1, won=0)
    assert authority(Transport([row + row])).read_for_operation(prepared.target_key, "operation") == (
        VersionedAuthorityRecord(prepared, 1)
    )


def test_only_absent_slot_and_absent_history_allow_new_admission():
    transport = Transport([])
    assert authority(transport).read_for_operation("b" * 64, "fresh-id") is None
    assert len(transport.calls) == 1
    assert "INSERT" not in transport.calls[0][0]


@pytest.mark.parametrize("failure", ["commit", "after_output"])
def test_ambiguous_operation_read_never_grants_admission(failure):
    current, _, row = history(Phase.COMPLETED)
    with pytest.raises(ClusterPublicationError, match="READ_UNKNOWN"):
        authority(Transport([row], failure=failure)).read_for_operation(current.target_key, "fresh-id")
