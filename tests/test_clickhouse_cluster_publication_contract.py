from __future__ import annotations

from dataclasses import replace

import pytest

from dpone.contracts.clickhouse_cluster_publication import (
    AggregatePublicationState,
    AuthorityMutationStatus,
    AuthorityPhase,
    AuthorityRecord,
    GenerationIdentity,
    QueueHostResult,
    QueueHostState,
    QueueState,
    ReplicaGeneration,
    ReplicaPublicationState,
    classify_aggregate,
    classify_replica,
)
from dpone.runtime.sinks.clickhouse_cluster_publication_authority import ClickHouseKeeperMapAuthority


def _identity(uuid: str) -> GenerationIdentity:
    return GenerationIdentity(
        uuid, "ReplicatedMergeTree('/tables/x', '{replica}')", "schema", "default", f"/tables/{uuid}"
    )


@pytest.mark.parametrize(
    ("status", "code", "text", "expected"),
    [
        ("Active", None, None, QueueHostState.IN_PROGRESS),
        ("Inactive", None, None, QueueHostState.IN_PROGRESS),
        ("Finished", 0, "", QueueHostState.TERMINAL_SUCCESS),
        ("Finished", 10, "failure", QueueHostState.TERMINAL_FAILURE),
        ("Finished", None, None, QueueHostState.UNKNOWN),
        ("Finished", -1, "failure", QueueHostState.UNKNOWN),
        ("Finished", "bad", "failure", QueueHostState.UNKNOWN),
        ("Removing", 0, "", QueueHostState.UNKNOWN),
        ("Unknown", 0, "", QueueHostState.UNKNOWN),
        (None, None, None, QueueHostState.UNKNOWN),
        ("Active", 0, "", QueueHostState.UNKNOWN),
    ],
)
def test_queue_status_normalization_is_exhaustive(status, code, text, expected) -> None:
    assert QueueHostResult("node-1", status, code, text).normalized is expected


def test_replica_and_aggregate_classification_never_accepts_unknown() -> None:
    old, new = _identity("old"), _identity("new")
    assert (
        classify_replica(ReplicaGeneration("a", old, new), desired=new, predecessor=old)
        is ReplicaPublicationState.PENDING
    )
    assert (
        classify_replica(ReplicaGeneration("a", new, old), desired=new, predecessor=old)
        is ReplicaPublicationState.COMMITTED
    )
    assert (
        classify_aggregate((ReplicaPublicationState.PENDING, ReplicaPublicationState.COMMITTED), QueueState.IN_PROGRESS)
        is AggregatePublicationState.PARTIAL_IN_PROGRESS
    )
    assert (
        classify_aggregate(
            (ReplicaPublicationState.PENDING, ReplicaPublicationState.COMMITTED), QueueState.TERMINAL_FAILURE
        )
        is AggregatePublicationState.PARTIAL_TERMINAL
    )
    assert (
        classify_aggregate((ReplicaPublicationState.COMMITTED,) * 2, QueueState.UNKNOWN)
        is AggregatePublicationState.UNKNOWN
    )


class _Connection:
    def __init__(self, *, raises: bool = False) -> None:
        self.calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.raises = raises

    def execute(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if self.raises:
            raise TimeoutError("response lost")


class _Connector:
    def __init__(self, record: AuthorityRecord, *, raises: bool = False, bytes_hash: bool = False) -> None:
        self.connection = _Connection(raises=raises)
        self.record = record
        self.bytes_hash = bytes_hash

    def get_records(self, query, params=None):
        record = self.record
        return [
            (
                record.operation_id,
                record.fence_token,
                record.phase.value,
                record.dispatch_epoch,
                record.payload,
                record.payload_sha256.encode() if self.bytes_hash else record.payload_sha256,
                0,
            )
        ]


def _record() -> AuthorityRecord:
    return AuthorityRecord(
        target_key="target",
        operation_id="operation",
        fence_token="fence",
        phase=AuthorityPhase.PREPARED,
        dispatch_epoch=0,
        inventory_digest="inventory",
        plan_digest="plan",
        database="analytics",
        target="target",
        candidate="candidate",
        desired=_identity("new"),
        predecessor=_identity("old"),
        staged_rows=2,
    )


def test_authority_create_uses_exactly_one_raw_transport_call() -> None:
    record = _record()
    connector = _Connector(record)
    result = ClickHouseKeeperMapAuthority(connector, "analytics").create_if_absent(record)
    assert result.status is AuthorityMutationStatus.VERIFIED
    assert len(connector.connection.calls) == 1
    _, kwargs = connector.connection.calls[0]
    assert kwargs["settings"] == {"keeper_map_strict_mode": 1, "insert_keeper_max_retries": 0}


def test_lost_authority_response_is_unknown_and_never_retried() -> None:
    connector = _Connector(_record(), raises=True)
    result = ClickHouseKeeperMapAuthority(connector, "analytics").create_if_absent(_record())
    assert result.status is AuthorityMutationStatus.OUTCOME_UNKNOWN
    assert result.permit is None
    assert len(connector.connection.calls) == 1


def test_http_fixed_string_hash_is_decoded_without_losing_authority() -> None:
    record = _record()
    observed = ClickHouseKeeperMapAuthority(_Connector(record, bytes_hash=True), "analytics").read_versioned("target")
    assert observed is not None
    assert observed.record == record


def test_dispatch_permit_requires_exact_next_keeper_version() -> None:
    before = _record()
    desired = replace(before, phase=AuthorityPhase.DISPATCHING, dispatch_epoch=1, ddl_correlation_token="token")
    connector = _Connector(desired)
    # The fake returns version zero rather than the required version one.
    result = ClickHouseKeeperMapAuthority(connector, "analytics").compare_and_swap(
        type("Version", (), {"record": before, "version": 0})(), desired
    )
    assert result.status is AuthorityMutationStatus.CONFLICT
    assert result.permit is None
