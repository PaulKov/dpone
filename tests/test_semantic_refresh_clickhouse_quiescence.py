"""Protected ClickHouse operation-query quiescence tests."""

from __future__ import annotations

import pytest

from dpone.adapters.semantic_refresh_clickhouse_connection import (
    ClickHouseConnectionAuthorityVerifier,
)
from dpone.adapters.semantic_refresh_clickhouse_quiescence import (
    ClickHouseAttemptQuiescenceError,
    ClickHouseHttpAttemptQuiescenceObserver,
)
from dpone.ports.semantic_refresh_clickhouse_connection import (
    ClickHouseClusterConnectionAuthority,
    clickhouse_cluster_topology_sha256,
)

_BINDING = "sha256:" + "a" * 64
_OPERATION = "sha256:" + "b" * 64
_ATTEMPT = "sha256:" + "c" * 64
_CLUSTER = "sha256:" + "d" * 64
_ENDPOINT = "http://127.0.0.1:58123/"


class _Client:
    endpoint_authority_id = _ENDPOINT

    def __init__(self, *, active_query_id: str | None = None) -> None:
        self.active_query_id = active_query_id
        self.statements: list[str] = []

    def execute(self, statement: str) -> list[tuple[object, ...]]:
        self.statements.append(statement)
        if "FROM system.clusters" in statement:
            return [("localhost", 9000, 1, 1)]
        return [] if self.active_query_id is None else [(self.active_query_id,)]


def _observer(client: _Client) -> ClickHouseHttpAttemptQuiescenceObserver:
    authority = ClickHouseClusterConnectionAuthority(
        clickhouse_cluster_authority_id=_CLUSTER,
        endpoint_authority_id=_ENDPOINT,
        cluster_name="default",
        host_names=("localhost",),
        topology_sha256=clickhouse_cluster_topology_sha256((("localhost", 9000, 1, 1),)),
    )
    return ClickHouseHttpAttemptQuiescenceObserver(
        client=client,
        connection=ClickHouseConnectionAuthorityVerifier(client=client, authority=authority),
    )


def test_observer_returns_closed_zero_query_proof() -> None:
    client = _Client()

    proof = _observer(client).prove_quiescent(
        workflow_execution_binding_sha256=_BINDING,
        operation_id=_OPERATION,
        original_attempt_binding_sha256=_ATTEMPT,
        clickhouse_cluster_authority_id=_CLUSTER,
        observed_at="2026-08-10T08:00:00.000000Z",
    )

    assert proof.active_query_ids == ()
    assert proof.query_id_prefix in client.statements[-1]
    assert "FROM system.processes" in client.statements[-1]


def test_observer_rejects_active_operation_query() -> None:
    client = _Client(active_query_id="dpone-semref-active")

    with pytest.raises(ClickHouseAttemptQuiescenceError, match="still active"):
        _observer(client).prove_quiescent(
            workflow_execution_binding_sha256=_BINDING,
            operation_id=_OPERATION,
            original_attempt_binding_sha256=_ATTEMPT,
            clickhouse_cluster_authority_id=_CLUSTER,
            observed_at="2026-08-10T08:00:00.000000Z",
        )
