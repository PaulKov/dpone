"""One acknowledged capability permits one physical invocation, never a retry."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier, Lock
from types import SimpleNamespace

import pytest

from dpone.contracts.clickhouse_cluster_publication import AuthorityPhase, DispatchPermit
from dpone.runtime.sinks.clickhouse_cluster_publication_ddl import ClickHouseClusterPublicationDdl
from tests.test_mssql_publication_authority import record


class Transport:
    def __init__(self, *, unknown=False):
        self.calls = []
        self.lock = Lock()
        self.unknown = unknown

    def execute(self, sql, **kwargs):
        with self.lock:
            self.calls.append(sql)
        if self.unknown:
            raise OSError("lost acknowledgement")


def prepared(*, cleanup=False, unknown=False):
    transport = Transport(unknown=unknown)
    connector = SimpleNamespace(connection=transport)
    ddl = ClickHouseClusterPublicationDdl(connector, object())
    desired = record().dispatching(token="publish-token", query_digest="pending")
    if cleanup:
        desired = replace(
            desired,
            phase=AuthorityPhase.CLEANUP_DISPATCHING,
            cleanup_correlation_token="cleanup-token",
            cleanup_query_digest=ddl.cleanup_query_digest(desired, cluster="replicas"),
        )
    else:
        desired = replace(desired, ddl_query_digest=ddl.publication_query_digest(desired, cluster="replicas"))
    return transport, connector, desired, DispatchPermit.for_record(desired)


@pytest.mark.parametrize("cleanup", [False, True])
@pytest.mark.parametrize("unknown", [False, True])
def test_same_permit_across_adapters_dispatches_only_once_even_after_unknown(cleanup, unknown):
    transport, connector, desired, permit = prepared(cleanup=cleanup, unknown=unknown)
    for attempt in range(2):
        ddl = ClickHouseClusterPublicationDdl(connector, object())
        dispatch = ddl.drop_predecessor if cleanup else ddl.dispatch_publication
        if attempt:
            with pytest.raises(ValueError, match="consumed"):
                dispatch(desired, permit, cluster="replicas")
        elif unknown:
            with pytest.raises(OSError, match="acknowledgement"):
                dispatch(desired, permit, cluster="replicas")
        else:
            dispatch(desired, permit, cluster="replicas")
    assert len(transport.calls) == 1


def test_concurrent_same_permit_has_one_transport_invocation():
    transport, connector, desired, permit = prepared()
    barrier = Barrier(6)

    def dispatch(_):
        barrier.wait()
        try:
            ClickHouseClusterPublicationDdl(connector, object()).dispatch_publication(
                desired, permit, cluster="replicas"
            )
            return True
        except ValueError:
            return False

    with ThreadPoolExecutor(max_workers=6) as pool:
        assert sum(pool.map(dispatch, range(6))) == 1
    assert len(transport.calls) == 1


@pytest.mark.parametrize("change", ["phase", "cluster", "token", "target", "digest"])
def test_permit_binds_exact_effect_before_transport(change):
    transport, connector, desired, permit = prepared()
    cluster = "replicas"
    if change == "cluster":
        cluster = "different-replicas"
    elif change == "phase":
        desired = replace(desired, phase=AuthorityPhase.COMMITTED)
    elif change == "token":
        desired = replace(desired, ddl_correlation_token="different-token")
    elif change == "target":
        desired = replace(desired, target="different_target")
    else:
        desired = replace(desired, ddl_query_digest="f" * 64)
    with pytest.raises(ValueError, match="permit"):
        ClickHouseClusterPublicationDdl(connector, object()).dispatch_publication(desired, permit, cluster=cluster)
    assert not transport.calls
