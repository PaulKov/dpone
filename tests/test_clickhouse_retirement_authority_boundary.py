"""Legacy authority adapters cannot import the MSSQL retirement contract."""

from dataclasses import replace

import pytest

from dpone.ports.clickhouse_cluster_publication import contracts
from dpone.runtime.sinks.clickhouse_cluster_publication_authority import ClickHouseKeeperMapAuthority
from tests.test_clickhouse_quality_authority import Connector, ready, record


@pytest.fixture(params=["replicated", "keeper"])
def adapter(request):
    connector = Connector()
    authority = (
        ClickHouseKeeperMapAuthority(connector, "analytics") if request.param == "replicated" else ready(connector)
    )
    return authority, connector


def test_native_operation_read_preserves_legacy_publication_behavior(adapter):
    authority, connector = adapter
    value = record()
    connector.observe(value, 1)
    assert authority.read_for_operation(value.target_key, value.operation_id) == contracts.VersionedAuthorityRecord(
        value, 1
    )
    assert not connector.calls


def test_unsupported_retirement_cannot_be_read_as_operation_admission(adapter):
    authority, connector = adapter
    value = replace(record(), phase=contracts.AuthorityPhase.RETIRED_UNPUBLISHED)
    connector.observe(value, 1)
    with pytest.raises(contracts.ClusterPublicationError, match="RETIREMENT_UNSUPPORTED"):
        authority.read_for_operation(value.target_key, "another-operation")
    assert not connector.calls


@pytest.mark.parametrize("action", ["create", "cas_from", "cas_to"])
def test_native_writes_cannot_forge_or_consume_retirement(adapter, action):
    authority, connector = adapter
    value = record()
    retired = replace(value, phase=contracts.AuthorityPhase.RETIRED_UNPUBLISHED)
    with pytest.raises(contracts.ClusterPublicationError, match="RETIREMENT_UNSUPPORTED"):
        if action == "create":
            authority.create_if_absent(retired)
        else:
            before, after = (retired, value) if action == "cas_from" else (value, retired)
            authority.compare_and_swap(contracts.VersionedAuthorityRecord(before, 1), after)
    assert not connector.calls
