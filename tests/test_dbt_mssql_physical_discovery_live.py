"""Opt-in real P-only SQL2022 discovery; parent owns the isolated live host."""

import importlib
import json
import os
from dataclasses import replace

import pytest

from dpone.adapters.dbt_mssql_physical_discovery import PhysicalDiscoveryReadError
from tests.support.dbt_mssql_physical_catalog_v2_live import selected_profile
from tests.support.dbt_mssql_physical_discovery_live import (
    MODEL_SCHEMA,
    DiscoveryArchiveAuthority,
    DiscoveryFixture,
    DiscoverySourceFixture,
    require_ddl_transition,
    require_sql_rejection,
)

pytestmark = [
    pytest.mark.integration_live,
    pytest.mark.skipif(
        os.environ.get("DPONE_RUN_PHYSICAL_DISCOVERY_LIVE") != "1", reason="isolated P-only discovery disabled"
    ),
]


@pytest.fixture(params=("same_database", "two_database"))
def discovery(tmp_path, monkeypatch, request, record_property):
    for name in ("DPONE_NATIVE_SQL_TEST_HOST", "DPONE_NATIVE_SQL_TEST_PASSWORD"):
        if not os.environ.get(name):
            pytest.skip("isolated SQL fixture input unavailable: " + name)
    source = DiscoverySourceFixture(importlib.import_module("pyodbc"), selected_profile(), layout=request.param)
    source.authority = DiscoveryArchiveAuthority(tmp_path / "archive", monkeypatch, 1)
    try:
        source.install()
        fixture = DiscoveryFixture(source)
        before = fixture.snapshot()
        observed = fixture.read()
        assert observed.object_count == 3 and observed.filegroup_name == "PRIMARY"
        assert observed.fencing_epoch == source.owner.receipt.guard_epochs[0].fencing_epoch
        fixture.require_no_generation()
        assert fixture.snapshot() == before
        record_property("discovery_environment", json.dumps(fixture.evidence(), sort_keys=True))
        yield fixture
    finally:
        source.cleanup()


def test_real_owner_only_absence_and_exact_replay(discovery):
    before = discovery.snapshot()
    first = discovery.read()
    assert discovery.read() == first
    assert first.model_principal == discovery.source.registration.principals.metadata.model
    assert first.control_principal == discovery.source.registration.principals.metadata.control
    assert discovery.snapshot() == before
    discovery.require_no_generation()


def test_any_object_collision_rejects_without_changing_P(discovery):
    before = discovery.snapshot()
    with discovery.source.connection(autocommit=True) as admin:
        admin.execute(f"CREATE VIEW [{MODEL_SCHEMA}].[orders] AS SELECT CONVERT(int,1) AS id")
    try:
        created = discovery.snapshot()
        require_ddl_transition(before, created, layout=discovery.source.layout, event="CREATE_VIEW")
        with pytest.raises(PhysicalDiscoveryReadError) as caught:
            discovery.read()
        require_sql_rejection(caught.value, name="DPONE_DISCOVERY_NAMESPACE_COLLISION", number=51480)
        assert discovery.snapshot() == created
    finally:
        before_drop = discovery.snapshot()
        with discovery.source.connection(autocommit=True) as admin:
            admin.execute(f"DROP VIEW [{MODEL_SCHEMA}].[orders]")
        dropped = discovery.snapshot()
        require_ddl_transition(before_drop, dropped, layout=discovery.source.layout, event="DROP_VIEW")
    discovery.require_no_generation()
    assert discovery.read().object_count == 3
    assert discovery.snapshot() == dropped


def test_wrong_epoch_cannot_reuse_real_owner(discovery):
    before = discovery.snapshot()
    stale = replace(
        discovery.request,
        guard=replace(discovery.request.guard, fencing_epoch=discovery.request.guard.fencing_epoch + 1),
    )
    with pytest.raises(PhysicalDiscoveryReadError) as caught:
        discovery.read(stale)
    require_sql_rejection(caught.value, name="DPONE_NATIVE_GENERATION_PHYSICAL_OWNER_CHANGED", number=51301)
    assert discovery.snapshot() == before
    discovery.require_no_generation()
    assert discovery.read().fencing_epoch == discovery.request.guard.fencing_epoch
