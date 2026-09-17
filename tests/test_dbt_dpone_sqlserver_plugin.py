"""Installed dbt API identity and no-I/O wrapper tests."""

from types import SimpleNamespace

import pytest
from dbt.adapters.dpone_sqlserver import Plugin
from dbt.adapters.dpone_sqlserver.connections import DpOneSQLServerConnectionManager
from dbt.adapters.dpone_sqlserver.credentials import DpOneSQLServerCredentials
from dbt.adapters.dpone_sqlserver.impl import DpOneSQLServerAdapter
from dbt.adapters.sqlserver import SQLServerAdapter
from dbt.context.providers import ParseDatabaseWrapper, RuntimeDatabaseWrapper


def test_real_plugin_has_distinct_identity_and_inherits_ordinary_behavior():
    assert Plugin.adapter is DpOneSQLServerAdapter
    assert Plugin.credentials is DpOneSQLServerCredentials
    assert Plugin.dependencies == ["sqlserver"]
    assert DpOneSQLServerAdapter.ConnectionManager is DpOneSQLServerConnectionManager
    assert DpOneSQLServerConnectionManager.TYPE == "dpone_sqlserver"
    assert DpOneSQLServerCredentials.type.fget(None) == "dpone_sqlserver"
    assert DpOneSQLServerAdapter.execute is SQLServerAdapter.execute
    assert "dpone_physical_protocol_v1" not in SQLServerAdapter._available_


def test_actual_parse_wrapper_replacement_does_not_touch_connections_or_delivery():
    adapter = object.__new__(DpOneSQLServerAdapter)
    adapter.config = SimpleNamespace(quoting={})
    wrapper = ParseDatabaseWrapper(adapter, None)
    assert wrapper.dpone_physical_protocol_v1("attach", {}) is None
    assert not hasattr(adapter, "_physical_delivery")


def test_actual_runtime_wrapper_fails_closed_without_authority():
    adapter = object.__new__(DpOneSQLServerAdapter)
    adapter.config = SimpleNamespace(quoting={})
    with pytest.raises(ValueError, match="unavailable"):
        RuntimeDatabaseWrapper(adapter, None).dpone_physical_protocol_v1("attach", {})


def test_normal_factory_loads_plugin_without_registry_patches():
    from dbt.adapters.factory import FACTORY

    assert FACTORY.load_plugin("dpone_sqlserver") is DpOneSQLServerCredentials


def test_delivery_owner_is_inert_and_failed_consumption_is_terminal():
    from dbt.adapters.dpone_sqlserver.delivery import PhysicalTransportDeliveryOwner

    owner = PhysicalTransportDeliveryOwner()
    for _ in range(2):
        with pytest.raises(ValueError):
            owner.consume(profile_name="example", target_name="target", credentials=None, profile_file=None)
