"""An audit-only binding must not create source checkpoint state or use a sink alias."""

from copy import deepcopy
from dataclasses import replace

import pytest

from dpone.contracts.runtime_connection import ResolvedBindingConnection, ResolvedConnectionDescriptor
from dpone.dag.load_config_builder import LoadConfigBuilder
from dpone.runtime.credentials.authority import resolve_runtime_connections
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.credentials.runtime_context import RuntimeConnectionContext
from tests.test_runtime_connection_composition_root import _connection, _Resolver


def _config(storage=None):
    return {
        "name": "events",
        "source": {
            "type": "mssql",
            "connection_ref": "source-main",
            "table": {"schema": "raw", "name": "events"},
        },
        "sink": {
            "type": "clickhouse",
            "connection_ref": "sink-main",
            "table": {"schema": "analytics", "name": "events"},
            "strategy": {"mode": "full_refresh"},
            "options": {
                "load_governance": {
                    "audit": {
                        "storage": storage
                        or {
                            "type": "mssql",
                            "connection_ref": "audit-main",
                            "provisioning": "external",
                        }
                    }
                }
            },
        },
        "state": {"type": "disabled"},
    }


def _setup():
    events = []
    connections = {
        "source-main": _connection("mssql"),
        "sink-main": _connection("clickhouse"),
        "audit-main": ResolvedBindingConnection(
            credentials=CredentialsConfig(database="Connection_Default", schema="ignored"),
            safe_metadata={},
            descriptor=ResolvedConnectionDescriptor(
                connection_type="mssql",
                properties={"database": "Example_Metadata", "schema": "ops"},
            ),
        ),
    }
    context = RuntimeConnectionContext("test", {}, {}, {}, _Resolver(events, connections))
    return events, connections, context, _config()


def test_disabled_state_resolves_independent_audit_from_registry_not_credential_defaults():
    events, connections, context, config = _setup()
    resolved = resolve_runtime_connections(
        config=config,
        load_config=LoadConfigBuilder().build(config),
        context=context,
    )
    assert resolved.state is None
    assert getattr(resolved, "audit", None) is connections["audit-main"]
    assert resolved.audit_location.database == "Example_Metadata"
    assert resolved.audit_location.schema == "ops"
    assert resolved.audit_location.loads_table == "dpone_load_audit"
    assert resolved.audit_location.steps_table == "__dpone__load_steps"
    assert events.count("resolve:audit-main") == 1
    assert resolved.by_ref["audit-main"] is connections["audit-main"]


def test_audit_only_alias_is_deduplicated_with_an_explicit_source_alias():
    events, connections, context, config = _setup()
    config["source"]["connection_ref"] = "audit-main"
    resolved = resolve_runtime_connections(
        config=config,
        load_config=LoadConfigBuilder().build(config),
        context=context,
    )
    assert getattr(resolved, "audit", None) is resolved.source
    assert events.count("resolve:audit-main") == 1
    assert resolved.state is None


def test_absent_selector_preserves_legacy_connection_resolution():
    events, _, context, config = _setup()
    del config["sink"]["options"]["load_governance"]["audit"]["storage"]
    resolved = resolve_runtime_connections(
        config=config,
        load_config=LoadConfigBuilder().build(config),
        context=context,
    )
    assert getattr(resolved, "audit", None) is None
    assert "resolve:audit-main" not in events


def test_inferred_mssql_state_cannot_add_a_second_audit_pair():
    _, _, context, config = _setup()
    config["state"] = {"connection_ref": "audit-main"}
    with pytest.raises(ValueError, match="audit"):
        resolve_runtime_connections(config=config, load_config=LoadConfigBuilder().build(config), context=context)


@pytest.mark.parametrize("changed", ["missing", "table", "alias"])
def test_direct_hydration_rejects_a_different_load_config_audit_selection(changed):
    _, _, context, config = _setup()
    load_config = LoadConfigBuilder().build(config)
    # Builder retains nested objects: detach before simulating divergent callers.
    load_config.options = deepcopy(load_config.options)
    audit = load_config.options["load_governance"]["audit"]
    if changed == "missing":
        del audit["storage"]
    elif changed == "table":
        audit["loads_table"] = "other"
    else:
        audit["storage"]["connection_ref"] = "other"
    with pytest.raises(ValueError, match="audit"):
        resolve_runtime_connections(config=config, load_config=load_config, context=context)


@pytest.mark.parametrize("defect", ["type", "descriptor", "database", "schema", "schema-conflict"])
def test_invalid_registry_audit_location_cannot_fall_back(defect):
    _, connections, context, config = _setup()
    descriptor = connections["audit-main"].descriptor
    if defect == "descriptor":
        descriptor = None
    elif defect == "type":
        descriptor = ResolvedConnectionDescriptor(connection_type="clickhouse", properties=descriptor.properties)
    elif defect in {"database", "schema"}:
        properties = dict(descriptor.properties)
        del properties[defect]
        descriptor = ResolvedConnectionDescriptor(connection_type="mssql", properties=properties)
    else:
        config["sink"]["options"]["load_governance"]["audit"]["state_schema"] = "other"
    connections["audit-main"] = replace(connections["audit-main"], descriptor=descriptor)
    with pytest.raises(ValueError, match="audit"):
        resolve_runtime_connections(config=config, load_config=LoadConfigBuilder().build(config), context=context)


@pytest.mark.parametrize(
    "storage",
    [
        None,
        {},
        False,
        "audit-main",
        {"type": "clickhouse", "connection_ref": "audit-main"},
        {"type": "mssql", "connection_ref": " audit-main "},
        {"type": "mssql", "connection_ref": "audit-main", "provisioning": "runtime"},
        {"type": "mssql", "connection_ref": "audit-main", "database": "ignored"},
    ],
)
def test_builder_rejects_malformed_selector_without_normalizing_it(storage):
    config = _config()
    config["sink"]["options"]["load_governance"]["audit"]["storage"] = deepcopy(storage)
    with pytest.raises(ValueError, match="audit"):
        LoadConfigBuilder().build(config)


@pytest.mark.parametrize("conflict", ["source", "state", "table", "qualified-table"])
def test_builder_rejects_ambiguous_or_misplaced_selection(conflict):
    config = _config()
    audit = config["sink"]["options"]["load_governance"]["audit"]
    if conflict == "source":
        config["source"]["options"] = {"load_governance": {"audit": audit}}
        config["sink"]["options"] = {}
    elif conflict == "state":
        config["state"] = {"type": "mssql", "connection_ref": "audit-main"}
    elif conflict == "table":
        audit.update(loads_table="events", steps_table="EVENTS")
    else:
        audit["steps_table"] = "other.events"
    with pytest.raises(ValueError, match="audit"):
        LoadConfigBuilder().build(config)
