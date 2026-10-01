"""Publication credentials are independent of disabled source checkpoint state."""

from dataclasses import asdict, replace

import pytest

from dpone.config.load_strategy import SOURCE_BYTE_BUDGET_OPTION, LoadStrategy
from dpone.runtime.bootstrap_hydrator import DefaultRuntimeHydrator
from dpone.runtime.credentials.authority import resolve_runtime_connections
from dpone.runtime.credentials.runtime_context import RuntimeConnectionContext
from tests.test_publication_authority_composition import _BINDING, connection
from tests.test_runtime_connection_composition_root import (
    _connection,
    _ContextLoader,
    _EndpointFactory,
    _load_config,
    _Resolver,
    _StateBootstrap,
)


def setup(*, environment="test", selected=True):
    events = []
    connections = {"source": _connection("mssql"), "sink": _connection("clickhouse"), "metadata": connection()}
    context = RuntimeConnectionContext(environment, {}, {}, {}, _Resolver(events, connections))
    config = {
        "source": {"type": "mssql", "connection_ref": "source"},
        "sink": {"type": "clickhouse", "connection_ref": "sink", "options": {}},
        "state": {"type": "disabled"},
    }
    if selected:
        config["sink"]["options"]["publication_authority"] = asdict(_BINDING)
    load = _load_config()
    load.options = {
        SOURCE_BYTE_BUDGET_OPTION: 100000,
        "physical_design": {
            "storage": {
                "clickhouse": {
                    "cluster": "replicas",
                    "engine": "ReplicatedMergeTree",
                    "ddl_scope": "cluster",
                }
            }
        },
    }
    if selected:
        load.options.update(
            publication_authority=asdict(_BINDING), sink_options={"publication_authority": asdict(_BINDING)}
        )
    return events, context, config, load


def test_disabled_state_still_resolves_independent_mssql_authority():
    events, context, config, load = setup()
    resolved = resolve_runtime_connections(config=config, load_config=load, context=context)
    assert resolved.state is None
    assert resolved.publication_authority is context.resolver.connections["metadata"]
    assert resolved.publication_binding == _BINDING
    assert events.count("resolve:metadata") == 1


def test_old_manifest_has_no_new_dependency():
    events, context, config, load = setup(selected=False)
    resolved = resolve_runtime_connections(config=config, load_config=load, context=context)
    assert resolved.publication_authority is None and resolved.publication_binding is None
    assert "resolve:metadata" not in events


@pytest.mark.parametrize(
    "defect", ["environment", "missing-context", "backend", "descriptor", "local", "strategy", "null"]
)
def test_explicit_invalid_binding_never_falls_back(defect):
    events, context, config, load = setup()
    if defect == "environment":
        context = replace(context, environment="prod")
    elif defect == "missing-context":
        context = None
    elif defect == "backend":
        config["sink"]["options"]["publication_authority"]["backend"] = "clickhouse"
    elif defect == "descriptor":
        context.resolver.connections["metadata"] = _connection("clickhouse")
    elif defect == "local":
        load.options = {}
    elif defect == "strategy":
        load.load_strategy = LoadStrategy.REPLACE
    else:
        config["sink"]["options"]["publication_authority"] = None
    with pytest.raises(ValueError, match="publication_authority"):
        resolve_runtime_connections(config=config, load_config=load, context=context)


def test_sql_admission_failure_precedes_source_and_sink_construction(monkeypatch):
    from dpone.runtime.credentials.resolved_connector_factory import ResolvedConnectorFactory

    events, context, config, load = setup()

    def unavailable(*args, **kwargs):
        events.append("authority-admission")
        raise OSError("database unavailable")

    monkeypatch.setattr(ResolvedConnectorFactory, "create", unavailable)
    with pytest.raises(ValueError, match="publication_authority"):
        DefaultRuntimeHydrator(
            state_bootstrap=_StateBootstrap(events),
            endpoint_factory=_EndpointFactory(events),
            connection_context_loader=_ContextLoader(context),
        ).build(config=config, load_config=load)
    assert "authority-admission" in events
    assert "source" not in events and "sink" not in events and "state" not in events


@pytest.mark.parametrize("strict", [False, True])
def test_direct_endpoint_factory_must_not_ignore_selected_binding(strict):
    from dpone.runtime.bootstrap_sources_sinks import RuntimeEndpointFactory

    _, context, config, _ = setup()
    sink_config = config["sink"]
    with pytest.raises(ValueError, match="publication_authority"):
        if strict:
            RuntimeEndpointFactory.build_sink_resolved(sink_config, context.resolver.connections["sink"], None)
        else:
            RuntimeEndpointFactory.build_sink(sink_config, None)
