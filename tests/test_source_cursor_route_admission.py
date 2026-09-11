"""Admission observes current facade bindings before strategy I/O."""

from importlib import import_module
from types import SimpleNamespace

import pytest

from dpone.config import LoadStrategy


@pytest.mark.parametrize("kind", ["mssql", "mysql", "clickhouse"])
def test_route_admission_preserves_helper_identity_and_getter_order(kind, monkeypatch):
    suffix = "clickhouse_incremental_extract" if kind == "clickhouse" else kind + "_incremental"
    strategy_module = import_module(f"dpone.runtime.sources.strategies.{kind}.{suffix}")
    facade = import_module(f"dpone.runtime.sources.{kind}")
    helper = (
        "assert_clickhouse_mssql_cursor_supported"
        if kind == "clickhouse"
        else "assert_target_max_mssql_cursor_supported"
    )
    assert getattr(facade, helper) is getattr(strategy_module, helper)
    name = {"mssql": "MSSQL", "mysql": "MySQL", "clickhouse": "ClickHouse"}[kind]
    strategy = getattr(strategy_module, name + "IncrementalExtractStrategy")
    events = []
    sink = object()

    class Options(dict):
        def get(self, key, default=None):
            events.append(key)
            return super().get(key, default)

    class Config:
        @property
        def options(self):
            events.append("options")
            return Options(target_type="target")

    class Binding:
        @property
        def target_max_cursor_source_type(self):
            events.append("source")
            return "custom-source"

        @property
        def sink_connector(self):
            events.append("sink")
            return sink

    calls = []
    monkeypatch.setattr(strategy_module, helper, lambda **kw: calls.append(kw))
    strategy.require_source_route_safe(Config(), Binding())
    assert events == ["options", *([] if kind == "clickhouse" else ["source"]), "sink_type", "target_type", "sink"]
    assert calls == [
        {
            "configured_sink": "target",
            "sink_connector": sink,
            **({} if kind == "clickhouse" else {"source_type": "custom-source"}),
        }
    ]


@pytest.mark.parametrize("kind", ["mssql", "mysql", "clickhouse"])
def test_facade_rechecks_current_binding_on_every_resolution(kind, monkeypatch):
    facade = import_module(f"dpone.runtime.sources.{kind}")
    name = {"mssql": "MSSQL", "mysql": "MySQL", "clickhouse": "ClickHouse"}[kind]
    source_cls = getattr(facade, name + "Source")
    strategy_cls = getattr(facade, name + "IncrementalExtractStrategy")
    events = []
    source = source_cls(SimpleNamespace(), SimpleNamespace(), sink_connector=None)
    strategy = source._incremental_extract
    config = SimpleNamespace(load_strategy=LoadStrategy.INCREMENTAL_APPEND, options={})
    monkeypatch.setattr(
        strategy_cls,
        "require_source_route_safe",
        staticmethod(lambda cfg, binding: events.append((cfg.options.copy(), binding.sink_connector))),
    )
    assert source._resolve_strategy(config) is strategy
    replacement = object()
    source.sink_connector = replacement
    config.options["sink_type"] = "mssql"
    assert source._resolve_strategy(config) is strategy
    assert events == [({}, None), ({"sink_type": "mssql"}, replacement)]


@pytest.mark.parametrize("kind", ["mssql", "mysql", "clickhouse"])
def test_strategy_construction_precedes_first_admission(kind, monkeypatch):
    facade = import_module(f"dpone.runtime.sources.{kind}")
    name = {"mssql": "MSSQL", "mysql": "MySQL", "clickhouse": "ClickHouse"}[kind]
    source_cls = getattr(facade, name + "Source")
    strategy_cls = getattr(facade, name + "IncrementalExtractStrategy")
    original = strategy_cls.__init__
    events = []

    def construct(self, *args, **kwargs):
        original(self, *args, **kwargs)
        events.append("constructed")

    monkeypatch.setattr(strategy_cls, "__init__", construct)
    monkeypatch.setattr(strategy_cls, "require_source_route_safe", staticmethod(lambda *_: events.append("admitted")))
    source = source_cls(SimpleNamespace(), SimpleNamespace(), sink_connector=None)
    assert events == ["constructed"]
    source._resolve_strategy(SimpleNamespace(load_strategy=LoadStrategy.INCREMENTAL_APPEND))
    assert events == ["constructed", "admitted"]


@pytest.mark.parametrize("kind", ["mssql", "mysql"])
def test_explicit_facade_source_override_reaches_owning_policy(kind, monkeypatch):
    facade = import_module(f"dpone.runtime.sources.{kind}")
    policy_owner = import_module(f"dpone.runtime.sources.strategies.{kind}.{kind}_incremental")
    source_cls = getattr(facade, {"mssql": "MSSQLSource", "mysql": "MySQLSource"}[kind])
    calls = []

    class CustomSource(source_cls):
        target_max_cursor_source_type = "custom-source"

    monkeypatch.setattr(policy_owner, "assert_target_max_mssql_cursor_supported", lambda **kw: calls.append(kw))
    source = CustomSource(SimpleNamespace(), SimpleNamespace(), sink_connector=None)
    source._resolve_strategy(SimpleNamespace(load_strategy=LoadStrategy.INCREMENTAL_APPEND, options={}))
    assert calls == [{"source_type": "custom-source", "configured_sink": None, "sink_connector": None}]
