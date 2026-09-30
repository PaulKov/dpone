"""Exercise runtime audit propagation through normal and lane composition."""

from types import SimpleNamespace

import pytest

from dpone.contracts.run_context import RunContext
from dpone.dag.config_models import ETLProcessConfig
from dpone.ports.runtime_hydrator import RuntimeBindings
from dpone.runtime.bootstrap_runner import DefaultProcessRunner, _worker_chunk_runner_factory
from dpone.runtime.lineage.audit import LoadIdentityService
from tests.test_runtime_decision_audit import _DecisionLogger, _DecisionSink, _DecisionSource, _etl_load_config
from tests.test_selected_runtime_audit import _pair


def _bindings(pair):
    return RuntimeBindings(
        source_obj=_DecisionSource(),
        sink_obj=_DecisionSink(),
        etl_logger=_DecisionLogger(),
        load_identity_service=LoadIdentityService(audit_storage=pair.loads),
        audit_bindings=pair,
    )


class _SelectedRouteFactory:
    """An extension that requires selected bindings; omission cannot go unnoticed."""

    def build(self, *, load_config, source, sink, logger, audit_bindings):
        assert audit_bindings.loads.connector is audit_bindings.connector
        assert audit_bindings.require_steps().connector is audit_bindings.connector
        return None


def _config():
    load = _etl_load_config()
    load.options["load_governance"]["audit"]["enabled"] = True
    return ETLProcessConfig(name="example_sync", load_config=load, raw_config={"name": "example_sync"})


def _assert_decision_written(pair):
    inserts = [params for sql, params in pair.connector.queries if "INSERT INTO" in sql]
    assert any(params[2] == "test.runtime_branch" for params in inserts)


def test_normal_runner_keeps_selected_audit_after_config_hydration():
    pair = _pair()
    config = _config()
    config.apply_runtime_bindings(_bindings(pair))
    result = DefaultProcessRunner(route_capability_factory=_SelectedRouteFactory()).run(
        SimpleNamespace(config=config, current_state=None), context=RunContext(run_id="selected-run")
    )
    assert result.status == "success"
    _assert_decision_written(pair)
    assert any(r["step_id"] == "test.runtime_branch" for r in result.details["load_steps"])


def test_default_hydrator_transfers_admitted_pair_and_same_load_identity():
    from dpone.runtime.bootstrap_hydrator import DefaultRuntimeHydrator
    from dpone.runtime.bootstrap_state_models import RuntimeStateBindings
    from dpone.runtime.credentials.runtime_context import RuntimeConnectionContext
    from tests.test_runtime_connection_composition_root import (
        _connection,
        _ContextLoader,
        _EndpointFactory,
        _load_config,
        _Resolver,
        _StateBootstrap,
    )

    pair = _pair()

    class StateBootstrap(_StateBootstrap):
        def build_resolved(self, **kwargs):
            return RuntimeStateBindings(
                state_type="mssql",
                proxy_config={},
                xmin_state_storage=None,
                load_audit_storage=pair.loads,
                audit_bindings=pair,
            )

    context = RuntimeConnectionContext(
        environment="test",
        binding_set={},
        connection_registry={},
        credential_runtime={},
        resolver=_Resolver(
            events=[],
            connections={
                "source-main": _connection("postgres"),
                "sink-main": _connection("clickhouse"),
            },
        ),
    )
    bindings = DefaultRuntimeHydrator(
        state_bootstrap=StateBootstrap([]),
        endpoint_factory=_EndpointFactory([]),
        connection_context_loader=_ContextLoader(context),
    ).build(
        config={
            "source": {"type": "postgres", "connection_ref": "source-main"},
            "sink": {"type": "clickhouse", "connection_ref": "sink-main"},
            "state": {"type": "disabled"},
        },
        load_config=_load_config(),
    )
    assert bindings.audit_bindings is pair
    assert bindings.load_identity_service.audit_storage is pair.loads


@pytest.mark.parametrize("lane_kind", ["thread", "process"])
def test_worker_uses_fresh_lane_audit_not_parent_and_disposes_once(monkeypatch, lane_kind):
    from dpone.ports import runtime_hydrator
    from dpone.runtime.etl.backfill_process_runtime import BackfillProcessLanePayload, open_backfill_process_lane

    parent = _pair()
    config = _config()
    config.apply_runtime_bindings(_bindings(parent))
    lanes, closed = [], []

    def hydrate(**kwargs):
        pair = _pair()
        pair.connector.close = lambda: closed.append(pair.connector)
        lanes.append(pair)
        return _bindings(pair)

    monkeypatch.setattr(runtime_hydrator, "ensure_runtime_hydrator", lambda: SimpleNamespace(build=hydrate))
    monkeypatch.setattr("dpone.runtime.route_runtime_factory.RouteCapabilityRuntimeFactory", _SelectedRouteFactory)
    context = RunContext(run_id="selected-worker-run")
    for lane in range(2):
        if lane_kind == "thread":
            factory = _worker_chunk_runner_factory(
                config,
                route_capability_factory=_SelectedRouteFactory(),
                run_context=context,
                dag_id="example_sync",
                execution_date=None,
            )
            manager = factory(lane)
        else:
            manager = open_backfill_process_lane(
                lane,
                BackfillProcessLanePayload(
                    raw_config=config.raw_config,
                    run_context=context,
                    dag_id="example_sync",
                    execution_date=None,
                ),
                operation_lease_factory=None,
            )
        with manager as run:
            result = run(config.load_config)
            assert result["status"] == "success"
    assert len(lanes) == 2
    assert lanes[0].connector is not lanes[1].connector
    assert closed == [p.connector for p in lanes]
    assert parent.connector.queries == []
    for pair in lanes:
        _assert_decision_written(pair)
