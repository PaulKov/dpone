"""Exact runtime handoff and compatibility at the default composition seam."""

from dataclasses import fields
from types import SimpleNamespace

import pytest

from dpone.dag.config_models import ETLProcessConfig
from dpone.runtime import bootstrap_runner
from dpone.runtime.etl import processor


def bindings(runtime, activation):
    return SimpleNamespace(
        source_obj=object(),
        sink_obj=object(),
        etl_logger=None,
        run_state_storage=None,
        partition_checkpoint_store=None,
        credential_resolution_receipts=(),
        postgres_mssql_correctness_runtime=runtime,
        postgres_mssql_correctness_activation=activation,
    )


@pytest.mark.parametrize("present", [False, True])
def test_config_to_default_processor_preserves_runtime_identity(present, monkeypatch):
    runtime = object() if present else None
    activation = object()
    bound = bindings(runtime, activation)
    config = ETLProcessConfig("test", SimpleNamespace(log_sample_rows=0))
    config.apply_runtime_bindings(bound)
    captured = {}
    monkeypatch.setattr(processor, "ETLProcessor", lambda **kwargs: captured.update(kwargs))
    bootstrap_runner._processor(
        config,
        load_config=config.load_config,
        native_transfer_runtime_service=None,
        route_capability_orchestrator=None,
        load_governance_service=None,
    )
    assert config.postgres_mssql_correctness_activation is activation
    assert config.postgres_mssql_correctness_runtime is runtime
    assert captured["source"] is bound.source_obj
    if present:
        assert captured["postgres_mssql_correctness_runtime"] is runtime
    else:
        assert "postgres_mssql_correctness_runtime" not in captured


def test_separately_hydrated_lanes_keep_distinct_runtimes(monkeypatch):
    captured = []
    monkeypatch.setattr(processor, "ETLProcessor", lambda **kwargs: captured.append(kwargs))
    lanes = [bindings(object(), object()), bindings(object(), object())]
    for lane in lanes:
        bootstrap_runner._processor(
            lane,
            load_config=SimpleNamespace(log_sample_rows=0),
            native_transfer_runtime_service=None,
            route_capability_orchestrator=None,
            load_governance_service=None,
        )
    assert all(
        value["postgres_mssql_correctness_runtime"] is lane.postgres_mssql_correctness_runtime
        for value, lane in zip(captured, lanes, strict=True)
    )
    assert captured[0]["postgres_mssql_correctness_runtime"] is not captured[1]["postgres_mssql_correctness_runtime"]


def test_config_positional_field_prefix_is_unchanged():
    assert tuple(field.name for field in fields(ETLProcessConfig))[:18] == (
        "name",
        "load_config",
        "transforms",
        "dependencies",
        "options",
        "description",
        "unique_key",
        "load_strategy",
        "task_group",
        "raw_config",
        "source_obj",
        "sink_obj",
        "etl_logger",
        "run_state_storage",
        "xmin_handoff_state_storage",
        "partition_checkpoint_store",
        "load_identity_service",
        "credential_resolution_receipts",
    )


def test_composition_rejects_runtime_type_name_spoof_before_binding():
    from dpone.runtime.bootstrap_postgres_source_authority import bind_postgres_mssql_source_schema_runtime

    spoof = type(
        "PostgresMssqlSourceSchemaRuntimeV1",
        (),
        {"__module__": "dpone.runtime.postgres_mssql_source_schema_runtime", "prepare_boundary": lambda self: None},
    )()
    calls = []
    source = SimpleNamespace(bind_postgres_mssql_source_schema_runtime=lambda runtime: calls.append(runtime))
    with pytest.raises(Exception, match="SOURCE_SCHEMA_AUTHORITY_RUNTIME_REQUIRED"):
        bind_postgres_mssql_source_schema_runtime(source_obj=source, runtime=spoof)
    assert calls == []


def test_source_retains_prevalidated_capability_and_rejects_rebinding():
    from dpone.runtime.sources.postgres import PostgresSource

    source = object.__new__(PostgresSource)
    source._postgres_mssql_source_schema_runtime = None
    runtime = SimpleNamespace(prepare_boundary=lambda **kwargs: None)
    source.bind_postgres_mssql_source_schema_runtime(runtime)
    source.bind_postgres_mssql_source_schema_runtime(runtime)
    assert source._postgres_mssql_source_schema_runtime is runtime
    with pytest.raises(ValueError, match="already_bound"):
        source.bind_postgres_mssql_source_schema_runtime(SimpleNamespace(prepare_boundary=lambda **kwargs: None))
    assert source._postgres_mssql_source_schema_runtime is runtime


@pytest.mark.parametrize("replacement", [None, object()])
@pytest.mark.parametrize("routed", [False, True])
def test_extract_rechecks_runtime_after_mutating_hook(replacement, routed):
    from dpone.runtime.etl.processor_runtime import ProcessorRuntimeServices
    from dpone.runtime.postgres_mssql_r1_execution import (
        POSTGRES_MSSQL_R1_EXECUTION_OPTION,
        PostgresMssqlR1ExecutionError,
        bind_postgres_mssql_r1_execution,
    )

    calls = []
    runtime = SimpleNamespace(
        prepare_admission=lambda config, **kwargs: bind_postgres_mssql_r1_execution(config, runtime)
    )
    source = SimpleNamespace(extract=lambda *args, **kwargs: calls.append("source"))
    route = SimpleNamespace(extract=lambda **kwargs: calls.append("route")) if routed else None
    service = ProcessorRuntimeServices(
        source=source,
        sink=object(),
        source_state_service=object(),
        payload_load_service=object(),
        source_extraction_lifecycle_service=SimpleNamespace(capture=lambda invoke: invoke()),
        postgres_mssql_correctness_runtime=runtime,
    )
    config = service.prepare_admission(SimpleNamespace(options={}), run_context=None, load_record=None, dag_id=None)
    if replacement is None:
        config.options.pop(POSTGRES_MSSQL_R1_EXECUTION_OPTION)
    else:
        config.options[POSTGRES_MSSQL_R1_EXECUTION_OPTION] = replacement
    with pytest.raises(PostgresMssqlR1ExecutionError, match="execution_context_"):
        service.extract(config, None, None, route)
    assert calls == []


@pytest.mark.parametrize("replacement", [None, object()])
@pytest.mark.parametrize("stage", ["route", "state", "preflight", "replay"])
def test_runtime_services_recheck_context_before_delegated_io(replacement, stage):
    from dpone.runtime.etl.processor_runtime import ProcessorRuntimeServices
    from dpone.runtime.postgres_mssql_r1_execution import (
        POSTGRES_MSSQL_R1_EXECUTION_OPTION,
        PostgresMssqlR1ExecutionError,
        bind_postgres_mssql_r1_execution,
    )

    calls = []
    runtime = SimpleNamespace(replay_result=lambda config: calls.append("replay"))
    service = ProcessorRuntimeServices(
        source=object(),
        sink=SimpleNamespace(preflight_before_extract=lambda **kwargs: calls.append("sink")),
        source_state_service=SimpleNamespace(load_for_extract=lambda *args: calls.append("state")),
        payload_load_service=object(),
        source_extraction_lifecycle_service=SimpleNamespace(assert_supported=lambda *args: calls.append("support")),
        route_capability_orchestrator=SimpleNamespace(prepare=lambda **kwargs: calls.append("route")),
        postgres_mssql_correctness_runtime=runtime,
    )
    config = bind_postgres_mssql_r1_execution(SimpleNamespace(options={}), runtime)
    if replacement is None:
        config.options.pop(POSTGRES_MSSQL_R1_EXECUTION_OPTION)
    else:
        config.options[POSTGRES_MSSQL_R1_EXECUTION_OPTION] = replacement
    action = {
        "route": lambda: service.prepare_route_capabilities(config, None),
        "state": lambda: service.load_incremental_state(config),
        "preflight": lambda: service.preflight_before_extract(config, None),
        "replay": lambda: service.replay_result(config),
    }[stage]
    with pytest.raises(PostgresMssqlR1ExecutionError, match="execution_context_"):
        action()
    assert calls == []


@pytest.mark.parametrize("replacement", [None, object()])
@pytest.mark.parametrize("stage", ["sink", "route", "state"])
def test_callback_mutation_cannot_reach_next_io_boundary(replacement, stage):
    from dpone.runtime.etl.processor_runtime import ProcessorRuntimeServices
    from dpone.runtime.postgres_mssql_r1_execution import (
        POSTGRES_MSSQL_R1_EXECUTION_OPTION,
        PostgresMssqlR1ExecutionError,
        bind_postgres_mssql_r1_execution,
    )

    calls = []
    runtime = object()
    config = bind_postgres_mssql_r1_execution(SimpleNamespace(options={}), runtime)

    def mutate(*args, **kwargs):
        calls.append(stage)
        if replacement is None:
            config.options.pop(POSTGRES_MSSQL_R1_EXECUTION_OPTION)
        else:
            config.options[POSTGRES_MSSQL_R1_EXECUTION_OPTION] = replacement

    service = ProcessorRuntimeServices(
        source=SimpleNamespace(extract=lambda *args: calls.append("extract")),
        sink=SimpleNamespace(preflight_before_extract=mutate),
        source_state_service=SimpleNamespace(load_for_extract=mutate),
        payload_load_service=SimpleNamespace(
            schema_evolution_service=SimpleNamespace(preflight_before_extract=lambda **kwargs: calls.append("schema"))
        ),
        source_extraction_lifecycle_service=SimpleNamespace(
            assert_supported=lambda *args: None, capture=lambda invoke: invoke()
        ),
        route_capability_orchestrator=SimpleNamespace(prepare=mutate),
        postgres_mssql_correctness_runtime=runtime,
    )
    with pytest.raises(PostgresMssqlR1ExecutionError, match="execution_context_"):
        if stage == "sink":
            service.preflight_before_extract(config, None)
        elif stage == "route":
            service.prepare_route_capabilities(config, None)
            service.load_incremental_state(config)
        else:
            service.load_incremental_state(config)
            service.extract(config, None, None, None)
    assert calls == [stage]
