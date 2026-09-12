"""Compatibility and dependency boundaries for load normalization and parse tracing."""

from __future__ import annotations

import importlib
import pickle
import subprocess
import sys
from typing import get_type_hints

import pytest

from dpone.config.load_config import ROUTE_IDENTITY_V1_ENDPOINT_TYPES_OPTION
from dpone.config.load_strategy import LoadStrategy
from dpone.dag import load_config_builder_support as support
from dpone.dag import parse_trace

MOVES = (
    ("load_config_endpoint_identity", "inject_endpoint_identity_options", "load_config_builder_support"),
    ("load_config_partition_contract", "resolve_partition_contract", "load_config_builder_support"),
    ("load_config_builder_support", "LoadConfigParseTracer", "parse_trace"),
    ("load_config_builder_support", "record_load_fields", "parse_trace"),
    ("load_config_builder_support", "record_runtime_contract_fields", "parse_trace"),
)


@pytest.mark.parametrize(("previous", "name", "current"), MOVES)
def test_old_import_and_pickle_lookup_preserve_exact_object(previous: str, name: str, current: str) -> None:
    old_module = importlib.import_module(f"dpone.dag.{previous}")
    new_module = importlib.import_module(f"dpone.dag.{current}")
    old_object = getattr(old_module, name)
    assert old_object is getattr(new_module, name)
    assert old_object.__module__ == f"dpone.dag.{current}"
    assert pickle.loads(pickle.dumps(old_object)) is old_object
    historical_lookup = f"cdpone.dag.{previous}\n{name}\n.".encode("ascii")
    assert pickle.loads(historical_lookup) is old_object
    assert get_type_hints(old_object) == get_type_hints(getattr(new_module, name))


@pytest.mark.parametrize(
    "entrypoint",
    [
        "parse_trace",
        "load_config_scope",
        "load_config_endpoint_identity",
        "load_config_partition_contract",
        "load_config_builder",
    ],
)
def test_fresh_process_import_boundaries(entrypoint: str) -> None:
    script = f"""
import importlib, sys
module = importlib.import_module('dpone.dag.{entrypoint}')
if {entrypoint!r} in ('parse_trace', 'load_config_scope'):
    assert not any(n == 'dpone.config' or n.startswith('dpone.config.') for n in sys.modules)
    assert 'dpone.dag.load_config_builder_support' not in sys.modules
assert not any(n == 'airflow' or n.startswith('airflow.') for n in sys.modules)
"""
    result = subprocess.run([sys.executable, "-B", "-c", script], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("aliased", [False, True])
def test_endpoint_identity_replaces_stale_legacy_projection(aliased: bool) -> None:
    options: dict[str, object] = {ROUTE_IDENTITY_V1_ENDPOINT_TYPES_OPTION: {"stale": True}, "keep": 7}
    support.inject_endpoint_identity_options(
        options, "postgresql" if aliased else "postgres", "mssql", "postgres", "mssql"
    )
    assert options["source_type"] == "postgres"
    assert options["sink_type"] == "mssql"
    assert options["keep"] == 7
    if aliased:
        assert options[ROUTE_IDENTITY_V1_ENDPOINT_TYPES_OPTION] == {"source_type": "postgresql", "sink_type": "mssql"}
    else:
        assert ROUTE_IDENTITY_V1_ENDPOINT_TYPES_OPTION not in options


@pytest.mark.parametrize(
    ("mode", "column", "strategy", "expected"),
    [
        (" AUTO ", " day ", LoadStrategy.PARTITION_REPLACE, {"column": "day", "values_from_staging": True}),
        ("auto", "  ", LoadStrategy.PARTITION_REPLACE, {}),
        ("auto", 42, LoadStrategy.PARTITION_REPLACE, {}),
        ("auto", "day", LoadStrategy.FULL_REFRESH, {}),
        ("full_refresh", "day", LoadStrategy.PARTITION_REPLACE, {}),
        (None, "day", LoadStrategy.PARTITION_REPLACE, {}),
    ],
)
def test_partition_contract_boundaries(mode: object, column: object, strategy: LoadStrategy, expected: object) -> None:
    assert (
        support.resolve_partition_contract(
            strategy_config={"mode": mode}, source_options={"partition_column": column}, load_strategy=strategy
        )
        == expected
    )


def test_explicit_partition_is_not_copied_or_reinterpreted() -> None:
    authored: dict[str, object] = {}
    assert (
        support.resolve_partition_contract(
            strategy_config={"partition": authored, "mode": "auto"},
            source_options={"partition_column": "day"},
            load_strategy=LoadStrategy.PARTITION_REPLACE,
        )
        is authored
    )


@pytest.mark.parametrize("authored", [False, True])
def test_trace_order_values_sources_and_operations(authored: bool) -> None:
    tracer = parse_trace.ParseTracer()
    source = {"unique_key": "id", "batch_size": 2, "export_format": "json", "compress_export": True} if authored else {}
    sink = {"log_sample_rows": 0} if authored else {}
    support.record_load_fields(
        parse_tracer=tracer,
        source_options=source,
        sink_options=sink,
        strategy_cfg={"custom_predicate": None} if authored else {},
        sink_custom_predicate=None,
    )
    support.record_runtime_contract_fields(
        config={"reconciliation": False, "tech_schema": None} if authored else {},
        reconciliation=False,
        tech_schema=None,
        parse_tracer=tracer,
    )
    records = tracer.to_trace().records
    assert [r.target for r in records] == [
        "load_config.unique_key",
        "load_config.custom_predicate",
        "load_config.batch_size",
        "load_config.log_sample_rows",
        "load_config.export_format",
        "load_config.compress_export",
        "load_config.reconciliation",
        "load_config.tech_schema",
    ]
    assert [r.value for r in records] == (
        ["id", None, 2, 0, "json", True, False, None] if authored else [None, None, 10000, 5, "csv", False, False, None]
    )
    assert [r.sources for r in records] == [
        ("source.options.unique_key",),
        ("sink.strategy.custom_predicate",),
        ("source.options.batch_size",),
        ("sink.options.log_sample_rows",),
        ("source.options.export_format",),
        ("source.options.compress_export",),
        ("reconciliation",),
        ("tech_schema",),
    ]
    assert [r.operation for r in records] == (["copy"] * 8 if authored else ["default"] * 8)
    assert all(r.kind == "load_config.field" and r.details == {} for r in records)
    assert pickle.loads(pickle.dumps(tracer.to_trace())) == tracer.to_trace()


def test_disabled_runtime_tracing_does_not_inspect_configuration() -> None:
    class Unreadable(dict[str, object]):
        def __contains__(self, key: object) -> bool:
            raise AssertionError("disabled tracing must not inspect fields")

    support.record_runtime_contract_fields(
        config=Unreadable(), reconciliation=False, tech_schema=None, parse_tracer=None
    )


def test_builder_keeps_injectable_function_bindings(monkeypatch: pytest.MonkeyPatch) -> None:
    from dpone.dag import load_config_builder as builder

    called: list[str] = []
    endpoint = builder.inject_endpoint_identity_options
    partition = builder.resolve_partition_contract

    def inject(*args: object, **kwargs: object) -> None:
        called.append("endpoint")
        endpoint(*args, **kwargs)  # type: ignore[arg-type]

    def resolve(**kwargs: object) -> object:
        called.append("partition")
        return partition(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(builder, "inject_endpoint_identity_options", inject)
    monkeypatch.setattr(builder, "resolve_partition_contract", resolve)
    builder.LoadConfigBuilder().build(
        {
            "source": {"type": "postgres", "table": {"schema": "public", "name": "items"}},
            "sink": {
                "type": "postgres",
                "table": {"schema": "public", "name": "items"},
                "strategy": {"mode": "full_refresh"},
            },
        }
    )
    assert called == ["partition", "endpoint"]
