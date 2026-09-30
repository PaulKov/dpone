"""Compiler-to-runtime coverage for compact MSSQL target schema labels."""

from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pytest

from dpone.dag.load_config_builder import LoadConfigBuilder
from dpone.ports.source_state_storage import MssqlStateLocation
from dpone.runtime.sources.strategies.postgres.postgres_xmin_extract import PostgresXMinExtractStrategy
from dpone.runtime.state import mssql_route_preflight, mssql_target_identity
from dpone.runtime.state.mssql_target_identity import (
    MssqlPhysicalTargetIdentityError,
    resolve_mssql_physical_target_identity,
)
from dpone.runtime.state.mssql_transactional import MssqlTransactionalStateService
from tests.test_mssql_target_identity import _SESSION, _RegistryConnector


@pytest.fixture
def registry(monkeypatch):
    """Keep SQL-backed registry lookup real; isolate catalog/topology proofs."""
    monkeypatch.setattr(mssql_target_identity, "require_target_identity_registry_contract", lambda *a, **k: None)
    monkeypatch.setattr(mssql_route_preflight, "require_atomic_mssql_route", lambda *a, **k: _SESSION)
    return _RegistryConnector(case_sensitive=False)


def _config():
    config = LoadConfigBuilder().build(
        {
            "source": {
                "type": "postgres",
                "connection_id": "pg",
                "table": {"database": "source_db", "schema": "public", "name": "metrics_value"},
                "options": {"incremental_strategy": "xmin"},
            },
            "sink": {
                "type": "mssql",
                "connection_id": "mssql",
                "table": {"database": "dwh_example", "schema": "sample_metrics", "name": "metrics_value"},
                "strategy": {"mode": "incremental_merge", "unique_key": ["id"]},
            },
        }
    )
    config.options["state_identity"] = {"environment": "dev", "process": "metrics"}
    config.options["schema_contract"] = {"columns": {"id": {"nullable": False}}}
    return config


def _strategy(registry):
    return PostgresXMinExtractStrategy(
        connector=SimpleNamespace(database="source_db"),
        sink_connector=registry,
        state_storage=object(),
        logger=object(),
    )


def test_compiled_target_matches_bare_schema_state_and_receipt_identity(registry):
    compiled = _config()
    assert compiled.target_schema == "dwh_example.sample_metrics"
    bare = deepcopy(compiled)
    bare.target_schema = "sample_metrics"
    keys = []
    for config in (compiled, bare):
        strategy = _strategy(registry)
        strategy._preflight_atomic_route(config)
        assert (config.target_database, config.target_schema, config.target_table) == (
            "dwh_example",
            "sample_metrics",
            "Metrics_Value",
        )
        keys.append(strategy._snapshot_extractor.state_key(config, [("id", "int")]))
        # Restoring the equivalent compiled label must retain the proven binding.
        config.target_schema = "dwh_example.sample_metrics"
        assert strategy.physical_target_identity(config) == keys[-1].target_identity
        count = len(registry.calls)
        strategy._preflight_atomic_route(config)
        assert len(registry.calls) == count
    assert keys[0].digest == keys[1].digest
    assert keys[0].target_identity == keys[1].target_identity
    transaction = MssqlTransactionalStateService(
        registry, MssqlStateLocation("state_db", "dbo", "source_state", "load_receipt")
    )
    for key in keys:
        assert transaction.probe_receipt(key=key, load_id="same-run") is None
    assert registry.calls[-1][1] == registry.calls[-2][1] == (keys[0].target_identity, keys[0].digest, "same-run")


def test_direct_registry_resolution_accepts_equivalent_compact_label(registry):
    identities = [
        resolve_mssql_physical_target_identity(
            registry, session=_SESSION, database="dwh_example", schema=schema, table="metrics_value"
        )
        for schema in ("sample_metrics", "dwh_example.sample_metrics")
    ]
    assert identities[0] == identities[1]


@pytest.mark.parametrize(
    ("database", "schema", "table", "error"),
    [
        (None, "dwh_example.sample_metrics", "metrics_value", "coordinates_incomplete"),
        ("dwh_example", "", "metrics_value", "coordinates_incomplete"),
        ("dwh_example", "sample_metrics", "", "coordinates_incomplete"),
        ("dwh_example", "other_db.sample_metrics", "metrics_value", "coordinates_invalid"),
        ("dwh_example", "db.schema.extra", "metrics_value", "coordinates_invalid"),
    ],
)
def test_invalid_coordinates_fail_closed_before_registry_io(registry, database, schema, table, error):
    config = SimpleNamespace(target_database=database, target_schema=schema, target_table=table)
    with pytest.raises(ValueError, match=error):
        _strategy(registry)._preflight_atomic_route(config)
    with pytest.raises(MssqlPhysicalTargetIdentityError, match=error):
        resolve_mssql_physical_target_identity(
            registry, session=_SESSION, database=database, schema=schema, table=table
        )
    assert registry.calls == []


def test_compact_label_keeps_missing_registry_binding_fail_closed(registry):
    config = _config()
    config.target_table = "not_registered"
    with pytest.raises(MssqlPhysicalTargetIdentityError, match="registry_binding_missing_or_ambiguous"):
        _strategy(registry)._preflight_atomic_route(config)
    assert registry.calls[-1][1] == ("sample_metrics", "not_registered")
