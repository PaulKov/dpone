"""Focused cross-dialect backfill scope and identity contracts."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from dpone.backfill.campaign_contract import campaign_identity_hashes
from dpone.backfill.execution_policy import normalize_backfill_execution_policy, plan_hash
from dpone.backfill.planner import plan_chunks
from dpone.backfill.portable_scope_campaign import (
    CampaignPortableScopeBinding,
    compose_campaign_binding_identity,
)
from dpone.backfill.runtime_execution import BackfillChunkExecutor
from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.mssql_transaction_governance import InvocationIdentity, MssqlAttemptRequest
from dpone.contracts.portable_scope_binding import (
    PORTABLE_SCOPE_BINDING_OPTION,
    PortableScopeColumnContract,
    bind_portable_scope,
)
from dpone.contracts.portable_scope_resolution import resolve_bound_portable_scope
from dpone.contracts.source_physical_identity import SourcePhysicalIdentity
from dpone.runtime.etl.backfill_orchestrator import BackfillOrchestrator
from dpone.runtime.etl.mssql_transaction_identity import (
    invocation_route_fingerprint,
    operation_request,
)
from dpone.runtime.sinks.strategies.mssql.mssql_portable_scope import render_mssql_portable_scope
from dpone.runtime.sources.strategies.postgres.postgres_portable_scope import render_postgres_portable_scope

_SOURCE_AUTHORITY_SHA256 = "sha256:" + "a" * 64


def _config(tmp_path: Path, *, predicate_dialect: str = "generic") -> LoadConfig:
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="public",
        source_table="events",
        target_database="DWH",
        target_schema="dbo",
        target_table="events",
        load_strategy=LoadStrategy.BACKFILL,
        options={
            "source_type": "postgres",
            "sink_type": "mssql",
            "postgres_source_authority_sha256": _SOURCE_AUTHORITY_SHA256,
            "backfill": {
                "inner_mode": "replace",
                "chunk": {"column": "id", "from": 1, "to": 2, "step": 1, "kind": "integer"},
                "state_dir": str(tmp_path / "state"),
                "predicate_dialect": predicate_dialect,
            },
        },
    )


def _chunk_configs(tmp_path: Path) -> tuple[LoadConfig, LoadConfig]:
    base = _config(tmp_path)
    policy = normalize_backfill_execution_policy(base.options["backfill"])
    assert policy.chunk is not None
    chunks = plan_chunks(policy.chunk, run_key="portable-backfill")
    digest = plan_hash(chunks, execution_policy=policy)
    configs = tuple(
        BackfillChunkExecutor._chunk_load_config(
            base,
            chunk,
            run_key="portable-backfill",
            plan_hash=digest,
            owner=f"worker-{chunk.index}",
            lease_expires_at_utc=datetime.now(UTC) + timedelta(minutes=5),
        )
        for chunk in chunks
    )
    assert len(configs) == 2
    return configs


def _bind(config: LoadConfig) -> LoadConfig:
    assert config.portable_scope is not None
    binding = bind_portable_scope(
        config.portable_scope,
        PortableScopeColumnContract("id", "integer", None, "id", "int", None),
    )
    return replace(
        config,
        options={**config.options, PORTABLE_SCOPE_BINDING_OPTION: binding},
    )


def test_campaign_column_contract_binds_plan_and_config_identity(tmp_path: Path) -> None:
    config = _config(tmp_path)
    policy = normalize_backfill_execution_policy(config.options["backfill"])
    assert policy.chunk is not None
    chunks = plan_chunks(policy.chunk, run_key="portable-backfill")
    base = campaign_identity_hashes(
        chunks=chunks,
        dataset="dbo.events",
        execution_policy=policy,
        campaign_contract=None,
    )
    first = CampaignPortableScopeBinding(
        PortableScopeColumnContract("id", "integer", None, "id", "int", None),
        identity_bound=True,
    )
    second = CampaignPortableScopeBinding(
        PortableScopeColumnContract("id", "integer", None, "id", "bigint", None),
        identity_bound=True,
    )
    first_identity = campaign_identity_hashes(
        chunks=chunks,
        dataset="dbo.events",
        execution_policy=policy,
        campaign_contract=compose_campaign_binding_identity(None, first),
    )
    second_identity = campaign_identity_hashes(
        chunks=chunks,
        dataset="dbo.events",
        execution_policy=policy,
        campaign_contract=compose_campaign_binding_identity(None, second),
    )
    legacy = replace(first, identity_bound=False)

    assert CampaignPortableScopeBinding.from_jsonable(first.to_jsonable()) == first
    assert first_identity[0] != base[0] and first_identity[1] != base[1]
    assert second_identity[0] != first_identity[0] and second_identity[1] != first_identity[1]
    assert compose_campaign_binding_identity(None, legacy) is None


def test_cross_dialect_chunk_owns_only_canonical_ast_and_runtime_proof(tmp_path: Path) -> None:
    first, _second = _chunk_configs(tmp_path)
    operation_scope = first.options["__dpone_mssql_operation_scope"]
    chunk_context = first.options["backfill"]["chunk_context"]

    assert first.custom_predicate is None
    assert "source_custom_predicate" not in first.options
    assert first.portable_scope is not None
    assert operation_scope["portable_scope"] == first.portable_scope.to_contract()
    assert operation_scope["portable_scope_sha256"] == chunk_context["portable_scope_sha256"]
    assert chunk_context["portable_scope"] == operation_scope["portable_scope"]
    assert "source_predicate" not in operation_scope
    assert "target_predicate" not in operation_scope


def test_backfill_route_is_chunk_invariant_but_operation_and_dialect_sql_are_not(tmp_path: Path) -> None:
    first, second = (_bind(config) for config in _chunk_configs(tmp_path))
    source_identity = SourcePhysicalIdentity(
        dialect="postgres",
        cluster_identifier="cluster",
        database="source_db",
        effective_principal="etl",
        session_principal="etl",
        topology_role="primary",
        version=2,
        authority_sha256=_SOURCE_AUTHORITY_SHA256,
        timeline_id=1,
        database_oid=10,
        effective_principal_oid=11,
        session_principal_oid=11,
        schema="public",
        schema_oid=12,
        relation="events",
        relation_oid=13,
    )
    target_identity = b"t" * 32
    invocation = InvocationIdentity("run", "process", "pipeline:task")

    assert invocation_route_fingerprint(
        first,
        target_identity=target_identity,
        source_identity=source_identity,
    ) == invocation_route_fingerprint(
        second,
        target_identity=target_identity,
        source_identity=source_identity,
    )
    assert operation_request(first, invocation).scope_hash != operation_request(second, invocation).scope_hash

    resolved = resolve_bound_portable_scope(first)
    assert resolved is not None
    postgres = render_postgres_portable_scope(resolved.scope, resolved.binding)
    mssql = render_mssql_portable_scope(
        resolved.scope,
        resolved.binding,
        quote_identifier=lambda value: f"[{value}]",
    )
    assert postgres.params == mssql.params == (1, 1)
    postgres_sql = getattr(postgres.sql, "as_string", None)
    if callable(postgres_sql):
        assert postgres_sql(None) == '"id" >= %s AND "id" <= %s'
    assert mssql.sql == "[id] >= ? AND [id] <= ?"


def test_predicate_dialect_cannot_change_cross_dialect_plan_or_config(tmp_path: Path) -> None:
    first = _config(tmp_path, predicate_dialect="postgres")
    second = _config(tmp_path, predicate_dialect="mssql")
    first_policy = normalize_backfill_execution_policy(first.options["backfill"])
    second_policy = normalize_backfill_execution_policy(second.options["backfill"])
    assert first_policy.digest == second_policy.digest
    assert first_policy.chunk is not None and second_policy.chunk is not None
    assert plan_chunks(first_policy.chunk, run_key="same") == plan_chunks(second_policy.chunk, run_key="same")


def test_cross_dialect_raw_scope_rejects_before_ledger_or_chunk_runner(tmp_path: Path) -> None:
    config = replace(_config(tmp_path), custom_predicate="id >= 1")
    calls: list[LoadConfig] = []

    with pytest.raises(ValueError, match="cross_dialect_raw_predicate_unsupported"):
        BackfillOrchestrator(chunk_runner=lambda chunk: calls.append(chunk) or {}).run(config)

    assert calls == []
    assert not (tmp_path / "state").exists()


def test_operation_key_uses_the_bound_ast_not_rendered_predicate(tmp_path: Path) -> None:
    first, _second = (_bind(config) for config in _chunk_configs(tmp_path))
    invocation = InvocationIdentity("run", "process", "pipeline:task")
    attempt = MssqlAttemptRequest(
        invocation=invocation,
        target_identity=b"t" * 32,
        route_fingerprint=b"r" * 32,
        load_id="load",
        target_database="DWH",
        target_schema="dbo",
        target_table="events",
        strategy="backfill",
    )

    request = operation_request(first, invocation)

    assert len(request.operation_key(attempt)) == 32
