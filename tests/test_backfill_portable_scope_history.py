"""Legacy campaign binding history proofs before durable migration."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import pytest

from dpone.backfill.execution_policy import lease_expires_at, normalize_backfill_execution_policy, plan_hash
from dpone.backfill.planner import plan_chunks
from dpone.backfill.portable_scope_campaign import CampaignPortableScopeBinding
from dpone.backfill.runtime_execution import BackfillChunkExecutor
from dpone.backfill.shadow_append_authority import (
    SHADOW_APPEND_AUTHORITY_OPTION,
    issue_shadow_append_authority,
)
from dpone.backfill.state import BackfillChunkRecord, BackfillLedger
from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.portable_scope_binding import PortableScopeColumnContract
from dpone.contracts.run_context import RunContext
from dpone.contracts.source_physical_identity import SourcePhysicalIdentity
from dpone.runtime.etl.backfill_portable_scope_history import (
    BackfillPortableScopeHistoryError,
    MssqlBackfillPortableScopeHistoryProof,
    build_mssql_backfill_portable_scope_history_proof,
)
from dpone.runtime.etl.mssql_transaction_identity import (
    build_mssql_attempt_request,
    invocation_identity,
    operation_request,
)
from dpone.runtime.governance.mssql_hook_replay_policy import (
    MSSQL_HOOK_GRAPH_SHA256_OPTION,
    bind_replay_safe_mssql_hook_graph,
)
from dpone.runtime.state.mssql_generic_operation_history import (
    MssqlAttemptOperationHistory,
)

_AUTHORITY = "sha256:" + "a" * 64


def test_matching_unledgered_operation_authorizes_exact_receipt_namespace() -> None:
    proof, state, ledger, chunks, binding, target_calls = _fixture()
    ledger.chunks[0].attempts = 1
    ledger.chunks[0].status = "success"
    candidate = proof.build_candidate(
        load_config=_shadow_config(),
        ledger=ledger,
        chunks=chunks,
        campaign_binding=binding,
    )
    persisted_scope = dict(candidate.chunk_scope_hashes)[ledger.chunks[0].index]
    state.history = MssqlAttemptOperationHistory(
        candidate.attempt.attempt_key,
        frozenset({persisted_scope}),
        frozenset({persisted_scope}),
    )

    proof(
        load_config=_shadow_config(),
        ledger=ledger,
        chunks=chunks,
        campaign_binding=binding,
    )

    assert len(candidate.scope_hashes) == len(chunks)
    assert state.requests[-1] == candidate.attempt
    assert (candidate.attempt.target_schema, candidate.attempt.target_table) == ("dbo", "events__shadow")
    assert target_calls[-1] == ("DWH", "dbo", "events")


@pytest.mark.parametrize("hook_config", (None, {"pre_hook": [], "post_hook": []}))
def test_history_candidate_matches_admission_hook_canonical_identity(hook_config: object) -> None:
    proof, _state, ledger, chunks, binding, _target_calls = _fixture()
    config = _shadow_config()
    if hook_config is not None:
        config.options["hooks"] = hook_config
    candidate = proof.build_candidate(
        load_config=config,
        ledger=ledger,
        chunks=chunks,
        campaign_binding=binding,
    )
    raw_chunk = BackfillChunkExecutor.build_chunk_load_config(
        config,
        chunks[0],
        run_key=ledger.run_key,
        plan_hash=ledger.plan_hash,
        owner="dpone-backfill-portable-scope-history-proof",
        lease_expires_at_utc=lease_expires_at(config),
        portable_scope_campaign=binding,
    )
    canonical = bind_replay_safe_mssql_hook_graph(raw_chunk)
    invocation = invocation_identity(proof.run_context, canonical, dag_id=proof.dag_id)
    expected_attempt = build_mssql_attempt_request(
        canonical,
        invocation=invocation,
        target_identity=b"t" * 32,
        source_identity=_source_identity(),
        load_id="dpone-backfill-portable-scope-history-proof",
        request_coordinates=(
            candidate.attempt.target_database,
            candidate.attempt.target_schema,
            candidate.attempt.target_table,
        ),
    )

    assert canonical.options[MSSQL_HOOK_GRAPH_SHA256_OPTION]
    assert candidate.attempt == expected_attempt
    assert (
        dict(candidate.chunk_scope_hashes)[chunks[0].index]
        == operation_request(
            canonical,
            invocation,
        ).scope_hash
    )


@pytest.mark.parametrize(
    "drifted_columns",
    (
        PortableScopeColumnContract("id", "bigint", None, "id", "int", None),
        PortableScopeColumnContract("id", "bigint", None, "id", "bigint", "Latin1_General_100_BIN2"),
    ),
)
def test_type_or_collation_drift_rejects_before_migration(
    drifted_columns: PortableScopeColumnContract,
) -> None:
    proof, state, ledger, chunks, binding, _target_calls = _fixture()
    ledger.chunks[0].attempts = 1
    original = proof.build_candidate(
        load_config=_shadow_config(),
        ledger=ledger,
        chunks=chunks,
        campaign_binding=binding,
    )
    state.history = MssqlAttemptOperationHistory(original.attempt.attempt_key, original.scope_hashes)

    with pytest.raises(BackfillPortableScopeHistoryError, match="operation_scope_mismatch"):
        proof(
            load_config=_shadow_config(),
            ledger=ledger,
            chunks=chunks,
            campaign_binding=CampaignPortableScopeBinding(drifted_columns, identity_bound=False),
        )


def test_empty_history_is_allowed_only_for_truly_new_legacy_ledger() -> None:
    proof, state, ledger, chunks, binding, _target_calls = _fixture()

    proof(
        load_config=_shadow_config(),
        ledger=ledger,
        chunks=chunks,
        campaign_binding=binding,
    )

    ledger.chunks[0].attempts = 1
    ledger.chunks[0].status = "failed"
    ledger.chunks[0].error = "catalog preflight failed before operation admission"
    proof(
        load_config=_shadow_config(),
        ledger=ledger,
        chunks=chunks,
        campaign_binding=binding,
    )

    ledger.chunks[0].status = "success"
    with pytest.raises(BackfillPortableScopeHistoryError, match="operation_history_missing"):
        proof(
            load_config=_shadow_config(),
            ledger=ledger,
            chunks=chunks,
            campaign_binding=binding,
        )
    assert state.history is None


def test_wrong_attempt_identity_is_rejected() -> None:
    proof, state, ledger, chunks, binding, _target_calls = _fixture()
    candidate = proof.build_candidate(
        load_config=_shadow_config(),
        ledger=ledger,
        chunks=chunks,
        campaign_binding=binding,
    )
    state.history = MssqlAttemptOperationHistory(b"x" * 32, candidate.scope_hashes)

    with pytest.raises(BackfillPortableScopeHistoryError, match="attempt_identity_mismatch"):
        proof(
            load_config=_shadow_config(),
            ledger=ledger,
            chunks=chunks,
            campaign_binding=binding,
        )


def test_success_scope_missing_rejects_even_when_another_operation_has_a_receipt() -> None:
    proof, state, ledger, chunks, binding, _target_calls = _fixture()
    candidate = proof.build_candidate(
        load_config=_shadow_config(),
        ledger=ledger,
        chunks=chunks,
        campaign_binding=binding,
    )
    scopes = dict(candidate.chunk_scope_hashes)
    ledger.chunks[0].status = "success"
    other_scope = scopes[ledger.chunks[1].index]
    state.history = MssqlAttemptOperationHistory(
        candidate.attempt.attempt_key,
        frozenset({other_scope}),
        frozenset({other_scope}),
    )

    with pytest.raises(BackfillPortableScopeHistoryError, match="committed_receipt_missing"):
        proof(
            load_config=_shadow_config(),
            ledger=ledger,
            chunks=chunks,
            campaign_binding=binding,
        )


def test_success_operation_without_committed_receipt_rejects() -> None:
    proof, state, ledger, chunks, binding, _target_calls = _fixture()
    candidate = proof.build_candidate(
        load_config=_shadow_config(),
        ledger=ledger,
        chunks=chunks,
        campaign_binding=binding,
    )
    ledger.chunks[0].status = "success"
    success_scope = dict(candidate.chunk_scope_hashes)[ledger.chunks[0].index]
    state.history = MssqlAttemptOperationHistory(
        candidate.attempt.attempt_key,
        frozenset({success_scope}),
    )

    with pytest.raises(BackfillPortableScopeHistoryError, match="committed_receipt_missing"):
        proof(
            load_config=_shadow_config(),
            ledger=ledger,
            chunks=chunks,
            campaign_binding=binding,
        )


def test_new_or_persisted_campaign_never_reads_legacy_history() -> None:
    proof, state, ledger, chunks, binding, target_calls = _fixture()
    ledger.portable_scope_column_contract = binding.to_jsonable()

    proof(
        load_config=_shadow_config(),
        ledger=ledger,
        chunks=chunks,
        campaign_binding=binding,
    )
    proof(
        load_config=_shadow_config(),
        ledger=replace(ledger, portable_scope_column_contract=None),
        chunks=chunks,
        campaign_binding=replace(binding, identity_bound=True),
    )

    assert state.requests == []
    assert target_calls == []


def test_database_authority_tamper_rejects_before_history_or_target_catalog() -> None:
    proof, state, ledger, chunks, binding, target_calls = _fixture()

    def reject_tamper(_connector) -> None:
        raise RuntimeError("mssql_transaction.database_authority_binding_mismatch")

    proof = replace(
        proof,
        state_storage=SimpleNamespace(
            atomicity="target_atomic",
            connector=object(),
            require_database_authority_binding=lambda: None,
            verify_database_authority=reject_tamper,
        ),
    )

    with pytest.raises(RuntimeError, match="database_authority_binding_mismatch"):
        proof(
            load_config=_shadow_config(),
            ledger=ledger,
            chunks=chunks,
            campaign_binding=binding,
        )

    assert state.requests == []
    assert target_calls == []


def test_public_composition_is_independent_of_process_lanes() -> None:
    proof, state, _ledger, _chunks, _binding, _target_calls = _fixture()

    composed = build_mssql_backfill_portable_scope_history_proof(
        source=proof.source,
        sink=proof.sink,
        run_context=proof.run_context,
        dag_id=proof.dag_id,
        state=state,
    )

    assert composed.state is state
    assert composed.state_storage is proof.sink.state_storage


def _fixture():
    config = _shadow_config()
    policy = normalize_backfill_execution_policy(config.options["backfill"])
    assert policy.chunk is not None
    chunks = plan_chunks(policy.chunk, run_key="legacy")
    ledger = BackfillLedger(
        run_key="legacy",
        dataset="dbo.events",
        inner_mode=policy.inner_mode,
        plan_hash=plan_hash(chunks, execution_policy=policy),
        config_hash="legacy-config",
        chunk_config=policy.chunk.to_jsonable(),
        chunks=[BackfillChunkRecord(chunk.index, chunk.start, chunk.end, chunk.idempotency_key) for chunk in chunks],
    )
    state = _HistoryState()
    target_calls: list[tuple[str, str, str]] = []

    def target_resolver(_connector, _storage, *, database, schema, table):
        target_calls.append((database, schema, table))
        return SimpleNamespace(
            digest=b"t" * 32,
            database_name=database,
            schema_name=schema,
            table_name=table,
        )

    storage = SimpleNamespace(
        atomicity="target_atomic",
        connector=object(),
        require_database_authority_binding=lambda: None,
        verify_database_authority=lambda _connector: None,
    )
    source = SimpleNamespace(
        mssql_transaction_source_physical_identity=lambda _config: _source_identity(),
    )
    proof = MssqlBackfillPortableScopeHistoryProof(
        state=state,
        source=source,
        sink=SimpleNamespace(connector=object(), state_storage=storage),
        state_storage=storage,
        run_context=RunContext(
            "scheduler-retry",
            config={"process": "process", "pipeline_id": "pipeline", "task_id": "task"},
        ),
        dag_id="dag",
        target_resolver=target_resolver,
    )
    binding = CampaignPortableScopeBinding(
        PortableScopeColumnContract("id", "bigint", None, "id", "bigint", None),
        identity_bound=False,
    )
    return proof, state, ledger, chunks, binding, target_calls


def _shadow_config() -> LoadConfig:
    config = LoadConfig(
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
            "postgres_source_authority_sha256": _AUTHORITY,
            "backfill": {
                "backfill_id": "legacy",
                "inner_mode": "incremental_append",
                "chunk": {"column": "id", "from": 1, "to": 20, "step": 10, "kind": "integer"},
                "publication": {"mode": "shadow_swap", "retain_backup": True},
                "state": {"backend": "audit_schema", "schema": "dbo", "require_distributed_lock": True},
            },
        },
    )
    authority = issue_shadow_append_authority(
        config,
        run_key="legacy",
        live_table="events",
        shadow_table="events__shadow",
    )
    config.target_table = authority.shadow_table
    config.options[SHADOW_APPEND_AUTHORITY_OPTION] = authority
    return config


def _source_identity() -> SourcePhysicalIdentity:
    return SourcePhysicalIdentity(
        dialect="postgres",
        cluster_identifier="cluster",
        database="source",
        effective_principal="reader",
        session_principal="reader",
        topology_role="standby",
        version=2,
        authority_sha256=_AUTHORITY,
        timeline_id=1,
        database_oid=10,
        effective_principal_oid=11,
        session_principal_oid=11,
        schema="public",
        schema_oid=12,
        relation="events",
        relation_oid=13,
    )


class _HistoryState:
    def __init__(self) -> None:
        self.history: MssqlAttemptOperationHistory | None = None
        self.requests = []

    def operation_history(self, request):
        self.requests.append(request)
        return self.history
