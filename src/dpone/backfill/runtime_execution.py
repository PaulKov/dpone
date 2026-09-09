"""Lease-safe execution support shared by internal and mapped backfills."""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from dataclasses import replace
from datetime import datetime
from typing import TYPE_CHECKING

from dpone.backfill.chunk_lifecycle import BackfillChunkLifecyclePort, DefaultBackfillChunkLifecycle
from dpone.backfill.execution_policy import (
    BACKFILL_RUNTIME_AUTHORITY_OPTION,
    combined_predicate,
    execution_policy_from_load_config,
    issue_backfill_runtime_authority,
    render_chunk_predicate,
)
from dpone.backfill.lease_heartbeat import BackfillLeaseHeartbeat
from dpone.backfill.portable_scope_campaign import (
    CampaignPortableScopeBinding,
    bind_campaign_portable_scope,
)
from dpone.backfill.portable_scope_runtime import is_postgres_mssql_backfill_route
from dpone.backfill.runtime_chunk_execution import BackfillChunkExecutionMixin, ChunkRunner
from dpone.backfill.runtime_results import (
    build_backfill_result,
    load_or_create_backfill_ledger,
)
from dpone.backfill.worker_runtime import (
    BackfillProcessLaneRuntime,
    BackfillWorkerChunkRunnerFactory,
)
from dpone.contracts.portable_relation_scope import portable_scope_contract

if TYPE_CHECKING:
    from dpone.backfill.models import BackfillChunk
    from dpone.backfill.worker_state import BackfillWorkerStateStoreFactory
    from dpone.config.load_config import LoadConfig


class BackfillChunkExecutor(BackfillChunkExecutionMixin):
    """Execute chunks while enforcing lease-owner compare-and-set completion."""

    def __init__(
        self,
        chunk_runner: ChunkRunner,
        *,
        heartbeat_factory: Callable[..., BackfillLeaseHeartbeat] | None = None,
        lifecycle_port: BackfillChunkLifecyclePort | None = None,
        worker_state_store_factory: BackfillWorkerStateStoreFactory | None = None,
        worker_chunk_runner_factory: BackfillWorkerChunkRunnerFactory | BackfillProcessLaneRuntime | None = None,
    ) -> None:
        self._chunk_runner = chunk_runner
        self._heartbeat_factory = heartbeat_factory or BackfillLeaseHeartbeat
        self._lifecycle = lifecycle_port or DefaultBackfillChunkLifecycle()
        self._worker_state_store_factory = worker_state_store_factory
        self._worker_chunk_runner_factory = worker_chunk_runner_factory

    @staticmethod
    def build_chunk_load_config(
        load_config: LoadConfig,
        chunk: BackfillChunk,
        *,
        run_key: str,
        plan_hash: str | None,
        owner: str,
        lease_expires_at_utc: datetime,
        portable_scope_campaign: CampaignPortableScopeBinding | None = None,
    ) -> LoadConfig:
        options = deepcopy(load_config.options) if load_config.options is not None else {}
        backfill_options = dict(options.get("backfill") or {})
        execution_policy = execution_policy_from_load_config(load_config)
        cross_dialect = is_postgres_mssql_backfill_route(load_config)
        global_source_predicate = options.get("source_custom_predicate")
        if cross_dialect:
            options.pop("source_custom_predicate", None)
            source_predicate = None
            target_predicate = None
        else:
            chunk_predicate = render_chunk_predicate(backfill_options, chunk)
            target_predicate = combined_predicate(load_config.custom_predicate, chunk_predicate)
            source_predicate = combined_predicate(global_source_predicate, chunk_predicate)
            options["source_custom_predicate"] = source_predicate
        scope_contract = portable_scope_contract(chunk.portable_scope)
        chunk_context = {
            "schema": "dpone.backfill.chunk-context.v1",
            "run_key": run_key,
            "plan_hash": plan_hash,
            "index": chunk.index,
            "idempotency_key": chunk.idempotency_key,
            "start": chunk.start,
            "end": chunk.end,
            "portable_scope": scope_contract,
            "portable_scope_sha256": chunk.portable_scope_sha256,
            "execution_policy_sha256": execution_policy.digest,
        }
        backfill_options["chunk_context"] = chunk_context
        options["backfill"] = backfill_options
        operation_scope = {
            "kind": "backfill_disjoint_range_v1",
            "run_key": run_key,
            "plan_hash": plan_hash,
            "chunk_index": chunk.index,
            "chunk_idempotency_key": chunk.idempotency_key,
            "start": chunk.start,
            "end": chunk.end,
            "portable_scope": scope_contract,
            "portable_scope_sha256": chunk.portable_scope_sha256,
            "global_source_predicate": global_source_predicate,
            "global_target_predicate": load_config.custom_predicate,
            "proven_disjoint": True,
            "lease_owner": owner,
            "lease_expires_at_utc": lease_expires_at_utc.isoformat(),
            "execution_policy_sha256": execution_policy.digest,
        }
        options["__dpone_mssql_operation_scope"] = operation_scope
        options[BACKFILL_RUNTIME_AUTHORITY_OPTION] = issue_backfill_runtime_authority(
            policy=execution_policy,
            chunk_context=chunk_context,
            operation_scope=operation_scope,
        )
        prepared = replace(
            load_config,
            custom_predicate=target_predicate,
            portable_scope=chunk.portable_scope if cross_dialect else None,
            options=options,
        )
        if cross_dialect and portable_scope_campaign is not None:
            return bind_campaign_portable_scope(prepared, portable_scope_campaign)
        return prepared

    # Kept for internal callers and integrations released before the public
    # migration-proof builder acquired a stable name.
    _chunk_load_config = build_chunk_load_config


__all__ = ["BackfillChunkExecutor", "ChunkRunner", "build_backfill_result", "load_or_create_backfill_ledger"]
