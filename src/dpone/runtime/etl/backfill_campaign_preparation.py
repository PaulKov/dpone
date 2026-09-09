"""Fenced campaign preparation for resumable chunked backfills."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dpone.backfill.execution_policy import select_pending_chunks
from dpone.backfill.portable_scope_campaign import (
    CampaignPortableScopeBinding,
    persist_campaign_portable_scope_binding,
    resolve_campaign_portable_scope_binding,
)
from dpone.backfill.worker_runtime import BackfillProcessLaneRuntime, BackfillWorkerChunkRunnerFactory

if TYPE_CHECKING:
    from dpone.backfill.campaign_lease import BackfillCampaignLease
    from dpone.backfill.campaign_lifecycle import BackfillCampaignLifecyclePort
    from dpone.backfill.models import BackfillChunk
    from dpone.backfill.state import BackfillLedger, BackfillStateStore
    from dpone.backfill.target_publication import BackfillTargetPublicationPort
    from dpone.config.load_config import LoadConfig

PortableScopeColumnResolver = Callable[[Any, str], Any]
PortableScopeHistoryProof = Callable[..., None]


@dataclass(frozen=True, slots=True)
class BackfillCampaignPreparation:
    """Immutable output of target, lifecycle and binding preparation."""

    ledger: BackfillLedger
    load_config: LoadConfig
    binding: CampaignPortableScopeBinding | None
    committed_indexes: set[int]
    pending_chunks: list[BackfillChunk]


class BackfillCampaignPreparationService:
    """Prepare one campaign under its lease before any chunk reads rows.

    The service owns the order-sensitive publisher/lifecycle handshake and
    the fail-closed upgrade of legacy portable-scope ledgers.  Dependency
    composition accepts either explicit parent ports or the same ports
    carried by a process-lane runtime, but never two different authorities.
    """

    def __init__(
        self,
        *,
        column_resolver: PortableScopeColumnResolver | None,
        history_proof: PortableScopeHistoryProof | None,
    ) -> None:
        self._column_resolver = column_resolver
        self._history_proof = history_proof

    @classmethod
    def compose(
        cls,
        *,
        worker_runtime: BackfillWorkerChunkRunnerFactory | BackfillProcessLaneRuntime | None,
        column_resolver: PortableScopeColumnResolver | None,
        history_proof: PortableScopeHistoryProof | None,
    ) -> BackfillCampaignPreparationService:
        """Resolve explicit and process-runtime ports into one authority."""

        process_runtime = worker_runtime if isinstance(worker_runtime, BackfillProcessLaneRuntime) else None
        return cls(
            column_resolver=_consistent_port(
                column_resolver,
                process_runtime.portable_scope_column_resolver if process_runtime is not None else None,
                conflict="backfill portable-scope resolvers disagree",
            ),
            history_proof=_consistent_port(
                history_proof,
                process_runtime.portable_scope_history_proof if process_runtime is not None else None,
                conflict="backfill portable-scope history proofs disagree",
            ),
        )

    def resolve_initial_binding(
        self,
        load_config: LoadConfig,
        *,
        column: str,
        existing_ledger: BackfillLedger | None,
    ) -> CampaignPortableScopeBinding | None:
        """Read durable authority, or certify a brand-new campaign once."""

        persisted = existing_ledger.portable_scope_column_contract if existing_ledger is not None else None
        return resolve_campaign_portable_scope_binding(
            load_config,
            column=column,
            persisted=persisted,
            identity_bound=existing_ledger is None,
            # A legacy recertification must happen under the campaign lease.
            resolver=self._column_resolver if existing_ledger is None else None,
        )

    def prepare(
        self,
        load_config: LoadConfig,
        *,
        ledger: BackfillLedger,
        store: BackfillStateStore,
        lease: BackfillCampaignLease,
        chunks: tuple[BackfillChunk, ...],
        column: str,
        retry_policy: str,
        binding: CampaignPortableScopeBinding | None,
        lifecycle: BackfillCampaignLifecyclePort | None,
        publisher: BackfillTargetPublicationPort | None,
    ) -> BackfillCampaignPreparation:
        """Prepare runtime target and durable binding under one campaign lease."""

        chunk_config = load_config
        publisher_prepared = False
        publication = ledger.publication
        # Published legacy rows install their durable SQL head before XMin
        # migrates a legacy NULL seed-receipt binding.
        if publisher is not None and publication is not None and publication.phase == "published":
            chunk_config = lease.run_fenced(lambda: self._prepare_target(publisher, load_config, ledger, store, lease))
            publisher_prepared = True
        if lifecycle is not None:
            ledger = lease.run_fenced(lambda: lifecycle.before_chunks(load_config, ledger, store))
        if publisher is not None and not publisher_prepared:
            chunk_config = lease.run_fenced(lambda: self._prepare_target(publisher, load_config, ledger, store, lease))

        committed = ledger.committed_indexes()
        pending = select_pending_chunks(chunks, ledger=ledger, retry_policy=retry_policy)
        if binding is None and pending:
            binding = resolve_campaign_portable_scope_binding(
                chunk_config,
                column=column,
                persisted=ledger.portable_scope_column_contract,
                identity_bound=False,
                resolver=self._column_resolver,
            )
        ledger = self._prove_and_persist_binding(
            store,
            ledger,
            binding,
            lease=lease,
            load_config=chunk_config,
            chunks=chunks,
        )
        return BackfillCampaignPreparation(
            ledger=ledger,
            load_config=chunk_config,
            binding=binding,
            committed_indexes=committed,
            pending_chunks=pending,
        )

    def resolve_mapped_binding(
        self,
        load_config: LoadConfig,
        *,
        column: str,
        binding: CampaignPortableScopeBinding | None,
    ) -> CampaignPortableScopeBinding | None:
        """Certify a mapped legacy campaign while its initialization lock is held."""

        if binding is not None:
            return binding
        return resolve_campaign_portable_scope_binding(
            load_config,
            column=column,
            persisted=None,
            identity_bound=False,
            resolver=self._column_resolver,
        )

    def persist_mapped_binding(
        self,
        store: BackfillStateStore,
        ledger: BackfillLedger,
        binding: CampaignPortableScopeBinding | None,
        *,
        lease_factory: Callable[[Any, str], BackfillCampaignLease],
        load_config: LoadConfig,
        chunks: tuple[BackfillChunk, ...],
    ) -> BackfillLedger:
        """Prove a mapped legacy upgrade under both initialization and campaign locks."""

        if binding is None:
            return ledger
        if not binding.identity_bound and ledger.portable_scope_column_contract is None:
            lease = lease_factory(store, ledger.run_key)
            lease.acquire()
            try:
                return self._prove_and_persist_binding(
                    store,
                    ledger,
                    binding,
                    lease=lease,
                    load_config=load_config,
                    chunks=chunks,
                )
            finally:
                lease.release()
        return persist_campaign_portable_scope_binding(store, ledger, binding)

    def _prove_and_persist_binding(
        self,
        store: BackfillStateStore,
        ledger: BackfillLedger,
        binding: CampaignPortableScopeBinding | None,
        *,
        lease: BackfillCampaignLease,
        load_config: LoadConfig,
        chunks: tuple[BackfillChunk, ...],
    ) -> BackfillLedger:
        if binding is None:
            return ledger
        legacy_upgrade = not binding.identity_bound and ledger.portable_scope_column_contract is None
        if legacy_upgrade:
            proof = self._history_proof
            if not callable(proof):
                raise RuntimeError("backfill.portable_scope_history.proof_required")
            lease.run_fenced(
                lambda: proof(
                    load_config=load_config,
                    ledger=ledger,
                    chunks=chunks,
                    campaign_binding=binding,
                )
            )
        return lease.run_fenced(lambda: persist_campaign_portable_scope_binding(store, ledger, binding))

    @staticmethod
    def _prepare_target(
        publisher: BackfillTargetPublicationPort,
        load_config: LoadConfig,
        ledger: BackfillLedger,
        store: BackfillStateStore,
        lease: BackfillCampaignLease,
    ) -> LoadConfig:
        fenced_prepare = getattr(publisher, "before_chunks_fenced", None)
        if callable(fenced_prepare):
            return fenced_prepare(load_config, ledger, store, campaign_owner=lease.owner)
        return publisher.before_chunks(load_config, ledger, store)


def _consistent_port(port: Any, process_port: Any, *, conflict: str) -> Any:
    if port is not None and process_port is not None and port is not process_port:
        raise ValueError(conflict)
    return port or process_port


__all__ = ["BackfillCampaignPreparation", "BackfillCampaignPreparationService"]
