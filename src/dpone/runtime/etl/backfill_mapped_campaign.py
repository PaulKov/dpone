"""Mapped-campaign initialization under durable ownership locks."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from dpone.backfill.mapping import AirflowBackfillMappingViolation
from dpone.backfill.runtime_execution import BackfillChunkExecutor

if TYPE_CHECKING:
    from dpone.backfill import BackfillChunk, BackfillLedger, FileBackfillStateStore
    from dpone.backfill.campaign_lease import BackfillCampaignLease
    from dpone.config.load_config import LoadConfig
    from dpone.runtime.etl.backfill_campaign_preparation import (
        BackfillCampaignPreparationService,
    )


class MappedCampaignExecutor(BackfillChunkExecutor):
    """Add durable scope preparation without duplicating mapped execution."""

    def __init__(
        self,
        *,
        executor: BackfillChunkExecutor,
        preparation: BackfillCampaignPreparationService,
        binding: Any,
        lease_factory: Callable[[Any, str], BackfillCampaignLease],
        column: str,
        chunks: tuple[BackfillChunk, ...],
    ) -> None:
        self._executor = executor
        self._preparation = preparation
        self._binding = binding
        self._lease_factory = lease_factory
        self._column = column
        self._chunks = chunks

    def run(
        self,
        pending: list[BackfillChunk],
        load_config: LoadConfig,
        ledger: BackfillLedger,
        store: FileBackfillStateStore,
        *,
        workers: int = 1,
        coordinator_tick: Callable[[], None] | None = None,
        coordinator_reprove: Callable[[], None] | None = None,
    ) -> list[str]:
        """Persist a legacy binding under mapped initialization ownership."""

        owner = f"dpone-backfill-mapping-binding:{uuid4().hex}"
        if not store.acquire_initialization_lock(ledger.run_key, owner=owner):
            raise AirflowBackfillMappingViolation(
                "DPONE_AIRFLOW_MAPPING_CAMPAIGN_BUSY",
                f"mapped backfill campaign initialization is busy for {ledger.run_key}",
            )
        try:
            current = store.load(ledger.run_key) or ledger
            binding = self._preparation.resolve_mapped_binding(
                load_config,
                column=self._column,
                binding=self._binding,
            )
            current = self._preparation.persist_mapped_binding(
                store,
                current,
                binding,
                lease_factory=self._lease_factory,
                load_config=load_config,
                chunks=self._chunks,
            )
        finally:
            store.release_initialization_lock(ledger.run_key, owner=owner)
        return self._executor.run(
            pending,
            load_config,
            current,
            store,
            workers=workers,
            coordinator_tick=coordinator_tick,
            coordinator_reprove=coordinator_reprove,
        )


__all__ = ["MappedCampaignExecutor"]
