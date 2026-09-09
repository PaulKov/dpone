"""SQL Server adapter for resumable append-only backfill state."""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.backfill import sql_state_journal_writers as journal_writers
from dpone.backfill import sql_state_mssql_sql as mssql_sql
from dpone.backfill.sql_locks import MSSQLApplicationCampaignLock
from dpone.backfill.sql_state_base import (
    BackfillChunkRecord,
    BackfillLedger,
    SQLBackfillStateStore,
    SQLStateLockCoordinator,
)
from dpone.backfill.sql_state_journal import load_chunks, replace_chunk
from dpone.backfill.sql_state_mssql_campaign import MssqlBackfillCampaignFenceMixin
from dpone.backfill.sql_state_mssql_control import MssqlBackfillProcessLaneControlMixin
from dpone.backfill.sql_state_mssql_migration import has_inline_chunks, overlay_loaded_chunks
from dpone.backfill.sql_state_mssql_mutations import (
    MSSQLCommittedMutation,
    MSSQLLedgerMutation,
    MSSQLLedgerMutationCoordinator,
    MSSQLLedgerTransitionPolicy,
    aware_utc,
    future_utc_iso,
)


class MSSQLJournalDialectMixin:
    """Provide SQL Server-specific journal qualification, DDL and reads."""

    if TYPE_CHECKING:
        connector: Any
        schema: str
        campaigns_table: str
        chunks_table: str

        @property
        def campaign_fq_table(self) -> str: ...

        @property
        def chunk_fq_table(self) -> str: ...

    def _fq(self, table: str) -> str:
        qualified = getattr(self.connector, "qualified_name", None)
        if callable(qualified):
            return str(qualified(self.schema, table))
        return f"[{self.schema}].[{table}]"

    def _ensure_schema(self) -> None:
        quote = getattr(self.connector, "quote_identifier", lambda value: f"[{value}]")
        self.connector.execute_query(
            f"IF SCHEMA_ID(?) IS NULL EXEC('CREATE SCHEMA {quote(self.schema)}')",
            (self.schema,),
        )

    def _campaign_ddl(self) -> str:
        return mssql_sql.campaign_ddl(
            schema=self.schema,
            table=self.campaigns_table,
            qualified_table=self.campaign_fq_table,
        )

    def _chunk_ddl(self) -> str:
        return mssql_sql.chunk_ddl(
            schema=self.schema,
            table=self.chunks_table,
            qualified_table=self.chunk_fq_table,
        )

    def _select_campaign_sql(self, run_key: str) -> str:
        return mssql_sql.select_campaign(self.campaign_fq_table, run_key)

    def _select_chunks_sql(self, run_key: str) -> str:
        return mssql_sql.select_chunks(self.chunk_fq_table, run_key)


class MSSQLBackfillStateStore(
    MssqlBackfillProcessLaneControlMixin,
    MssqlBackfillCampaignFenceMixin,
    MSSQLJournalDialectMixin,
    SQLBackfillStateStore,
):
    """Persist state with session fencing and SERIALIZABLE latest-row CAS."""

    dialect = "mssql"
    distributed_lock_scope = "mssql_application_campaign"
    default_journal_writer = journal_writers.MssqlBackfillJournalWriter()

    def __init__(
        self,
        *args: Any,
        campaign_lock_connector: Any | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._initialize_campaign_fence(campaign_lock_connector)
        self._state_coordinator = SQLStateLockCoordinator(
            chunk_lock=MSSQLApplicationCampaignLock(self.connector),
            initialization_lock=MSSQLApplicationCampaignLock(self.connector),
        )
        self._transitions = MSSQLLedgerTransitionPolicy()
        self._mutations = MSSQLLedgerMutationCoordinator(
            self.connector,
            ensure_tables=self._ensure_tables,
            load_for_update=self._load_campaign_for_mutation,
            load_chunks=self._load_chunk_revisions,
            append_snapshot=self._record_snapshot_with_connector,
        )

    def save(self, ledger: BackfillLedger) -> Path:
        """Append one merged campaign revision without rewriting chunk rows."""

        committed = self._mutations.run(
            ledger.run_key,
            lambda current: self._transitions.save(current, incoming=ledger),
        )
        self._synchronize_caller(ledger, committed)
        return self.sync_local_cache(ledger)

    def update_chunk(self, ledger: BackfillLedger, record: BackfillChunkRecord) -> None:
        """Append only one changed chunk under the latest campaign row lock."""

        committed = self._mutations.run(
            ledger.run_key,
            lambda current: self._transitions.update_chunk(
                self._with_chunk_revisions(current, (record.index,)),
                incoming=ledger,
                record=record,
            ),
        )
        self._synchronize_caller(ledger, committed)

    def acquire_chunk_lease(
        self,
        run_key: str,
        index: int,
        *,
        owner: str,
        lease_expires_at: datetime,
    ) -> bool:
        expiry = future_utc_iso(lease_expires_at)
        return self._run_chunk_transition(
            run_key,
            index,
            lambda current: self._transitions.acquire_chunk(
                current,
                index=index,
                owner=owner,
                expiry=expiry,
            ),
        )

    def renew_chunk_lease(
        self,
        run_key: str,
        index: int,
        *,
        owner: str,
        lease_expires_at: datetime,
    ) -> bool:
        expiry = future_utc_iso(lease_expires_at)
        return self._run_chunk_transition(
            run_key,
            index,
            lambda current: self._transitions.renew_chunk(
                current,
                index=index,
                owner=owner,
                expiry=expiry,
            ),
        )

    def complete_chunk_if_owned(
        self,
        run_key: str,
        record: BackfillChunkRecord,
        *,
        owner: str,
    ) -> bool:
        return self._run_chunk_transition(
            run_key,
            record.index,
            lambda current: self._transitions.complete_chunk(
                current,
                record=record,
                owner=owner,
            ),
        )

    def recover_stale_running(self, run_key: str, *, now: datetime) -> list[int]:
        committed = self._mutations.run(
            run_key,
            lambda current: self._transitions.recover_chunks(
                self._with_running_chunks(current),
                now=aware_utc(now),
            ),
        )
        return list(committed.result)

    def request_cancel(self, run_key: str, *, reason: str, requested_by: str) -> None:
        if not reason.strip() or not requested_by.strip():
            raise ValueError("backfill cancellation requires non-empty reason and requested_by")
        self._mutations.run(
            run_key,
            lambda current: self._transitions.cancel(
                current,
                reason=reason,
                requested_by=requested_by,
            ),
        )

    def _run_chunk_transition(
        self,
        run_key: str,
        index: int,
        transition: Callable[[BackfillLedger | None], MSSQLLedgerMutation[bool]],
    ) -> bool:
        assert self._state_coordinator is not None
        outcome = self._state_coordinator.run_chunk_transition(
            run_key,
            index,
            lambda: self._mutations.run(
                run_key,
                lambda current: transition(self._with_chunk_revisions(current, (index,))),
            ),
        )
        if outcome is False:
            return False
        assert isinstance(outcome, MSSQLCommittedMutation)
        return bool(outcome.result)

    @staticmethod
    def _synchronize_caller(ledger: BackfillLedger, committed: MSSQLCommittedMutation[Any]) -> None:
        if committed.ledger is None:
            return
        candidate = deepcopy(committed.ledger)
        if ledger.chunks and len(candidate.chunks) < len(ledger.chunks):
            changed = candidate.chunks
            candidate.chunks = deepcopy(ledger.chunks)
            for record in changed:
                replace_chunk(candidate, record)
        ledger.__dict__.clear()
        ledger.__dict__.update(candidate.__dict__)

    def _with_chunk_revisions(
        self,
        campaign: BackfillLedger | None,
        indexes: tuple[int, ...],
    ) -> BackfillLedger | None:
        if campaign is not None:
            overlay_loaded_chunks(
                campaign,
                self._load_chunk_revisions(campaign.run_key, self.connector, indexes),
            )
        return campaign

    def _load_campaign_for_mutation(
        self,
        run_key: str,
        connector: Any,
    ) -> BackfillLedger | None:
        """Hydrate a legacy inline authority once before compacting it."""

        campaign = self._load_mssql_campaign_for_update(run_key, connector=connector)
        if campaign is not None and has_inline_chunks(campaign):
            overlay_loaded_chunks(
                campaign,
                self._load_chunks_with_connector(run_key, connector=connector),
            )
        return campaign

    def _with_running_chunks(self, campaign: BackfillLedger | None) -> BackfillLedger | None:
        if campaign is not None:
            overlay_loaded_chunks(
                campaign,
                self._load_running_chunks(campaign.run_key, connector=self.connector),
            )
        return campaign

    def _load_chunk_revisions(
        self,
        run_key: str,
        connector: Any,
        indexes: tuple[int, ...],
    ) -> tuple[BackfillChunkRecord, ...]:
        if not indexes:
            return ()
        return load_chunks(
            connector,
            mssql_sql.select_chunk_revisions(self.chunk_fq_table, indexes),
            BackfillChunkRecord.from_dict,
            params=(run_key, *indexes),
        )

    def _load_running_chunks(
        self,
        run_key: str,
        *,
        connector: Any,
    ) -> tuple[BackfillChunkRecord, ...]:
        return load_chunks(
            connector,
            mssql_sql.select_running_chunks(self.chunk_fq_table),
            BackfillChunkRecord.from_dict,
            params=(run_key,),
        )


__all__ = ["MSSQLBackfillStateStore"]
