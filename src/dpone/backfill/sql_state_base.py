"""Shared SQL-backed backfill state and lease coordination."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.backfill import sql_state_journal_writers as journal_writers
from dpone.backfill.sql_state_coordination import SQLStateLockCoordinator
from dpone.backfill.sql_state_journal import (
    latest_campaign_details,
    load_campaign,
    load_chunks,
    record_snapshot,
    replace_chunk,
)
from dpone.backfill.sql_state_mssql_migration import campaign_projection_from_payload
from dpone.backfill.sql_state_rendering import clickhouse_identifier, postgres_identifier, sql_string
from dpone.backfill.sql_state_schema import journal_shape_query, validate_journal_shape
from dpone.backfill.state import (
    CHUNK_STATUS_FAILED,
    CHUNK_STATUS_RUNNING,
    CHUNK_STATUS_SUCCESS,
    BackfillChunkRecord,
    BackfillLedger,
    BackfillPublicationRecord,
    FileBackfillStateStore,
)


class CoordinatedLeaseMixin:
    """Route lease transitions through the distributed SQL lock coordinator."""

    _state_coordinator: SQLStateLockCoordinator | None

    def acquire_chunk_lease(self, run_key: str, index: int, *, owner: str, lease_expires_at: datetime) -> bool:
        delegate = super().acquire_chunk_lease  # type: ignore[misc]
        if self._state_coordinator is None:
            return delegate(run_key, index, owner=owner, lease_expires_at=lease_expires_at)
        return bool(
            self._state_coordinator.run_chunk_transition(
                run_key,
                index,
                lambda: delegate(run_key, index, owner=owner, lease_expires_at=lease_expires_at),
            )
        )

    def complete_chunk_if_owned(self, run_key: str, record: BackfillChunkRecord, *, owner: str) -> bool:
        delegate = super().complete_chunk_if_owned  # type: ignore[misc]
        if self._state_coordinator is None:
            return delegate(run_key, record, owner=owner)
        return bool(
            self._state_coordinator.run_chunk_transition(
                run_key,
                record.index,
                lambda: delegate(run_key, record, owner=owner),
            )
        )

    def renew_chunk_lease(self, run_key: str, index: int, *, owner: str, lease_expires_at: datetime) -> bool:
        delegate = super().renew_chunk_lease  # type: ignore[misc]
        if self._state_coordinator is None:
            return delegate(run_key, index, owner=owner, lease_expires_at=lease_expires_at)
        return bool(
            self._state_coordinator.run_chunk_transition(
                run_key,
                index,
                lambda: delegate(run_key, index, owner=owner, lease_expires_at=lease_expires_at),
            )
        )

    def acquire_initialization_lock(self, run_key: str, *, owner: str) -> bool:
        delegate = super().acquire_initialization_lock  # type: ignore[misc]
        if self._state_coordinator is None:
            return delegate(run_key, owner=owner)
        return self._state_coordinator.acquire_initialization(run_key, owner=owner)

    def release_initialization_lock(self, run_key: str, *, owner: str) -> None:
        delegate = super().release_initialization_lock  # type: ignore[misc]
        if self._state_coordinator is None:
            delegate(run_key, owner=owner)
            return
        self._state_coordinator.release_initialization(run_key, owner=owner)


class SQLTransactionalPublicationMixin:
    """Persist and verify a campaign receipt inside a caller transaction."""

    if TYPE_CHECKING:
        dialect: str
        _journal_writer: journal_writers.BackfillJournalWriter

        @property
        def campaign_fq_table(self) -> str: ...

        @property
        def chunk_fq_table(self) -> str: ...

        def _ensure_tables(self) -> None: ...

        def _select_chunks_sql(self, run_key: str) -> str: ...

    def persist_campaign_in_transaction(
        self,
        ledger: BackfillLedger,
        *,
        connector: Any,
        campaign_owner: str | None = None,
    ) -> BackfillLedger:
        """Append one campaign receipt on the caller-owned target transaction."""

        if connector is None:
            raise ValueError("transactional campaign persistence requires a connector")
        self._ensure_tables()
        candidate = (
            self._publication_candidate_if_owned(ledger, connector=connector, campaign_owner=campaign_owner)
            if campaign_owner is not None
            else ledger
        )
        record_snapshot(
            connector,
            journal_writers.campaign_insert_prefix(self.campaign_fq_table),
            journal_writers.chunk_insert_prefix(self.chunk_fq_table),
            candidate,
            (),
            writer=self._journal_writer,
        )
        return candidate

    def _publication_candidate_if_owned(
        self,
        ledger: BackfillLedger,
        *,
        connector: Any,
        campaign_owner: str,
    ) -> BackfillLedger:
        if self.dialect != "mssql" or not campaign_owner:
            raise RuntimeError("mssql_backfill_publication.campaign_fence_required")
        current = self._load_mssql_campaign_for_update(ledger.run_key, connector=connector)
        if current is None:
            raise RuntimeError("mssql_backfill_publication.campaign_state_unavailable")
        if current.lock_owner != campaign_owner or current.status == "cancel_requested":
            raise RuntimeError("mssql_backfill_publication.campaign_fence_lost")
        if self._campaign_identity(current) != self._campaign_identity(ledger):
            raise RuntimeError("mssql_backfill_publication.campaign_identity_mismatch")
        current.publication = deepcopy(ledger.publication)
        return current

    def _load_mssql_campaign_for_update(
        self,
        run_key: str,
        *,
        connector: Any,
    ) -> BackfillLedger | None:
        """Read one causal latest campaign row under the caller transaction."""

        if self.dialect != "mssql":
            raise RuntimeError("mssql backfill campaign mutation requires the MSSQL state dialect")
        rows = connector.get_records(
            f"SELECT TOP (1) journal_id, details_json FROM {self.campaign_fq_table} "
            "WITH (UPDLOCK, HOLDLOCK) WHERE run_key = ? ORDER BY journal_id DESC",
            (run_key,),
            as_dict=True,
        )
        details = latest_campaign_details(rows)
        if details is None:
            return None
        payload = details if isinstance(details, dict) else json.loads(str(details))
        return campaign_projection_from_payload(payload)

    def _load_chunks_with_connector(
        self,
        run_key: str,
        *,
        connector: Any,
    ) -> tuple[BackfillChunkRecord, ...]:
        return load_chunks(connector, self._select_chunks_sql(run_key), BackfillChunkRecord.from_dict)

    @staticmethod
    def _campaign_identity(ledger: BackfillLedger) -> tuple[Any, ...]:
        return (
            ledger.run_key,
            ledger.dataset,
            ledger.inner_mode,
            ledger.plan_hash,
            ledger.config_hash,
            ledger.chunk_config,
        )


class SQLBackfillStateStore(CoordinatedLeaseMixin, SQLTransactionalPublicationMixin, FileBackfillStateStore):
    """File-compatible state store that mirrors snapshots to SQL audit tables."""

    dialect = "generic"
    distributed_lock_scope: str | None = None
    default_journal_writer: journal_writers.BackfillJournalWriter | None = None

    def __init__(
        self,
        connector: Any,
        *,
        schema: str = "DWH_Tech",
        campaigns_table: str = "__dpone__backfill_campaigns",
        chunks_table: str = "__dpone__backfill_chunks",
        cache_dir: str | Path | None = None,
        journal_writer: journal_writers.BackfillJournalWriter | None = None,
    ) -> None:
        super().__init__(cache_dir)
        self.connector = connector
        self.schema = schema
        self.campaigns_table = campaigns_table
        self.chunks_table = chunks_table
        resolved_writer = journal_writer or self.default_journal_writer
        if resolved_writer is None:
            raise ValueError("SQL backfill state store requires an explicit journal writer")
        self._journal_writer: journal_writers.BackfillJournalWriter = resolved_writer
        self._table_created = False
        self._state_coordinator: SQLStateLockCoordinator | None = None

    def save(self, ledger: BackfillLedger) -> Path:
        path = super().save(ledger)
        self._ensure_tables()
        self._record_snapshot(ledger, ledger.chunks)
        return path

    def load(self, run_key: str) -> BackfillLedger | None:
        campaign = self._load_campaign_from_sql(run_key)
        if campaign is None:
            return super().load(run_key)
        self._hydrate_campaign_chunks(campaign, self._load_chunks_from_sql(run_key))
        return campaign

    @staticmethod
    def _hydrate_campaign_chunks(
        campaign: BackfillLedger,
        records: tuple[BackfillChunkRecord, ...],
    ) -> None:
        """Join split journal projections while accepting legacy full rows."""

        if not campaign.chunks:
            campaign.chunks = list(records)
            return
        for record in records:
            expected = campaign.chunk(record.index)
            if (expected.start, expected.end, expected.idempotency_key) != (
                record.start,
                record.end,
                record.idempotency_key,
            ):
                raise ValueError(f"backfill SQL chunk {record.index} does not match the immutable campaign shape")
            replace_chunk(campaign, record)

    def update_chunk(self, ledger: BackfillLedger, record: BackfillChunkRecord) -> None:
        record.updated_at = datetime.now(timezone.utc).isoformat()  # noqa: UP017
        current = self.load(ledger.run_key) or ledger
        replace_chunk(current, record)
        FileBackfillStateStore.save(self, current)
        self._ensure_tables()
        self._record_snapshot(current, (record,))

    def sync_local_cache(self, ledger: BackfillLedger) -> Path:
        """Mirror an already-committed SQL campaign without appending SQL."""

        return FileBackfillStateStore.sync_committed_snapshot(self, ledger)

    @property
    def campaign_fq_table(self) -> str:
        return self._fq(self.campaigns_table)

    @property
    def chunk_fq_table(self) -> str:
        return self._fq(self.chunks_table)

    def state_capabilities(self) -> dict[str, Any]:
        distributed = self.distributed_lock_scope is not None
        return {
            "backend": "audit_schema",
            "dialect": self.dialect,
            "durable_read": True,
            "durable_write": True,
            "distributed_lock": distributed,
            "distributed_chunk_lease": distributed,
            "compare_and_set_completion": distributed,
            "lock_scope": self.distributed_lock_scope or "local_cache",
        }

    def _ensure_tables(self) -> None:
        if self._table_created:
            return
        self._ensure_schema()
        self.connector.execute_query(self._campaign_ddl())
        self.connector.execute_query(self._chunk_ddl())
        self._validate_table_shape(self.campaigns_table)
        self._validate_table_shape(self.chunks_table)
        self._table_created = True

    def _validate_table_shape(self, table: str) -> None:
        get_records = getattr(self.connector, "get_records", None)
        if not callable(get_records):
            raise ValueError("SQL backfill state connector must expose catalog-backed get_records")
        rows = get_records(
            journal_shape_query(dialect=self.dialect, schema=self.schema, table=table),
            as_dict=True,
        )
        validate_journal_shape(rows, dialect=self.dialect, schema=self.schema, table=table)

    def _record_snapshot(self, ledger: BackfillLedger, chunks: tuple[BackfillChunkRecord, ...] | list[Any]) -> None:
        self._record_snapshot_with_connector(self.connector, ledger, tuple(chunks))

    def _record_snapshot_with_connector(
        self,
        connector: Any,
        ledger: BackfillLedger,
        chunks: tuple[BackfillChunkRecord, ...],
    ) -> None:
        record_snapshot(
            connector,
            journal_writers.campaign_insert_prefix(self.campaign_fq_table),
            journal_writers.chunk_insert_prefix(self.chunk_fq_table),
            ledger,
            chunks,
            writer=self._journal_writer,
        )

    def _fq(self, table: str) -> str:
        raise NotImplementedError

    def _ensure_schema(self) -> None:
        raise NotImplementedError

    def _campaign_ddl(self) -> str:
        raise NotImplementedError

    def _chunk_ddl(self) -> str:
        raise NotImplementedError

    def _select_campaign_sql(self, run_key: str) -> str:
        return (
            f"SELECT journal_id, details_json FROM {self.campaign_fq_table} "
            f"WHERE run_key = {sql_string(run_key)} ORDER BY journal_id DESC"
        )

    def _select_chunks_sql(self, run_key: str) -> str:
        return (
            f"SELECT chunk_index, journal_id, details_json FROM {self.chunk_fq_table} "
            f"WHERE run_key = {sql_string(run_key)} "
            "ORDER BY chunk_index ASC, journal_id DESC"
        )

    def _load_campaign_from_sql(self, run_key: str) -> BackfillLedger | None:
        self._ensure_tables()
        return load_campaign(self.connector, self._select_campaign_sql(run_key), BackfillLedger.from_dict)

    def _load_chunks_from_sql(self, run_key: str) -> tuple[BackfillChunkRecord, ...]:
        return load_chunks(self.connector, self._select_chunks_sql(run_key), BackfillChunkRecord.from_dict)


__all__ = [
    "CHUNK_STATUS_FAILED",
    "CHUNK_STATUS_RUNNING",
    "CHUNK_STATUS_SUCCESS",
    "BackfillChunkRecord",
    "BackfillLedger",
    "BackfillPublicationRecord",
    "SQLBackfillStateStore",
    "SQLStateLockCoordinator",
    "clickhouse_identifier",
    "postgres_identifier",
    "sql_string",
]
