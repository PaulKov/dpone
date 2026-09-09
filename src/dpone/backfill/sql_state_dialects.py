"""ClickHouse and PostgreSQL adapters for the backfill audit journal."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from dpone.backfill import sql_state_journal_writers as journal_writers
from dpone.backfill.sql_locks import PostgresAdvisoryCampaignLock
from dpone.backfill.sql_state_base import (
    SQLBackfillStateStore,
    SQLStateLockCoordinator,
    clickhouse_identifier,
    postgres_identifier,
    sql_string,
)


class ClickHouseBackfillStateStore(SQLBackfillStateStore):
    """Persist backfill state in a ClickHouse replacing journal."""

    dialect = "clickhouse"
    default_journal_writer = journal_writers.ClickHouseBackfillJournalWriter()

    def _fq(self, table: str) -> str:
        return f"{clickhouse_identifier(self.schema)}.{clickhouse_identifier(table)}"

    def _ensure_schema(self) -> None:
        self.connector.execute_query(f"CREATE DATABASE IF NOT EXISTS {clickhouse_identifier(self.schema)}")

    def _campaign_ddl(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self.campaign_fq_table} (
            journal_id UInt64 DEFAULT generateSnowflakeID(),
            run_key String,
            dataset String,
            inner_mode String,
            status String,
            plan_hash String,
            config_hash String,
            chunk_config_json String,
            details_json String,
            __dpone__loaded_at DateTime64(6, 'UTC') DEFAULT now64(6)
        ) ENGINE = ReplacingMergeTree(journal_id)
        ORDER BY (run_key)
        """

    def _chunk_ddl(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self.chunk_fq_table} (
            journal_id UInt64 DEFAULT generateSnowflakeID(),
            run_key String,
            chunk_index UInt32,
            status String,
            start_value String,
            end_value String,
            idempotency_key String,
            run_id String,
            load_id String,
            error String,
            details_json String,
            __dpone__loaded_at DateTime64(6, 'UTC') DEFAULT now64(6)
        ) ENGINE = ReplacingMergeTree(journal_id)
        ORDER BY (run_key, chunk_index)
        """

    def _select_campaign_sql(self, run_key: str) -> str:
        return (
            f"SELECT journal_id, details_json FROM {self.campaign_fq_table} FINAL "
            f"WHERE run_key = {sql_string(run_key)} ORDER BY journal_id DESC LIMIT 1"
        )

    def _select_chunks_sql(self, run_key: str) -> str:
        return (
            f"SELECT chunk_index, journal_id, details_json FROM {self.chunk_fq_table} FINAL "
            f"WHERE run_key = {sql_string(run_key)} "
            "ORDER BY chunk_index ASC, journal_id DESC"
        )


class PostgresBackfillStateStore(SQLBackfillStateStore):
    """Persist state with PostgreSQL journal rows and advisory locks."""

    dialect = "postgres"
    distributed_lock_scope = "postgres_advisory_campaign"
    default_journal_writer = journal_writers.PostgresBackfillJournalWriter()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._campaign_lock = PostgresAdvisoryCampaignLock(self.connector)
        self._state_coordinator = SQLStateLockCoordinator(
            chunk_lock=PostgresAdvisoryCampaignLock(self.connector),
            initialization_lock=PostgresAdvisoryCampaignLock(self.connector),
        )

    def acquire_campaign_lock(self, run_key: str, *, owner: str, lease_expires_at: datetime) -> bool:
        if not self._campaign_lock.acquire(run_key):
            return False
        try:
            acquired = super().acquire_campaign_lock(run_key, owner=owner, lease_expires_at=lease_expires_at)
        except Exception:
            self._campaign_lock.release(run_key)
            raise
        if not acquired:
            self._campaign_lock.release(run_key)
        return acquired

    def release_campaign_lock(self, run_key: str, *, owner: str) -> None:
        ledger = self.load(run_key)
        should_release = ledger is not None and ledger.lock_owner == owner
        super().release_campaign_lock(run_key, owner=owner)
        if should_release:
            self._campaign_lock.release(run_key)

    def _fq(self, table: str) -> str:
        return f"{postgres_identifier(self.schema)}.{postgres_identifier(table)}"

    def _ensure_schema(self) -> None:
        self.connector.execute_query(f"CREATE SCHEMA IF NOT EXISTS {postgres_identifier(self.schema)}")

    def _campaign_ddl(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self.campaign_fq_table} (
            journal_id bigint GENERATED ALWAYS AS IDENTITY,
            run_key text NOT NULL,
            dataset text NOT NULL,
            inner_mode text NOT NULL,
            status text NOT NULL,
            plan_hash text NOT NULL,
            config_hash text NOT NULL,
            chunk_config_json jsonb NOT NULL,
            details_json jsonb NOT NULL,
            __dpone__loaded_at timestamp NOT NULL DEFAULT timezone('utc', now())
        );
        CREATE INDEX IF NOT EXISTS ix_dpone_backfill_campaign_run_key_journal
            ON {self.campaign_fq_table} (run_key, journal_id DESC)
        """

    def _chunk_ddl(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self.chunk_fq_table} (
            journal_id bigint GENERATED ALWAYS AS IDENTITY,
            run_key text NOT NULL,
            chunk_index integer NOT NULL,
            status text NOT NULL,
            start_value text NOT NULL,
            end_value text NOT NULL,
            idempotency_key text NOT NULL,
            run_id text NOT NULL,
            load_id text NOT NULL,
            error text NOT NULL,
            details_json jsonb NOT NULL,
            __dpone__loaded_at timestamp NOT NULL DEFAULT timezone('utc', now())
        );
        CREATE INDEX IF NOT EXISTS ix_dpone_backfill_chunk_run_key_index_journal
            ON {self.chunk_fq_table} (run_key, chunk_index, journal_id DESC)
        """

    def _select_campaign_sql(self, run_key: str) -> str:
        return (
            f"SELECT journal_id, details_json FROM {self.campaign_fq_table} "
            f"WHERE run_key = {sql_string(run_key)} ORDER BY journal_id DESC LIMIT 1"
        )

    def _select_chunks_sql(self, run_key: str) -> str:
        return (
            f"SELECT DISTINCT ON (chunk_index) chunk_index, journal_id, details_json "
            f"FROM {self.chunk_fq_table} WHERE run_key = {sql_string(run_key)} "
            "ORDER BY chunk_index ASC, journal_id DESC"
        )


__all__ = ["ClickHouseBackfillStateStore", "PostgresBackfillStateStore"]
