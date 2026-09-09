"""Lifecycle operations for MSSQL shadow-publication table objects."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dpone.runtime.sinks.mssql_backfill_publication_catalog import (
        MssqlPublicationTarget,
    )


class MssqlBackfillPublicationObjects:
    """Create, validate, and finish the physical shadow-table lifecycle."""

    def __init__(
        self,
        connector: Any,
        *,
        shadow_owner_property: str,
        read_supported_indexes: Callable[..., Sequence[Any]],
        require_matching_columns: Callable[..., None],
        require_shadow_owner: Callable[..., None],
        acquire_publication_lock: Callable[..., None],
        ensure_publication_generation_token: Callable[..., Any],
    ) -> None:
        self._connector = connector
        self._shadow_owner_property = shadow_owner_property
        self._read_supported_indexes = read_supported_indexes
        self._require_matching_columns = require_matching_columns
        self._require_shadow_owner = require_shadow_owner
        self._acquire_publication_lock = acquire_publication_lock
        self._ensure_publication_generation_token = ensure_publication_generation_token

    def create_shadow(
        self,
        live: MssqlPublicationTarget,
        shadow: MssqlPublicationTarget,
        *,
        run_key: str,
    ) -> None:
        """Create the owned empty shadow and its eager columnstore indexes."""

        self.require_table(live)
        indexes = self._read_supported_indexes(self._connector, live)
        self._connector.begin()
        try:
            self._connector.execute_query("SET XACT_ABORT ON")
            self._acquire_publication_lock(self._connector, live, phase="prepare")
            live_generation = self._ensure_publication_generation_token(self._connector, live)
            if live_generation.xmin_handoff_pending:
                raise RuntimeError("mssql_backfill_publication.predecessor_handoff_pending")
            if self.table_exists(shadow):
                raise RuntimeError("mssql_backfill_publication.concurrent_shadow_creation")
            self._connector.execute_query(f"SELECT TOP (0) * INTO {shadow.quoted()} FROM {live.quoted()}")
            self._connector.execute_query(
                f"EXEC {shadow.execution_scope_prefix}sys.sp_addextendedproperty "
                "@name=?, @value=?, @level0type=N'SCHEMA', @level0name=?, "
                "@level1type=N'TABLE', @level1name=?",
                (self._shadow_owner_property, run_key, shadow.schema, shadow.table),
            )
            self._ensure_publication_generation_token(self._connector, shadow)
            for index in indexes:
                if index.is_columnstore:
                    self._connector.execute_query(index.create_sql(shadow))
            self._require_matching_columns(self._connector, live, shadow)
            self._connector.commit_transaction()
        except BaseException:
            self._connector.rollback()
            raise

    def build_deferred_indexes(
        self,
        live: MssqlPublicationTarget,
        shadow: MssqlPublicationTarget,
    ) -> None:
        """Build supported non-columnstore indexes after the data load."""

        expected = self._read_supported_indexes(self._connector, live)
        actual = self._read_supported_indexes(self._connector, shadow)
        actual_names = {index.name for index in actual}
        missing = [index for index in expected if index.name not in actual_names]
        if not missing:
            return
        if any(index.is_columnstore for index in missing):
            raise RuntimeError("mssql_backfill_publication.clustered_columnstore_missing_after_load")
        self._connector.begin()
        try:
            self._connector.execute_query("SET XACT_ABORT ON")
            self._acquire_publication_lock(self._connector, live, phase="physical-design")
            for index in missing:
                self._connector.execute_query(index.create_sql(shadow))
            self._connector.commit_transaction()
        except BaseException:
            self._connector.rollback()
            raise

    def require_shadow_owner(self, shadow: MssqlPublicationTarget, run_key: str) -> None:
        """Require the campaign owner marker on an adopted shadow."""

        self._require_shadow_owner(self._connector, shadow, run_key=run_key)

    def require_table(self, target: MssqlPublicationTarget) -> None:
        """Reject a missing publication table with its exact identity."""

        if not self.table_exists(target):
            raise RuntimeError(f"mssql_backfill_publication.table_missing:{target.dataset}")

    def table_exists(self, target: MssqlPublicationTarget) -> bool:
        """Return whether the exact database-qualified table exists."""

        return bool(
            self._connector.table_exists(
                target.schema,
                target.table,
                database=target.database,
            )
        )


__all__ = ["MssqlBackfillPublicationObjects"]
