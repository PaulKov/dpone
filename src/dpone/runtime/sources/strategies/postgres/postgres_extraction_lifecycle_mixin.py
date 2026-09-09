"""PostgreSQL source snapshot and lazy-stream lifecycle operations."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from dpone.runtime.extraction_lifecycle import ExtractionClock, ExtractionLifecycleAuthority
from dpone.runtime.incremental_snapshot import snapshot_token_digest
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_failure import (
    rollback_preserving_primary,
)
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_lease import (
    PostgresRepeatableReadSnapshotLease,
    issue_repeatable_read_snapshot_lease,
)
from dpone.runtime.streaming_rows import StreamingRowsArtifact


class PostgresRepeatableReadSnapshotCompletion:
    """Close one caller-owned PostgreSQL snapshot exactly after source I/O."""

    def __init__(self, connector: Any, lifecycle: ExtractionLifecycleAuthority) -> None:
        self._connector = connector
        self._lifecycle = lifecycle
        self._active = True

    @property
    def active(self) -> bool:
        """Whether the read-only transaction still needs a terminal action."""

        return self._active

    def complete(self) -> None:
        """Publish extraction completion and commit the exact issuing session."""

        if not self._active:
            return
        self._lifecycle.complete()
        self._connector.commit_transaction()
        self._active = False


class PostgresExtractionLifecycleMixin:
    """Keep PostgreSQL transaction timing separate from schema/query concerns."""

    if TYPE_CHECKING:
        connector: Any
        _extraction_clock: ExtractionClock | None

        def _iter_guarded_dict_rows(
            self,
            connector: Any,
            query: Any,
            *,
            params: Sequence[Any] | None = None,
            batch_size: int,
            expected_columns: Sequence[str] = (),
        ) -> Iterator[Mapping[str, Any]]: ...

    def _new_extraction_lifecycle(self) -> ExtractionLifecycleAuthority:
        return ExtractionLifecycleAuthority(clock=self._extraction_clock)

    def _repeatable_read_snapshot_completion(
        self,
        lifecycle: ExtractionLifecycleAuthority,
    ) -> PostgresRepeatableReadSnapshotCompletion:
        """Return the single completion authority for an active RR transaction."""

        return PostgresRepeatableReadSnapshotCompletion(self.connector, lifecycle)

    def _open_repeatable_read_stream(
        self,
        query: Any,
        *,
        params: Any = None,
        batch_size: int,
        expected_columns: tuple[str, ...] = (),
    ) -> StreamingRowsArtifact:
        """Open a lazy row stream only after a stable PostgreSQL snapshot exists."""

        lifecycle = self._new_extraction_lifecycle()
        self._begin_repeatable_read_snapshot(lifecycle)
        try:
            if expected_columns:
                iterator = self._iter_guarded_dict_rows(
                    self.connector,
                    query,
                    params=params,
                    batch_size=batch_size,
                    expected_columns=expected_columns,
                )
            else:
                iterator = self.connector.get_records_iterator(query, params=params)
        except BaseException as primary:
            rollback_preserving_primary(self.connector, primary)
            raise
        return StreamingRowsArtifact(
            iterator=iterator,
            batch_size=batch_size,
            extraction_lifecycle=lifecycle,
            on_success=self.connector.commit_transaction,
            on_abort=self.connector.rollback,
        )

    def _begin_repeatable_read_snapshot(
        self,
        lifecycle: ExtractionLifecycleAuthority,
    ) -> PostgresRepeatableReadSnapshotLease:
        """Open and evidence one PostgreSQL repeatable-read snapshot."""

        transaction_open = False
        try:
            self.connector.begin()
            transaction_open = True
            self.connector.execute_query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            rows = self.connector.get_records(
                "SELECT txid_current_snapshot()::text AS snapshot_token, "
                "txid_snapshot_xmax(txid_current_snapshot())::bigint AS extraction_horizon",
                params=None,
                as_dict=True,
            )
            token = str(rows[0].get("snapshot_token") or "") if rows else ""
            if not token:
                raise ValueError("postgres_extraction_snapshot_token_unavailable")
            raw_horizon = rows[0].get("extraction_horizon")
            return issue_repeatable_read_snapshot_lease(
                connector=self.connector,
                lifecycle=lifecycle,
                raw_snapshot_token=token,
                visible_horizon=None if raw_horizon is None else int(raw_horizon),
            )
        except BaseException as primary:
            if transaction_open:
                rollback_preserving_primary(self.connector, primary)
            raise

    def _begin_exported_snapshot(self, lifecycle: ExtractionLifecycleAuthority) -> str:
        """Open one coordinator snapshot that partition workers can import."""

        return self._begin_snapshot(
            lifecycle,
            "SELECT pg_export_snapshot() AS snapshot_token",
            snapshot_authority="postgresql.exported_snapshot",
            missing_code="postgres_extraction_exported_snapshot_unavailable",
        )

    def _begin_snapshot(
        self,
        lifecycle: ExtractionLifecycleAuthority,
        token_query: str,
        *,
        snapshot_authority: str,
        missing_code: str,
    ) -> str:
        self.connector.begin()
        try:
            self.connector.execute_query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            rows = self.connector.get_records(token_query, params=None, as_dict=True)
            token = str(rows[0].get("snapshot_token") or "") if rows else ""
            if not token:
                raise ValueError(missing_code)
            lifecycle.acquire_snapshot(
                snapshot_authority=snapshot_authority,
                source_token=snapshot_token_digest(token),
            )
            return token
        except BaseException as primary:
            rollback_preserving_primary(self.connector, primary)
            raise


__all__ = ["PostgresExtractionLifecycleMixin", "PostgresRepeatableReadSnapshotCompletion"]
