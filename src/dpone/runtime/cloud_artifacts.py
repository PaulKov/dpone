from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.runtime.artifact_models import StagingTableArtifact
    from dpone.runtime.gcs_replacement import GcsAttemptScope
    from dpone.runtime.staging import StagingManager


import logging
from collections.abc import Sequence
from dataclasses import dataclass, field

from dpone.runtime.artifact_models import BaseExtractionArtifact
from dpone.runtime.staging import owned_staging_handle

logger = logging.getLogger(__name__)


@dataclass
class GCSExportArtifact(BaseExtractionArtifact):
    """Артефакт, представляющий данные, выгруженные в Google Cloud Storage.

    Используется для нативного потока ClickHouse → GCS → BigQuery без
    промежуточной загрузки в Python/pandas.
    """

    gcs_uri: str = field(init=False)
    format: str = field(default="parquet", init=False)
    pattern: str | None = field(default=None, init=False)
    hive_partitioning: bool = field(default=False, init=False)
    source_uri_prefix: str | None = field(default=None, init=False)
    new_partitions: Sequence[str] | None = field(default=None, init=False)
    load_patterns: dict[str, str] | None = field(default=None, init=False)
    lookback_partitions: Sequence[str] | None = field(default=None, init=False)
    incremental_partitions: Sequence[str] | None = field(default=None, init=False)
    incremental_column: str | None = field(default=None, init=False)
    column_timezone: str | None = field(default=None, init=False)
    columns: Sequence[str] = field(default_factory=list, init=False)
    cleanup_gcs: bool = field(default=False, init=False)
    bucket_name: str | None = field(default=None, init=False)
    attempt_table_prefix: str | None = field(default=None, init=False)
    prior_generation_prefixes: tuple[str, ...] = field(default=(), init=False)
    attempt_scope: GcsAttemptScope | None = field(default=None, init=False)

    def __init__(
        self,
        gcs_uri: str,
        columns: Sequence[str],
        *,
        format: str = "parquet",
        pattern: str | None = None,
        hive_partitioning: bool = False,
        source_uri_prefix: str | None = None,
        new_partitions: Sequence[str] | None = None,
        load_patterns: dict[str, str] | None = None,
        lookback_partitions: Sequence[str] | None = None,
        incremental_partitions: Sequence[str] | None = None,
        incremental_column: str | None = None,
        column_timezone: str | None = None,
        cleanup_gcs: bool = False,
        bucket_name: str | None = None,
        attempt_table_prefix: str | None = None,
        prior_generation_prefixes: Sequence[str] | None = None,
        attempt_scope: GcsAttemptScope | None = None,
        estimated_rows: int | None = None,
    ):
        super().__init__(estimated_rows=estimated_rows)
        self.gcs_uri = gcs_uri
        self.columns = columns
        self.format = format.lower()
        self.pattern = pattern
        self.hive_partitioning = hive_partitioning
        self.source_uri_prefix = source_uri_prefix
        self.new_partitions = new_partitions
        self.load_patterns = load_patterns
        self.lookback_partitions = lookback_partitions
        self.incremental_partitions = incremental_partitions
        self.incremental_column = incremental_column
        self.column_timezone = column_timezone
        self.cleanup_gcs = cleanup_gcs
        self.bucket_name = bucket_name
        self.attempt_table_prefix = attempt_table_prefix
        self.prior_generation_prefixes = tuple(prior_generation_prefixes or ())
        self.attempt_scope = attempt_scope

    def materialize(
        self,
        staging_manager: StagingManager,
        load_config,
        schema: Sequence[tuple[str, str]],
    ) -> StagingTableArtifact:
        """Загружает данные из GCS в staging таблицу BigQuery.

        Вызывает специальный метод staging_manager.load_from_gcs_artifact().
        """
        with owned_staging_handle(staging_manager, load_config, schema) as handle:
            inserted = staging_manager.load_from_gcs_artifact(handle, self)
            handle.row_count = inserted
            return handle

    def finalize_after_commit(self) -> None:
        """Record cleanup debt and retire prior GCS generations after target commit."""

        if not self.prior_generation_prefixes or not self.bucket_name or not self.attempt_table_prefix:
            return
        if self.attempt_scope is None:
            raise ValueError("GCS replacement artifact is missing attempt scope.")

        from dpone.runtime.gcs_replacement import (
            CommitOutcomeUnknownError,
            record_cleanup_debt,
            retire_prior_generations,
        )

        try:
            record_cleanup_debt(
                self.prior_generation_prefixes,
                bucket=self.bucket_name,
                scope=self.attempt_scope,
                reason="gcs_replacement_after_target_commit",
            )
        except CommitOutcomeUnknownError:
            raise
        except Exception as exc:
            raise CommitOutcomeUnknownError(
                "Unable to record GCS cleanup debt after target commit; reconciliation is required."
            ) from exc

        # Cleanup failure after durable debt must not replay a committed load.
        try:
            from dpone.runtime.support.gcs import create_gcs_client

            storage_client, _ = create_gcs_client()
            retire_prior_generations(
                storage_client,
                bucket=self.bucket_name,
                prefixes=self.prior_generation_prefixes,
                logger_instance=logger,
            )
        except Exception as exc:
            logger.warning("Failed to retire prior GCS generations at %s: %s", self.gcs_uri, exc)

    def cleanup(self) -> None:
        """Remove attempt-owned staging objects on failure or cancellation."""

        if not self.bucket_name or not self.attempt_table_prefix:
            if self.cleanup_gcs:
                logger.warning("GCS cleanup requested but attempt-owned identity is missing for %s", self.gcs_uri)
            return

        try:
            from dpone.runtime.gcs_replacement import cleanup_attempt_prefix
            from dpone.runtime.support.gcs import create_gcs_client

            storage_client, _ = create_gcs_client()
            deleted_count = cleanup_attempt_prefix(
                storage_client,
                bucket=self.bucket_name,
                attempt_prefix=self.attempt_table_prefix,
                logger_instance=logger,
            )
            if deleted_count > 0:
                logger.info(
                    "Cleaned up %d attempt-owned file(s) from gs://%s/%s",
                    deleted_count,
                    self.bucket_name,
                    self.attempt_table_prefix,
                )
        except Exception as exc:
            logger.warning(
                "Failed to cleanup attempt-owned GCS files at gs://%s/%s: %s",
                self.bucket_name,
                self.attempt_table_prefix,
                exc,
            )
