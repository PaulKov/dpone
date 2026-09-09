from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock

import pytest

from dpone.runtime.connectors.clickhouse_incremental_export import ClickHouseIncrementalExportService
from dpone.runtime.gcs_replacement import (
    CommitOutcomeUnknownError,
    GcsAttemptScope,
    InMemoryGcsCleanupDebtJournal,
    build_gcs_export_identity,
    derive_reconciliation_batch_path,
    finalize_gcs_replacement,
    plan_prior_generation_prefixes,
    record_cleanup_debt,
    resolve_gcs_attempt_scope,
    retire_prior_generations,
)


@dataclass
class _LoadConfig:
    options: dict[str, Any] = field(default_factory=dict)


def test_resolve_gcs_attempt_scope_requires_identity_before_io() -> None:
    with pytest.raises(ValueError, match="run_id and load_id"):
        resolve_gcs_attempt_scope(_LoadConfig())


def test_build_gcs_export_identity_is_deterministic() -> None:
    load_config = _LoadConfig(options={"run_id": "run-1", "load_id": "load-9"})
    _scope, attempt_prefix, attempt_uri, prior = build_gcs_export_identity(
        load_config,
        bucket_name="bucket-a",
        base_table_path="analytics/transfer/src/src.table",
        partition_labels=["2025-01-01"],
    )
    assert attempt_prefix == "analytics/transfer/src/src.table/attempts/run-1/load-9"
    assert attempt_uri == "gs://bucket-a/analytics/transfer/src/src.table/attempts/run-1/load-9"
    assert prior == ("analytics/transfer/src/src.table/dt_date=2025-01-01/",)


def test_plan_prior_generation_includes_legacy_cleanup_prefix() -> None:
    planned = plan_prior_generation_prefixes(
        "analytics/transfer/src/src.table",
        legacy_cleanup_prefix="legacy/path",
    )
    assert planned == ("legacy/path/", "analytics/transfer/src/src.table/")


def test_derive_reconciliation_batch_path_is_attempt_scoped() -> None:
    scope = GcsAttemptScope(run_id="run-1", load_id="load-9")
    path = derive_reconciliation_batch_path("proj.dataset.table", 3, scope)
    assert path == "reconciliation_tmp/attempts/run-1/load-9/proj.dataset.table/snapshot_batch_0003.csv.gz"


def test_incremental_export_does_not_delete_before_export() -> None:
    connector = MagicMock()
    connector.logger = MagicMock()
    service = ClickHouseIncrementalExportService(connector)
    load_config = _LoadConfig(options={"run_id": "run-1", "load_id": "load-9"})
    result = service.export_incremental_with_cleanup(
        query="SELECT 1",
        schema=[("dt", "Date")],
        partitions=[("2025-01-01", "toDate(dt) = '2025-01-01'")],
        bucket_name="bucket-a",
        base_table_path="analytics/transfer/src/src.table",
        base_gcs_uri="gs://ignored",
        lookback_partitions=["2025-01-01"],
        load_config=load_config,
    )
    connector.export_to_gcs.assert_called_once()
    assert "attempts/run-1/load-9" in connector.export_to_gcs.call_args.args[1]
    assert result["prior_generation_prefixes"] == ("analytics/transfer/src/src.table/dt_date=2025-01-01/",)


def test_finalize_records_debt_before_retiring_prior_generation() -> None:
    journal = InMemoryGcsCleanupDebtJournal()
    storage_client = MagicMock()
    scope = GcsAttemptScope(run_id="run-1", load_id="load-9")
    prior = ("analytics/transfer/src/src.table/dt_date=2025-01-01/",)

    from dpone.runtime.support import gcs as gcs_support

    class _Cleaner:
        @staticmethod
        def delete_prefix(*_args, **_kwargs) -> int:
            return 1

    original = gcs_support.GCSCleaner
    gcs_support.GCSCleaner = _Cleaner
    try:
        outcome = finalize_gcs_replacement(
            bucket="bucket-a",
            scope=scope,
            prior_generation_prefixes=prior,
            attempt_table_prefix="analytics/transfer/src/src.table/attempts/run-1/load-9",
            storage_client=storage_client,
            journal=journal,
        )
    finally:
        gcs_support.GCSCleaner = original

    assert len(journal.records) == 1
    assert journal.records[0].prefix == prior[0]
    assert outcome["prior_generations_retired"] == 1


def test_record_cleanup_debt_fails_closed_when_journal_unavailable() -> None:
    class _BrokenJournal:
        def record(self, debt: object) -> None:
            raise OSError("disk full")

    scope = GcsAttemptScope(run_id="run-1", load_id="load-9")
    with pytest.raises(CommitOutcomeUnknownError):
        record_cleanup_debt(
            ("analytics/transfer/src/src.table/",),
            bucket="bucket-a",
            scope=scope,
            reason="test",
            journal=_BrokenJournal(),
        )


def test_retire_prior_generations_is_noop_without_client() -> None:
    assert retire_prior_generations(None, bucket="bucket-a", prefixes=("a/",)) == 0


def test_finalize_after_commit_maps_debt_failure_to_unknown_outcome(monkeypatch: Any) -> None:
    from dpone.runtime.cloud_artifacts import GCSExportArtifact

    artifact = GCSExportArtifact(
        gcs_uri="gs://bucket-a/analytics/transfer/src/src.table/attempts/run-1/load-9/*.parquet",
        columns=("id",),
        bucket_name="bucket-a",
        attempt_table_prefix="analytics/transfer/src/src.table/attempts/run-1/load-9",
        prior_generation_prefixes=("analytics/transfer/src/src.table/",),
        attempt_scope=GcsAttemptScope(run_id="run-1", load_id="load-9"),
    )

    def _boom(*_args: Any, **_kwargs: Any) -> None:
        raise OSError("disk full")

    monkeypatch.setattr("dpone.runtime.gcs_replacement.record_cleanup_debt", _boom)
    with pytest.raises(CommitOutcomeUnknownError, match="cleanup debt"):
        artifact.finalize_after_commit()


def test_finalize_after_commit_keeps_debt_when_retire_client_fails(monkeypatch: Any) -> None:
    from dpone.runtime.cloud_artifacts import GCSExportArtifact

    journal = InMemoryGcsCleanupDebtJournal()
    artifact = GCSExportArtifact(
        gcs_uri="gs://bucket-a/analytics/transfer/src/src.table/attempts/run-1/load-9/*.parquet",
        columns=("id",),
        bucket_name="bucket-a",
        attempt_table_prefix="analytics/transfer/src/src.table/attempts/run-1/load-9",
        prior_generation_prefixes=("analytics/transfer/src/src.table/",),
        attempt_scope=GcsAttemptScope(run_id="run-1", load_id="load-9"),
    )

    def _record(prefixes: Any, **kwargs: Any) -> list[Any]:
        return record_cleanup_debt(prefixes, journal=journal, **kwargs)

    monkeypatch.setattr("dpone.runtime.gcs_replacement.record_cleanup_debt", _record)
    monkeypatch.setattr(
        "dpone.runtime.support.gcs.create_gcs_client",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("client unavailable")),
    )
    artifact.finalize_after_commit()
    assert len(journal.records) == 1
