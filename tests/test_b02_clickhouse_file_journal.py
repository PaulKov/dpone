"""Real immutable journal producer, limits and ownership under local faults."""

import hashlib
import json

import pytest

from dpone.runtime.sinks.clickhouse_validated_file_journal import ClickHouseFileAttemptJournal
from dpone.runtime.sinks.clickhouse_validated_file_models import ClickHouseValidatedFilePolicy, FileConsumptionError
from dpone.runtime.storage_policy import StoragePreflightService


def test_events_are_immutable_chained_and_source_values_absent(tmp_path):
    policy = ClickHouseValidatedFilePolicy(work_directory=tmp_path, max_spool_bytes=1_048_576)
    with ClickHouseFileAttemptJournal(policy, "a" * 32, storage=StoragePreflightService()) as journal:
        first = journal.record(phase="checking", outcome="running")
        second = journal.record(phase="prepared", outcome="running", rows_prepared=3)
        root = journal.directory
        assert first["previous_event_sha256"] is None
        assert (
            second["previous_event_sha256"]
            == hashlib.sha256((root / "events/000001/attempt.json").read_bytes()).hexdigest()
        )
        assert second["rows_observed"] is None
    assert json.loads((root / "events/000002/attempt.json").read_text()) == second
    with pytest.raises(FileExistsError):
        ClickHouseFileAttemptJournal(policy, "a" * 32, storage=StoragePreflightService())


def test_intent_writer_failure_does_not_advance_journal(tmp_path):
    def fail_writer(*args, **kwargs):
        raise OSError("fsync failed")

    policy = ClickHouseValidatedFilePolicy(work_directory=tmp_path, max_spool_bytes=1_048_576)
    with ClickHouseFileAttemptJournal(
        policy, "b" * 32, storage=StoragePreflightService(), event_writer=fail_writer
    ) as journal:
        with pytest.raises(FileConsumptionError, match="attempt_journal_unavailable"):
            journal.record(phase="creating_staging", outcome="running")
        assert journal.latest is None


def test_cumulative_spool_and_event_budget(tmp_path):
    policy = ClickHouseValidatedFilePolicy(work_directory=tmp_path, max_spool_bytes=4096)
    with ClickHouseFileAttemptJournal(policy, "c" * 32, storage=StoragePreflightService()) as journal:
        journal.reserve(4096)
        with pytest.raises(FileConsumptionError, match="resource_limit"):
            journal.reserve(1)


def test_work_directory_symlink_rejected(tmp_path):
    link = tmp_path / "alias"
    link.symlink_to(tmp_path, target_is_directory=True)
    policy = ClickHouseValidatedFilePolicy(work_directory=link, max_spool_bytes=4096)
    with pytest.raises(FileConsumptionError):
        ClickHouseFileAttemptJournal(policy, "d" * 32, storage=StoragePreflightService())


def test_event_size_limit_and_changed_directory_fail_closed(tmp_path):
    policy = ClickHouseValidatedFilePolicy(work_directory=tmp_path, max_spool_bytes=1_048_576)
    with ClickHouseFileAttemptJournal(policy, "e" * 32, storage=StoragePreflightService()) as journal:
        record = journal.record(phase="checking", outcome="running")
        with pytest.raises(FileConsumptionError, match="resource_limit"):
            journal.record(column="x" * 262_144)
        assert journal.latest == record
        journal.directory.rename(tmp_path / "moved")
        journal.directory.mkdir()
        with pytest.raises(OSError):
            journal.open_partial()
        assert not list(journal.directory.iterdir())
