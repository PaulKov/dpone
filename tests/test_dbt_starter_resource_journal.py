"""Durable synthetic resource-operation journals; no package qualification."""

import hashlib
import json
import os

import pytest
from tools.dbt_self_service.starter_resource_journal import (
    MAX_EVENTS,
    MAX_RESOURCE_BYTES,
    RESOURCE_PATHS,
    ResourceJournal,
    recovery_report,
)


def entries():
    return [
        {"path": path, "old": None, "desired_sha256": "sha256:" + hashlib.sha256(path.encode()).hexdigest()}
        for path in RESOURCE_PATHS
    ]


def test_empty_recovery_report_is_read_only(tmp_path):
    assert not recovery_report(tmp_path).pending
    assert list(tmp_path.iterdir()) == []


def test_new_manifest_uses_closed_v2_inventory(tmp_path):
    assert len(RESOURCE_PATHS) == 17
    assert RESOURCE_PATHS[-3].endswith("/physical-v1/catalog-v2.sql")
    with ResourceJournal.start(tmp_path, "a" * 40, entries()) as journal:
        manifest = json.loads((journal.log_path.parent / "manifest.json").read_bytes())
        assert manifest["schema"] == "dpone.starter-resource-transaction.v2"
        assert tuple(entry["path"] for entry in manifest["entries"]) == RESOURCE_PATHS


def test_durable_manifest_and_observations_roundtrip(tmp_path):
    with ResourceJournal.start(tmp_path, "a" * 40, entries()) as journal:
        report = recovery_report(tmp_path)
        assert report.pending and report.operation == journal.operation
        assert report.status == "PREPARING"
        journal.append({"phase": "PREPARED"})
        journal.append({"phase": "APPLYING", "path": RESOURCE_PATHS[0]})
        report = recovery_report(tmp_path)
        assert report.status == "APPLYING"
        assert report.unresolved == (RESOURCE_PATHS[0],)
    assert recovery_report(tmp_path).pending


def test_existing_operation_prevents_second_start(tmp_path):
    with ResourceJournal.start(tmp_path, "a" * 40, entries()):
        with pytest.raises(ValueError):
            ResourceJournal.start(tmp_path, "a" * 40, entries())


@pytest.mark.parametrize("change", ["missing", "extra", "duplicate", "traversal", "digest", "unknown_key"])
def test_invalid_manifest_never_creates_metadata(tmp_path, change):
    planned = entries()
    if change == "missing":
        planned.pop()
    elif change == "extra":
        planned.append(dict(planned[0], path="unknown"))
    elif change == "duplicate":
        planned[-1] = dict(planned[0])
    elif change == "traversal":
        planned[0]["path"] = "../PRIVATE_SENTINEL"
    elif change == "digest":
        planned[0]["desired_sha256"] = "PRIVATE_SENTINEL"
    else:
        planned[0]["PRIVATE_SENTINEL"] = True
    with pytest.raises(ValueError) as error:
        ResourceJournal.start(tmp_path, "a" * 40, planned)
    assert "PRIVATE_SENTINEL" not in str(error.value)
    assert list(tmp_path.iterdir()) == []


def test_malformed_event_is_rejected_before_append(tmp_path):
    with ResourceJournal.start(tmp_path, "a" * 40, entries()) as journal:
        before = journal.log_path.read_bytes()
        with pytest.raises(ValueError):
            journal.append({"phase": "PRIVATE_SENTINEL"})
        assert journal.log_path.read_bytes() == before


def test_partial_log_reports_recovery_without_repair(tmp_path):
    with ResourceJournal.start(tmp_path, "a" * 40, entries()) as journal:
        log = journal.log_path
    with log.open("ab") as stream:
        stream.write(b'{"phase":')
    before = log.read_bytes()
    report = recovery_report(tmp_path)
    assert report.pending and report.status == "INVALID"
    assert log.read_bytes() == before


def test_unknown_metadata_entry_is_preserved(tmp_path):
    root = tmp_path / ".dpone-starter-resource-transactions"
    root.mkdir()
    extra = root / "PRIVATE_SENTINEL"
    extra.write_bytes(b"keep")
    report = recovery_report(tmp_path)
    assert report.pending and report.status == "INVALID"
    assert "PRIVATE_SENTINEL" not in repr(report)
    with pytest.raises(ValueError):
        ResourceJournal.start(tmp_path, "a" * 40, entries())
    assert extra.read_bytes() == b"keep"


def test_symlinked_metadata_root_is_not_followed(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    (tmp_path / ".dpone-starter-resource-transactions").symlink_to(outside, target_is_directory=True)
    assert recovery_report(tmp_path).status == "INVALID"
    assert list(outside.iterdir()) == []


@pytest.mark.parametrize("kind", ["symlink", "unknown", "oversized"])
def test_invalid_backup_inventory_is_reported_not_followed(tmp_path, kind):
    with ResourceJournal.start(tmp_path, "a" * 40, entries()) as journal:
        backup = tmp_path / journal.directory / "old"
        backup.mkdir()
        if kind == "symlink":
            (backup / "000.bin").symlink_to(journal.log_path)
        elif kind == "unknown":
            (backup / "PRIVATE_SENTINEL").write_bytes(b"keep")
        else:
            with (backup / "000.bin").open("wb") as stream:
                stream.truncate(MAX_RESOURCE_BYTES + 1)
        report = recovery_report(tmp_path)
        assert report.pending and report.status == "INVALID"
        assert "PRIVATE_SENTINEL" not in repr(report)


def test_event_limit_rejects_without_extending_log(tmp_path):
    with ResourceJournal.start(tmp_path, "a" * 40, entries()) as journal:
        for _ in range(MAX_EVENTS - 1):
            journal.append({"phase": "PREPARING"})
        before = journal.log_path.read_bytes()
        with pytest.raises(ValueError):
            journal.append({"phase": "PREPARING"})
        assert journal.log_path.read_bytes() == before


def test_later_observation_does_not_erase_earlier_unpersisted_paths(tmp_path, monkeypatch):
    from pathlib import PurePosixPath

    from dpone.manifest.confined_atomic_exchange import ExchangeBackOutcome
    from dpone.manifest.confined_mutations import ConfinedReplaceOutcome

    with ResourceJournal.start(tmp_path, "a" * 40, entries()) as journal:
        append = journal.append
        first = True

        def fail_once(event):
            nonlocal first
            if first:
                first = False
                raise OSError("synthetic append failure")
            append(event)

        monkeypatch.setattr(journal, "append", fail_once)
        name = ".dpone-recovery-" + "a" * 32
        with pytest.raises(OSError):
            journal.observe_mutation(RESOURCE_PATHS[0], ConfinedReplaceOutcome(True, True, name))
        expected = str(PurePosixPath(RESOURCE_PATHS[0]).with_name(name))
        journal.observe_inverse(RESOURCE_PATHS[1], ExchangeBackOutcome(".dpone-recovery-" + "b" * 32))
        assert expected in journal.unpersisted_paths


def test_detached_log_is_not_reported_as_durably_observed(tmp_path, monkeypatch):
    with ResourceJournal.start(tmp_path, "a" * 40, entries()) as journal:
        fsync = os.fsync
        old = journal.log_path.with_name("detached.jsonl")

        def detach(descriptor):
            fsync(descriptor)
            journal.log_path.rename(old)
            journal.log_path.write_bytes(old.read_bytes())

        monkeypatch.setattr(os, "fsync", detach)
        with pytest.raises(ValueError):
            journal.append({"phase": "PREPARED"})
        assert old.exists() and journal.log_path.exists()


def test_foreign_log_content_prevents_append(tmp_path):
    with ResourceJournal.start(tmp_path, "a" * 40, entries()) as journal:
        journal.log_path.write_bytes(b"PRIVATE_SENTINEL")
        with pytest.raises(ValueError):
            journal.append({"phase": "PREPARED"})
        assert journal.log_path.read_bytes() == b"PRIVATE_SENTINEL"


@pytest.mark.parametrize(
    "payload",
    [
        b'{"sequence":0,"phase":"PREPARING","phase":"COMPLETE"}\n',
        b'{"sequence":1,"phase":"PREPARING"}\n',
        b'{"sequence":true,"phase":"PREPARING"}\n',
        b'{"sequence":0,"phase":"APPLIED"}\n',
    ],
)
def test_invalid_event_records_never_imply_completion(tmp_path, payload):
    with ResourceJournal.start(tmp_path, "a" * 40, entries()) as journal:
        log = journal.log_path
    log.write_bytes(payload)
    assert recovery_report(tmp_path).status == "INVALID"
