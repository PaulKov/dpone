"""Real SQLite authority: no replacement journal, expiry, or owner handoff."""

import json
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path

import pytest

from dpone.adapters.clickhouse_authority_sqlite import SQLitePublicationAuthority
from dpone.adapters.clickhouse_authority_storage import AuthorityStorageError
from dpone.adapters.clickhouse_publication_codec import PublicationRecordCodecError
from dpone.contracts.clickhouse_authority import (
    AuthorityConflict,
    AuthorityError,
    AuthoritySubject,
    DispatchGrant,
    TransportState,
)
from dpone.contracts.clickhouse_publication import PublicationState
from tests.test_clickhouse_publication_codec import example_record


@pytest.fixture
def authority(tmp_path):
    path = tmp_path / "authority.db"
    SQLitePublicationAuthority.provision(path, "deployment")
    return SQLitePublicationAuthority(path, "deployment")


def subject():
    return AuthoritySubject("deployment", "server", "db", "target")


def test_missing_file_open_never_creates_authority(tmp_path):
    path = tmp_path / "missing" / "authority.db"
    with pytest.raises(AuthorityError):
        SQLitePublicationAuthority(path, "deployment")
    assert not path.parent.exists()


def test_create_once_and_open_existing_preserves_owner(authority, tmp_path):
    binding = authority.acquire("deployment:one", subject(), "candidate")
    with pytest.raises(AuthorityError):
        SQLitePublicationAuthority.provision(tmp_path / "authority.db", "deployment")
    reopened = SQLitePublicationAuthority(tmp_path / "authority.db", "deployment")
    assert reopened.binding("deployment:one") == binding
    assert reopened.acquire("deployment:one", subject(), "candidate") == binding
    assert (tmp_path / "authority.db").stat().st_mode & 0o077 == 0


def test_owner_does_not_expire_or_release_on_restart(authority, tmp_path):
    original = authority.acquire("deployment:one", subject(), "candidate")
    os.utime(tmp_path / "authority.db", (0, 0))
    reopened = SQLitePublicationAuthority(tmp_path / "authority.db", "deployment")
    with pytest.raises(AuthorityConflict):
        reopened.acquire("deployment:two", subject(), "another_candidate")
    assert reopened.binding("deployment:one") == original


@pytest.mark.parametrize("changed", ["candidate", "target", "server", "deployment"])
def test_operation_cannot_be_rebound(authority, changed):
    authority.acquire("deployment:one", subject(), "candidate")
    new_subject = {
        "target": replace(subject(), target="other"),
        "server": replace(subject(), server_id="other"),
        "deployment": replace(subject(), deployment_id="other"),
    }.get(changed, subject())
    with pytest.raises(AuthorityError):
        authority.acquire("deployment:one", new_subject, "other" if changed == "candidate" else "candidate")


@pytest.mark.parametrize("damage", ["deployment", "version", "corrupt", "symlink", "permissions"])
def test_invalid_existing_authority_fails_closed(authority, tmp_path, damage):
    path = tmp_path / "authority.db"
    if damage == "version":
        with sqlite3.connect(path) as db:
            db.execute("UPDATE authority_metadata SET schema_version='unknown'")
    elif damage == "corrupt":
        path.write_bytes(b"not a database")
    elif damage == "symlink":
        moved = tmp_path / "original.db"
        path.rename(moved)
        path.symlink_to(moved)
    elif damage == "permissions":
        path.chmod(0o644)
    with pytest.raises(AuthorityError):
        SQLitePublicationAuthority(path, "wrong" if damage == "deployment" else "deployment")


def test_open_instance_rejects_replaced_database(authority, tmp_path):
    path = tmp_path / "authority.db"
    path.rename(tmp_path / "original.db")
    SQLitePublicationAuthority.provision(path, "deployment")
    with pytest.raises(AuthorityError):
        authority.acquire("deployment:one", subject(), "candidate")


def test_failed_acquire_leaves_no_partial_operation(authority):
    authority.acquire("deployment:one", subject(), "candidate")
    with pytest.raises(AuthorityConflict):
        authority.acquire("deployment:two", subject(), "candidate2")
    with pytest.raises(AuthorityError):
        authority.binding("deployment:two")
    assert authority.acquire("deployment:two", replace(subject(), target="other"), "candidate2").epoch == 1


def test_runtime_connections_use_durable_settings(authority):
    with authority._storage.connection() as db:
        assert db.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert db.execute("PRAGMA synchronous").fetchone()[0] == 2
        assert db.execute("PRAGMA foreign_keys").fetchone()[0] == 1


def prepared(authority):
    binding = authority.acquire("deployment:one", subject(), "candidate")
    return authority.prepare(binding, example_record().intent)


def test_prepare_is_immutable_and_subject_bound(authority):
    entry = prepared(authority)
    assert authority.prepare(authority.binding("deployment:one"), entry.record.intent) == entry
    before = entry.record.intent.before
    with pytest.raises(AuthorityError):
        authority.prepare(
            authority.binding("deployment:one"),
            replace(entry.record.intent, before=replace(before, subject=("foreign", "db", "target", "candidate"))),
        )
    assert authority.read("deployment:one") == entry


def test_claim_is_not_reissued_after_reload(authority, tmp_path):
    entry = prepared(authority)
    grant = authority.claim(entry)
    assert grant is not None
    assert authority.claim(entry) is None
    reopened = SQLitePublicationAuthority(tmp_path / "authority.db", "deployment")
    loaded = reopened.read("deployment:one")
    assert loaded.record.state == PublicationState.CLAIMED
    assert loaded.record.claim_granted is True
    assert reopened.claim(loaded) is None
    reopened.begin_send(grant)
    assert reopened.transport_state("deployment:one") == TransportState.MAY_HAVE_SENT
    with pytest.raises(AuthorityConflict):
        reopened.begin_send(grant)


def test_lost_claim_commit_ack_returns_no_grant(authority, tmp_path, monkeypatch):
    entry = prepared(authority)
    original = authority._storage.transaction

    @contextmanager
    def lost_ack():
        with original() as db:
            yield db
        raise AuthorityError("injected lost commit ACK")

    monkeypatch.setattr(authority._storage, "transaction", lost_ack)
    with pytest.raises(AuthorityError):
        authority.claim(entry)
    reopened = SQLitePublicationAuthority(tmp_path / "authority.db", "deployment")
    assert reopened.read("deployment:one").record.state == PublicationState.CLAIMED
    assert reopened.claim(reopened.read("deployment:one")) is None
    with pytest.raises(AuthorityError):
        reopened.begin_send(DispatchGrant("deployment:one", 1, "forged"))


def test_close_revokes_late_claimant_and_is_idempotent(authority):
    grant = authority.claim(prepared(authority))
    authority.close_without_send("deployment:one")
    authority.close_without_send("deployment:one")
    with pytest.raises(AuthorityConflict):
        authority.begin_send(grant)
    assert authority.transport_state("deployment:one") == TransportState.CLOSED_WITHOUT_SEND


def test_closed_prepared_record_cannot_be_claimed(authority):
    entry = prepared(authority)
    authority.close_without_send("deployment:one")
    assert authority.claim(entry) is None
    assert authority.claim(authority.read("deployment:one")) is None


def test_possible_send_cannot_close_as_unsent(authority):
    grant = authority.claim(prepared(authority))
    authority.begin_send(grant)
    with pytest.raises(AuthorityConflict):
        authority.close_without_send("deployment:one")
    with pytest.raises(AuthorityConflict):
        authority.acquire("deployment:two", subject(), "candidate2")


def test_stale_or_forged_grants_cannot_send(authority):
    grant = authority.claim(prepared(authority))
    for bad in (
        replace(grant, epoch=2),
        replace(grant, secret="forged"),
        replace(grant, operation_id="deployment:other"),
    ):
        with pytest.raises(AuthorityError):
            authority.begin_send(bad)
    assert authority.transport_state("deployment:one") == TransportState.NOT_STARTED


def completed_transport(authority):
    grant = authority.claim(prepared(authority))
    authority.begin_send(grant)
    authority.record_terminal(grant, "e" * 64)
    return grant


def test_terminal_requires_original_grant_and_possible_send(authority):
    grant = authority.claim(prepared(authority))
    with pytest.raises(AuthorityError):
        authority.record_terminal(grant, "e" * 64)
    authority.begin_send(grant)
    for bad in (replace(grant, epoch=2), replace(grant, secret="forged")):
        with pytest.raises(AuthorityError):
            authority.record_terminal(bad, "e" * 64)
    with pytest.raises(AuthorityError):
        authority.record_terminal(grant, "invalid")
    authority.record_terminal(grant, "e" * 64)
    authority.record_terminal(grant, "e" * 64)
    with pytest.raises(AuthorityError):
        authority.record_terminal(grant, "f" * 64)
    with pytest.raises(AuthorityConflict):
        authority.begin_send(grant)


def test_resolution_requires_closure_and_current_revision(authority):
    stale = prepared(authority)
    with pytest.raises(AuthorityError):
        authority.resolve(stale, PublicationState.NOT_PUBLISHED, stale.record.intent.before)
    authority.close_without_send("deployment:one")
    with pytest.raises(AuthorityConflict):
        authority.resolve(stale, PublicationState.NOT_PUBLISHED, stale.record.intent.before)
    fresh = authority.read("deployment:one")
    result = authority.resolve(fresh, PublicationState.NOT_PUBLISHED, fresh.record.intent.before)
    assert result.record.claim_granted is False
    assert result.record.intent == stale.record.intent


def test_committed_resolution_keeps_target_owned(authority, tmp_path):
    completed_transport(authority)
    entry = authority.read("deployment:one")
    before = entry.record.intent.before
    desired = replace(before, target=replace(before.candidate, uuid=before.target.uuid))
    result = authority.resolve(entry, PublicationState.COMMITTED, desired)
    assert result.record.state == PublicationState.COMMITTED
    assert result.record.claim_granted and result.record.intent == entry.record.intent
    reopened = SQLitePublicationAuthority(tmp_path / "authority.db", "deployment")
    assert reopened.read("deployment:one") == result
    assert reopened.resolve(result, PublicationState.COMMITTED, desired) == result
    with pytest.raises(AuthorityConflict):
        reopened.acquire("deployment:two", subject(), "candidate2")
    with pytest.raises(AuthorityError):
        reopened.resolve(result, PublicationState.NOT_PUBLISHED, before)


def test_unknown_keeps_claim_history_without_regrant(authority):
    authority.claim(prepared(authority))
    authority.close_without_send("deployment:one")
    entry = authority.read("deployment:one")
    unknown = authority.resolve(entry, PublicationState.UNKNOWN, entry.record.intent.before)
    assert unknown.record.claim_granted
    assert authority.claim(unknown) is None


def test_resolution_commit_ack_loss_is_source_free_readback(authority, tmp_path, monkeypatch):
    completed_transport(authority)
    entry = authority.read("deployment:one")
    original = authority._storage.transaction

    @contextmanager
    def lost_ack():
        with original() as db:
            yield db
        raise AuthorityError("injected resolution ACK loss")

    monkeypatch.setattr(authority._storage, "transaction", lost_ack)
    with pytest.raises(AuthorityError):
        authority.resolve(entry, PublicationState.UNKNOWN, entry.record.intent.before)
    reopened = SQLitePublicationAuthority(tmp_path / "authority.db", "deployment")
    assert reopened.read("deployment:one").record.state == PublicationState.UNKNOWN
    assert reopened.claim(reopened.read("deployment:one")) is None


def test_diagnostics_are_redacted_atomic_and_not_overwritten(authority, tmp_path):
    grant = completed_transport(authority)
    folder = tmp_path / "reports"
    folder.mkdir()
    output = folder / "operation.json"
    authority.write_diagnostics("deployment:one", output)
    payload = output.read_text()
    assert json.loads(payload) == authority.diagnostics("deployment:one")
    assert grant.secret not in payload and "grant_hash" not in payload
    assert "record" not in json.loads(payload)
    with pytest.raises(AuthorityError):
        authority.write_diagnostics("deployment:one", output)
    assert output.read_text() == payload


def test_failed_diagnostic_write_leaves_no_partial_public_file(authority, tmp_path, monkeypatch):
    prepared(authority)
    folder = tmp_path / "reports"
    folder.mkdir()
    output = folder / "operation.json"

    def fail_link(*args, **kwargs):
        raise OSError("injected atomic publication failure")

    monkeypatch.setattr(os, "link", fail_link)
    with pytest.raises(AuthorityError):
        authority.write_diagnostics("deployment:one", output)
    assert not output.exists()
    assert list(folder.iterdir()) == []


def test_diagnostics_cannot_occupy_authority_sidecar_path(authority, tmp_path):
    prepared(authority)
    with pytest.raises(AuthorityError):
        authority.write_diagnostics("deployment:one", tmp_path / "authority.db-wal")
    assert authority.read("deployment:one") is not None


def test_aborted_sqlite_write_rolls_back_without_losing_owner(authority):
    prepared(authority)
    with pytest.raises(AuthorityStorageError):
        with authority._storage.transaction() as db:
            db.execute("UPDATE operations SET revision=99 WHERE operation_id='deployment:one'")
            db.execute("INSERT INTO missing_table VALUES (1)")
    assert authority.read("deployment:one").revision == 1
    assert authority.binding("deployment:one").subject == subject()


def test_sqlite_full_rolls_back_without_losing_original_record(authority):
    entry = prepared(authority)
    with pytest.raises(AuthorityStorageError) as failure:
        with authority._storage.transaction() as db:
            pages = db.execute("PRAGMA page_count").fetchone()[0]
            db.execute(f"PRAGMA max_page_count={pages}")
            db.execute("UPDATE operations SET record=zeroblob(1048576)")
    assert failure.value.__cause__.sqlite_errorcode == sqlite3.SQLITE_FULL
    assert authority.read("deployment:one") == entry
    assert authority.binding("deployment:one").subject == subject()


def test_documented_first_use_example():
    guide = Path(__file__).resolve().parents[1] / "docs" / "clickhouse-authority-journal.md"
    example = guide.read_text().split("```python\n", 1)[1].split("```", 1)[0]
    exec(compile(example, str(guide), "exec"), {})


@pytest.mark.parametrize("suffix", ["-wal", "-shm", "-journal"])
def test_provision_preserves_occupied_sidecar_namespace(tmp_path, suffix):
    path = tmp_path / "authority.db"
    existing = tmp_path / (path.name + suffix)
    existing.write_bytes(b"retained data")
    with pytest.raises(AuthorityError):
        SQLitePublicationAuthority.provision(path, "deployment")
    assert existing.read_bytes() == b"retained data"
    assert not path.exists()


@pytest.mark.parametrize("suffix", ["-wal", "-shm", "-journal"])
def test_authority_basename_cannot_use_sqlite_sidecar_suffix(tmp_path, suffix):
    path = tmp_path / ("authority.db" + suffix)
    with pytest.raises(AuthorityError):
        SQLitePublicationAuthority.provision(path, "deployment")
    assert not path.exists()


def test_provision_requires_platform_owned_existing_directory(tmp_path):
    path = tmp_path / "not_provisioned" / "authority.db"
    with pytest.raises(AuthorityError):
        SQLitePublicationAuthority.provision(path, "deployment")
    assert not path.parent.exists()


def test_resolution_history_retains_each_immutable_observation(authority):
    completed_transport(authority)
    entry = authority.read("deployment:one")
    before = entry.record.intent.before
    uncertain = replace(before, target=replace(before.target, content_digest="d" * 64))
    unknown = authority.resolve(entry, PublicationState.UNKNOWN, uncertain)
    authority.resolve(unknown, PublicationState.NOT_PUBLISHED, before)
    with authority._storage.connection() as db:
        observations = db.execute("SELECT observed FROM history WHERE event='resolve' ORDER BY revision").fetchall()
        assert [json.loads(row[0])["target"]["content_digest"] for row in observations] == [
            "d" * 64,
            before.target.content_digest,
        ]
        with pytest.raises(sqlite3.IntegrityError):
            db.execute("UPDATE history SET observed=NULL WHERE event='resolve'")


def test_authority_translates_codec_encode_error(authority):
    binding = authority.acquire("deployment:one", subject(), "candidate")
    with pytest.raises(AuthorityError) as failure:
        authority.prepare(binding, replace(example_record().intent, method="invalid"))
    assert isinstance(failure.value.__cause__, PublicationRecordCodecError)
    assert authority.read("deployment:one") is None


def test_authority_translates_codec_decode_error(authority):
    prepared(authority)
    with authority._storage.transaction() as db:
        db.execute("UPDATE operations SET record='{}'")
    with pytest.raises(AuthorityError) as failure:
        authority.read("deployment:one")
    assert isinstance(failure.value.__cause__, PublicationRecordCodecError)


def test_authority_translates_invalid_persisted_revision(authority):
    prepared(authority)
    with authority._storage.transaction() as db:
        db.execute("UPDATE operations SET revision=0")
    with pytest.raises(AuthorityError) as failure:
        authority.read("deployment:one")
    assert isinstance(failure.value.__cause__, ValueError)


def test_storage_failure_cannot_leak_a_claim_grant(authority, monkeypatch):
    entry = prepared(authority)

    def fail_storage():
        raise AuthorityStorageError("injected storage failure")

    with monkeypatch.context() as scoped:
        scoped.setattr(authority._storage, "_check_identity", fail_storage)
        with pytest.raises(AuthorityError) as failure:
            authority.claim(entry)
        assert isinstance(failure.value.__cause__, AuthorityStorageError)
    assert authority.read("deployment:one") == entry
