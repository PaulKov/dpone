"""Real SQLite authority: no replacement journal, expiry, or owner handoff."""

import os
import sqlite3
from contextlib import contextmanager
from dataclasses import replace

import pytest

from dpone.adapters.clickhouse_authority_sqlite import SQLitePublicationAuthority
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
