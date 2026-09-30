"""Real SQLite authority: no replacement journal, expiry, or owner handoff."""

import os
import sqlite3
from dataclasses import replace

import pytest

from dpone.adapters.clickhouse_authority_sqlite import SQLitePublicationAuthority
from dpone.contracts.clickhouse_authority import AuthorityConflict, AuthorityError, AuthoritySubject


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
