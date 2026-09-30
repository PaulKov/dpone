"""Private local SQLite lifecycle; never recreate a missing authority on open.

The directory is a deployment security boundary. This adapter detects path
replacement during its lifetime, not rollback to a valid old backup on restart.
It does not certify filesystem durability or defend against its own OS user.
"""

from __future__ import annotations

import os
import sqlite3
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from dpone.contracts.clickhouse_authority import AuthorityError, require_text

SCHEMA_VERSION = "dpone.clickhouse.authority.v1"
_SCHEMA = """
CREATE TABLE authority_metadata (singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    schema_version TEXT NOT NULL, deployment TEXT NOT NULL);
CREATE TABLE subjects (subject_key TEXT PRIMARY KEY, payload TEXT NOT NULL,
    owner TEXT NOT NULL UNIQUE, epoch INTEGER NOT NULL CHECK(epoch>0));
CREATE TABLE operations (operation_id TEXT PRIMARY KEY, subject_key TEXT NOT NULL UNIQUE
    REFERENCES subjects(subject_key), candidate TEXT NOT NULL, query_id TEXT NOT NULL UNIQUE,
    revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0), record TEXT, intent TEXT,
    transport TEXT NOT NULL DEFAULT 'not_started' CHECK(transport IN
      ('not_started','may_have_sent','closed_without_send','closed_terminal')),
    grant_hash TEXT, completion_digest TEXT, observed TEXT);
CREATE TABLE history (operation_id TEXT NOT NULL REFERENCES operations(operation_id),
    revision INTEGER NOT NULL, event TEXT NOT NULL, transport TEXT NOT NULL,
    record_digest TEXT, PRIMARY KEY(operation_id,revision));
CREATE TRIGGER history_no_update BEFORE UPDATE ON history BEGIN
    SELECT RAISE(ABORT,'Immutable authority history'); END;
CREATE TRIGGER history_no_delete BEFORE DELETE ON history BEGIN
    SELECT RAISE(ABORT,'Immutable authority history'); END;
CREATE TRIGGER subject_no_update BEFORE UPDATE ON subjects BEGIN
    SELECT RAISE(ABORT,'Retained authority owner'); END;
CREATE TRIGGER subject_no_delete BEFORE DELETE ON subjects BEGIN
    SELECT RAISE(ABORT,'Retained authority owner'); END;
"""


def _private(path: Path, *, directory: bool) -> os.stat_result:
    info = path.lstat()
    valid_type = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
    if not valid_type or info.st_uid != os.getuid() or info.st_mode & 0o077 or not directory and info.st_nlink != 1:
        raise AuthorityError("Authority requires private owned directories and regular files")
    return info


class AuthorityStorage:
    """Short durable transactions against one existing deployment-owned inode."""

    def __init__(self, path: Path, deployment_id: str) -> None:
        require_text(deployment_id)
        self.path = path.absolute()
        self.deployment_id = deployment_id
        try:
            _private(self.path.parent, directory=True)
            info = _private(self.path, directory=False)
            self._identity = (info.st_dev, info.st_ino)
            with self.connection() as db:
                if db.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                    raise AuthorityError("Authority database integrity check failed")
        except (OSError, sqlite3.Error) as error:
            raise AuthorityError("Cannot open existing authority") from error

    @staticmethod
    def provision(path: Path, deployment_id: str) -> None:
        """Initialize once; leave failed initialization quarantined, never reset it."""
        require_text(deployment_id)
        path = path.absolute()
        try:
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            _private(path.parent, directory=True)
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
            os.close(descriptor)
            db = sqlite3.connect(path, isolation_level=None)
            try:
                db.execute("PRAGMA journal_mode=WAL")
                db.execute("PRAGMA synchronous=FULL")
                db.execute("PRAGMA foreign_keys=ON")
                db.executescript("BEGIN IMMEDIATE;" + _SCHEMA)
                db.execute("INSERT INTO authority_metadata VALUES (1,?,?)", (SCHEMA_VERSION, deployment_id))
                db.commit()
            finally:
                db.close()
            descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        except (OSError, sqlite3.Error) as error:
            raise AuthorityError("Create-once authority provisioning failed") from error

    def _check_identity(self) -> None:
        _private(self.path.parent, directory=True)
        info = _private(self.path, directory=False)
        if (info.st_dev, info.st_ino) != self._identity:
            raise AuthorityError("Authority file was replaced; automatic recovery is prohibited")
        for suffix in ("-wal", "-shm"):
            sidecar = Path(str(self.path) + suffix)
            if sidecar.exists() or sidecar.is_symlink():
                _private(sidecar, directory=False)

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        """Verify identity, deployment and mandatory settings on every connection."""
        db = None
        try:
            self._check_identity()
            db = sqlite3.connect(self.path.as_uri() + "?mode=rw", uri=True, timeout=30, isolation_level=None)
            self._check_identity()
            db.execute("PRAGMA synchronous=FULL")
            db.execute("PRAGMA foreign_keys=ON")
            expected = {"journal_mode": "wal", "synchronous": 2, "foreign_keys": 1}
            if any(db.execute(f"PRAGMA {key}").fetchone()[0] != value for key, value in expected.items()):
                raise AuthorityError("Required authority durability settings unavailable")
            if db.execute("SELECT schema_version,deployment FROM authority_metadata").fetchall() != [
                (SCHEMA_VERSION, self.deployment_id)
            ]:
                raise AuthorityError("Authority schema or deployment mismatch")
            yield db
        except (OSError, sqlite3.Error) as error:
            raise AuthorityError("Authority storage unavailable; retain ownership") from error
        finally:
            if db is not None:
                db.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                self._check_identity()
                db.commit()
            except BaseException:
                db.rollback()
                raise
