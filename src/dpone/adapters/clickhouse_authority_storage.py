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

from dpone.adapters.clickhouse_authority_schema import AuthorityVersion, schema_sql
from dpone.contracts.clickhouse_authority import AuthorityStorageIdentity

SCHEMA_VERSION = "dpone.clickhouse.authority.v1"
_SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal")


class AuthorityStorageError(RuntimeError):
    """Private persistence failure; callers decide domain recovery policy."""


def _private(path: Path, *, directory: bool) -> os.stat_result:
    info = path.lstat()
    valid_type = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
    if not valid_type or info.st_uid != os.getuid() or info.st_mode & 0o077 or not directory and info.st_nlink != 1:
        raise AuthorityStorageError("Authority requires private owned directories and regular files")
    return info


def _authority_path(path: Path) -> Path:
    if path.name.endswith(_SIDECAR_SUFFIXES):
        raise AuthorityStorageError("Authority filename cannot use a SQLite sidecar suffix")
    return path.absolute()


class AuthorityStorage:
    """Short durable transactions against one existing deployment-owned inode."""

    def __init__(self, path: Path, deployment_id: str, *, version: AuthorityVersion = AuthorityVersion.V1) -> None:
        if type(version) is not AuthorityVersion:
            raise AuthorityStorageError("Unsupported authority version")
        self.version = version
        self.path = _authority_path(path)
        self.deployment_id = deployment_id
        try:
            _private(self.path.parent, directory=True)
            info = _private(self.path, directory=False)
            self._identity = (info.st_dev, info.st_ino)
            with self.connection() as db:
                if db.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                    raise AuthorityStorageError("Authority database integrity check failed")
        except (OSError, sqlite3.Error) as error:
            raise AuthorityStorageError("Cannot open existing authority") from error

    @staticmethod
    def provision(path: Path, deployment_id: str, *, version: AuthorityVersion = AuthorityVersion.V1) -> None:
        """Initialize once; leave failed initialization quarantined, never reset it."""
        if type(version) is not AuthorityVersion:
            raise AuthorityStorageError("Unsupported authority version")
        path = _authority_path(path)
        try:
            _private(path.parent, directory=True)
            if any(os.path.lexists(str(path) + suffix) for suffix in _SIDECAR_SUFFIXES):
                raise AuthorityStorageError("Authority sidecar namespace is occupied; preserve existing files")
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
            os.close(descriptor)
            db = sqlite3.connect(path, isolation_level=None)
            try:
                db.execute("PRAGMA journal_mode=WAL")
                db.execute("PRAGMA synchronous=FULL")
                db.execute("PRAGMA foreign_keys=ON")
                db.executescript("BEGIN IMMEDIATE;" + schema_sql(version))
                db.execute("INSERT INTO authority_metadata VALUES (1,?,?)", (version.value, deployment_id))
                db.commit()
            finally:
                db.close()
            descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        except (OSError, sqlite3.Error) as error:
            raise AuthorityStorageError("Create-once authority provisioning failed") from error

    def _check_identity(self) -> None:
        _private(self.path.parent, directory=True)
        info = _private(self.path, directory=False)
        if (info.st_dev, info.st_ino) != self._identity:
            raise AuthorityStorageError("Authority file was replaced; automatic recovery is prohibited")
        for suffix in ("-wal", "-shm"):
            sidecar = Path(str(self.path) + suffix)
            if sidecar.exists() or sidecar.is_symlink():
                _private(sidecar, directory=False)

    def execution_identity(self) -> AuthorityStorageIdentity:
        """Verify the existing database before deriving its local exclusion key."""
        with self.connection():
            return AuthorityStorageIdentity(str(self.path), *self._identity, self.deployment_id)

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
                raise AuthorityStorageError("Required authority durability settings unavailable")
            if db.execute("SELECT schema_version,deployment FROM authority_metadata").fetchall() != [
                (self.version.value, self.deployment_id)
            ]:
                raise AuthorityStorageError("Authority schema or deployment mismatch")
            yield db
        except (OSError, sqlite3.Error) as error:
            raise AuthorityStorageError("Authority storage unavailable") from error
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
