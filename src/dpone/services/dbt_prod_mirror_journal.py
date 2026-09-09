"""Durable recovery journal for one prod-mirror filesystem transaction."""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import shutil
import stat
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from dpone.contracts.dbt_promotion import DBT_MIRROR_TRANSACTION_DIRECTORY
from dpone.services.dbt_prod_promotion_contract import DbtProdMirrorError

TRANSACTION_DIRECTORY = DBT_MIRROR_TRANSACTION_DIRECTORY
_JOURNAL_FILENAME = "journal.json"
_JOURNAL_SCHEMA = "dpone.dbt-prod-mirror-journal.v1"
_MAX_JOURNAL_BYTES = 64 * 1024
_STATES = frozenset({"staged", "backed_up", "installing", "installed"})


@dataclass(slots=True)
class MirrorReplacement:
    """One journaled destination and its staged/backup paths."""

    destination: Path
    staged: Path
    backup: Path
    had_previous: bool
    state: str = "staged"


class DbtProdMirrorJournal:
    """Persist state before and after every non-idempotent rename."""

    def __init__(
        self,
        *,
        repository_root: Path,
        transaction_root: Path,
        replacements: list[MirrorReplacement],
    ) -> None:
        self._root = repository_root
        self._transaction_root = transaction_root
        self._replacements = replacements

    @classmethod
    def begin(
        cls,
        repository_root: Path,
    ) -> DbtProdMirrorJournal:
        transaction_root = repository_root / TRANSACTION_DIRECTORY
        try:
            transaction_root.mkdir(mode=0o700)
        except FileExistsError as exc:
            raise DbtProdMirrorError("stale prod promotion transaction was not recovered") from exc
        fsync_directory(repository_root)
        journal = cls(
            repository_root=repository_root,
            transaction_root=transaction_root,
            replacements=[],
        )
        journal._write()
        return journal

    @property
    def transaction_root(self) -> Path:
        return self._transaction_root

    def set_replacements(self, replacements: list[MirrorReplacement]) -> None:
        if not replacements:
            raise DbtProdMirrorError("prod promotion transaction has no replacements")
        self._replacements[:] = replacements
        self._write()

    def mark(self, index: int, state: str) -> None:
        if state not in _STATES:
            raise ValueError("prod mirror journal state is invalid")
        self._replacements[index].state = state
        self._write()

    def rollback(self) -> None:
        _rollback_replacements(self._root, self._transaction_root, self._replacements)
        self.cleanup()

    def cleanup(self) -> None:
        _remove_transaction_root(self._transaction_root)
        fsync_directory(self._root)

    def _write(self) -> None:
        payload = {
            "schema": _JOURNAL_SCHEMA,
            "replacements": [
                {
                    "destination": item.destination.relative_to(self._root).as_posix(),
                    "staged": item.staged.relative_to(self._transaction_root).as_posix(),
                    "backup": item.backup.relative_to(self._transaction_root).as_posix(),
                    "had_previous": item.had_previous,
                    "state": item.state,
                }
                for item in self._replacements
            ],
        }
        encoded = (json.dumps(payload, allow_nan=False, separators=(",", ":"), sort_keys=True) + "\n").encode()
        temporary = self._transaction_root / ".journal.new"
        journal_path = self._transaction_root / _JOURNAL_FILENAME
        descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            pending = memoryview(encoded)
            while pending:
                written = os.write(descriptor, pending)
                if written <= 0:
                    raise OSError("prod promotion journal write failed")
                pending = pending[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, journal_path)
        fsync_directory(self._transaction_root)


@contextmanager
def serialized_prod_mirror(repository_root: Path) -> Iterator[None]:
    """Serialize a repository transaction and recover an interrupted predecessor."""

    with locked_prod_mirror(repository_root):
        recover_interrupted_prod_mirror(repository_root)
        yield


@contextmanager
def readable_prod_mirror(repository_root: Path) -> Iterator[None]:
    """Hold the writer lock without repository writes; refuse pending recovery."""

    with locked_prod_mirror(repository_root):
        if os.path.lexists(repository_root / TRANSACTION_DIRECTORY):
            raise DbtProdMirrorError("prod promotion transaction is pending; recover it before verification")
        yield


@contextmanager
def locked_prod_mirror(repository_root: Path) -> Iterator[None]:
    """Acquire the shared exclusive lock without reading or recovering a journal."""

    lock_path = _lock_path(repository_root)
    try:
        file_lock = importlib.import_module("fcntl")
    except ImportError as exc:
        raise DbtProdMirrorError("prod promotion filesystem locking is unavailable") from exc
    descriptor = os.open(
        lock_path,
        os.O_CREAT | os.O_CLOEXEC | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        try:
            file_lock.flock(descriptor, file_lock.LOCK_EX | file_lock.LOCK_NB)
        except BlockingIOError as exc:
            raise DbtProdMirrorError("another prod promotion transaction is active") from exc
        yield
    finally:
        try:
            file_lock.flock(descriptor, file_lock.LOCK_UN)
        finally:
            os.close(descriptor)


def recover_interrupted_prod_mirror(repository_root: Path) -> None:
    """Restore pre-transaction bytes from a bounded, project-confined journal."""

    transaction_root = repository_root / TRANSACTION_DIRECTORY
    if not os.path.lexists(transaction_root):
        return
    metadata = transaction_root.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise DbtProdMirrorError("prod promotion transaction journal is unsafe")
    replacements = _read_replacements(repository_root, transaction_root)
    _rollback_replacements(repository_root, transaction_root, replacements)
    _remove_transaction_root(transaction_root)
    fsync_directory(repository_root)


def _read_replacements(
    repository_root: Path,
    transaction_root: Path,
) -> list[MirrorReplacement]:
    journal_path = transaction_root / _JOURNAL_FILENAME
    try:
        metadata = journal_path.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            raise OSError("journal is not a regular file")
        if metadata.st_size > _MAX_JOURNAL_BYTES:
            raise OSError("journal exceeds limit")
        payload = json.loads(
            journal_path.read_bytes(),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise DbtProdMirrorError("prod promotion recovery journal is invalid") from exc
    if not isinstance(payload, dict) or set(payload) != {"schema", "replacements"}:
        raise DbtProdMirrorError("prod promotion recovery journal is invalid")
    if payload["schema"] != _JOURNAL_SCHEMA or not isinstance(payload["replacements"], list):
        raise DbtProdMirrorError("prod promotion recovery journal is invalid")
    replacements = [
        _replacement_from_record(repository_root, transaction_root, record) for record in payload["replacements"]
    ]
    identities = [(item.destination, item.staged, item.backup) for item in replacements]
    if len(replacements) > 3 or len({item for triple in identities for item in triple}) != len(identities) * 3:
        raise DbtProdMirrorError("prod promotion recovery journal is invalid")
    return replacements


def _replacement_from_record(
    repository_root: Path,
    transaction_root: Path,
    record: object,
) -> MirrorReplacement:
    required = {"destination", "staged", "backup", "had_previous", "state"}
    if not isinstance(record, dict) or set(record) != required:
        raise DbtProdMirrorError("prod promotion recovery journal is invalid")
    if not isinstance(record["had_previous"], bool) or record["state"] not in _STATES:
        raise DbtProdMirrorError("prod promotion recovery journal is invalid")
    return MirrorReplacement(
        destination=_confined_path(repository_root, record["destination"]),
        staged=_confined_path(transaction_root, record["staged"]),
        backup=_confined_path(transaction_root, record["backup"]),
        had_previous=record["had_previous"],
        state=record["state"],
    )


def _rollback_replacements(
    repository_root: Path,
    transaction_root: Path,
    replacements: list[MirrorReplacement],
) -> None:
    for item in reversed(replacements):
        backup_exists = os.path.lexists(item.backup)
        if backup_exists:
            _remove_destination(item.destination)
            os.replace(item.backup, item.destination)
            fsync_directory(item.destination.parent)
        elif item.state in {"backed_up", "installing", "installed"} and item.had_previous:
            raise DbtProdMirrorError("prod promotion recovery backup is missing")
        elif item.state in {"installing", "installed"} and not item.had_previous:
            _remove_destination(item.destination)
            fsync_directory(item.destination.parent)
        elif item.state == "backed_up" and not item.had_previous:
            _remove_destination(item.destination)
            fsync_directory(item.destination.parent)
        _require_confined(repository_root, item.destination)
        _require_confined(transaction_root, item.staged)


def _remove_destination(path: Path) -> None:
    if not os.path.lexists(path):
        return
    metadata = path.lstat()
    if stat.S_ISDIR(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode):
        shutil.rmtree(path)
    else:
        path.unlink()


def _remove_transaction_root(path: Path) -> None:
    if not os.path.lexists(path):
        return
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise DbtProdMirrorError("prod promotion transaction journal is unsafe")
    shutil.rmtree(path)


def _confined_path(root: Path, value: object) -> Path:
    if not isinstance(value, str) or not value or "\\" in value:
        raise DbtProdMirrorError("prod promotion recovery journal is invalid")
    relative = Path(value)
    if relative.is_absolute() or ".." in relative.parts:
        raise DbtProdMirrorError("prod promotion recovery journal is invalid")
    candidate = root.joinpath(*relative.parts)
    _require_confined(root, candidate)
    return candidate


def _require_confined(root: Path, candidate: Path) -> None:
    try:
        candidate.resolve(strict=False).relative_to(root.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise DbtProdMirrorError("prod promotion recovery path escapes its root") from exc


def _lock_path(repository_root: Path) -> Path:
    identity = hashlib.sha256(os.fsencode(repository_root.resolve(strict=True))).hexdigest()
    return Path(tempfile.gettempdir()) / f"dpone-dbt-promotion-{identity}.lock"


def _unique_object(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant {value}")


def fsync_directory(directory: Path) -> None:
    """Durably persist directory metadata or fail the transaction closed."""

    try:
        descriptor = os.open(
            directory,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
    except OSError as exc:
        raise DbtProdMirrorError("prod promotion directory durability is unavailable") from exc
    try:
        os.fsync(descriptor)
    except OSError as exc:
        raise DbtProdMirrorError("prod promotion directory durability could not be proven") from exc
    finally:
        os.close(descriptor)


__all__ = [
    "DbtProdMirrorJournal",
    "MirrorReplacement",
    "TRANSACTION_DIRECTORY",
    "fsync_directory",
    "locked_prod_mirror",
    "readable_prod_mirror",
    "recover_interrupted_prod_mirror",
    "serialized_prod_mirror",
]
