"""A verifier must not recover or mutate a repository while acquiring its lock."""

from pathlib import Path

import pytest

from dpone.services import dbt_prod_mirror_journal as journal
from dpone.services.dbt_prod_promotion_contract import DbtProdMirrorError


def test_lock_only_preserves_pending_recovery_bytes(tmp_path: Path) -> None:
    pending = journal.DbtProdMirrorJournal.begin(tmp_path)
    marker = pending.transaction_root / "staged-output"
    marker.write_bytes(b"uncommitted")
    before = _files(tmp_path)

    with journal.locked_prod_mirror(tmp_path):
        assert _files(tmp_path) == before

    assert _files(tmp_path) == before
    # Compatibility: the installer still recovers an interrupted transaction.
    with journal.serialized_prod_mirror(tmp_path):
        assert not pending.transaction_root.exists()


def test_read_lock_rejects_pending_journal_without_recovery(tmp_path: Path) -> None:
    pending = journal.DbtProdMirrorJournal.begin(tmp_path)
    (pending.transaction_root / "staged-output").write_bytes(b"uncommitted")
    before = _files(tmp_path)

    with pytest.raises(DbtProdMirrorError, match="pending.*recover"):
        with journal.readable_prod_mirror(tmp_path):
            pytest.fail("pending transaction was admitted for verification")

    assert _files(tmp_path) == before


@pytest.mark.parametrize("outer", ["locked_prod_mirror", "readable_prod_mirror", "serialized_prod_mirror"])
@pytest.mark.parametrize("inner", ["locked_prod_mirror", "readable_prod_mirror", "serialized_prod_mirror"])
def test_readers_and_writers_share_the_same_exclusive_lock(tmp_path: Path, outer: str, inner: str) -> None:
    with getattr(journal, outer)(tmp_path):
        with pytest.raises(DbtProdMirrorError, match="another.*active"):
            with getattr(journal, inner)(tmp_path):
                pytest.fail("competing owner acquired the lock")
    with getattr(journal, inner)(tmp_path):
        assert not list(tmp_path.iterdir())


def test_reader_releases_lock_after_failure(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="reader failed"):
        with journal.readable_prod_mirror(tmp_path):
            raise RuntimeError("reader failed")
    with journal.serialized_prod_mirror(tmp_path):
        assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("kind", ["file", "directory", "symlink"])
def test_reader_rejects_any_pending_control_path_without_following_it(tmp_path: Path, kind: str) -> None:
    path = tmp_path / journal.TRANSACTION_DIRECTORY
    if kind == "directory":
        path.mkdir()
    elif kind == "file":
        path.write_bytes(b"invalid journal")
    else:
        path.symlink_to(tmp_path / "missing")
    metadata = path.lstat()
    with pytest.raises(DbtProdMirrorError, match="pending.*recover"):
        with journal.readable_prod_mirror(tmp_path):
            pytest.fail("unsafe control path admitted")
    assert path.lstat() == metadata


def _files(root: Path) -> dict[str, bytes]:
    return {path.relative_to(root).as_posix(): path.read_bytes() for path in root.rglob("*") if path.is_file()}
