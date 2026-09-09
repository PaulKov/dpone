"""Ownership-aware shared cache directory contracts for PVC mount roots."""

from __future__ import annotations

import errno
import os
import stat
from pathlib import Path

import pytest
from dpone_airflow_pack import cache_permissions
from dpone_airflow_pack.cache_layout import EXACT_DEPLOYMENT_LAYOUT, ensure_cache_layout
from dpone_airflow_pack.cache_permissions import (
    SHARED_DIRECTORY_MODE,
    ForeignDirectoryContract,
    directory_meets_mode_contract,
    ensure_directory_mode,
    ensure_private_directory,
    ensure_shared_directory,
    ensure_shared_work_directory,
)


def test_minimum_contract_accepts_required_bits_with_extra_world_write() -> None:
    assert directory_meets_mode_contract(
        0o2777,
        SHARED_DIRECTORY_MODE,
        foreign_contract=ForeignDirectoryContract.MINIMUM,
    )
    assert not directory_meets_mode_contract(
        0o0755,
        SHARED_DIRECTORY_MODE,
        foreign_contract=ForeignDirectoryContract.MINIMUM,
    )


def test_exact_contract_rejects_shared_superset_for_private_trees() -> None:
    assert not directory_meets_mode_contract(
        0o2777,
        0o0700,
        foreign_contract=ForeignDirectoryContract.EXACT,
    )


def test_directory_meets_mode_contract_rejects_unknown_contract() -> None:
    with pytest.raises(ValueError, match="unsupported foreign directory contract"):
        directory_meets_mode_contract(
            0o0700,
            0o0700,
            foreign_contract="exact",  # type: ignore[arg-type]
        )


def test_ensure_shared_directory_accepts_foreign_owned_superset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "cache"
    root.mkdir()
    os.chmod(root, 0o2777)
    monkeypatch.setattr(cache_permissions, "_inode_owned_by_effective_user", lambda _metadata: False)

    def fail_if_called(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("foreign-owned shared roots must not be chmod'd")

    monkeypatch.setattr(os, "fchmod", fail_if_called)

    ensure_shared_directory(root)

    assert stat.S_IMODE(root.stat().st_mode) == 0o2777


def test_ensure_cache_layout_succeeds_on_foreign_owned_shared_superset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "cache"
    root.mkdir()
    os.chmod(root, 0o2777)
    monkeypatch.setattr(cache_permissions, "_inode_owned_by_effective_user", lambda _metadata: False)

    payload = ensure_cache_layout(root, expected_layout=EXACT_DEPLOYMENT_LAYOUT)

    assert payload["layout"] == EXACT_DEPLOYMENT_LAYOUT
    assert (root / ".dpone-cache-layout.json").is_file()


def test_ensure_shared_directory_fails_closed_when_foreign_root_missing_bits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "cache"
    root.mkdir()
    os.chmod(root, 0o0755)
    monkeypatch.setattr(cache_permissions, "_inode_owned_by_effective_user", lambda _metadata: False)

    with pytest.raises(OSError) as exc:
        ensure_shared_directory(root)

    assert exc.value.errno == errno.EPERM
    assert "minimum contract" in str(exc.value)


def test_ensure_private_directory_rejects_foreign_owned_world_writable_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "private"
    root.mkdir()
    os.chmod(root, 0o2777)
    monkeypatch.setattr(cache_permissions, "_inode_owned_by_effective_user", lambda _metadata: False)

    with pytest.raises(OSError) as exc:
        ensure_private_directory(root)

    assert exc.value.errno == errno.EPERM
    assert "exact contract" in str(exc.value)


def test_ensure_shared_work_directory_rejects_foreign_owned_world_writable_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "work"
    root.mkdir()
    os.chmod(root, 0o2777)
    monkeypatch.setattr(cache_permissions, "_inode_owned_by_effective_user", lambda _metadata: False)

    with pytest.raises(OSError) as exc:
        ensure_shared_work_directory(root)

    assert exc.value.errno == errno.EPERM
    assert "exact contract" in str(exc.value)


def test_ensure_directory_mode_rejects_foreign_owned_world_writable_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "generic"
    root.mkdir()
    os.chmod(root, 0o2777)
    monkeypatch.setattr(cache_permissions, "_inode_owned_by_effective_user", lambda _metadata: False)

    with pytest.raises(OSError) as exc:
        ensure_directory_mode(root, 0o2775)

    assert exc.value.errno == errno.EPERM
    assert "exact contract" in str(exc.value)


def test_owned_shared_directory_is_normalized_to_exact_mode(tmp_path: Path) -> None:
    root = tmp_path / "owned"
    root.mkdir()
    os.chmod(root, 0o0755)

    ensure_shared_directory(root)

    assert stat.S_IMODE(root.stat().st_mode) == SHARED_DIRECTORY_MODE
