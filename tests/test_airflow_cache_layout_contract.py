from __future__ import annotations

import json
import stat
from pathlib import Path

import pytest
from dpone_airflow_pack import cache_layout as cache_layout_module
from dpone_airflow_pack.cache_layout import (
    EXACT_DEPLOYMENT_LAYOUT,
    LAYOUT_MARKER_NAME,
    LAYOUT_SCHEMA,
    LEGACY_PACK_INDEX_LAYOUT,
    CacheLayoutError,
    ensure_cache_layout,
)


def test_cache_layout_marker_is_stable_and_idempotent(tmp_path: Path) -> None:
    root = tmp_path / "cache"

    first = ensure_cache_layout(root, expected_layout=LEGACY_PACK_INDEX_LAYOUT)
    second = ensure_cache_layout(root, expected_layout=LEGACY_PACK_INDEX_LAYOUT)

    assert (
        first
        == second
        == {
            "schema": LAYOUT_SCHEMA,
            "layout": LEGACY_PACK_INDEX_LAYOUT,
            "version": "1",
        }
    )
    assert json.loads((root / LAYOUT_MARKER_NAME).read_text(encoding="utf-8")) == first


def test_cache_layout_marker_fsync_failure_is_structured_and_cleans_temporary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "cache"

    def fail_fsync(_descriptor: int) -> None:
        raise OSError("private fsync detail")

    monkeypatch.setattr(cache_layout_module, "_OS_FSYNC", fail_fsync)

    with pytest.raises(CacheLayoutError) as exc:
        ensure_cache_layout(root, expected_layout=EXACT_DEPLOYMENT_LAYOUT)

    assert exc.value.code == "DPONE_CACHE_LAYOUT_WRITE_FAILED"
    assert "private fsync detail" not in str(exc.value)
    assert not (root / LAYOUT_MARKER_NAME).exists()
    assert list(root.glob(".*.tmp")) == []


def test_legacy_writer_cannot_mutate_exact_cache_root(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    activation = root / "activations" / "dev" / ("sha256-" + "a" * 64)
    activation.mkdir(parents=True)
    (root / "current").symlink_to(activation.relative_to(root), target_is_directory=True)

    with pytest.raises(CacheLayoutError) as exc:
        ensure_cache_layout(root, expected_layout=LEGACY_PACK_INDEX_LAYOUT)

    assert exc.value.code == "DPONE_CACHE_LAYOUT_MISMATCH"
    assert (root / "current").is_symlink()
    assert not (root / LAYOUT_MARKER_NAME).exists()


def test_exact_writer_cannot_mutate_legacy_cache_root(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    (root / "generations" / "abc123").mkdir(parents=True)
    (root / "current").write_text("abc123", encoding="utf-8")

    with pytest.raises(CacheLayoutError) as exc:
        ensure_cache_layout(root, expected_layout=EXACT_DEPLOYMENT_LAYOUT)

    assert exc.value.code == "DPONE_CACHE_LAYOUT_MISMATCH"
    assert (root / "current").read_text(encoding="utf-8") == "abc123"


def test_ambiguous_unmarked_cache_root_fails_closed(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    (root / "generations").mkdir(parents=True)
    (root / "deployments").mkdir()

    with pytest.raises(CacheLayoutError) as exc:
        ensure_cache_layout(root, expected_layout=EXACT_DEPLOYMENT_LAYOUT)

    assert exc.value.code == "DPONE_CACHE_LAYOUT_AMBIGUOUS"


def test_declared_layout_rejects_later_cross_layout_contamination(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    ensure_cache_layout(root, expected_layout=LEGACY_PACK_INDEX_LAYOUT)
    (root / "deployments").mkdir()

    with pytest.raises(CacheLayoutError) as exc:
        ensure_cache_layout(root, expected_layout=LEGACY_PACK_INDEX_LAYOUT)

    assert exc.value.code == "DPONE_CACHE_LAYOUT_MISMATCH"


def test_exact_promotion_lock_rejects_legacy_root_before_mutation(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache_common import DeploymentCacheError, promotion_lock

    root = tmp_path / "cache"
    (root / "generations" / "abc123").mkdir(parents=True)
    (root / "current").write_text("abc123", encoding="utf-8")

    with pytest.raises(DeploymentCacheError) as exc:
        with promotion_lock(root):
            pass

    assert exc.value.code == "DPONE_CACHE_LAYOUT_MISMATCH"
    assert (root / "current").read_text(encoding="utf-8") == "abc123"
    assert not (root / ".promotion.lock").exists()


def test_legacy_sync_rejects_exact_root_before_control_file_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack import cache_sync as cache_sync_module
    from dpone_airflow_pack.cache_sync import AirflowPackSyncOptions, sync_airflow_pack_cache

    root = tmp_path / "cache"
    ensure_cache_layout(root, expected_layout=EXACT_DEPLOYMENT_LAYOUT)
    marker = root / LAYOUT_MARKER_NAME
    marker_mode = stat.S_IMODE(marker.stat().st_mode)
    monkeypatch.setattr(
        cache_sync_module,
        "ArtifactReader",
        lambda **_: pytest.fail("remote reader must not be created for a mismatched layout"),
    )

    with pytest.raises(CacheLayoutError) as exc:
        sync_airflow_pack_cache(
            AirflowPackSyncOptions(
                index_uri="s3://bucket/latest/pack-index.json",
                cache_dir=root,
            )
        )

    assert exc.value.code == "DPONE_CACHE_LAYOUT_MISMATCH"
    assert {entry.name for entry in root.iterdir()} == {LAYOUT_MARKER_NAME}
    assert stat.S_IMODE(marker.stat().st_mode) == marker_mode


def test_cache_layout_rejects_symlink_root_without_touching_target(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    configured = tmp_path / "cache"
    configured.symlink_to(target, target_is_directory=True)

    with pytest.raises(CacheLayoutError) as exc:
        ensure_cache_layout(configured, expected_layout=LEGACY_PACK_INDEX_LAYOUT)

    assert exc.value.code == "DPONE_CACHE_LAYOUT_INVALID"
    assert list(target.iterdir()) == []
