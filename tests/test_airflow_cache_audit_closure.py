from __future__ import annotations

import hashlib
import json
import os
import warnings
from collections.abc import Callable
from pathlib import Path

import pytest
from dpone_airflow_pack import (
    cache_generation_files,
    cache_generation_lease,
    cache_generation_store,
    cli_cache_status,
    cli_sync,
)
from dpone_airflow_pack.artifact_store import ArtifactReader, ArtifactStoreError
from dpone_airflow_pack.cache_activation_contract import cache_write_lease
from dpone_airflow_pack.cache_generation_preparation import prepared_generation
from dpone_airflow_pack.cache_generation_retention import enforce_cache_retention
from dpone_airflow_pack.cache_generation_store import (
    GENERATION_RECEIPT_NAME,
    PENDING_COMMIT_PATH,
    STAGE_MARKER_NAME,
    commit_generation,
    create_generation_stage,
    finalize_generation_stage,
    inspect_existing_generation,
    read_current_commit,
    recover_interrupted_commit,
)
from dpone_airflow_pack.cache_layout import EXACT_DEPLOYMENT_LAYOUT, LEGACY_PACK_INDEX_LAYOUT, ensure_cache_layout
from dpone_airflow_pack.cache_status import read_airflow_pack_cache_status
from dpone_airflow_pack.cache_sync import AirflowPackSyncOptions, sync_airflow_pack_cache
from dpone_airflow_pack.dag_spec_cache_paths import pinned_dag_specs
from dpone_airflow_pack.dag_spec_loader import load_dag_spec_file
from dpone_airflow_pack.pack_storage_consumer import STORAGE_MODE_ENV


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


@pytest.mark.parametrize(
    "operation",
    [
        lambda path: cache_generation_files.tree_inventory(path, ignored_names=frozenset()),
        cache_generation_files.tree_metadata_digest,
        cache_generation_files.seal_tree,
    ],
)
def test_generation_authority_walkers_fail_closed_on_unreadable_subtree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: Callable[[Path], object],
) -> None:
    generation = tmp_path / "generation"
    generation.mkdir()

    def denied_walk(*_args: object, **kwargs: object) -> object:
        onerror = kwargs.get("onerror")
        assert callable(onerror)
        onerror(PermissionError("subtree denied"))
        return iter(())

    monkeypatch.setattr(cache_generation_files.os, "walk", denied_walk)

    with pytest.raises(PermissionError, match="subtree denied"):
        operation(generation)


def _remote_index(root: Path, generation: str) -> Path:
    pack = b'{"kind":"gitops.airflow_pack"}'
    spec = b'{"kind":"gitops.airflow_dag_spec","dag_id":"DAG__demo"}'
    pack_path = root / generation / "orders" / "airflow-pack.json"
    spec_path = root / generation / "airflow" / "_dags" / "DAG__demo.dag-spec.json"
    pack_path.parent.mkdir(parents=True)
    spec_path.parent.mkdir(parents=True)
    pack_path.write_bytes(pack)
    spec_path.write_bytes(spec)
    index = {
        "git_sha": generation,
        "artifacts": {
            "orders": {
                "path": "orders/airflow-pack.json",
                "uri": pack_path.as_uri(),
                "sha256": _sha256(pack),
                "bytes": len(pack),
            }
        },
        "dag_specs": {
            "DAG__demo": {
                "path": "airflow/_dags/DAG__demo.dag-spec.json",
                "uri": spec_path.as_uri(),
                "sha256": _sha256(spec),
                "bytes": len(spec),
            }
        },
    }
    latest = root / "latest" / "pack-index.json"
    latest.parent.mkdir(parents=True)
    latest.write_text(json.dumps(index), encoding="utf-8")
    return latest


def _sync(root: Path, cache: Path, generation: str) -> None:
    result = sync_airflow_pack_cache(
        AirflowPackSyncOptions(index_uri=_remote_index(root, generation).as_uri(), cache_dir=cache)
    )
    assert result["status"] == "success"


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFO contract is POSIX-only")
def test_local_artifact_reader_rejects_fifo_without_blocking(tmp_path: Path) -> None:
    fifo = tmp_path / "artifact.json"
    os.mkfifo(fifo)

    with pytest.raises(ArtifactStoreError, match="regular file"):
        ArtifactReader().read_bytes(fifo.as_uri(), max_bytes=1024)


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFO contract is POSIX-only")
def test_cache_status_rejects_fifo_current_without_blocking(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / ".promotion.lock").touch()
    os.mkfifo(cache / "current")

    status = read_airflow_pack_cache_status(cache)

    assert status["status"] == "blocked"
    assert status["blockers"][0]["code"] == "DPONE_CACHE_STATUS_READ_FAILED"


def test_cache_status_cli_returns_json_when_existing_root_has_no_lock(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()

    result = cli_cache_status.main(["--cache-dir", str(cache), "--json"])

    captured = capsys.readouterr()
    assert result == 1
    assert json.loads(captured.out)["status"] == "blocked"
    assert captured.err == ""


def test_dangling_pending_commit_is_fail_visible(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    ensure_cache_layout(cache, expected_layout=LEGACY_PACK_INDEX_LAYOUT)
    pending = cache / PENDING_COMMIT_PATH
    pending.parent.mkdir(parents=True, exist_ok=True)
    pending.symlink_to("missing-receipt.json")

    with cache_write_lease(cache):
        with pytest.raises((OSError, ValueError)):
            recover_interrupted_commit(cache)

    assert os.path.lexists(pending)


def test_dangling_generation_receipt_is_not_treated_as_unmanaged(tmp_path: Path) -> None:
    generation = tmp_path / "generations" / ("a" * 40)
    generation.mkdir(parents=True)
    (generation / "pack-index.json").write_text("{}", encoding="utf-8")
    (generation / GENERATION_RECEIPT_NAME).symlink_to("missing-receipt.json")

    with pytest.raises(ValueError, match="existing generation receipt is invalid"):
        inspect_existing_generation(generation)


def test_cli_diagnostic_failure_does_not_mask_sync_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(cli_sync, "sync_airflow_pack_cache", lambda _options: (_ for _ in ()).throw(ValueError("x")))
    monkeypatch.setattr(
        cli_sync, "write_sync_warning", lambda *_args, **_kwargs: (_ for _ in ()).throw(PermissionError)
    )

    result = cli_sync.main(["--once", "--index-uri", "file:///missing", "--cache-dir", str(tmp_path / "cache")])

    assert result == 1
    assert "Airflow pack sync failed" in capsys.readouterr().err


def test_stage_lease_release_failure_preserves_body_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import fcntl

    stage = create_generation_stage(tmp_path / "cache", generation="a" * 40)

    class _FailingUnlock:
        LOCK_SH = fcntl.LOCK_SH
        LOCK_UN = fcntl.LOCK_UN

        @staticmethod
        def flock(descriptor: int, operation: int) -> None:
            if operation == fcntl.LOCK_UN:
                raise OSError("injected unlock failure")
            fcntl.flock(descriptor, operation)

    monkeypatch.setattr(cache_generation_lease, "import_module", lambda _name: _FailingUnlock)
    with pytest.warns(RuntimeWarning, match="STAGE_LEASE_RELEASE_WARNING"):
        with pytest.raises(ValueError, match="primary failure"):
            with cache_generation_lease.generation_stage_lease(stage, marker_name=STAGE_MARKER_NAME):
                raise ValueError("primary failure")


def test_stage_lease_release_warning_cannot_mask_body_under_werror(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import fcntl

    stage = create_generation_stage(tmp_path / "cache", generation="b" * 40)

    class _FailingUnlock:
        LOCK_SH = fcntl.LOCK_SH
        LOCK_UN = fcntl.LOCK_UN

        @staticmethod
        def flock(descriptor: int, operation: int) -> None:
            if operation == fcntl.LOCK_UN:
                raise OSError("injected unlock failure")
            fcntl.flock(descriptor, operation)

    monkeypatch.setattr(cache_generation_lease, "import_module", lambda _name: _FailingUnlock)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        with pytest.raises(ValueError, match="primary failure"):
            with cache_generation_lease.generation_stage_lease(
                stage,
                marker_name=STAGE_MARKER_NAME,
            ):
                raise ValueError("primary failure")


def test_corrupt_stage_marker_during_cleanup_preserves_primary_failure(tmp_path: Path) -> None:
    remote = tmp_path / "remote"
    cache = tmp_path / "cache"
    generation = "a" * 40
    index_path = _remote_index(remote, generation)
    index_bytes = index_path.read_bytes()
    preparation = None

    with pytest.raises(RuntimeError, match="primary failure"):
        with prepared_generation(
            options=AirflowPackSyncOptions(index_uri=index_path.as_uri(), cache_dir=cache),
            reader=ArtifactReader(),
            index_bytes=index_bytes,
            index=json.loads(index_bytes),
            generation=generation,
        ) as prepared:
            preparation = prepared
            marker = prepared.candidate.path / STAGE_MARKER_NAME
            marker.chmod(0o600)
            marker.write_text("{}", encoding="utf-8")
            raise RuntimeError("primary failure")

    assert preparation is not None
    assert "cache_stage_cleanup_failed" in preparation.warnings
    assert preparation.candidate.path.exists()


def test_cli_failure_does_not_mutate_exact_cache_root(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    ensure_cache_layout(cache, expected_layout=EXACT_DEPLOYMENT_LAYOUT)
    before = {path.relative_to(cache).as_posix(): path.read_bytes() for path in cache.rglob("*") if path.is_file()}

    with pytest.warns(RuntimeWarning, match="EXTERNAL_WARNING_UNPUBLISHED"):
        result = cli_sync.main(["--once", "--index-uri", "file:///missing", "--cache-dir", str(cache)])

    after = {path.relative_to(cache).as_posix(): path.read_bytes() for path in cache.rglob("*") if path.is_file()}
    assert result == 1
    assert after == before


def test_corrupt_active_generation_blocks_retention_of_last_known_good(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    first = "a" * 40
    second = "b" * 40
    _sync(tmp_path / "remote-a", cache, first)
    _sync(tmp_path / "remote-b", cache, second)
    spec = cache / "generations" / second / "airflow" / "_dags" / "DAG__demo.dag-spec.json"
    spec.chmod(0o640)
    spec.write_bytes(spec.read_bytes() + b"corrupt")

    result = enforce_cache_retention(
        cache,
        keep_generations=1,
        max_total_bytes=512 * 1024 * 1024,
        high_watermark_pct=80,
        low_watermark_pct=60,
        partial_download_ttl_minutes=30,
    )

    assert result.blockers
    assert {path.name for path in (cache / "generations").iterdir()} == {first, second}


def test_managed_dag_spec_is_verified_against_pack_index(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = tmp_path / "cache"
    generation = "a" * 40
    _sync(tmp_path / "remote", cache, generation)
    spec_path = cache / "generations" / generation / "airflow" / "_dags" / "DAG__demo.dag-spec.json"
    spec_path.chmod(0o640)
    spec_path.write_bytes(spec_path.read_bytes() + b"corrupt")
    monkeypatch.setenv("DPONE_AIRFLOW_PACK_CACHE_DIR", str(cache))
    monkeypatch.setenv(STORAGE_MODE_ENV, "remote")

    with pinned_dag_specs(tmp_path) as specs:
        payload, issues = load_dag_spec_file(
            specs[0].path,
            expected_sha256=specs[0].expected_sha256,
            expected_bytes=specs[0].expected_bytes,
            confined_root=specs[0].confined_root,
        )

    assert payload is None
    assert issues[0].code in {"DPONE_CACHE_ARTIFACT_SIZE_MISMATCH", "DPONE_CACHE_CHECKSUM_MISMATCH"}


def test_finalized_stage_metadata_is_rechecked_immediately_before_commit(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    with cache_write_lease(cache):
        ensure_cache_layout(cache, expected_layout=LEGACY_PACK_INDEX_LAYOUT)
    stage = create_generation_stage(cache, generation="a" * 40)
    index_path = stage / "pack-index.json"
    index_path.write_text(json.dumps({"git_sha": "a" * 40}), encoding="utf-8")
    candidate = finalize_generation_stage(stage, generation="a" * 40)
    index_path.chmod(0o640)
    index_path.write_bytes(index_path.read_bytes() + b"corrupt")

    with cache_write_lease(cache):
        with pytest.raises(ValueError, match="generation changed before commit"):
            commit_generation(
                cache,
                candidate=candidate,
                expected_commit_id=None,
                existing=None,
                committed_at="2026-08-03T00:00:00Z",
            )

    assert not (cache / "current").exists()


def test_commit_lease_rehashes_candidate_payload_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dpone_airflow_pack.cache_generation_store as generation_store

    cache = tmp_path / "cache"
    with cache_write_lease(cache):
        ensure_cache_layout(cache, expected_layout=LEGACY_PACK_INDEX_LAYOUT)
    stage = create_generation_stage(cache, generation="a" * 40)
    (stage / "pack-index.json").write_text(json.dumps({"git_sha": "a" * 40}), encoding="utf-8")
    candidate = finalize_generation_stage(stage, generation="a" * 40)

    original_verify = generation_store.verify_generation_receipt
    calls = 0

    def tracked_rehash(*args: object, **kwargs: object) -> object:
        nonlocal calls
        calls += 1
        return original_verify(*args, **kwargs)

    monkeypatch.setattr(generation_store, "verify_generation_receipt", tracked_rehash)
    with cache_write_lease(cache):
        receipt, committed = commit_generation(
            cache,
            candidate=candidate,
            expected_commit_id=None,
            existing=None,
            committed_at="2026-08-03T00:00:00Z",
        )

    assert committed is True
    assert receipt.generation == "a" * 40
    assert calls == 3


def test_commit_rejects_stage_replaced_after_prepublication_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import shutil

    import dpone_airflow_pack.cache_generation_store as generation_store

    cache = tmp_path / "cache"
    with cache_write_lease(cache):
        ensure_cache_layout(cache, expected_layout=LEGACY_PACK_INDEX_LAYOUT)
    stage = create_generation_stage(cache, generation="a" * 40)
    (stage / "pack-index.json").write_text(json.dumps({"git_sha": "a" * 40}), encoding="utf-8")
    candidate = finalize_generation_stage(stage, generation="a" * 40)
    reviewed = stage.with_name(stage.name + ".reviewed")
    original_verify = generation_store._verify_candidate

    def replace_after_verification(value: object) -> None:
        original_verify(value)
        stage.rename(reviewed)
        shutil.copytree(reviewed, stage)

    monkeypatch.setattr(generation_store, "_verify_candidate", replace_after_verification)

    with cache_write_lease(cache):
        with pytest.raises(ValueError, match="published generation path was replaced"):
            commit_generation(
                cache,
                candidate=candidate,
                expected_commit_id=None,
                existing=None,
                committed_at="2026-08-03T00:00:00Z",
            )

    assert not (cache / "current").exists()
    assert reviewed.exists()


def test_commit_rejects_same_size_rewrite_with_restored_mtime(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    with cache_write_lease(cache):
        ensure_cache_layout(cache, expected_layout=LEGACY_PACK_INDEX_LAYOUT)
    stage = create_generation_stage(cache, generation="a" * 40)
    index_path = stage / "pack-index.json"
    index_path.write_text(json.dumps({"git_sha": "a" * 40}), encoding="utf-8")
    candidate = finalize_generation_stage(stage, generation="a" * 40)
    before = index_path.stat()
    original = index_path.read_bytes()
    replacement = original.replace(b"a", b"b", 1)
    assert len(replacement) == len(original)
    index_path.chmod(0o640)
    index_path.write_bytes(replacement)
    os.utime(index_path, ns=(before.st_atime_ns, before.st_mtime_ns))
    index_path.chmod(0o440)

    with cache_write_lease(cache):
        with pytest.raises(ValueError, match="generation changed before commit"):
            commit_generation(
                cache,
                candidate=candidate,
                expected_commit_id=None,
                existing=None,
                committed_at="2026-08-03T00:00:00Z",
            )

    assert not (cache / "current").exists()


def test_oversized_historical_generation_is_rejected_before_hashing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation = tmp_path / "generations" / ("a" * 40)
    generation.mkdir(parents=True)
    (generation / "pack-index.json").write_bytes(b"{}")
    monkeypatch.setattr(
        cache_generation_files,
        "file_sha256",
        lambda _path: pytest.fail("oversized historical bytes must not be hashed"),
    )

    with pytest.raises(ValueError, match="generation_invalid"):
        inspect_existing_generation(generation, max_generation_bytes=1)


@pytest.mark.parametrize(
    ("symbol", "ordinal", "expected_generation"),
    [
        ("write_json_durable", 1, "a" * 40),
        ("write_text_durable", 1, "b" * 40),
        ("_replace_generation_symlink", 1, "b" * 40),
        ("write_json_durable", 2, "b" * 40),
        ("_clear_pending_commit", 1, "b" * 40),
    ],
)
def test_commit_crash_matrix_recovers_consistent_authority(
    symbol: str,
    ordinal: int,
    expected_generation: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = tmp_path / "cache"
    first = "a" * 40
    second = "b" * 40
    _sync(tmp_path / "remote", cache, first)
    base = read_current_commit(cache)
    assert base is not None
    stage = create_generation_stage(cache, generation=second)
    (stage / "pack-index.json").write_text(json.dumps({"git_sha": second}), encoding="utf-8")
    candidate = finalize_generation_stage(stage, generation=second)
    original = getattr(cache_generation_store, symbol)
    calls = 0

    def fail_at_transition(*args: object, **kwargs: object) -> object:
        nonlocal calls
        calls += 1
        if calls == ordinal:
            raise OSError(f"injected {symbol} crash")
        return original(*args, **kwargs)

    monkeypatch.setattr(cache_generation_store, symbol, fail_at_transition)
    with cache_write_lease(cache):
        with pytest.raises(OSError, match="injected"):
            commit_generation(
                cache,
                candidate=candidate,
                expected_commit_id=base.commit_id,
                existing=None,
                committed_at="2026-08-03T00:00:00Z",
            )
    monkeypatch.setattr(cache_generation_store, symbol, original)

    with cache_write_lease(cache):
        recovered = recover_interrupted_commit(cache)

    assert recovered is not None and recovered.generation == expected_generation
    status = read_airflow_pack_cache_status(cache)
    assert status["status"] == "success"
    assert status["current_generation"] == expected_generation
