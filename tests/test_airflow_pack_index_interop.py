"""Publisher ``artifacts`` / ``size_bytes`` interop with cache sync index reader."""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest
from dpone_airflow_pack.dag_spec_cache_paths import discover_dag_spec_paths
from dpone_airflow_pack.pack_index import (
    dag_spec_index_entries,
    index_entries,
    safe_pack_relative_path,
)
from dpone_airflow_pack.pack_storage_consumer import STORAGE_MODE_ENV


class _LocalArtifactReader:
    def read_bytes(self, uri: str, *, max_bytes: int | None = None) -> bytes:
        return _read_local_artifact(uri, max_bytes=max_bytes)


def _read_local_artifact(uri: str, *, max_bytes: int | None) -> bytes:
    path = Path(uri.removeprefix("file://"))
    if max_bytes is None:
        return path.read_bytes()
    with path.open("rb") as handle:
        payload = handle.read(max_bytes + 1)
    if len(payload) > max_bytes:
        raise ValueError("artifact exceeds configured byte limit")
    return payload


def _write_legacy_remote(root: Path, generation: str, payload: bytes) -> Path:
    pack_path = root / generation / "airflow" / "orders" / "airflow-pack.json"
    pack_path.parent.mkdir(parents=True, exist_ok=True)
    pack_path.write_bytes(payload)
    latest = root / "latest" / "pack-index.json"
    latest.parent.mkdir(parents=True, exist_ok=True)
    latest.write_text(
        json.dumps(
            {
                "git_sha": generation,
                "artifacts": {
                    "orders": {
                        "path": "orders/airflow-pack.json",
                        "uri": pack_path.as_uri(),
                        "sha256": __import__("hashlib").sha256(payload).hexdigest(),
                    }
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return latest


def test_index_entries_reads_publisher_artifacts_shape() -> None:
    index = {
        "kind": "gitops.airflow_pack_index",
        "git_sha": "deadbeef",
        "artifacts": {
            "orders": {
                "uri": "s3://bucket/deadbeef/airflow/orders/airflow-pack.json",
                "sha256": "a" * 64,
                "size_bytes": 1234,
            }
        },
    }
    entries = index_entries(index)
    assert len(entries) == 1
    entry = entries[0]
    assert entry.workload_id == "orders"
    assert entry.path == "orders/airflow-pack.json"
    assert entry.bytes == 1234
    assert entry.sha256 == "a" * 64
    assert entry.uri == "s3://bucket/deadbeef/airflow/orders/airflow-pack.json"
    assert safe_pack_relative_path(entry) == "orders/airflow-pack.json"


def test_index_entries_still_reads_legacy_packs_shape() -> None:
    index = {
        "packs": {
            "orders": {
                "path": "airflow/orders/airflow-pack.json",
                "bytes": 99,
            }
        }
    }
    entry = index_entries(index)[0]
    assert entry.path == "airflow/orders/airflow-pack.json"
    assert entry.bytes == 99


def test_dag_spec_index_entries_from_publisher_shape() -> None:
    index = {
        "dag_specs": {
            "DAG__demo": {
                "uri": "s3://bucket/deadbeef/airflow/_dags/DAG__demo.dag-spec.json",
                "sha256": "b" * 64,
                "size_bytes": 456,
            }
        }
    }
    entry = dag_spec_index_entries(index)[0]
    assert entry.workload_id == "DAG__demo"
    assert entry.path == "airflow/_dags/DAG__demo.dag-spec.json"
    assert entry.bytes == 456


def test_index_entries_skips_empty_packs_when_artifacts_present() -> None:
    index = {
        "packs": {},
        "artifacts": {
            "orders": {
                "uri": "s3://bucket/deadbeef/airflow/orders/airflow-pack.json",
                "sha256": "a" * 64,
                "size_bytes": 1234,
            }
        },
    }
    entries = index_entries(index)
    assert len(entries) == 1
    assert entries[0].workload_id == "orders"


def test_cache_sync_downloads_artifacts_index(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """End-to-end: sync reads publisher-shaped index and materializes packs + dag-specs."""
    from dpone_airflow_pack.cache_sync import AirflowPackSyncOptions, sync_airflow_pack_cache

    generation = "abc123"
    pack_body = b'{"kind":"gitops.airflow_pack"}'
    spec_body = b'{"kind":"gitops.airflow_dag_spec","dag_id":"DAG__demo"}'
    release_dir = tmp_path / "releases" / generation / "airflow"
    release_dir.mkdir(parents=True)
    pack_hash = __import__("hashlib").sha256(pack_body).hexdigest()
    spec_hash = __import__("hashlib").sha256(spec_body).hexdigest()
    (release_dir / "orders").mkdir(parents=True, exist_ok=True)
    pack_file = release_dir / "orders" / "airflow-pack.json"
    pack_file.write_bytes(pack_body)
    (release_dir / "airflow" / "_dags").mkdir(parents=True, exist_ok=True)
    spec_file = release_dir / "airflow" / "_dags" / "DAG__demo.dag-spec.json"
    spec_file.write_bytes(spec_body)
    index = {
        "git_sha": generation,
        "artifacts": {
            "orders": {
                "path": "orders/airflow-pack.json",
                "uri": pack_file.as_uri(),
                "sha256": pack_hash,
                "size_bytes": len(pack_body),
            }
        },
        "dag_specs": {
            "DAG__demo": {
                "path": "airflow/_dags/DAG__demo.dag-spec.json",
                "uri": spec_file.as_uri(),
                "sha256": spec_hash,
                "size_bytes": len(spec_body),
            }
        },
    }
    latest = tmp_path / "releases" / "latest" / "pack-index.json"
    latest.parent.mkdir(parents=True)
    latest.write_text(json.dumps(index), encoding="utf-8")

    class _Reader:
        def read_bytes(self, uri: str, *, max_bytes: int | None = None) -> bytes:
            return _read_local_artifact(uri, max_bytes=max_bytes)

    monkeypatch.setattr(
        "dpone_airflow_pack.cache_sync.ArtifactReader",
        lambda **_: _Reader(),
    )
    cache_dir = tmp_path / "cache"
    result = sync_airflow_pack_cache(
        AirflowPackSyncOptions(
            index_uri=f"file://{latest}",
            cache_dir=cache_dir,
        )
    )
    assert result["downloaded_pack_count"] == 1
    assert result["downloaded_dag_spec_count"] == 1
    gen_dir = cache_dir / "generations" / generation
    assert (gen_dir / "orders" / "airflow-pack.json").read_bytes() == pack_body
    assert (gen_dir / "airflow" / "_dags" / "DAG__demo.dag-spec.json").read_bytes() == spec_body


def test_cache_sync_remote_download_does_not_block_cache_readers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack.cache_activation_contract import cache_read_lease
    from dpone_airflow_pack.cache_sync import AirflowPackSyncOptions, sync_airflow_pack_cache

    generation = "abc123"
    pack_body = b'{"kind":"gitops.airflow_pack"}'
    pack_path = tmp_path / "releases" / generation / "airflow" / "orders" / "airflow-pack.json"
    pack_path.parent.mkdir(parents=True)
    pack_path.write_bytes(pack_body)
    latest = tmp_path / "releases" / "latest" / "pack-index.json"
    latest.parent.mkdir(parents=True)
    latest.write_text(
        json.dumps(
            {
                "git_sha": generation,
                "artifacts": {
                    "orders": {
                        "path": "orders/airflow-pack.json",
                        "uri": pack_path.as_uri(),
                        "sha256": __import__("hashlib").sha256(pack_body).hexdigest(),
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    pack_read_started = threading.Event()
    release_pack_read = threading.Event()

    class _Reader:
        def read_bytes(self, uri: str, *, max_bytes: int | None = None) -> bytes:
            path = Path(uri.removeprefix("file://"))
            if path == pack_path:
                pack_read_started.set()
                assert release_pack_read.wait(timeout=5)
            return _read_local_artifact(uri, max_bytes=max_bytes)

    monkeypatch.setattr("dpone_airflow_pack.cache_sync.ArtifactReader", lambda **_: _Reader())
    cache_dir = tmp_path / "cache"
    errors: list[BaseException] = []

    def run_sync() -> None:
        try:
            sync_airflow_pack_cache(AirflowPackSyncOptions(index_uri=latest.as_uri(), cache_dir=cache_dir))
        except BaseException as exc:  # pragma: no cover - assertion reports the captured worker failure.
            errors.append(exc)

    sync_thread = threading.Thread(target=run_sync)
    sync_thread.start()
    assert pack_read_started.wait(timeout=5)

    with cache_read_lease(cache_dir) as lease:
        assert lease.root_available is True

    release_pack_read.set()
    sync_thread.join(timeout=5)
    assert not sync_thread.is_alive()
    assert errors == []


def test_cache_sync_rejects_conflicting_existing_generation_without_deleting_active_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack.cache_sync import AirflowPackSyncOptions, sync_airflow_pack_cache

    remote = tmp_path / "remote"
    generation = "a" * 40
    latest = _write_legacy_remote(remote, generation, b'{"version":1}')
    monkeypatch.setattr("dpone_airflow_pack.cache_sync.ArtifactReader", lambda **_: _LocalArtifactReader())
    cache = tmp_path / "cache"
    options = AirflowPackSyncOptions(index_uri=latest.as_uri(), cache_dir=cache)
    sync_airflow_pack_cache(options)
    active_pack = cache / "generations" / generation / "orders" / "airflow-pack.json"

    _write_legacy_remote(remote, generation, b'{"version":2}')
    with pytest.raises(ValueError, match="airflow_pack_generation_conflict"):
        sync_airflow_pack_cache(options)

    assert (cache / "current").read_text(encoding="utf-8") == generation
    assert active_pack.read_bytes() == b'{"version":1}'


def test_cache_sync_rename_failure_preserves_previous_active_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack.cache_sync import AirflowPackSyncOptions, sync_airflow_pack_cache

    remote = tmp_path / "remote"
    old_generation = "a" * 40
    new_generation = "b" * 40
    latest = _write_legacy_remote(remote, old_generation, b'{"version":1}')
    monkeypatch.setattr("dpone_airflow_pack.cache_sync.ArtifactReader", lambda **_: _LocalArtifactReader())
    cache = tmp_path / "cache"
    sync_airflow_pack_cache(AirflowPackSyncOptions(index_uri=latest.as_uri(), cache_dir=cache))
    _write_legacy_remote(remote, new_generation, b'{"version":2}')
    original_rename = Path.rename

    def fail_new_generation_rename(path: Path, target: Path) -> Path:
        if path.parent.name == ".staging" and target.name == new_generation:
            raise OSError("rename failed before commit")
        return original_rename(path, target)

    monkeypatch.setattr(Path, "rename", fail_new_generation_rename)

    with pytest.raises(OSError, match="rename failed before commit"):
        sync_airflow_pack_cache(AirflowPackSyncOptions(index_uri=latest.as_uri(), cache_dir=cache))

    assert (cache / "current").read_text(encoding="utf-8") == old_generation
    assert (cache / "generations" / old_generation).is_dir()
    assert not (cache / "generations" / new_generation).exists()


def test_cache_sync_reports_prune_failure_after_successful_commit_as_warning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack import cache_sync as cache_sync_module

    remote = tmp_path / "remote"
    generation = "b" * 40
    latest = _write_legacy_remote(remote, generation, b'{"version":2}')
    monkeypatch.setattr(cache_sync_module, "ArtifactReader", lambda **_: _LocalArtifactReader())
    from dpone_airflow_pack.cache_generation_retention import CacheRetentionResult

    monkeypatch.setattr(
        cache_sync_module,
        "enforce_cache_retention",
        lambda *_args, **_kwargs: CacheRetentionResult(
            total_bytes=1,
            deleted_paths=(),
            warnings=("cache_generation_prune_failed",),
            blockers=(),
        ),
    )

    result = cache_sync_module.sync_airflow_pack_cache(
        cache_sync_module.AirflowPackSyncOptions(index_uri=latest.as_uri(), cache_dir=tmp_path / "cache")
    )

    assert result["status"] == "warning"
    assert result["current_generation"] == generation
    assert "cache_generation_prune_failed" in result["warnings"]


def test_cache_sync_accepts_injected_bounded_artifact_reader(tmp_path: Path) -> None:
    from dpone_airflow_pack.cache_sync import AirflowPackSyncOptions, sync_airflow_pack_cache

    generation = "c" * 40
    latest = _write_legacy_remote(tmp_path / "remote", generation, b'{"version":3}')

    result = sync_airflow_pack_cache(
        AirflowPackSyncOptions(index_uri=latest.as_uri(), cache_dir=tmp_path / "cache"),
        reader=_LocalArtifactReader(),
    )

    assert result["status"] in {"success", "warning"}
    assert result["current_generation"] == generation


def test_existing_generation_metadata_is_rechecked_under_commit_lease(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack import cache_sync as cache_sync_module

    remote = tmp_path / "remote"
    generation = "a" * 40
    latest = _write_legacy_remote(remote, generation, b'{"version":1}')
    monkeypatch.setattr(cache_sync_module, "ArtifactReader", lambda **_: _LocalArtifactReader())
    cache = tmp_path / "cache"
    options = cache_sync_module.AirflowPackSyncOptions(index_uri=latest.as_uri(), cache_dir=cache)
    cache_sync_module.sync_airflow_pack_cache(options)
    original_inspect = cache_sync_module.inspect_existing_generation

    def inspect_then_tamper(path: Path, **kwargs: object):
        snapshot = original_inspect(path, **kwargs)
        pack = path / "orders" / "airflow-pack.json"
        pack.chmod(0o644)
        pack.write_bytes(b'{"version":2}')
        pack.chmod(0o444)
        return snapshot

    monkeypatch.setattr(cache_sync_module, "inspect_existing_generation", inspect_then_tamper)

    with pytest.raises(ValueError, match="existing generation bytes differ"):
        cache_sync_module.sync_airflow_pack_cache(options)


def test_discover_dag_spec_paths_reads_publisher_airflow_prefix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = tmp_path / "cache"
    generation = "955e6b0f"
    spec_dir = cache / "generations" / generation / "airflow" / "_dags"
    spec_dir.mkdir(parents=True)
    spec_path = spec_dir / "DAG__datamarts__account_activity__refresh.dag-spec.json"
    spec_path.write_text('{"dag_id":"DAG__datamarts__account_activity__refresh"}', encoding="utf-8")
    (cache / "current").write_text(generation, encoding="utf-8")
    monkeypatch.setenv("DPONE_AIRFLOW_PACK_CACHE_DIR", str(cache))
    monkeypatch.setenv(STORAGE_MODE_ENV, "remote")

    paths = discover_dag_spec_paths(tmp_path)

    assert paths == (spec_path,)
