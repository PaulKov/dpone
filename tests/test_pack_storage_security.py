"""Security hardening for remote pack storage / dag-spec cache consumption."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from dpone_airflow_pack.cache_sync import AirflowPackSyncOptions, sync_airflow_pack_cache
from dpone_airflow_pack.dag_spec_cache_paths import discover_dag_spec_paths
from dpone_airflow_pack.pack_index import AirflowPackIndexEntry
from dpone_airflow_pack.pack_index_security import (
    validate_entry_uri,
    validate_index_generation,
)
from dpone_airflow_pack.pack_storage_consumer import (
    STORAGE_MODE_ENV,
    DagSpecCacheMissingError,
)


def _write_gitops_spec(repo_root: Path, dag_id: str = "from_gitops") -> Path:
    root = repo_root / ".dpone" / "gitops" / "airflow" / "_dags"
    root.mkdir(parents=True)
    path = root / f"{dag_id}.dag-spec.json"
    path.write_text(json.dumps({"dag_id": dag_id}), encoding="utf-8")
    return path


def _read_local_artifact(uri: str, *, max_bytes: int | None) -> bytes:
    path = Path(uri.removeprefix("file://"))
    if max_bytes is None:
        return path.read_bytes()
    with path.open("rb") as handle:
        payload = handle.read(max_bytes + 1)
    if len(payload) > max_bytes:
        raise ValueError("artifact exceeds configured byte limit")
    return payload


def test_remote_mode_empty_cache_fails_closed_without_gitops_fallthrough(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    gitops_spec = _write_gitops_spec(tmp_path)
    monkeypatch.setenv("DPONE_AIRFLOW_PACK_CACHE_DIR", str(cache))
    monkeypatch.setenv(STORAGE_MODE_ENV, "remote")

    with pytest.raises(DagSpecCacheMissingError, match="dag_spec_cache_missing"):
        discover_dag_spec_paths(tmp_path)

    assert gitops_spec.exists()


def test_remote_mode_generation_without_dags_fails_loudly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = tmp_path / "cache"
    generation = cache / "generations" / "abc123"
    generation.mkdir(parents=True)
    (cache / "current").write_text("abc123", encoding="utf-8")
    _write_gitops_spec(tmp_path)
    monkeypatch.setenv("DPONE_AIRFLOW_PACK_CACHE_DIR", str(cache))
    monkeypatch.setenv(STORAGE_MODE_ENV, "remote")

    with pytest.raises(DagSpecCacheMissingError, match="no dag-spec artifacts"):
        discover_dag_spec_paths(tmp_path)


def test_hybrid_mode_empty_cache_falls_back_to_gitops(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    gitops_spec = _write_gitops_spec(tmp_path, "hybrid_dag")
    monkeypatch.setenv("DPONE_AIRFLOW_PACK_CACHE_DIR", str(cache))
    monkeypatch.setenv(STORAGE_MODE_ENV, "hybrid")

    paths = discover_dag_spec_paths(tmp_path)

    assert paths == (gitops_spec,)


def test_hybrid_mode_empty_dags_falls_back_to_gitops(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = tmp_path / "cache"
    (cache / "generations" / "abc123").mkdir(parents=True)
    (cache / "current").write_text("abc123", encoding="utf-8")
    gitops_spec = _write_gitops_spec(tmp_path, "hybrid_empty_dags")
    monkeypatch.setenv("DPONE_AIRFLOW_PACK_CACHE_DIR", str(cache))
    monkeypatch.setenv(STORAGE_MODE_ENV, "hybrid")

    paths = discover_dag_spec_paths(tmp_path)

    assert paths == (gitops_spec,)


def test_cache_sync_rejects_index_entry_without_sha256(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation = "abc123"
    release = tmp_path / "releases" / generation
    pack_path = release / "orders" / "airflow-pack.json"
    pack_path.parent.mkdir(parents=True)
    pack_path.write_bytes(b'{"kind":"gitops.airflow_pack"}')
    latest = tmp_path / "releases" / "latest" / "pack-index.json"
    latest.parent.mkdir(parents=True)
    latest.write_text(
        json.dumps(
            {
                "git_sha": generation,
                "packs": {
                    "orders": {
                        "path": "orders/airflow-pack.json",
                        "uri": pack_path.as_uri(),
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    class _Reader:
        def read_bytes(self, uri: str, *, max_bytes: int | None = None) -> bytes:
            return _read_local_artifact(uri, max_bytes=max_bytes)

    monkeypatch.setattr("dpone_airflow_pack.cache_sync.ArtifactReader", lambda **_: _Reader())

    with pytest.raises(ValueError, match="missing sha256"):
        sync_airflow_pack_cache(AirflowPackSyncOptions(index_uri=f"file://{latest}", cache_dir=tmp_path / "cache"))


def test_cache_sync_rejects_malicious_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    latest = tmp_path / "releases" / "latest" / "pack-index.json"
    latest.parent.mkdir(parents=True)
    latest.write_text(json.dumps({"git_sha": "../x", "packs": {}}), encoding="utf-8")

    class _Reader:
        def read_bytes(self, uri: str, *, max_bytes: int | None = None) -> bytes:
            return _read_local_artifact(uri, max_bytes=max_bytes)

    monkeypatch.setattr("dpone_airflow_pack.cache_sync.ArtifactReader", lambda **_: _Reader())

    with pytest.raises(ValueError, match="invalid pack index generation"):
        sync_airflow_pack_cache(AirflowPackSyncOptions(index_uri=f"file://{latest}", cache_dir=tmp_path / "cache"))
    assert not (tmp_path / "cache" / "generations" / "..").exists()


def test_validate_index_generation_rejects_path_segments() -> None:
    with pytest.raises(ValueError):
        validate_index_generation("../x")
    with pytest.raises(ValueError):
        validate_index_generation("abc/def")
    with pytest.raises(ValueError):
        validate_index_generation("abc\\def")
    validate_index_generation("abc123")
    validate_index_generation("deadbeefcafebabe")


def test_validate_entry_uri_rejects_outside_index_prefix() -> None:
    entry = AirflowPackIndexEntry(
        workload_id="orders",
        path="orders/airflow-pack.json",
        sha256="a" * 64,
        uri="s3://evil-bucket/abc123/orders/airflow-pack.json",
    )
    with pytest.raises(ValueError, match="host/bucket mismatch"):
        validate_entry_uri(
            index_uri="s3://good-bucket/releases/latest/pack-index.json",
            generation="abc123",
            entry=entry,
            declared_uri=entry.uri or "",
        )


def test_validate_entry_uri_rejects_file_escape(tmp_path: Path) -> None:
    index = tmp_path / "releases" / "latest" / "pack-index.json"
    index.parent.mkdir(parents=True)
    index.write_text("{}", encoding="utf-8")
    outside = tmp_path / "outside" / "airflow-pack.json"
    outside.parent.mkdir(parents=True)
    outside.write_bytes(b"{}")
    entry = AirflowPackIndexEntry(
        workload_id="orders",
        path="orders/airflow-pack.json",
        sha256="a" * 64,
        uri=outside.as_uri(),
    )
    with pytest.raises(ValueError, match="escapes release root"):
        validate_entry_uri(
            index_uri=index.as_uri(),
            generation="abc123",
            entry=entry,
            declared_uri=outside.as_uri(),
        )


def test_pack_wiring_remote_refuses_gitops_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack import pack_wiring as pack_wiring_module
    from dpone_airflow_pack.runtime_adapter import DponeAirflowContractError

    local = tmp_path / ".dpone" / "gitops" / "airflow" / "orders" / "airflow-pack.json"
    local.parent.mkdir(parents=True)
    local.write_text("{}", encoding="utf-8")
    monkeypatch.setenv(STORAGE_MODE_ENV, "remote")
    monkeypatch.setenv("DPONE_AIRFLOW_PACK_CACHE_DIR", str(tmp_path / "empty-cache"))

    def raise_cache_miss(ref: object, **_kwargs: object) -> tuple[dict[str, object], dict[str, object]]:
        raise DponeAirflowContractError(
            "cache miss",
            blockers=({"code": "airflow_pack_cache_missing", "path": str(ref), "message": "missing"},),
        )

    monkeypatch.setattr(pack_wiring_module, "load_dpone_airflow_pack_with_provenance", raise_cache_miss)

    with pytest.raises(DponeAirflowContractError):
        pack_wiring_module.resolve_pack_reference(tmp_path, "orders")
