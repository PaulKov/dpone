from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from dpone.readiness.safe_sample_pinned_source import (
    PinnedWorkloadSourceError,
    PinnedWorkloadSourceVerifier,
)

_RELEASE_ID = "sha256:" + "a" * 64
_ARTIFACT_REF = "cache://releases/sha256-" + "a" * 64 + "/packs/orders_daily.airflow-pack.json"


def _pack_bytes(
    *,
    manifest_path: str,
    manifest_sha256: str,
    include_pin: bool = True,
    authoring_dependencies: list[dict[str, str]] | None = None,
) -> bytes:
    dependencies = [{"kind": "manifest", "path": manifest_path, "sha256": manifest_sha256}] if include_pin else []
    dependencies.extend(authoring_dependencies or [])
    return (
        json.dumps(
            {
                "kind": "gitops.airflow_pack",
                "schema_version": "3",
                "workload": {"workload_id": "orders_daily", "manifest": manifest_path},
                "workload_dependencies": dependencies,
            },
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _indexed_pack(payload: bytes, *, artifact_ref: str = _ARTIFACT_REF) -> dict[str, object]:
    return {
        "id": "orders_daily",
        "artifact_ref": artifact_ref,
        "sha256": "sha256:" + hashlib.sha256(payload).hexdigest(),
        "bytes": len(payload),
    }


def test_pinned_source_verifier_accepts_exact_primary_manifest(tmp_path: Path) -> None:
    source = tmp_path / "pipelines" / "orders_daily" / "pipeline.yaml"
    source.parent.mkdir(parents=True)
    source.write_text("schema: dpone.pipeline.v1\n", encoding="utf-8")
    source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    pack = _pack_bytes(
        manifest_path="pipelines/orders_daily/pipeline.yaml",
        manifest_sha256=source_sha,
    )
    reads: list[str] = []

    result = PinnedWorkloadSourceVerifier().verify(
        workload_id="orders_daily",
        release_id=_RELEASE_ID,
        indexed_packs=(_indexed_pack(pack),),
        source_path=source,
        source_sha256="sha256:" + source_sha,
        source_root=tmp_path,
        pack_reader=lambda ref: reads.append(ref) or pack,
    )

    assert reads == [_ARTIFACT_REF]
    assert result.workload_id == "orders_daily"
    assert result.manifest_sha256 == "sha256:" + source_sha
    assert result.pack_sha256 == "sha256:" + hashlib.sha256(pack).hexdigest()


def test_pinned_source_verifier_rejects_changed_source_bytes(tmp_path: Path) -> None:
    source = tmp_path / "pipeline.yaml"
    source.write_text("schema: changed.after.release\n", encoding="utf-8")
    pinned_sha = hashlib.sha256(b"schema: dpone.pipeline.v1\n").hexdigest()
    pack = _pack_bytes(manifest_path="pipeline.yaml", manifest_sha256=pinned_sha)

    with pytest.raises(PinnedWorkloadSourceError) as exc:
        PinnedWorkloadSourceVerifier().verify(
            workload_id="orders_daily",
            release_id=_RELEASE_ID,
            indexed_packs=(_indexed_pack(pack),),
            source_path=source,
            source_sha256="sha256:" + hashlib.sha256(source.read_bytes()).hexdigest(),
            source_root=tmp_path,
            pack_reader=lambda ref: pack,
        )

    assert exc.value.code == "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_FINGERPRINT_MISMATCH"


def test_pinned_source_verifier_accepts_exact_folder_fragment_graph(tmp_path: Path) -> None:
    source = tmp_path / "pipelines/orders_daily/pipeline.yaml"
    fragment = source.parent / "steps/load.yaml"
    fragment.parent.mkdir(parents=True)
    source.write_text("kind: dpone.flow.v1\n", encoding="utf-8")
    fragment.write_text("kind: dpone.flow-fragment.v1\n", encoding="utf-8")
    source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    fragment_sha = hashlib.sha256(fragment.read_bytes()).hexdigest()
    fragment_dependency = {
        "kind": "authoring_fragment",
        "path": "pipelines/orders_daily/steps/load.yaml",
        "sha256": fragment_sha,
    }
    pack = _pack_bytes(
        manifest_path="pipelines/orders_daily/pipeline.yaml",
        manifest_sha256=source_sha,
        authoring_dependencies=[fragment_dependency],
    )

    result = PinnedWorkloadSourceVerifier().verify(
        workload_id="orders_daily",
        release_id=_RELEASE_ID,
        indexed_packs=(_indexed_pack(pack),),
        source_path=source,
        source_sha256="sha256:" + source_sha,
        source_dependencies=({**fragment_dependency, "sha256": "sha256:" + fragment_sha},),
        source_root=tmp_path,
        pack_reader=lambda ref: pack,
    )

    assert result.authoring_dependencies == (("pipelines/orders_daily/steps/load.yaml", "sha256:" + fragment_sha),)


def test_pinned_source_verifier_rejects_changed_folder_fragment_graph(tmp_path: Path) -> None:
    source = tmp_path / "pipeline.yaml"
    source.write_text("kind: dpone.flow.v1\n", encoding="utf-8")
    source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    pack = _pack_bytes(
        manifest_path="pipeline.yaml",
        manifest_sha256=source_sha,
        authoring_dependencies=[{"kind": "authoring_fragment", "path": "steps/load.yaml", "sha256": "a" * 64}],
    )

    with pytest.raises(PinnedWorkloadSourceError) as exc:
        PinnedWorkloadSourceVerifier().verify(
            workload_id="orders_daily",
            release_id=_RELEASE_ID,
            indexed_packs=(_indexed_pack(pack),),
            source_path=source,
            source_sha256="sha256:" + source_sha,
            source_dependencies=(
                {"kind": "authoring_fragment", "path": "steps/load.yaml", "sha256": "sha256:" + "b" * 64},
            ),
            source_root=tmp_path,
            pack_reader=lambda ref: pack,
        )

    assert exc.value.code == "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_FINGERPRINT_MISMATCH"


def test_pinned_source_verifier_rejects_changed_recipe_closure(tmp_path: Path) -> None:
    source = tmp_path / "pipeline.yaml"
    source.write_text("kind: dpone.flow.v1\n", encoding="utf-8")
    source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    pack = _pack_bytes(
        manifest_path="pipeline.yaml",
        manifest_sha256=source_sha,
        authoring_dependencies=[
            {"kind": "recipe", "path": "platform/recipes/orders.yaml", "sha256": "a" * 64},
            {"kind": "component", "path": "platform/components/load.yaml", "sha256": "b" * 64},
        ],
    )

    with pytest.raises(PinnedWorkloadSourceError) as exc:
        PinnedWorkloadSourceVerifier().verify(
            workload_id="orders_daily",
            release_id=_RELEASE_ID,
            indexed_packs=(_indexed_pack(pack),),
            source_path=source,
            source_sha256="sha256:" + source_sha,
            source_dependencies=(
                {
                    "kind": "recipe",
                    "path": "platform/recipes/orders.yaml",
                    "sha256": "sha256:" + "c" * 64,
                },
                {
                    "kind": "component",
                    "path": "platform/components/load.yaml",
                    "sha256": "sha256:" + "b" * 64,
                },
            ),
            source_root=tmp_path,
            pack_reader=lambda ref: pack,
        )

    assert exc.value.code == "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_FINGERPRINT_MISMATCH"


def test_pinned_source_verifier_rejects_authoring_dependency_kind_relabel(tmp_path: Path) -> None:
    source = tmp_path / "pipeline.yaml"
    source.write_text("kind: dpone.flow.v1\n", encoding="utf-8")
    source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    dependency = {
        "kind": "recipe",
        "path": "platform/recipes/orders.yaml",
        "sha256": "a" * 64,
    }
    pack = _pack_bytes(
        manifest_path="pipeline.yaml",
        manifest_sha256=source_sha,
        authoring_dependencies=[dependency],
    )

    with pytest.raises(PinnedWorkloadSourceError) as exc:
        PinnedWorkloadSourceVerifier().verify(
            workload_id="orders_daily",
            release_id=_RELEASE_ID,
            indexed_packs=(_indexed_pack(pack),),
            source_path=source,
            source_sha256="sha256:" + source_sha,
            source_dependencies=({**dependency, "kind": "profile", "sha256": "sha256:" + "a" * 64},),
            source_root=tmp_path,
            pack_reader=lambda ref: pack,
        )

    assert exc.value.code == "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_FINGERPRINT_MISMATCH"


def test_pinned_source_verifier_rejects_changed_sql_dependency(tmp_path: Path) -> None:
    source = tmp_path / "pipeline.yaml"
    sql_file = tmp_path / "queries/orders.sql"
    sql_file.parent.mkdir()
    source.write_text("kind: dpone.flow.v1\n", encoding="utf-8")
    sql_file.write_text("SELECT 2\n", encoding="utf-8")
    source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    pinned_sql_sha = hashlib.sha256(b"SELECT 1\n").hexdigest()
    pack = _pack_bytes(
        manifest_path="pipeline.yaml",
        manifest_sha256=source_sha,
        authoring_dependencies=[{"kind": "sql_file", "path": "queries/orders.sql", "sha256": pinned_sql_sha}],
    )

    with pytest.raises(PinnedWorkloadSourceError) as exc:
        PinnedWorkloadSourceVerifier().verify(
            workload_id="orders_daily",
            release_id=_RELEASE_ID,
            indexed_packs=(_indexed_pack(pack),),
            source_path=source,
            source_sha256="sha256:" + source_sha,
            source_root=tmp_path,
            source_dependency_digester=lambda path: (
                "sha256:" + hashlib.sha256((tmp_path / path).read_bytes()).hexdigest()
            ),
            pack_reader=lambda ref: pack,
        )

    assert exc.value.code == "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_FINGERPRINT_MISMATCH"


def test_pinned_source_verifier_rejects_pack_without_manifest_pin(tmp_path: Path) -> None:
    source = tmp_path / "pipeline.yaml"
    source.write_text("schema: dpone.pipeline.v1\n", encoding="utf-8")
    source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    pack = _pack_bytes(manifest_path="pipeline.yaml", manifest_sha256=source_sha, include_pin=False)

    with pytest.raises(PinnedWorkloadSourceError) as exc:
        PinnedWorkloadSourceVerifier().verify(
            workload_id="orders_daily",
            release_id=_RELEASE_ID,
            indexed_packs=(_indexed_pack(pack),),
            source_path=source,
            source_sha256="sha256:" + source_sha,
            source_root=tmp_path,
            pack_reader=lambda ref: pack,
        )

    assert exc.value.code == "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_PIN_MISSING"


def test_pinned_source_verifier_rejects_unpinned_release_before_reader(tmp_path: Path) -> None:
    calls = 0

    def unexpected_reader(ref: str) -> bytes:
        nonlocal calls
        calls += 1
        return b"{}"

    with pytest.raises(PinnedWorkloadSourceError) as exc:
        PinnedWorkloadSourceVerifier().verify(
            workload_id="orders_daily",
            release_id=_RELEASE_ID,
            indexed_packs=(_indexed_pack(b"{}", artifact_ref="cache://current/packs/orders.json"),),
            source_path=tmp_path / "pipeline.yaml",
            source_sha256="sha256:" + "b" * 64,
            source_root=tmp_path,
            pack_reader=unexpected_reader,
        )

    assert exc.value.code == "DPONE_CACHE_UNPINNED_REFERENCE"
    assert calls == 0


def test_pinned_source_verifier_rejects_source_outside_explicit_root(tmp_path: Path) -> None:
    source_root = tmp_path / "project"
    source_root.mkdir()
    source = tmp_path / "outside" / "pipeline.yaml"
    source.parent.mkdir()
    source.write_text("schema: dpone.pipeline.v1\n", encoding="utf-8")
    source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    pack = _pack_bytes(manifest_path="../outside/pipeline.yaml", manifest_sha256=source_sha)

    with pytest.raises(PinnedWorkloadSourceError) as exc:
        PinnedWorkloadSourceVerifier().verify(
            workload_id="orders_daily",
            release_id=_RELEASE_ID,
            indexed_packs=(_indexed_pack(pack),),
            source_path=source,
            source_sha256="sha256:" + source_sha,
            source_root=source_root,
            pack_reader=lambda ref: pack,
        )

    assert exc.value.code == "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_FINGERPRINT_MISMATCH"


def test_pinned_source_verifier_rejects_symlink_escape_from_explicit_root(tmp_path: Path) -> None:
    source_root = tmp_path / "project"
    source_root.mkdir()
    outside = tmp_path / "outside.yaml"
    outside.write_text("schema: dpone.pipeline.v1\n", encoding="utf-8")
    source = source_root / "pipeline.yaml"
    source.symlink_to(outside)
    source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    pack = _pack_bytes(manifest_path="pipeline.yaml", manifest_sha256=source_sha)

    with pytest.raises(PinnedWorkloadSourceError) as exc:
        PinnedWorkloadSourceVerifier().verify(
            workload_id="orders_daily",
            release_id=_RELEASE_ID,
            indexed_packs=(_indexed_pack(pack),),
            source_path=source,
            source_sha256="sha256:" + source_sha,
            source_root=source_root,
            pack_reader=lambda ref: pack,
        )

    assert exc.value.code == "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_FINGERPRINT_MISMATCH"
