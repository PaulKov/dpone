"""Compact v1 releases must publish optional runtime_payloads bytes."""

from __future__ import annotations

from dpone.runtime.airflow_artifact_inventory import (
    declared_release_artifacts,
    release_includes_runtime_payloads,
)


def _sha(ch: str) -> str:
    return "sha256:" + ch * 64


def test_v1_without_runtime_payloads_section_skips_upload() -> None:
    assert (
        release_includes_runtime_payloads(
            "dpone.release-set.v1",
            {"dag_specs": [], "workload_packs": [], "canonical_schemas": []},
        )
        is False
    )


def test_v1_with_runtime_payloads_section_is_published() -> None:
    artifacts = {
        "dag_specs": [
            {
                "id": "orders",
                "path": "dags/orders.dag-spec.json",
                "sha256": _sha("a"),
                "bytes": 1,
            }
        ],
        "workload_packs": [],
        "canonical_schemas": [],
        "runtime_payloads": [
            {
                "id": "dbt_manifest",
                "kind": "dbt_manifest",
                "path": "runtime/dbt/manifest.json",
                "sha256": _sha("b"),
                "bytes": 2,
                "media_type": "application/vnd.dbt.manifest+json",
            }
        ],
    }
    assert release_includes_runtime_payloads("dpone.release-set.v1", artifacts) is True
    declared = declared_release_artifacts({"schema": "dpone.release-set.v1", "artifacts": artifacts})
    paths = {path.as_posix() for path, _digest in declared}
    assert "runtime/dbt/manifest.json" in paths
    assert "dags/orders.dag-spec.json" in paths


def test_v2_includes_runtime_payloads() -> None:
    assert release_includes_runtime_payloads("dpone.release-set.v2", {}) is True
