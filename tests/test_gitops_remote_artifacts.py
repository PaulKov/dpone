from __future__ import annotations

import json
from pathlib import Path

from dpone.gitops.airflow_pack_publisher import (
    AirflowPackIndexBuilder,
    AirflowPackPublisher,
    ObjectArtifactStore,
)
from dpone.storage import LocalObjectStorageClient, ObjectStorageUri


def test_pack_publisher_writes_immutable_pack_index_and_latest_pointer(tmp_path: Path) -> None:
    store = ObjectArtifactStore(LocalObjectStorageClient(tmp_path / "s3"))
    pack_path = tmp_path / "packs/orders/airflow-pack.json"
    pack_path.parent.mkdir(parents=True)
    pack_path.write_text(json.dumps(_pack("orders")), encoding="utf-8")

    result = AirflowPackPublisher(store=store).publish(
        packs={"orders": pack_path},
        uri_prefix="s3://example-data-bucket/dpone-artifacts/prod/example-workloads/abc123/airflow/",
        latest_index_uri="s3://example-data-bucket/dpone-artifacts/prod/example-workloads/latest/pack-index.json",
        git_sha="abc123",
    )

    latest = tmp_path / "s3/s3/example-data-bucket/dpone-artifacts/prod/example-workloads/latest/pack-index.json"
    marker = tmp_path / "s3/s3/example-data-bucket/dpone-artifacts/prod/example-workloads/abc123/airflow/release-marker.json"
    assert result["kind"] == "gitops.airflow_pack_publish"
    assert result["passed"] is True
    assert result["pack_count"] == 1
    assert latest.exists()
    assert marker.exists()
    assert json.loads(latest.read_text(encoding="utf-8"))["artifacts"]["orders"]["uri"].endswith(
        "/abc123/airflow/orders/airflow-pack.json"
    )


def test_index_builder_blocks_pack_above_size_limit(tmp_path: Path) -> None:
    pack_path = tmp_path / "airflow-pack.json"
    pack_path.write_text(json.dumps(_pack("orders")), encoding="utf-8")

    result = AirflowPackIndexBuilder(max_pack_bytes=3).build(
        packs={"orders": pack_path},
        uri_prefix=ObjectStorageUri.parse("s3://example-data-bucket/dpone-artifacts/prod/repo/abc/airflow/"),
        git_sha="abc",
    )

    assert result.blockers == ("airflow_pack_size_limit_exceeded:orders",)


def _pack(workload_id: str) -> dict[str, object]:
    return {
        "kind": "gitops.airflow_pack",
        "schema_version": "3",
        "workload": {"workload_id": workload_id},
        "kpo_kwargs": {"task_id": f"{workload_id}__dpone_runtime", "image": "dpone:dev"},
    }
