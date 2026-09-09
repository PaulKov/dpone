from __future__ import annotations

from pathlib import Path

from dpone.runtime.object_storage_access import (
    ObjectStorageAccessPreflightService,
    ObjectStorageAccessRequest,
    ObjectStorageConnectionRef,
    ObjectStorageReadContract,
    ObjectStorageRuntimeAccess,
)
from dpone.runtime.object_storage_retention import ObjectStorageRetentionPolicy
from dpone.storage import LocalObjectStorageClient


def test_access_request_builds_retention_policy_from_options() -> None:
    request = ObjectStorageAccessRequest.from_options(
        {
            "uri_prefix": "s3://dpone-stage/msql/{run_id}/",
            "runtime_access": {
                "connection_type": "airflow",
                "connection_id": "s3_dpone_stage_writer",
            },
            "clickhouse_read_access": {
                "mode": "named_collection",
                "named_collection": "dpone_stage",
            },
            "retention": {
                "bucket_limit_bytes": "200GiB",
                "block_usage_pct": 85,
            },
        },
    )

    assert request.retention_policy.bucket_limit_bytes == 200 * 1024**3
    assert request.retention_policy.block_usage_pct == 85


def test_preflight_blocks_on_budget_before_clickhouse_probe(tmp_path: Path) -> None:
    root = tmp_path / "store" / "s3" / "dpone-stage" / "msql" / "other-run"
    root.mkdir(parents=True)
    (root / "large.parquet").write_bytes(b"x" * 90)
    probe = _RecordingClickHouseProbe()
    request = ObjectStorageAccessRequest(
        uri_prefix="s3://dpone-stage/msql/{run_id}/",
        runtime_access=ObjectStorageRuntimeAccess(
            connection=ObjectStorageConnectionRef(connection_type="env", connection_id="s3_writer"),
            required_permissions=("put_object", "get_object", "list_prefix", "delete_prefix"),
        ),
        clickhouse_read_access=ObjectStorageReadContract(mode="named_collection", named_collection="dpone_stage"),
        retention_policy=ObjectStorageRetentionPolicy.from_options(
            {"bucket_limit_bytes": "100", "block_usage_pct": 85}
        ),
    )

    evidence = ObjectStorageAccessPreflightService(
        object_client=LocalObjectStorageClient(root_dir=tmp_path / "store"),
        clickhouse_probe=probe,
    ).run(request, run_id="run-1")

    assert evidence.passed is False
    assert "object_storage_budget_block_threshold_exceeded" in evidence.blockers
    assert evidence.to_dict()["budget_guard"]["status"] == "blocked"
    assert probe.sql is None
    assert not list((tmp_path / "store").rglob("__dpone_sentinel.parquet"))


class _RecordingClickHouseProbe:
    sql: str | None = None

    def check(self, *, sql: str) -> None:
        self.sql = sql
