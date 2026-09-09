from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from dpone.runtime.object_storage_retention import (
    ObjectStorageBudgetProbe,
    ObjectStorageLifecycleAdvisor,
    ObjectStorageRetentionPolicy,
    ObjectStorageRunMarker,
    ObjectStorageSweeper,
)
from dpone.storage import ObjectStorageObject, ObjectStorageUri


def test_retention_policy_parses_defaults_and_bucket_limit() -> None:
    policy = ObjectStorageRetentionPolicy.from_options(
        {
            "enabled": True,
            "bucket_limit_bytes": "200GiB",
            "require_lifecycle_rule": "warn",
        }
    )

    assert policy.enabled is True
    assert policy.bucket_limit_bytes == 200 * 1024**3
    assert policy.warn_usage_pct == 70
    assert policy.block_usage_pct == 85
    assert policy.failed_run_ttl_hours == 24
    assert policy.lifecycle_expiration_days == 2
    assert policy.abort_incomplete_multipart_days == 1
    assert policy.to_evidence()["schema_version"] == "dpone.object_storage.retention_policy.v1"


def test_budget_probe_warns_and_blocks_by_thresholds() -> None:
    client = _InventoryClient(
        {
            "dpone-stage/prod/mart/table/run-1/chunk.parquet": 130 * 1024**3,
            "dpone-stage/prod/mart/table/run-2/chunk.parquet": 20 * 1024**3,
        }
    )
    policy = ObjectStorageRetentionPolicy.from_options({"bucket_limit_bytes": "200GiB"})

    warning = ObjectStorageBudgetProbe(client).check(
        ObjectStorageUri.parse("s3://example-data-bucket/dpone-stage/prod/"),
        policy=policy,
        expected_run_bytes=0,
    )
    blocked = ObjectStorageBudgetProbe(client).check(
        ObjectStorageUri.parse("s3://example-data-bucket/dpone-stage/prod/"),
        policy=policy,
        expected_run_bytes=25 * 1024**3,
    )

    assert warning.status == "warning"
    assert warning.warnings == ("object_storage_budget_warning_threshold_exceeded",)
    assert blocked.status == "blocked"
    assert blocked.blockers == ("object_storage_budget_block_threshold_exceeded",)
    assert blocked.to_dict()["schema_version"] == "dpone.object_storage.budget_guard.v1"


def test_sweeper_rejects_unsafe_prefixes_and_keeps_unmarked_fresh_objects() -> None:
    client = _InventoryClient({"dpone-stage/prod/mart/table/run-1/chunk.parquet": 10})
    sweeper = ObjectStorageSweeper(client)

    rejected = sweeper.plan(
        ObjectStorageUri.parse("s3://example-data-bucket/dpone-stage/"),
        policy=ObjectStorageRetentionPolicy(),
        now=_now(),
    )
    safe = sweeper.plan(
        ObjectStorageUri.parse("s3://example-data-bucket/dpone-stage/prod/"),
        policy=ObjectStorageRetentionPolicy(orphan_ttl_hours=12),
        now=_now(),
    )

    assert rejected.blockers == ("object_storage_cleanup_prefix_not_specific_enough",)
    assert safe.candidates == ()


def test_sweeper_deletes_expired_failed_marker_and_old_orphan_prefix() -> None:
    now = _now()
    failed = ObjectStorageRunMarker(
        run_id="run-failed",
        workload_id="workload",
        table="table",
        created_at=now - timedelta(hours=30),
        expires_at=now - timedelta(hours=1),
        status="failed",
    )
    client = _InventoryClient(
        {
            "dpone-stage/prod/mart/table/run-failed/__dpone_run_marker.json": len(json.dumps(failed.to_dict())),
            "dpone-stage/prod/mart/table/run-failed/chunk.parquet": 10,
            "dpone-stage/prod/mart/table/run-orphan/chunk.parquet": 20,
        },
        markers={"dpone-stage/prod/mart/table/run-failed/__dpone_run_marker.json": failed},
        last_modified={
            "dpone-stage/prod/mart/table/run-orphan/chunk.parquet": now - timedelta(hours=20),
        },
    )
    sweeper = ObjectStorageSweeper(client)

    report = sweeper.sweep(
        ObjectStorageUri.parse("s3://example-data-bucket/dpone-stage/prod/"),
        policy=ObjectStorageRetentionPolicy(orphan_ttl_hours=12),
        now=now,
        dry_run=False,
    )

    assert report.deleted_prefixes == (
        "s3://example-data-bucket/dpone-stage/prod/mart/table/run-failed/",
        "s3://example-data-bucket/dpone-stage/prod/mart/table/run-orphan/",
    )
    assert client.deleted == [
        "dpone-stage/prod/mart/table/run-failed/",
        "dpone-stage/prod/mart/table/run-orphan/",
    ]
    assert report.to_dict()["schema_version"] == "dpone.object_storage.cleanup_sweep.v1"


def test_lifecycle_advisor_renders_prefix_scoped_s3_rule() -> None:
    policy = ObjectStorageRetentionPolicy()

    rendered = ObjectStorageLifecycleAdvisor().render(
        ObjectStorageUri.parse("s3://example-data-bucket/dpone-stage/"),
        policy=policy,
    )

    rule = rendered["rules"][0]
    assert rule["Filter"]["Prefix"] == "dpone-stage/"
    assert rule["Expiration"]["Days"] == 2
    assert rule["AbortIncompleteMultipartUpload"]["DaysAfterInitiation"] == 1
    assert rendered["schema_version"] == "dpone.object_storage.lifecycle_readiness.v1"


def test_object_storage_manifest_schema_documents_retention_contract() -> None:
    config_schema = json.loads(Path("src/dpone/schema/etl-config.schema.json").read_text(encoding="utf-8"))
    batch_schema = json.loads(Path("src/dpone/schema/etl-batch-manifest.schema.json").read_text(encoding="utf-8"))

    for source_options in (
        config_schema["properties"]["source"]["properties"]["options"]["properties"],
        batch_schema["definitions"]["process_fragment"]["properties"]["source"]["properties"]["options"]["properties"],
    ):
        object_storage = source_options["native_transfer"]["properties"]["snapshot"]["properties"][
            "columnar_fast_path"
        ]["properties"]["object_storage"]["properties"]
        retention = object_storage["retention"]["properties"]

        assert retention["bucket_limit_bytes"]["default"] == "200GiB"
        assert retention["warn_usage_pct"]["default"] == 70
        assert retention["block_usage_pct"]["default"] == 85
        assert retention["require_lifecycle_rule"]["enum"] == ["off", "warn", "required"]


class _InventoryClient:
    def __init__(
        self,
        objects: dict[str, int],
        *,
        markers: dict[str, ObjectStorageRunMarker] | None = None,
        last_modified: dict[str, datetime] | None = None,
    ) -> None:
        self.objects = dict(objects)
        self.markers = dict(markers or {})
        self.last_modified = dict(last_modified or {})
        self.deleted: list[str] = []

    def list_objects(self, prefix: ObjectStorageUri):
        now = _now()
        for key, size in sorted(self.objects.items()):
            if key.startswith(prefix.key):
                yield ObjectStorageObject(
                    uri=str(ObjectStorageUri(prefix.provider, prefix.bucket, key)),
                    size_bytes=size,
                    sha256="0" * 64,
                    metadata={"last_modified": self.last_modified.get(key, now).isoformat()},
                )

    def read_marker(self, marker_uri: ObjectStorageUri) -> ObjectStorageRunMarker | None:
        return self.markers.get(marker_uri.key)

    def delete_prefix(self, prefix: ObjectStorageUri) -> int:
        self.deleted.append(prefix.key)
        keys = [key for key in self.objects if key.startswith(prefix.key)]
        for key in keys:
            self.objects.pop(key)
        return len(keys)


def _now() -> datetime:
    return datetime(2026, 6, 30, 10, 0, tzinfo=UTC)
